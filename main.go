package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type Config struct {
	Token             string
	YTDLPBin          string
	CookiesFile       string
	DownloadDir       string
	MaxFileMB         int64
	MaxFileBytes      int64
	DownloadTimeout   time.Duration
	MaxConcurrentJobs int
	AllowedUsers      map[int64]bool
	AllowPrivateURLs  bool
	UserAgent         string
	ForceIPv4         bool
}

var (
	urlRegex         = regexp.MustCompile(`https?://[^\s<>"']+`)
	activeDownloads int64
	totalDownloads  int64
)

func main() {
	cfg := loadConfig()

	if cfg.Token == "" {
		log.Fatal("TELEGRAM_BOT_TOKEN is required")
	}

	if err := os.MkdirAll(cfg.DownloadDir, 0o755); err != nil {
		log.Fatalf("create DOWNLOAD_DIR failed: %v", err)
	}

	if _, err := exec.LookPath(cfg.YTDLPBin); err != nil {
		log.Fatalf("yt-dlp not found. Set YTDLP_BIN or install yt-dlp. Error: %v", err)
	}

	if _, err := exec.LookPath("ffmpeg"); err != nil {
		log.Printf("warning: ffmpeg not found. Some videos may fail to merge/remux to mp4: %v", err)
	}

	if cfg.CookiesFile != "" {
		if _, err := os.Stat(cfg.CookiesFile); err != nil {
			log.Printf("warning: YTDLP_COOKIES_FILE is set but file does not exist: %s (%v)", cfg.CookiesFile, err)
		} else {
			log.Printf("yt-dlp cookies enabled: %s", cfg.CookiesFile)
		}
	}

	startOptionalHealthServer()

	bot, err := tgbotapi.NewBotAPI(cfg.Token)
	if err != nil {
		log.Fatalf("create Telegram bot failed: %v", err)
	}

	bot.Debug = envBool("BOT_DEBUG", false)
	log.Printf("Authorized on @%s", bot.Self.UserName)

	updateConfig := tgbotapi.NewUpdate(0)
	updateConfig.Timeout = 60

	updates := bot.GetUpdatesChan(updateConfig)
	sem := make(chan struct{}, cfg.MaxConcurrentJobs)

	for update := range updates {
		if update.Message == nil || update.Message.Text == "" {
			continue
		}

		msg := update.Message

		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("panic recovered: %v", r)
				}
			}()

			handleMessage(bot, cfg, sem, msg)
		}()
	}
}

func loadConfig() Config {
	maxFileMB := envInt64("MAX_FILE_MB", 48)
	if maxFileMB < 1 {
		maxFileMB = 48
	}
	if maxFileMB > 50 {
		maxFileMB = 50
	}

	timeoutMinutes := envInt("DOWNLOAD_TIMEOUT_MINUTES", 10)
	if timeoutMinutes < 1 {
		timeoutMinutes = 10
	}

	maxConcurrent := envInt("MAX_CONCURRENT_DOWNLOADS", 2)
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	return Config{
		Token:             strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN")),
		YTDLPBin:          envString("YTDLP_BIN", "yt-dlp"),
		CookiesFile:       strings.TrimSpace(os.Getenv("YTDLP_COOKIES_FILE")),
		DownloadDir:       envString("DOWNLOAD_DIR", "downloads"),
		MaxFileMB:         maxFileMB,
		MaxFileBytes:      maxFileMB * 1024 * 1024,
		DownloadTimeout:   time.Duration(timeoutMinutes) * time.Minute,
		MaxConcurrentJobs: maxConcurrent,
		AllowedUsers:      parseAllowedUsers(os.Getenv("ALLOWED_USER_IDS")),
		AllowPrivateURLs:  envBool("ALLOW_PRIVATE_URLS", false),
		UserAgent:         envString("YTDLP_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"),
		ForceIPv4:         envBool("YTDLP_FORCE_IPV4", true),
	}
}

func startOptionalHealthServer() {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		return
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintf(
			w,
			"ok\nactive_downloads=%d\ntotal_downloads=%d\n",
			atomic.LoadInt64(&activeDownloads),
			atomic.LoadInt64(&totalDownloads),
		)
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("health server listening on :%s", port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("health server failed: %v", err)
		}
	}()
}

func handleMessage(bot *tgbotapi.BotAPI, cfg Config, sem chan struct{}, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	text := strings.TrimSpace(msg.Text)

	if cfg.AllowedUsers != nil {
		if msg.From == nil || !cfg.AllowedUsers[msg.From.ID] {
			sendText(bot, chatID, "⛔ You are not allowed to use this bot.")
			return
		}
	}

	if strings.HasPrefix(text, "/start") || strings.HasPrefix(text, "/help") {
		sendText(bot, chatID, helpText(cfg))
		return
	}

	if strings.HasPrefix(text, "/status") {
		sendText(bot, chatID, fmt.Sprintf(
			"📊 Bot status\n\nActive downloads: %d\nTotal downloads: %d\nMax file: %dMB\nCookies: %s",
			atomic.LoadInt64(&activeDownloads),
			atomic.LoadInt64(&totalDownloads),
			cfg.MaxFileMB,
			yesNo(cfg.CookiesFile != ""),
		))
		return
	}

	link := extractFirstURL(text)
	if link == "" {
		sendText(bot, chatID, "សូមផ្ញើ link video មកខ្ញុំ។\nExample:\nhttps://example.com/video")
		return
	}

	validateCtx, cancelValidate := context.WithTimeout(context.Background(), 8*time.Second)
	err := validatePublicHTTPURL(validateCtx, link, cfg.AllowPrivateURLs)
	cancelValidate()
	if err != nil {
		sendText(bot, chatID, "❌ Link មិនត្រឹមត្រូវ ឬមិនមានសុវត្ថិភាព:\n"+err.Error())
		return
	}

	status, _ := bot.Send(tgbotapi.NewMessage(chatID, "⏳ Added to queue...\nកំពុងរង់ចាំ download slot..."))

	sem <- struct{}{}
	atomic.AddInt64(&activeDownloads, 1)
	defer func() {
		<-sem
		atomic.AddInt64(&activeDownloads, -1)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DownloadTimeout)
	defer cancel()

	editStatus(bot, chatID, status.MessageID, fmt.Sprintf(
		"⬇️ Downloading...\nMax: %dMB\nPlease wait.",
		cfg.MaxFileMB,
	))

	_ = sendChatAction(bot, chatID, "upload_video")

	title, videoPath, err := downloadVideo(ctx, cfg, link, safeFromID(msg))
	if err != nil {
		editStatus(bot, chatID, status.MessageID, "❌ Download failed:\n"+truncate(err.Error(), 3500))
		return
	}

	jobDir := filepath.Dir(videoPath)
	defer func() {
		if err := os.RemoveAll(jobDir); err != nil {
			log.Printf("cleanup failed %s: %v", jobDir, err)
		}
	}()

	size, err := fileSize(videoPath)
	if err != nil {
		editStatus(bot, chatID, status.MessageID, "❌ Cannot read downloaded file size.")
		return
	}

	if size > cfg.MaxFileBytes {
		editStatus(bot, chatID, status.MessageID, fmt.Sprintf(
			"⚠️ Downloaded file is too large: %.2fMB\nTelegram bot upload limit here is %dMB.\nTry a shorter/lower-quality video.",
			float64(size)/(1024*1024),
			cfg.MaxFileMB,
		))
		return
	}

	editStatus(bot, chatID, status.MessageID, fmt.Sprintf(
		"📤 Uploading to Telegram...\nFile size: %.2fMB",
		float64(size)/(1024*1024),
	))

	caption := fmt.Sprintf("✅ %s\n\nSource: %s", truncate(title, 150), truncate(link, 700))

	video := tgbotapi.NewVideo(chatID, tgbotapi.FilePath(videoPath))
	video.Caption = caption
	video.SupportsStreaming = true

	if _, err := bot.Send(video); err != nil {
		log.Printf("send video failed, trying document: %v", err)

		doc := tgbotapi.NewDocument(chatID, tgbotapi.FilePath(videoPath))
		doc.Caption = caption

		if _, docErr := bot.Send(doc); docErr != nil {
			editStatus(bot, chatID, status.MessageID, "❌ Upload failed:\n"+truncate(docErr.Error(), 3500))
			return
		}
	}

	atomic.AddInt64(&totalDownloads, 1)
	editStatus(bot, chatID, status.MessageID, "✅ Done.")
}

func downloadVideo(ctx context.Context, cfg Config, link string, userID int64) (string, string, error) {
	jobDir := filepath.Join(cfg.DownloadDir, fmt.Sprintf("%d_%d", userID, time.Now().UnixNano()))
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		return "", "", fmt.Errorf("create job dir failed: %w", err)
	}

	outputTemplate := filepath.Join(jobDir, "%(title).120B_[%(id)s].%(ext)s")

	args := []string{
		"--no-playlist",
		"--restrict-filenames",
		"--no-warnings",
		"--no-mtime",
		"--retries", "3",
		"--fragment-retries", "3",
		"--retry-sleep", "exp=1:8",
		"--sleep-requests", "1",
		"--merge-output-format", "mp4",
		"--remux-video", "mp4",
		"-f", "bv*[height<=720]+ba/b[height<=720]/best[height<=720]/best",
		"--max-filesize", fmt.Sprintf("%dM", cfg.MaxFileMB),
		"--user-agent", cfg.UserAgent,
		"-o", outputTemplate,
	}

	if cfg.ForceIPv4 {
		args = append(args, "--force-ipv4")
	}

	if cfg.CookiesFile != "" {
		if _, err := os.Stat(cfg.CookiesFile); err != nil {
			_ = os.RemoveAll(jobDir)
			return "", "", fmt.Errorf("cookies file not found: %s. Error: %w", cfg.CookiesFile, err)
		}

		args = append(args, "--cookies", cfg.CookiesFile)
	}

	args = append(args, link)

	cmd := exec.CommandContext(ctx, cfg.YTDLPBin, args...)
	output, err := cmd.CombinedOutput()
	outText := string(output)

	if ctx.Err() != nil {
		_ = os.RemoveAll(jobDir)
		return "", "", fmt.Errorf("download timeout after %s", cfg.DownloadTimeout)
	}

	if err != nil {
		_ = os.RemoveAll(jobDir)

		if isYouTubeBotBlock(outText) {
			if cfg.CookiesFile == "" {
				return "", "", fmt.Errorf(
					"YouTube blocked this server as bot traffic.\n\nFix: add YouTube cookies file on Render Secret Files and set:\nYTDLP_COOKIES_FILE=/etc/secrets/youtube_cookies.txt\n\nOriginal yt-dlp output:\n%s",
					truncate(outText, 2800),
				)
			}

			return "", "", fmt.Errorf(
				"YouTube still blocked this request even with cookies. Your cookies may be expired or invalid. Export fresh cookies and update Render Secret File.\n\nOriginal yt-dlp output:\n%s",
				truncate(outText, 2800),
			)
		}

		return "", "", fmt.Errorf("yt-dlp error: %v\n%s", err, truncate(outText, 3500))
	}

	videoPath, err := findDownloadedVideo(jobDir)
	if err != nil {
		_ = os.RemoveAll(jobDir)
		return "", "", err
	}

	title := titleFromFilename(videoPath)
	return title, videoPath, nil
}

func isYouTubeBotBlock(s string) bool {
	s = strings.ToLower(s)
	return strings.Contains(s, "sign in to confirm") ||
		strings.Contains(s, "not a bot") ||
		strings.Contains(s, "use --cookies-from-browser") ||
		strings.Contains(s, "use --cookies for the authentication")
}

func findDownloadedVideo(root string) (string, error) {
	allowedExt := map[string]bool{
		".mp4":  true,
		".m4v":  true,
		".mov":  true,
		".webm": true,
		".mkv":  true,
	}

	var bestPath string
	var bestSize int64

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}

		name := strings.ToLower(d.Name())
		if strings.HasSuffix(name, ".part") || strings.HasSuffix(name, ".ytdl") || strings.HasSuffix(name, ".tmp") {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(name))
		if !allowedExt[ext] {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		if info.Size() > bestSize {
			bestSize = info.Size()
			bestPath = path
		}

		return nil
	})

	if err != nil {
		return "", err
	}
	if bestPath == "" {
		return "", errors.New("download completed but no video file was found")
	}

	return bestPath, nil
}

func validatePublicHTTPURL(ctx context.Context, raw string, allowPrivate bool) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return err
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("only http/https links are allowed")
	}

	host := parsed.Hostname()
	if host == "" {
		return errors.New("missing hostname")
	}

	if allowPrivate {
		return nil
	}

	if strings.EqualFold(host, "localhost") {
		return errors.New("localhost URLs are blocked")
	}

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return fmt.Errorf("cannot resolve hostname: %w", err)
	}

	for _, addr := range ips {
		ip := addr.IP
		if ip == nil {
			continue
		}
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
			return errors.New("private/local network URLs are blocked")
		}
	}

	return nil
}

func extractFirstURL(text string) string {
	raw := urlRegex.FindString(text)
	raw = strings.TrimRight(raw, ".,;!?)\n\r\t")
	return raw
}

func helpText(cfg Config) string {
	return fmt.Sprintf(`🎬 Telegram Video Downloader Bot

របៀបប្រើ:
1. ផ្ញើ video link មក bot
2. Bot នឹង download video
3. Bot ផ្ញើ video ត្រឡប់ទៅ Telegram

Commands:
/help - show help
/status - show bot status

Example:
https://example.com/video

Limits:
- Max file: %dMB
- Max quality: 720p
- Playlist disabled
- Public http/https URLs only
- Cookies enabled: %s

YouTube note:
If YouTube says "Sign in to confirm you're not a bot", add cookies with:
YTDLP_COOKIES_FILE=/etc/secrets/youtube_cookies.txt

ចំណាំ:
ប្រើសម្រាប់ video ដែលអ្នកមានសិទ្ធិ download ប៉ុណ្ណោះ។ Bot នេះមិន bypass DRM/private/paid content ទេ។`,
		cfg.MaxFileMB,
		yesNo(cfg.CookiesFile != ""),
	)
}

func sendText(bot *tgbotapi.BotAPI, chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	_, _ = bot.Send(msg)
}

func editStatus(bot *tgbotapi.BotAPI, chatID int64, messageID int, text string) {
	if messageID == 0 {
		return
	}
	edit := tgbotapi.NewEditMessageText(chatID, messageID, text)
	_, _ = bot.Send(edit)
}

func sendChatAction(bot *tgbotapi.BotAPI, chatID int64, action string) error {
	_, err := bot.Send(tgbotapi.NewChatAction(chatID, action))
	return err
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func titleFromFilename(path string) string {
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	title := strings.TrimSuffix(base, ext)
	title = strings.ReplaceAll(title, "_", " ")
	title = strings.TrimSpace(title)
	if title == "" {
		return "video"
	}
	return title
}

func truncate(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}

func envString(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func envInt64(key string, def int64) int64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return i
}

func envBool(key string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return def
	}
}

func parseAllowedUsers(raw string) map[int64]bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	result := make(map[int64]bool)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			log.Printf("invalid ALLOWED_USER_IDS value ignored: %q", part)
			continue
		}

		result[id] = true
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

func safeFromID(msg *tgbotapi.Message) int64 {
	if msg != nil && msg.From != nil {
		return msg.From.ID
	}
	return msg.Chat.ID
}

func yesNo(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}
