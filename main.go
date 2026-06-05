package main

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"html"
	"io"
	"io/fs"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type Config struct {
	Token                string
	PublicBaseURL        string
	UploadDir            string
	MaxProjectMB         int64
	MaxProjectBytes      int64
	MaxZipEntries        int
	LinkTTL              time.Duration
	MaxConcurrentUploads int
	AllowedUsers         map[int64]bool
	SPAFallback          bool
	KeepFilesOnStartup   bool
}

type HostedSite struct {
	Token        string
	BaseDir      string
	RootDir      string
	OriginalName string
	UploadedBy   int64
	SizeBytes    int64
	FileCount    int
	CreatedAt    time.Time
	ExpiresAt    time.Time
}

type SiteStore struct {
	mu    sync.RWMutex
	sites map[string]HostedSite
}

var (
	startedAt     = time.Now()
	activeUploads int64
	totalSites    int64
	store         = &SiteStore{sites: make(map[string]HostedSite)}
)

func main() {
	cfg := loadConfig()

	if cfg.Token == "" {
		log.Fatal("TELEGRAM_BOT_TOKEN is required")
	}

	if err := os.MkdirAll(cfg.UploadDir, 0o755); err != nil {
		log.Fatalf("create UPLOAD_DIR failed: %v", err)
	}

	if !cfg.KeepFilesOnStartup {
		if err := clearUploadDir(cfg.UploadDir); err != nil {
			log.Printf("warning: cannot clear upload dir on startup: %v", err)
		}
	}

	go startHTTPServer(cfg)
	go cleanupExpiredSites()

	bot, err := tgbotapi.NewBotAPI(cfg.Token)
	if err != nil {
		log.Fatalf("create Telegram bot failed: %v", err)
	}

	bot.Debug = envBool("BOT_DEBUG", false)
	log.Printf("Authorized on @%s", bot.Self.UserName)

	updateConfig := tgbotapi.NewUpdate(0)
	updateConfig.Timeout = 60

	updates := bot.GetUpdatesChan(updateConfig)
	sem := make(chan struct{}, cfg.MaxConcurrentUploads)

	for update := range updates {
		if update.Message == nil {
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
	maxProjectMB := envInt64("MAX_PROJECT_MB", 50)
	if maxProjectMB < 1 {
		maxProjectMB = 50
	}
	if maxProjectMB > 512 {
		maxProjectMB = 512
	}

	ttlMinutes := envInt("LINK_TTL_MINUTES", 60)
	if ttlMinutes < 1 {
		ttlMinutes = 60
	}

	maxConcurrent := envInt("MAX_CONCURRENT_UPLOADS", 2)
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	maxZipEntries := envInt("MAX_ZIP_ENTRIES", 1000)
	if maxZipEntries < 1 {
		maxZipEntries = 1000
	}

	return Config{
		Token:                strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN")),
		PublicBaseURL:        trimRightSlash(envString("PUBLIC_BASE_URL", "")),
		UploadDir:            envString("UPLOAD_DIR", "uploads"),
		MaxProjectMB:         maxProjectMB,
		MaxProjectBytes:      maxProjectMB * 1024 * 1024,
		MaxZipEntries:        maxZipEntries,
		LinkTTL:              time.Duration(ttlMinutes) * time.Minute,
		MaxConcurrentUploads: maxConcurrent,
		AllowedUsers:         parseAllowedUsers(os.Getenv("ALLOWED_USER_IDS")),
		SPAFallback:          envBool("SPA_FALLBACK", true),
		KeepFilesOnStartup:   envBool("KEEP_FILES_ON_STARTUP", false),
	}
}

func handleMessage(bot *tgbotapi.BotAPI, cfg Config, sem chan struct{}, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID

	if cfg.AllowedUsers != nil {
		if msg.From == nil || !cfg.AllowedUsers[msg.From.ID] {
			sendText(bot, chatID, "⛔ You are not allowed to use this bot.")
			return
		}
	}

	text := strings.TrimSpace(msg.Text)

	if strings.HasPrefix(text, "/start") || strings.HasPrefix(text, "/help") {
		sendText(bot, chatID, helpText(cfg))
		return
	}

	if strings.HasPrefix(text, "/status") {
		sendText(bot, chatID, statusText(cfg))
		return
	}

	if msg.Document == nil {
		sendText(bot, chatID, "📦 Please upload a static website project as .zip.\n\nZIP must contain index.html.\nSupported: HTML, CSS, JS, images, fonts, JSON, assets.")
		return
	}

	if cfg.PublicBaseURL == "" {
		sendText(bot, chatID, "❌ PUBLIC_BASE_URL is not set.\n\nOn Render set:\nPUBLIC_BASE_URL=https://your-service-name.onrender.com")
		return
	}

	doc := msg.Document
	fileName := strings.TrimSpace(doc.FileName)
	if fileName == "" {
		fileName = "project.zip"
	}

	ext := strings.ToLower(filepath.Ext(fileName))
	if ext != ".zip" && ext != ".html" && ext != ".htm" {
		sendText(bot, chatID, "❌ Unsupported file.\n\nPlease upload .zip project or single .html file.")
		return
	}

	if doc.FileSize > 0 && int64(doc.FileSize) > cfg.MaxProjectBytes {
		sendText(bot, chatID, fmt.Sprintf(
			"❌ File too large: %.2fMB\nMax allowed: %dMB",
			float64(doc.FileSize)/(1024*1024),
			cfg.MaxProjectMB,
		))
		return
	}

	status, _ := bot.Send(tgbotapi.NewMessage(chatID, "⏳ Added to queue...\nកំពុងរង់ចាំ upload slot..."))

	sem <- struct{}{}
	atomic.AddInt64(&activeUploads, 1)
	defer func() {
		<-sem
		atomic.AddInt64(&activeUploads, -1)
	}()

	editStatus(bot, chatID, status.MessageID, "⬇️ Downloading project from Telegram...")

	tempDir, err := os.MkdirTemp(cfg.UploadDir, "incoming_*")
	if err != nil {
		editStatus(bot, chatID, status.MessageID, "❌ Cannot create temp folder.")
		return
	}
	defer os.RemoveAll(tempDir)

	localFile := filepath.Join(tempDir, safeLocalName(fileName))
	if err := downloadTelegramFile(bot, doc.FileID, localFile, cfg.MaxProjectBytes); err != nil {
		editStatus(bot, chatID, status.MessageID, "❌ Cannot download file from Telegram:\n"+truncate(err.Error(), 3000))
		return
	}

	editStatus(bot, chatID, status.MessageID, "📦 Preparing static website...")

	token, err := newToken()
	if err != nil {
		editStatus(bot, chatID, status.MessageID, "❌ Cannot create secure site token.")
		return
	}

	siteBaseDir := filepath.Join(cfg.UploadDir, token)
	if err := os.MkdirAll(siteBaseDir, 0o755); err != nil {
		editStatus(bot, chatID, status.MessageID, "❌ Cannot create site folder.")
		return
	}

	var rootDir string
	var sizeBytes int64
	var fileCount int

	if ext == ".zip" {
		rootDir, sizeBytes, fileCount, err = extractZipProject(localFile, siteBaseDir, cfg)
	} else {
		rootDir, sizeBytes, fileCount, err = installSingleHTML(localFile, siteBaseDir, cfg)
	}

	if err != nil {
		_ = os.RemoveAll(siteBaseDir)
		editStatus(bot, chatID, status.MessageID, "❌ Project invalid:\n"+truncate(err.Error(), 3000))
		return
	}

	now := time.Now()
	site := HostedSite{
		Token:        token,
		BaseDir:      siteBaseDir,
		RootDir:      rootDir,
		OriginalName: fileName,
		UploadedBy:   safeFromID(msg),
		SizeBytes:    sizeBytes,
		FileCount:    fileCount,
		CreatedAt:    now,
		ExpiresAt:    now.Add(cfg.LinkTTL),
	}

	store.Add(site)
	atomic.AddInt64(&totalSites, 1)

	publicURL := cfg.PublicBaseURL + "/s/" + token + "/"

	reply := fmt.Sprintf(
		"✅ Website hosted successfully\n\nProject: %s\nFiles: %d\nSize: %.2fMB\nExpires in: %s\n\n🌐 Public URL:\n%s\n\nចំណាំ: Link នេះនឹងអស់សុពលភាពក្រោយ %s ហើយ files នឹងត្រូវ auto delete។",
		truncate(fileName, 160),
		fileCount,
		float64(sizeBytes)/(1024*1024),
		humanDuration(time.Until(site.ExpiresAt)),
		publicURL,
		humanDuration(cfg.LinkTTL),
	)

	editStatus(bot, chatID, status.MessageID, reply)
}

func downloadTelegramFile(bot *tgbotapi.BotAPI, fileID string, dest string, maxBytes int64) error {
	file, err := bot.GetFile(tgbotapi.FileConfig{FileID: fileID})
	if err != nil {
		return err
	}

	downloadURL := file.Link(bot.Token)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Telegram file HTTP status: %s", resp.Status)
	}

	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	limited := &limitedReader{
		R:   resp.Body,
		Max: maxBytes + 1,
	}

	written, err := io.Copy(out, limited)
	if err != nil {
		return err
	}

	if written > maxBytes {
		return fmt.Errorf("file exceeds max size %dMB", maxBytes/(1024*1024))
	}

	return nil
}

func extractZipProject(zipPath string, destDir string, cfg Config) (string, int64, int, error) {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", 0, 0, fmt.Errorf("cannot open zip: %w", err)
	}
	defer reader.Close()

	if len(reader.File) == 0 {
		return "", 0, 0, errors.New("zip is empty")
	}
	if len(reader.File) > cfg.MaxZipEntries {
		return "", 0, 0, fmt.Errorf("too many files in zip: %d. Max: %d", len(reader.File), cfg.MaxZipEntries)
	}

	var total int64
	var count int

	destClean, err := filepath.Abs(destDir)
	if err != nil {
		return "", 0, 0, err
	}

	for _, f := range reader.File {
		if f.FileInfo().IsDir() {
			continue
		}

		if f.FileInfo().Mode()&os.ModeSymlink != 0 {
			return "", 0, 0, fmt.Errorf("symlink not allowed in zip: %s", f.Name)
		}

		cleanName, err := cleanZipName(f.Name)
		if err != nil {
			return "", 0, 0, err
		}

		target := filepath.Join(destClean, cleanName)
		if !isInsideBase(destClean, target) {
			return "", 0, 0, fmt.Errorf("unsafe file path in zip: %s", f.Name)
		}

		if f.UncompressedSize64 > uint64(cfg.MaxProjectBytes) {
			return "", 0, 0, fmt.Errorf("file too large in zip: %s", f.Name)
		}

		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return "", 0, 0, err
		}

		src, err := f.Open()
		if err != nil {
			return "", 0, 0, err
		}

		dst, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			_ = src.Close()
			return "", 0, 0, err
		}

		remain := cfg.MaxProjectBytes - total
		if remain < 0 {
			remain = 0
		}

		n, copyErr := io.Copy(dst, io.LimitReader(src, remain+1))
		closeErr1 := src.Close()
		closeErr2 := dst.Close()

		if copyErr != nil {
			return "", 0, 0, copyErr
		}
		if closeErr1 != nil {
			return "", 0, 0, closeErr1
		}
		if closeErr2 != nil {
			return "", 0, 0, closeErr2
		}

		total += n
		count++

		if total > cfg.MaxProjectBytes {
			return "", 0, 0, fmt.Errorf("extracted project exceeds max size %dMB", cfg.MaxProjectMB)
		}
	}

	if count == 0 {
		return "", 0, 0, errors.New("zip contains no files")
	}

	root, err := findSiteRoot(destClean)
	if err != nil {
		return "", 0, 0, err
	}

	return root, total, count, nil
}

func installSingleHTML(htmlPath string, destDir string, cfg Config) (string, int64, int, error) {
	info, err := os.Stat(htmlPath)
	if err != nil {
		return "", 0, 0, err
	}
	if info.Size() > cfg.MaxProjectBytes {
		return "", 0, 0, fmt.Errorf("html file exceeds max size %dMB", cfg.MaxProjectMB)
	}

	target := filepath.Join(destDir, "index.html")
	in, err := os.Open(htmlPath)
	if err != nil {
		return "", 0, 0, err
	}
	defer in.Close()

	out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return "", 0, 0, err
	}
	defer out.Close()

	n, err := io.Copy(out, in)
	if err != nil {
		return "", 0, 0, err
	}

	return destDir, n, 1, nil
}

func findSiteRoot(destDir string) (string, error) {
	rootIndex := filepath.Join(destDir, "index.html")
	if fileExists(rootIndex) {
		return destDir, nil
	}

	var indexes []string

	err := filepath.WalkDir(destDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if strings.EqualFold(d.Name(), "index.html") {
			indexes = append(indexes, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	if len(indexes) == 0 {
		return "", errors.New("project must contain index.html")
	}

	sort.Strings(indexes)
	return filepath.Dir(indexes[0]), nil
}

func startHTTPServer(cfg Config) {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		writeHomePage(w, cfg)
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintf(
			w,
			"ok\nuptime=%s\nactive_uploads=%d\ntotal_sites=%d\nhosted_sites=%d\n",
			time.Since(startedAt).Round(time.Second),
			atomic.LoadInt64(&activeUploads),
			atomic.LoadInt64(&totalSites),
			store.Count(),
		)
	})

	mux.HandleFunc("/s/", func(w http.ResponseWriter, r *http.Request) {
		handleSiteRequest(w, r, cfg)
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           securityHeaders(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("static site host listening on :%s", port)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http server failed: %v", err)
	}
}

func handleSiteRequest(w http.ResponseWriter, r *http.Request, cfg Config) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed.", http.StatusMethodNotAllowed)
		return
	}

	rest := strings.TrimPrefix(r.URL.Path, "/s/")
	if rest == "" {
		http.NotFound(w, r)
		return
	}

	parts := strings.SplitN(rest, "/", 2)
	token := parts[0]
	if token == "" {
		http.NotFound(w, r)
		return
	}

	site, ok := store.Get(token)
	if !ok {
		http.Error(w, "Site not found or expired.", http.StatusGone)
		return
	}

	if time.Now().After(site.ExpiresAt) {
		store.Delete(token)
		_ = os.RemoveAll(site.BaseDir)
		http.Error(w, "This site link has expired.", http.StatusGone)
		return
	}

	if len(parts) == 1 {
		http.Redirect(w, r, "/s/"+token+"/", http.StatusMovedPermanently)
		return
	}

	relPath := parts[1]
	if relPath == "" {
		relPath = "index.html"
	}

	cleanRel, err := cleanURLPath(relPath)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	rootAbs, err := filepath.Abs(site.RootDir)
	if err != nil {
		http.Error(w, "Server error.", http.StatusInternalServerError)
		return
	}

	target := filepath.Join(rootAbs, cleanRel)
	if !isInsideBase(rootAbs, target) {
		http.NotFound(w, r)
		return
	}

	info, err := os.Stat(target)
	if err != nil {
		if cfg.SPAFallback {
			target = filepath.Join(rootAbs, "index.html")
			info, err = os.Stat(target)
		}
		if err != nil {
			http.NotFound(w, r)
			return
		}
	}

	if info.IsDir() {
		indexPath := filepath.Join(target, "index.html")
		if !isInsideBase(rootAbs, indexPath) || !fileExists(indexPath) {
			http.NotFound(w, r)
			return
		}
		target = indexPath
		info, err = os.Stat(target)
		if err != nil {
			http.NotFound(w, r)
			return
		}
	}

	setStaticHeaders(w, target, site)

	http.ServeFile(w, r, target)
}

func writeHomePage(w http.ResponseWriter, cfg Config) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	_, _ = fmt.Fprintf(w, `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Telegram Static Site Host Bot</title>
<style>
body{font-family:Arial,sans-serif;background:#0b1020;color:#e8eefc;margin:0;padding:32px}
.card{max-width:900px;margin:auto;background:#121a33;border:1px solid #26345e;border-radius:18px;padding:24px;box-shadow:0 20px 50px rgba(0,0,0,.35)}
h1{margin-top:0}
.badge{display:inline-block;background:#1f2a4d;border:1px solid #344678;border-radius:999px;padding:6px 10px;margin:4px 4px 4px 0}
.small{color:#aebce3;font-size:14px}
code{background:#0b1020;border:1px solid #26345e;border-radius:8px;padding:2px 6px}
hr{border:0;border-top:1px solid #26345e;margin:20px 0}
</style>
</head>
<body>
<div class="card">
<h1>🌐 Telegram Static Site Host Bot</h1>
<p>Upload a ZIP project to the Telegram bot. The ZIP must contain <code>index.html</code>. The bot hosts it as a temporary public website.</p>
<div>
<span class="badge">TTL: %s</span>
<span class="badge">Max project: %dMB</span>
<span class="badge">Hosted sites: %d</span>
<span class="badge">Active uploads: %d</span>
</div>
<hr>
<p class="small">Supported static files only: HTML, CSS, JS, images, fonts, JSON, assets. No PHP, Python, Node backend, database, or server-side code execution.</p>
</div>
</body>
</html>`,
		html.EscapeString(humanDuration(cfg.LinkTTL)),
		cfg.MaxProjectMB,
		store.Count(),
		atomic.LoadInt64(&activeUploads),
	)
}

func cleanupExpiredSites() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		expired := store.Expired(now)

		for _, s := range expired {
			store.Delete(s.Token)
			if err := os.RemoveAll(s.BaseDir); err != nil {
				log.Printf("cleanup expired site failed: %s: %v", s.BaseDir, err)
			} else {
				log.Printf("expired site removed: %s", s.BaseDir)
			}
		}
	}
}

func setStaticHeaders(w http.ResponseWriter, path string, site HostedSite) {
	ext := strings.ToLower(filepath.Ext(path))
	contentType := mime.TypeByExtension(ext)
	if contentType == "" {
		switch ext {
		case ".html", ".htm":
			contentType = "text/html; charset=utf-8"
		case ".css":
			contentType = "text/css; charset=utf-8"
		case ".js", ".mjs":
			contentType = "text/javascript; charset=utf-8"
		case ".json":
			contentType = "application/json; charset=utf-8"
		default:
			contentType = "application/octet-stream"
		}
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Link-Expires-At", site.ExpiresAt.Format(time.RFC3339))
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Robots-Tag", "noindex, nofollow")
		next.ServeHTTP(w, r)
	})
}

func (s *SiteStore) Add(site HostedSite) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sites[site.Token] = site
}

func (s *SiteStore) Get(token string) (HostedSite, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	site, ok := s.sites[token]
	return site, ok
}

func (s *SiteStore) Delete(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sites, token)
}

func (s *SiteStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sites)
}

func (s *SiteStore) Expired(now time.Time) []HostedSite {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var expired []HostedSite
	for _, site := range s.sites {
		if now.After(site.ExpiresAt) {
			expired = append(expired, site)
		}
	}
	return expired
}

func cleanZipName(name string) (string, error) {
	name = strings.ReplaceAll(name, "\\", "/")
	name = strings.TrimPrefix(name, "/")
	name = filepath.Clean(name)

	if name == "." || name == "" {
		return "", errors.New("empty file path in zip")
	}
	if filepath.IsAbs(name) || strings.HasPrefix(name, ".."+string(filepath.Separator)) || name == ".." {
		return "", fmt.Errorf("unsafe file path in zip: %s", name)
	}

	return name, nil
}

func cleanURLPath(path string) (string, error) {
	path = strings.ReplaceAll(path, "\\", "/")
	path = strings.TrimPrefix(path, "/")
	path = filepath.Clean(path)

	if path == "." || path == "" {
		return "index.html", nil
	}
	if filepath.IsAbs(path) || strings.HasPrefix(path, ".."+string(filepath.Separator)) || path == ".." {
		return "", errors.New("unsafe URL path")
	}

	return path, nil
}

func isInsideBase(base string, target string) bool {
	baseAbs, err := filepath.Abs(base)
	if err != nil {
		return false
	}
	targetAbs, err := filepath.Abs(target)
	if err != nil {
		return false
	}

	rel, err := filepath.Rel(baseAbs, targetAbs)
	if err != nil {
		return false
	}

	return rel == "." || (!strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != "..")
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

type limitedReader struct {
	R   io.Reader
	Max int64
	N   int64
}

func (lr *limitedReader) Read(p []byte) (int, error) {
	if lr.N >= lr.Max {
		return 0, errors.New("file too large")
	}
	if int64(len(p)) > lr.Max-lr.N {
		p = p[:lr.Max-lr.N]
	}
	n, err := lr.R.Read(p)
	lr.N += int64(n)
	return n, err
}

func safeLocalName(name string) string {
	name = filepath.Base(strings.ReplaceAll(name, "\\", "/"))
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == "/" {
		return "upload.zip"
	}

	var b strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		}
	}

	out := b.String()
	if out == "" {
		return "upload.zip"
	}
	if len(out) > 120 {
		out = out[:120]
	}

	return out
}

func clearUploadDir(uploadDir string) error {
	entries, err := os.ReadDir(uploadDir)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(uploadDir, e.Name())); err != nil {
			return err
		}
	}

	return nil
}

func helpText(cfg Config) string {
	return fmt.Sprintf(`🌐 Telegram Static Site Host Bot

របៀបប្រើ:
1. Compress your HTML project to .zip
2. Make sure the project contains index.html
3. Upload the .zip file to this bot
4. Bot will return a public website URL
5. Link expires after %s and files auto delete

Supported:
- HTML, CSS, JavaScript
- Images, fonts, JSON, assets
- Single .html file also works

Not supported:
- PHP / Python / Node backend
- Database server
- Server-side code execution

Commands:
/help - show help
/status - show bot status

Limits:
- Max project: %dMB
- Max zip files: %d
- Link TTL: %s
- SPA fallback: %s`,
		humanDuration(cfg.LinkTTL),
		cfg.MaxProjectMB,
		cfg.MaxZipEntries,
		humanDuration(cfg.LinkTTL),
		yesNo(cfg.SPAFallback),
	)
}

func statusText(cfg Config) string {
	return fmt.Sprintf(
		"📊 Bot status\n\nUptime: %s\nActive uploads: %d\nTotal hosted sites: %d\nHosted sites now: %d\nMax project: %dMB\nMax zip entries: %d\nLink TTL: %s\nPublic base URL: %s\nSPA fallback: %s",
		time.Since(startedAt).Round(time.Second),
		atomic.LoadInt64(&activeUploads),
		atomic.LoadInt64(&totalSites),
		store.Count(),
		cfg.MaxProjectMB,
		cfg.MaxZipEntries,
		humanDuration(cfg.LinkTTL),
		cfg.PublicBaseURL,
		yesNo(cfg.SPAFallback),
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

func newToken() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func humanDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)

	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60

	if h > 0 && m > 0 {
		return fmt.Sprintf("%dh %dm", h, m)
	}
	if h > 0 {
		return fmt.Sprintf("%dh", h)
	}
	if m > 0 {
		return fmt.Sprintf("%dm", m)
	}
	return fmt.Sprintf("%ds", s)
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

func trimRightSlash(s string) string {
	return strings.TrimRight(strings.TrimSpace(s), "/")
}
