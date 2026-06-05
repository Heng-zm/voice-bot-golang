package main

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"io"
	"io/fs"
	"log"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	qrcode "github.com/skip2/go-qrcode"
)

// ─────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────

type Config struct {
	Token                string
	PublicBaseURL        string
	UploadDir            string
	MaxProjectMB         int64
	MaxProjectBytes      int64
	MaxSingleFileMB      int64
	MaxSingleFileBytes   int64
	MaxZipEntries        int
	LinkTTL              time.Duration
	MaxTTL               time.Duration
	MaxConcurrentUploads int
	AllowedUsers         map[int64]bool
	AdminUserIDs         map[int64]bool
	SPAFallback          bool
	KeepFilesOnStartup   bool
	AdminPassword        string
	AdminUsername        string
	AdminPath            string
	CookieSecret         string
}

type HostedSite struct {
	Token        string
	BaseDir      string
	RootDir      string
	OriginalName string
	ProjectType  string
	UploadedBy   int64
	Username     string
	SizeBytes    int64
	FileCount    int
	ViewCount    int64
	CreatedAt    time.Time
	ExpiresAt    time.Time
	PasswordSalt string
	PasswordHash string
}

type UserSettings struct {
	NextPassword string
}

type SiteStore struct {
	mu    sync.RWMutex
	sites map[string]HostedSite
}

type UserStore struct {
	mu       sync.RWMutex
	settings map[int64]UserSettings
}

type ScanResult struct {
	BlockedFiles []string
	Warnings     []string
	FileCount    int
	TotalBytes   int64
}

// ─────────────────────────────────────────────
// Globals
// ─────────────────────────────────────────────

var (
	startedAt          = time.Now()
	activeUploads      int64
	totalSites         int64
	totalViews         int64
	store              = &SiteStore{sites: make(map[string]HostedSite)}
	users              = &UserStore{settings: make(map[int64]UserSettings)}
	telegramHTTPClient = &http.Client{Timeout: 3 * time.Minute}
)

// ─────────────────────────────────────────────
// main
// ─────────────────────────────────────────────

func main() {
	cfg := loadConfig()

	if cfg.Token == "" {
		log.Fatal("TELEGRAM_BOT_TOKEN is required")
	}

	if cfg.PublicBaseURL == "" {
		log.Println("warning: PUBLIC_BASE_URL is empty. Bot cannot create public URLs until you set it.")
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
		if update.CallbackQuery != nil {
			cq := update.CallbackQuery
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("panic recovered (callback): %v", r)
					}
				}()
				handleCallbackQuery(bot, cfg, cq)
			}()
			continue
		}

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

// ─────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────

func loadConfig() Config {
	maxProjectMB := envInt64("MAX_PROJECT_MB", 80)
	if maxProjectMB < 1 {
		maxProjectMB = 80
	}
	if maxProjectMB > 512 {
		maxProjectMB = 512
	}

	maxSingleFileMB := envInt64("MAX_SINGLE_FILE_MB", 25)
	if maxSingleFileMB < 1 {
		maxSingleFileMB = 25
	}
	if maxSingleFileMB > maxProjectMB {
		maxSingleFileMB = maxProjectMB
	}

	ttlMinutes := envInt("LINK_TTL_MINUTES", 60)
	if ttlMinutes < 1 {
		ttlMinutes = 60
	}

	maxTTLMinutes := envInt("MAX_LINK_TTL_MINUTES", 1440)
	if maxTTLMinutes < ttlMinutes {
		maxTTLMinutes = ttlMinutes
	}

	maxConcurrent := envInt("MAX_CONCURRENT_UPLOADS", 2)
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	maxZipEntries := envInt("MAX_ZIP_ENTRIES", 1000)
	if maxZipEntries < 1 {
		maxZipEntries = 1000
	}

	adminPath := sanitizeAdminPath(envString("ADMIN_PATH", "/admin"))

	adminUsername := envString("ADMIN_USERNAME", "admin")

	return Config{
		Token:                strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN")),
		PublicBaseURL:        trimRightSlash(envString("PUBLIC_BASE_URL", "")),
		UploadDir:            envString("UPLOAD_DIR", "uploads"),
		MaxProjectMB:         maxProjectMB,
		MaxProjectBytes:      maxProjectMB * 1024 * 1024,
		MaxSingleFileMB:      maxSingleFileMB,
		MaxSingleFileBytes:   maxSingleFileMB * 1024 * 1024,
		MaxZipEntries:        maxZipEntries,
		LinkTTL:              time.Duration(ttlMinutes) * time.Minute,
		MaxTTL:               time.Duration(maxTTLMinutes) * time.Minute,
		MaxConcurrentUploads: maxConcurrent,
		AllowedUsers:         parseUserIDs(os.Getenv("ALLOWED_USER_IDS")),
		AdminUserIDs:         parseUserIDs(os.Getenv("ADMIN_USER_IDS")),
		SPAFallback:          envBool("SPA_FALLBACK", true),
		KeepFilesOnStartup:   envBool("KEEP_FILES_ON_STARTUP", false),
		AdminPassword:        strings.TrimSpace(os.Getenv("ADMIN_PASSWORD")),
		AdminUsername:        adminUsername,
		AdminPath:            adminPath,
		CookieSecret:         loadCookieSecret(),
	}
}

func sanitizeAdminPath(raw string) string {
	adminPath := strings.TrimSpace(raw)
	if adminPath == "" {
		adminPath = "/admin"
	}
	if !strings.HasPrefix(adminPath, "/") {
		adminPath = "/" + adminPath
	}
	adminPath = "/" + strings.Trim(path.Clean(adminPath), "/")
	if adminPath == "/" || adminPath == "/s" || adminPath == "/healthz" {
		log.Printf("unsafe ADMIN_PATH %q replaced with /admin", raw)
		return "/admin"
	}
	return adminPath
}

func loadCookieSecret() string {
	secret := strings.TrimSpace(os.Getenv("COOKIE_SECRET"))
	if secret != "" {
		return secret
	}
	generated, err := newToken()
	if err != nil {
		log.Printf("warning: cannot generate COOKIE_SECRET, falling back to process timestamp: %v", err)
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	log.Println("warning: COOKIE_SECRET is not set; password cookies will reset on every restart")
	return generated
}

// ─────────────────────────────────────────────
// Message routing
// ─────────────────────────────────────────────

func handleMessage(bot *tgbotapi.BotAPI, cfg Config, sem chan struct{}, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	userID := safeFromID(msg)

	if cfg.AllowedUsers != nil {
		if msg.From == nil || !cfg.AllowedUsers[msg.From.ID] {
			sendMD(bot, chatID, "⛔ You are not allowed to use this bot.")
			return
		}
	}

	text := strings.TrimSpace(msg.Text)
	cmd := firstCommand(text)

	switch cmd {
	case "/start", "/help":
		sendMD(bot, chatID, helpText(cfg))
		return
	case "/status":
		sendMD(bot, chatID, statusText(cfg))
		return
	case "/my_sites":
		sendMD(bot, chatID, mySitesText(cfg, userID))
		return
	case "/delete_site":
		handleDeleteSiteCommand(bot, cfg, chatID, userID, text)
		return
	case "/extend_site":
		handleExtendSiteCommand(bot, cfg, chatID, userID, text)
		return
	case "/password":
		handlePasswordCommand(bot, chatID, userID, text)
		return
	}

	// No document and no recognized command
	if msg.Document == nil {
		sendWithButtons(bot, chatID,
			"📦 *Send me your static website project*\n\nUpload a `.zip` file that contains `index.html`, or a single `.html` file.",
			[][]tgbotapi.InlineKeyboardButton{
				tgbotapi.NewInlineKeyboardRow(
					tgbotapi.NewInlineKeyboardButtonData("📖 Help", "/help"),
					tgbotapi.NewInlineKeyboardButtonData("📊 Status", "/status"),
				),
				tgbotapi.NewInlineKeyboardRow(
					tgbotapi.NewInlineKeyboardButtonData("🌐 My Sites", "/my_sites"),
					tgbotapi.NewInlineKeyboardButtonData("🔐 Password", "/password"),
				),
			},
		)
		return
	}

	if cfg.PublicBaseURL == "" {
		sendMD(bot, chatID, "❌ *Server not configured*\n\nAsk the admin to set `PUBLIC_BASE_URL`.")
		return
	}

	doc := msg.Document
	fileName := strings.TrimSpace(doc.FileName)
	if fileName == "" {
		fileName = "project.zip"
	}

	ext := strings.ToLower(filepath.Ext(fileName))
	if ext != ".zip" && ext != ".html" && ext != ".htm" {
		sendMD(bot, chatID, "❌ *Unsupported file type*\n\nPlease upload a `.zip` project or a single `.html` file.")
		return
	}

	if doc.FileSize > 0 && int64(doc.FileSize) > cfg.MaxProjectBytes {
		sendMD(bot, chatID, fmt.Sprintf(
			"❌ *File too large*\n\nYour file: `%.2f MB`\nMax allowed: `%d MB`",
			float64(doc.FileSize)/(1024*1024),
			cfg.MaxProjectMB,
		))
		return
	}

	// Send queue status message
	statusMsg, _ := bot.Send(tgbotapi.NewMessage(chatID, "⏳ Added to queue, waiting for an upload slot..."))

	sem <- struct{}{}
	atomic.AddInt64(&activeUploads, 1)
	defer func() {
		<-sem
		atomic.AddInt64(&activeUploads, -1)
	}()

	editMD(bot, chatID, statusMsg.MessageID, "⬇️ Downloading from Telegram...")

	tempDir, err := os.MkdirTemp(cfg.UploadDir, "incoming_*")
	if err != nil {
		editMD(bot, chatID, statusMsg.MessageID, "❌ Cannot create temp folder. Please try again.")
		return
	}
	defer os.RemoveAll(tempDir)

	localFile := filepath.Join(tempDir, safeLocalName(fileName))
	if err := downloadTelegramFile(bot, doc.FileID, localFile, cfg.MaxProjectBytes); err != nil {
		editMD(bot, chatID, statusMsg.MessageID, "❌ *Download failed*\n\n`"+truncate(err.Error(), 200)+"`")
		return
	}

	editMD(bot, chatID, statusMsg.MessageID, "🛡️ Scanning for security issues...")

	token, err := newToken()
	if err != nil {
		editMD(bot, chatID, statusMsg.MessageID, "❌ Cannot generate secure token. Please try again.")
		return
	}

	siteBaseDir := filepath.Join(cfg.UploadDir, token)
	if err := os.MkdirAll(siteBaseDir, 0o755); err != nil {
		editMD(bot, chatID, statusMsg.MessageID, "❌ Cannot create site folder. Please try again.")
		return
	}

	var rootDir string
	var sizeBytes int64
	var fileCount int
	var projectType string
	var scan ScanResult

	editMD(bot, chatID, statusMsg.MessageID, "📦 Extracting and processing project...")

	if ext == ".zip" {
		rootDir, sizeBytes, fileCount, projectType, scan, err = extractZipProject(localFile, siteBaseDir, cfg)
	} else {
		rootDir, sizeBytes, fileCount, projectType, scan, err = installSingleHTML(localFile, siteBaseDir, cfg)
	}

	if err != nil {
		_ = os.RemoveAll(siteBaseDir)
		editMD(bot, chatID, statusMsg.MessageID, "❌ *Project rejected*\n\n`"+truncate(err.Error(), 400)+"`")
		return
	}

	userSettings := users.Get(userID)

	var salt, hashValue string
	if userSettings.NextPassword != "" {
		salt, hashValue, err = hashPassword(userSettings.NextPassword)
		if err != nil {
			_ = os.RemoveAll(siteBaseDir)
			editMD(bot, chatID, statusMsg.MessageID, "❌ Cannot set password protection. Please try again.")
			return
		}
	}

	now := time.Now()
	site := HostedSite{
		Token:        token,
		BaseDir:      siteBaseDir,
		RootDir:      rootDir,
		OriginalName: fileName,
		ProjectType:  projectType,
		UploadedBy:   userID,
		Username:     usernameFromMessage(msg),
		SizeBytes:    sizeBytes,
		FileCount:    fileCount,
		CreatedAt:    now,
		ExpiresAt:    now.Add(cfg.LinkTTL),
		PasswordSalt: salt,
		PasswordHash: hashValue,
	}

	store.Add(site)
	atomic.AddInt64(&totalSites, 1)

	publicURL := cfg.PublicBaseURL + "/s/" + token + "/"

	// Generate QR code
	qrPath := filepath.Join(tempDir, "site_qr.png")
	qrOK := qrcode.WriteFile(publicURL, qrcode.Medium, 512, qrPath) == nil

	// Build success reply
	passwordLine := "🔓 No password"
	if site.PasswordHash != "" {
		passwordLine = "🔒 Password protected"
	}

	warningsBlock := ""
	if len(scan.Warnings) > 0 {
		warningsBlock = "\n\n⚠️ *Warnings*\n" + "• " + strings.Join(scan.Warnings, "\n• ")
	}

	reply := fmt.Sprintf(
		"✅ *Website is live\\!*\n\n"+
			"📁 `%s`\n"+
			"🔧 %s\n"+
			"📄 %d files  •  %.2f MB\n"+
			"%s\n"+
			"⏱ Expires in *%s*\n\n"+
			"🌐 [Open Website](%s)\n"+
			"`%s`%s\n\n"+
			"📋 Token: `%s`",
		escapeMarkdownV2(truncate(fileName, 80)),
		escapeMarkdownV2(projectType),
		fileCount,
		float64(sizeBytes)/(1024*1024),
		escapeMarkdownV2(passwordLine),
		escapeMarkdownV2(humanDuration(time.Until(site.ExpiresAt))),
		publicURL,
		escapeMarkdownV2(publicURL),
		escapeMarkdownV2(warningsBlock),
		escapeMarkdownV2(token),
	)

	// Edit status with inline action buttons
	editMsgWithButtons(bot, chatID, statusMsg.MessageID, reply,
		[][]tgbotapi.InlineKeyboardButton{
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonURL("🌐 Open Website", publicURL),
			),
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("🗑 Delete Site", "/delete_site "+token),
				tgbotapi.NewInlineKeyboardButtonData("⏰ Extend +60m", "/extend_site "+token+" 60"),
			),
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("🌐 My Sites", "/my_sites"),
			),
		},
	)

	if qrOK {
		photo := tgbotapi.NewPhoto(chatID, tgbotapi.FilePath(qrPath))
		photo.Caption = "📱 Scan to open your website"
		_, _ = bot.Send(photo)
	}
}

// ─────────────────────────────────────────────
// Callback query handler (inline buttons)
// ─────────────────────────────────────────────

func handleCallbackQuery(bot *tgbotapi.BotAPI, cfg Config, cq *tgbotapi.CallbackQuery) {
	if cq == nil {
		return
	}

	// Acknowledge the callback to remove the loading spinner.
	callback := tgbotapi.NewCallback(cq.ID, "")
	if _, err := bot.Request(callback); err != nil {
		log.Printf("callback acknowledge failed: %v", err)
	}

	if cq.Message == nil || cq.From == nil {
		return
	}

	chatID := cq.Message.Chat.ID
	userID := cq.From.ID
	data := strings.TrimSpace(cq.Data)

	if cfg.AllowedUsers != nil && !cfg.AllowedUsers[userID] {
		sendMD(bot, chatID, "⛔ You are not allowed to use this bot.")
		return
	}

	switch {
	case data == "/help":
		sendMD(bot, chatID, helpText(cfg))
	case data == "/status":
		sendMD(bot, chatID, statusText(cfg))
	case data == "/my_sites":
		sendMD(bot, chatID, mySitesText(cfg, int64(userID)))
	case data == "/password":
		current := users.Get(int64(userID))
		if current.NextPassword == "" {
			sendMD(bot, chatID, "🔐 *Password protection is OFF*\n\nSend `/password 1234` to protect your next upload.\nSend `/password off` to disable.")
		} else {
			sendMD(bot, chatID, "🔐 *Password protection is ON*\n\nYour next upload will be password protected.\nSend `/password off` to disable.")
		}
	case strings.HasPrefix(data, "/delete_site "):
		handleDeleteSiteCommand(bot, cfg, chatID, int64(userID), data)
	case strings.HasPrefix(data, "/extend_site "):
		handleExtendSiteCommand(bot, cfg, chatID, int64(userID), data)
	}
}

// ─────────────────────────────────────────────
// Command handlers
// ─────────────────────────────────────────────

func handlePasswordCommand(bot *tgbotapi.BotAPI, chatID, userID int64, text string) {
	args := strings.TrimSpace(strings.TrimPrefix(text, "/password"))

	if args == "" {
		current := users.Get(userID)
		if current.NextPassword == "" {
			sendMD(bot, chatID, "🔐 *Password protection*\n\nStatus: OFF\n\nCommands:\n• `/password 1234` — protect next upload\n• `/password off` — disable protection")
		} else {
			sendMD(bot, chatID, "🔐 *Password protection*\n\nStatus: ON ✅\n\nYour next upload will require a password.\n\n• `/password off` — disable protection")
		}
		return
	}

	if strings.EqualFold(args, "off") || strings.EqualFold(args, "none") || strings.EqualFold(args, "disable") {
		users.SetPassword(userID, "")
		sendMD(bot, chatID, "✅ *Password protection disabled*\n\nYour next uploads will be public.")
		return
	}

	if len(args) < 4 {
		sendMD(bot, chatID, "❌ Password too short. Use at least 4 characters.")
		return
	}

	if len(args) > 64 {
		sendMD(bot, chatID, "❌ Password too long. Max 64 characters.")
		return
	}

	users.SetPassword(userID, args)
	sendMD(bot, chatID, "✅ *Password protection enabled*\n\nYour next uploaded website will require a password to view.\n\nTo disable: `/password off`")
}

func handleDeleteSiteCommand(bot *tgbotapi.BotAPI, cfg Config, chatID, userID int64, text string) {
	fields := strings.Fields(text)
	if len(fields) < 2 {
		sendMD(bot, chatID, "ℹ️ *Usage:* `/delete_site TOKEN`\n\nUse /my\\_sites to see your site tokens.")
		return
	}

	token := fields[1]
	site, ok := store.Get(token)
	if !ok {
		sendMD(bot, chatID, "❌ Site not found or already expired.")
		return
	}

	if site.UploadedBy != userID && !isAdminUser(cfg, userID) {
		sendMD(bot, chatID, "⛔ You can only delete your own sites.")
		return
	}

	store.Delete(token)
	_ = os.RemoveAll(site.BaseDir)
	sendMD(bot, chatID, fmt.Sprintf("✅ *Site deleted*\n\nToken: `%s`", escapeMarkdownV2(token)))
}

func handleExtendSiteCommand(bot *tgbotapi.BotAPI, cfg Config, chatID, userID int64, text string) {
	fields := strings.Fields(text)
	if len(fields) < 3 {
		sendMD(bot, chatID, "ℹ️ *Usage:* `/extend_site TOKEN MINUTES`\n\nExample: `/extend_site abc123 60`")
		return
	}

	token := fields[1]
	minutes, err := strconv.Atoi(fields[2])
	if err != nil || minutes < 1 {
		sendMD(bot, chatID, "❌ Invalid number of minutes.")
		return
	}

	site, ok := store.Get(token)
	if !ok {
		sendMD(bot, chatID, "❌ Site not found or expired.")
		return
	}

	if site.UploadedBy != userID && !isAdminUser(cfg, userID) {
		sendMD(bot, chatID, "⛔ You can only extend your own sites.")
		return
	}

	newExpiry := site.ExpiresAt.Add(time.Duration(minutes) * time.Minute)
	maxExpiry := site.CreatedAt.Add(cfg.MaxTTL)
	capped := false
	if newExpiry.After(maxExpiry) {
		newExpiry = maxExpiry
		capped = true
	}

	site.ExpiresAt = newExpiry
	store.Update(site)

	capNote := ""
	if capped {
		capNote = "\n⚠️ Capped at maximum allowed TTL."
	}
	sendMD(bot, chatID, fmt.Sprintf(
		"✅ *Site extended*\n\nToken: `%s`\nExpires in: *%s*%s",
		escapeMarkdownV2(token),
		escapeMarkdownV2(humanDuration(time.Until(site.ExpiresAt))),
		escapeMarkdownV2(capNote),
	))
}

// ─────────────────────────────────────────────
// Text builders
// ─────────────────────────────────────────────

func mySitesText(cfg Config, userID int64) string {
	sites := store.ByUser(userID)

	if len(sites) == 0 {
		return "📭 *No active sites*\n\nYou don't have any hosted websites yet\\.\n\nSend me a `.zip` file to get started\\!"
	}

	sort.Slice(sites, func(i, j int) bool {
		return sites[i].CreatedAt.After(sites[j].CreatedAt)
	})

	var b strings.Builder
	b.WriteString(fmt.Sprintf("🌐 *Your active sites* \\(%d\\)\n\n", min(len(sites), 10)))

	for i, s := range sites {
		if i >= 10 {
			b.WriteString("_Showing 10 most recent sites\\._")
			break
		}

		url := cfg.PublicBaseURL + "/s/" + s.Token + "/"
		pwd := "🔓 No password"
		if s.PasswordHash != "" {
			pwd = "🔒 Password protected"
		}

		b.WriteString(fmt.Sprintf(
			"*%d\\.* `%s`\n"+
				"   🔧 %s\n"+
				"   📄 %d files  •  %.2f MB\n"+
				"   👁 %d views  •  %s\n"+
				"   ⏱ Expires in *%s*\n"+
				"   🌐 [Open](%s)\n"+
				"   🗑 /delete\\_site `%s`\n"+
				"   ⏰ /extend\\_site `%s` 60\n\n",
			i+1,
			escapeMarkdownV2(truncate(s.OriginalName, 60)),
			escapeMarkdownV2(s.ProjectType),
			s.FileCount,
			float64(s.SizeBytes)/(1024*1024),
			s.ViewCount,
			escapeMarkdownV2(pwd),
			escapeMarkdownV2(humanDuration(time.Until(s.ExpiresAt))),
			url,
			escapeMarkdownV2(s.Token),
			escapeMarkdownV2(s.Token),
		))
	}

	return b.String()
}

func helpText(cfg Config) string {
	admin := "disabled"
	if cfg.AdminPassword != "" {
		admin = cfg.PublicBaseURL + cfg.AdminPath
	}

	return fmt.Sprintf(
		"🌐 *Telegram Static Site Host Bot V2*\n\n"+
			"*How it works:*\n"+
			"1\\. Compress your project to `.zip`\n"+
			"2\\. Make sure it contains `index\\.html`\n"+
			"3\\. Upload the `.zip` to this bot\n"+
			"4\\. Get a public URL \\+ QR code\n"+
			"5\\. Link expires after *%s* and files are auto\\-deleted\n\n"+
			"*Commands:*\n"+
			"/help — show this help\n"+
			"/status — bot status\n"+
			"/my\\_sites — list your active websites\n"+
			"/delete\\_site `TOKEN` — delete a website\n"+
			"/extend\\_site `TOKEN 60` — extend by 60 minutes\n"+
			"/password `1234` — set password for next upload\n"+
			"/password off — disable password protection\n\n"+
			"*Supported:*\n"+
			"HTML, CSS, JS, images, fonts, JSON, static assets\\.\n"+
			"React/Vite/Vue/Angular/Next static exports\\.\n\n"+
			"*Not supported:*\n"+
			"PHP, Python, Node backend, database, server\\-side code\\.\n\n"+
			"*Limits:*\n"+
			"• Max project: `%d MB`\n"+
			"• Max single file: `%d MB`\n"+
			"• Max zip entries: `%d`\n"+
			"• Default TTL: `%s`\n"+
			"• Max TTL: `%s`\n"+
			"• SPA fallback: `%s`\n\n"+
			"*Admin:* %s",
		escapeMarkdownV2(humanDuration(cfg.LinkTTL)),
		cfg.MaxProjectMB,
		cfg.MaxSingleFileMB,
		cfg.MaxZipEntries,
		escapeMarkdownV2(humanDuration(cfg.LinkTTL)),
		escapeMarkdownV2(humanDuration(cfg.MaxTTL)),
		escapeMarkdownV2(yesNo(cfg.SPAFallback)),
		escapeMarkdownV2(admin),
	)
}

func statusText(cfg Config) string {
	return fmt.Sprintf(
		"📊 *Bot Status*\n\n"+
			"⏰ Uptime: `%s`\n"+
			"🔄 Active uploads: `%d`\n"+
			"🌐 Hosted sites now: `%d`\n"+
			"📈 Total sites ever: `%d`\n"+
			"👁 Total views: `%d`\n\n"+
			"*Limits:*\n"+
			"• Max project: `%d MB`\n"+
			"• Max single file: `%d MB`\n"+
			"• Max zip entries: `%d`\n"+
			"• Link TTL: `%s`\n\n"+
			"*Server:* `%s`",
		escapeMarkdownV2(time.Since(startedAt).Round(time.Second).String()),
		atomic.LoadInt64(&activeUploads),
		store.Count(),
		atomic.LoadInt64(&totalSites),
		atomic.LoadInt64(&totalViews),
		cfg.MaxProjectMB,
		cfg.MaxSingleFileMB,
		cfg.MaxZipEntries,
		escapeMarkdownV2(humanDuration(cfg.LinkTTL)),
		escapeMarkdownV2(cfg.PublicBaseURL),
	)
}

// ─────────────────────────────────────────────
// File download
// ─────────────────────────────────────────────

func downloadTelegramFile(bot *tgbotapi.BotAPI, fileID string, dest string, maxBytes int64) error {
	file, err := bot.GetFile(tgbotapi.FileConfig{FileID: fileID})
	if err != nil {
		return err
	}

	downloadURL := file.Link(bot.Token)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return err
	}

	resp, err := telegramHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("telegram file server returned HTTP %s", resp.Status)
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
		return fmt.Errorf("file exceeds max size %d MB", maxBytes/(1024*1024))
	}

	return nil
}

// ─────────────────────────────────────────────
// ZIP extraction
// ─────────────────────────────────────────────

func extractZipProject(zipPath string, destDir string, cfg Config) (string, int64, int, string, ScanResult, error) {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", 0, 0, "", ScanResult{}, fmt.Errorf("cannot open zip: %w", err)
	}
	defer reader.Close()

	scan, err := scanZip(reader.File, cfg)
	if err != nil {
		return "", 0, 0, "", scan, err
	}

	destClean, err := filepath.Abs(destDir)
	if err != nil {
		return "", 0, 0, "", scan, err
	}

	var total int64
	var count int

	for _, f := range reader.File {
		if f.FileInfo().IsDir() {
			continue
		}

		cleanName, err := cleanZipName(f.Name)
		if err != nil {
			return "", 0, 0, "", scan, err
		}

		if isIgnoredArchivePath(cleanName) {
			continue
		}

		target := filepath.Join(destClean, filepath.FromSlash(cleanName))
		if !isInsideBase(destClean, target) {
			return "", 0, 0, "", scan, fmt.Errorf("unsafe file path in zip: %s", f.Name)
		}

		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return "", 0, 0, "", scan, err
		}

		src, err := f.Open()
		if err != nil {
			return "", 0, 0, "", scan, err
		}

		dst, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			_ = src.Close()
			return "", 0, 0, "", scan, err
		}

		remain := cfg.MaxProjectBytes - total
		if remain < 0 {
			remain = 0
		}

		n, copyErr := io.Copy(dst, io.LimitReader(src, remain+1))
		closeErr1 := src.Close()
		closeErr2 := dst.Close()

		if copyErr != nil {
			return "", 0, 0, "", scan, copyErr
		}
		if closeErr1 != nil {
			return "", 0, 0, "", scan, closeErr1
		}
		if closeErr2 != nil {
			return "", 0, 0, "", scan, closeErr2
		}

		total += n
		count++

		if total > cfg.MaxProjectBytes {
			return "", 0, 0, "", scan, fmt.Errorf("extracted project exceeds max size %d MB", cfg.MaxProjectMB)
		}
	}

	root, projectType, err := detectProjectRootAndType(destClean)
	if err != nil {
		return "", 0, 0, "", scan, err
	}

	return root, total, count, projectType, scan, nil
}

// ─────────────────────────────────────────────
// ZIP security scanner
// ─────────────────────────────────────────────

func scanZip(files []*zip.File, cfg Config) (ScanResult, error) {
	result := ScanResult{}

	if len(files) == 0 {
		return result, errors.New("zip is empty")
	}

	if len(files) > cfg.MaxZipEntries {
		return result, fmt.Errorf("too many files in zip: %d (max %d)", len(files), cfg.MaxZipEntries)
	}

	indexFound := false

	for _, f := range files {
		if f.FileInfo().IsDir() {
			continue
		}

		result.FileCount++

		cleanName, err := cleanZipName(f.Name)
		if err != nil {
			return result, err
		}
		if isIgnoredArchivePath(cleanName) {
			continue
		}

		if f.FileInfo().Mode()&os.ModeSymlink != 0 {
			return result, fmt.Errorf("symlinks not allowed in zip: %s", f.Name)
		}

		ext := strings.ToLower(filepath.Ext(cleanName))
		base := strings.ToLower(filepath.Base(cleanName))
		lower := strings.ToLower(strings.ReplaceAll(cleanName, "\\", "/"))

		if isBlockedPath(lower, base, ext) {
			result.BlockedFiles = append(result.BlockedFiles, cleanName)
		}

		size := int64(f.UncompressedSize64)
		result.TotalBytes += size

		if size > cfg.MaxSingleFileBytes {
			return result, fmt.Errorf("file too large: %s (%.2f MB, max %d MB)",
				cleanName,
				float64(size)/(1024*1024),
				cfg.MaxSingleFileMB,
			)
		}

		if result.TotalBytes > cfg.MaxProjectBytes {
			return result, fmt.Errorf("project too large when unzipped: %.2f MB (max %d MB)",
				float64(result.TotalBytes)/(1024*1024),
				cfg.MaxProjectMB,
			)
		}

		if strings.Count(lower, "/") > 20 {
			return result, fmt.Errorf("folder nesting too deep: %s", cleanName)
		}

		if strings.EqualFold(filepath.Base(cleanName), "index.html") {
			indexFound = true
		}
	}

	if result.FileCount == 0 {
		return result, errors.New("zip contains no usable files")
	}

	if len(result.BlockedFiles) > 0 {
		show := result.BlockedFiles
		if len(show) > 10 {
			show = show[:10]
		}
		return result, fmt.Errorf("blocked unsafe files found:\n• %s", strings.Join(show, "\n• "))
	}

	if !indexFound {
		return result, errors.New("project must contain index.html")
	}

	if result.FileCount > 300 {
		result.Warnings = append(result.Warnings, fmt.Sprintf("Large project: %d files", result.FileCount))
	}

	return result, nil
}

func isIgnoredArchivePath(cleanName string) bool {
	lower := strings.ToLower(strings.ReplaceAll(cleanName, "\\", "/"))
	base := path.Base(lower)
	return strings.HasPrefix(lower, "__macosx/") || base == ".ds_store" || strings.HasSuffix(lower, "/thumbs.db")
}

func isBlockedPath(lowerPath, base, ext string) bool {
	blockedExt := map[string]bool{
		".php": true, ".phtml": true, ".py": true, ".rb": true, ".go": true, ".rs": true,
		".java": true, ".class": true, ".jar": true, ".war": true,
		".exe": true, ".dll": true, ".so": true, ".dylib": true,
		".sh": true, ".bash": true, ".zsh": true, ".fish": true,
		".bat": true, ".cmd": true, ".ps1": true, ".msi": true,
		".apk": true, ".ipa": true, ".deb": true, ".rpm": true,
		".sql": true, ".sqlite": true, ".db": true,
		".pem": true, ".key": true, ".p12": true, ".pfx": true,
	}

	if blockedExt[ext] {
		return true
	}

	blockedNames := map[string]bool{
		".env": true, ".env.local": true, ".env.production": true,
		"id_rsa": true, "id_dsa": true, "id_ed25519": true,
		"dockerfile": true, "docker-compose.yml": true,
	}

	if blockedNames[base] {
		return true
	}

	blockedSegments := []string{
		"/.git/", "/.svn/", "/node_modules/", "/vendor/", "/__pycache__/",
		"/.idea/", "/.vscode/", "/.next/cache/", "/dist/server/",
	}

	wrapped := "/" + lowerPath
	for _, seg := range blockedSegments {
		if strings.Contains(wrapped, seg) {
			return true
		}
	}

	return false
}

// ─────────────────────────────────────────────
// Single HTML install
// ─────────────────────────────────────────────

func installSingleHTML(htmlPath string, destDir string, cfg Config) (string, int64, int, string, ScanResult, error) {
	info, err := os.Stat(htmlPath)
	if err != nil {
		return "", 0, 0, "", ScanResult{}, err
	}
	if info.Size() > cfg.MaxProjectBytes {
		return "", 0, 0, "", ScanResult{}, fmt.Errorf("html file exceeds max size %d MB", cfg.MaxProjectMB)
	}

	target := filepath.Join(destDir, "index.html")
	in, err := os.Open(htmlPath)
	if err != nil {
		return "", 0, 0, "", ScanResult{}, err
	}
	defer in.Close()

	out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return "", 0, 0, "", ScanResult{}, err
	}
	defer out.Close()

	n, err := io.Copy(out, in)
	if err != nil {
		return "", 0, 0, "", ScanResult{}, err
	}

	scan := ScanResult{FileCount: 1, TotalBytes: n}
	return destDir, n, 1, "Single HTML", scan, nil
}

// ─────────────────────────────────────────────
// Project type detection
// ─────────────────────────────────────────────

func detectProjectRootAndType(destDir string) (string, string, error) {
	candidates := []string{
		destDir,
		filepath.Join(destDir, "dist"),
		filepath.Join(destDir, "build"),
		filepath.Join(destDir, "public"),
		filepath.Join(destDir, "out"),
		filepath.Join(destDir, "www"),
	}

	entries, _ := os.ReadDir(destDir)
	for _, e := range entries {
		if e.IsDir() {
			candidates = append(candidates,
				filepath.Join(destDir, e.Name()),
				filepath.Join(destDir, e.Name(), "dist"),
				filepath.Join(destDir, e.Name(), "build"),
				filepath.Join(destDir, e.Name(), "public"),
				filepath.Join(destDir, e.Name(), "out"),
			)
		}
	}

	for _, c := range candidates {
		if fileExists(filepath.Join(c, "index.html")) {
			return c, detectProjectType(c, destDir), nil
		}
	}

	// Deep search fallback
	var indexes []string
	err := filepath.WalkDir(destDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() && strings.EqualFold(d.Name(), "index.html") {
			indexes = append(indexes, path)
		}
		return nil
	})
	if err != nil {
		return "", "", err
	}

	if len(indexes) == 0 {
		return "", "", errors.New("project must contain index.html")
	}

	sort.Strings(indexes)
	root := filepath.Dir(indexes[0])
	return root, detectProjectType(root, destDir), nil
}

func detectProjectType(rootDir, allDir string) string {
	checks := []struct {
		Path string
		Type string
	}{
		{"vite.config.js", "Vite static build"},
		{"vite.config.ts", "Vite static build"},
		{"next.config.js", "Next.js static export"},
		{"nuxt.config.js", "Nuxt static export"},
		{"angular.json", "Angular static build"},
		{"vue.config.js", "Vue static build"},
		{"svelte.config.js", "Svelte static build"},
		{"astro.config.mjs", "Astro static build"},
		{"package.json", "JavaScript static build"},
		{"tailwind.config.js", "Tailwind static site"},
		{"tailwind.config.ts", "Tailwind static site"},
	}

	for _, base := range []string{rootDir, allDir} {
		for _, c := range checks {
			if fileExists(filepath.Join(base, c.Path)) {
				return c.Type
			}
		}
	}

	switch filepath.Base(rootDir) {
	case "dist":
		return "dist static build"
	case "build":
		return "build static site"
	case "public":
		return "public static site"
	}

	return "HTML static site"
}

// ─────────────────────────────────────────────
// HTTP server
// ─────────────────────────────────────────────

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
		_, _ = fmt.Fprintf(w,
			"ok\nuptime=%s\nactive_uploads=%d\ntotal_sites=%d\nhosted_sites=%d\ntotal_views=%d\n",
			time.Since(startedAt).Round(time.Second),
			atomic.LoadInt64(&activeUploads),
			atomic.LoadInt64(&totalSites),
			store.Count(),
			atomic.LoadInt64(&totalViews),
		)
	})

	mux.HandleFunc(cfg.AdminPath, func(w http.ResponseWriter, r *http.Request) {
		handleAdmin(w, r, cfg)
	})
	mux.HandleFunc(cfg.AdminPath+"/", func(w http.ResponseWriter, r *http.Request) {
		handleAdmin(w, r, cfg)
	})

	mux.HandleFunc("/s/", func(w http.ResponseWriter, r *http.Request) {
		handleSiteRequest(w, r, cfg)
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           securityHeaders(mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	log.Printf("HTTP server listening on :%s", port)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http server failed: %v", err)
	}
}

// ─────────────────────────────────────────────
// Admin dashboard
// ─────────────────────────────────────────────

func handleAdmin(w http.ResponseWriter, r *http.Request, cfg Config) {
	if cfg.AdminPassword == "" {
		http.Error(w, "Admin dashboard disabled. Set ADMIN_PASSWORD env var.", http.StatusForbidden)
		return
	}

	if !checkAdminAuth(r, cfg) {
		w.Header().Set("WWW-Authenticate", `Basic realm="Admin Dashboard"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, cfg.AdminPath)
	if path == "" || path == "/" {
		writeAdminDashboard(w, cfg)
		return
	}

	if strings.HasPrefix(path, "/delete/") {
		token := strings.TrimPrefix(path, "/delete/")
		if token != "" {
			if site, ok := store.Get(token); ok {
				store.Delete(token)
				_ = os.RemoveAll(site.BaseDir)
			}
		}
		http.Redirect(w, r, cfg.AdminPath, http.StatusSeeOther)
		return
	}

	if strings.HasPrefix(path, "/extend/") {
		token := strings.TrimPrefix(path, "/extend/")
		if token != "" {
			if site, ok := store.Get(token); ok {
				// Add the default TTL on top of current expiry, capped at MaxTTL from creation
				newExpiry := site.ExpiresAt.Add(cfg.LinkTTL)
				maxExpiry := site.CreatedAt.Add(cfg.MaxTTL)
				if newExpiry.After(maxExpiry) {
					newExpiry = maxExpiry
				}
				site.ExpiresAt = newExpiry
				store.Update(site)
			}
		}
		http.Redirect(w, r, cfg.AdminPath, http.StatusSeeOther)
		return
	}

	http.NotFound(w, r)
}

func checkAdminAuth(r *http.Request, cfg Config) bool {
	user, pass, ok := r.BasicAuth()
	if !ok {
		return false
	}
	userOK := subtle.ConstantTimeCompare([]byte(user), []byte(cfg.AdminUsername)) == 1
	passOK := subtle.ConstantTimeCompare([]byte(pass), []byte(cfg.AdminPassword)) == 1
	return userOK && passOK
}

func writeAdminDashboard(w http.ResponseWriter, cfg Config) {
	sites := store.All()
	sort.Slice(sites, func(i, j int) bool {
		return sites[i].CreatedAt.After(sites[j].CreatedAt)
	})

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	var rows strings.Builder
	for _, s := range sites {
		publicURL := cfg.PublicBaseURL + "/s/" + s.Token + "/"
		pwd := "No"
		if s.PasswordHash != "" {
			pwd = "Yes 🔒"
		}
		expiresIn := humanDuration(time.Until(s.ExpiresAt))

		rows.WriteString(fmt.Sprintf(`<tr>
<td><a href="%s" target="_blank">%s</a><br><code class="token">%s</code></td>
<td><span class="badge">%s</span></td>
<td>%s</td>
<td>%d</td>
<td>%.2f MB</td>
<td><span class="views">%d</span></td>
<td>%s</td>
<td><span class="ttl">%s</span></td>
<td class="actions">
  <a class="btn" href="%s/extend/%s">⏰ Extend</a>
  <a class="btn danger" href="%s/delete/%s" onclick="return confirm('Delete site %s?')">🗑 Delete</a>
</td>
</tr>`,
			html.EscapeString(publicURL),
			html.EscapeString(truncate(s.OriginalName, 40)),
			html.EscapeString(s.Token),
			html.EscapeString(s.ProjectType),
			html.EscapeString(s.Username),
			s.FileCount,
			float64(s.SizeBytes)/(1024*1024),
			s.ViewCount,
			html.EscapeString(pwd),
			html.EscapeString(expiresIn),
			html.EscapeString(cfg.AdminPath),
			html.EscapeString(s.Token),
			html.EscapeString(cfg.AdminPath),
			html.EscapeString(s.Token),
			html.EscapeString(s.Token),
		))
	}

	noSitesRow := ""
	if len(sites) == 0 {
		noSitesRow = `<tr><td colspan="9" style="text-align:center;padding:32px;color:#aebce3">No active sites</td></tr>`
	}

	_, _ = fmt.Fprintf(w, `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Admin Dashboard — Static Site Host</title>
<style>
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;background:#0b1020;color:#e8eefc;margin:0;padding:24px;min-height:100vh}
h1{margin:0 0 20px;font-size:22px}
h2{margin:0 0 16px;font-size:16px;color:#aebce3;font-weight:500;text-transform:uppercase;letter-spacing:.05em}
.topbar{display:flex;align-items:center;gap:12px;margin-bottom:24px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:24px}
.metric{background:#121a33;border:1px solid #26345e;border-radius:14px;padding:16px}
.metric label{display:block;font-size:12px;color:#8ab4ff;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px}
.metric b{display:block;font-size:28px;font-weight:700;line-height:1}
.card{background:#121a33;border:1px solid #26345e;border-radius:16px;padding:20px;overflow-x:auto}
table{width:100%%;border-collapse:collapse;font-size:13px}
th{padding:10px 12px;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:#8ab4ff;border-bottom:1px solid #26345e;white-space:nowrap}
td{padding:12px;border-bottom:1px solid #1a2444;vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:#16203a}
a{color:#8ab4ff;text-decoration:none}
a:hover{text-decoration:underline}
.token{font-size:11px;color:#aebce3;background:#0b1020;padding:2px 5px;border-radius:5px;border:1px solid #26345e}
.badge{display:inline-block;background:#1f2a4d;border:1px solid #344678;border-radius:20px;padding:3px 10px;font-size:12px;white-space:nowrap}
.views{color:#7be0a0;font-weight:600}
.ttl{color:#f0a050;font-weight:600}
.actions{white-space:nowrap}
.btn{display:inline-flex;align-items:center;gap:4px;background:#26345e;color:#e8eefc;padding:6px 12px;border-radius:8px;text-decoration:none;font-size:12px;margin:2px;transition:background .15s}
.btn:hover{background:#344678;text-decoration:none}
.btn.danger{background:#6b2230}
.btn.danger:hover{background:#7d2838}
code{background:#0b1020;border:1px solid #26345e;border-radius:6px;padding:2px 6px;font-size:12px}
</style>
</head><body>
<div class="topbar"><h1>🛠 Admin Dashboard</h1></div>
<div class="grid">
  <div class="metric"><label>Active Uploads</label><b>%d</b></div>
  <div class="metric"><label>Hosted Sites</label><b>%d</b></div>
  <div class="metric"><label>Total Sites</label><b>%d</b></div>
  <div class="metric"><label>Total Views</label><b>%d</b></div>
</div>
<div class="card">
<h2>Active Sites</h2>
<table>
<thead><tr><th>Site</th><th>Type</th><th>User</th><th>Files</th><th>Size</th><th>Views</th><th>Password</th><th>Expires</th><th>Actions</th></tr></thead>
<tbody>%s%s</tbody>
</table>
</div>
</body></html>`,
		atomic.LoadInt64(&activeUploads),
		store.Count(),
		atomic.LoadInt64(&totalSites),
		atomic.LoadInt64(&totalViews),
		rows.String(),
		noSitesRow,
	)
}

// ─────────────────────────────────────────────
// Site request handler
// ─────────────────────────────────────────────

func handleSiteRequest(w http.ResponseWriter, r *http.Request, cfg Config) {
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
		writeExpiredPage(w, "Site not found or expired.")
		return
	}

	if time.Now().After(site.ExpiresAt) {
		store.Delete(token)
		_ = os.RemoveAll(site.BaseDir)
		writeExpiredPage(w, "This site link has expired.")
		return
	}

	if site.PasswordHash != "" && !isPasswordAuthed(r, cfg, site) {
		if r.Method == http.MethodPost {
			handleSiteLogin(w, r, cfg, site)
			return
		}
		writePasswordPage(w, site)
		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed.", http.StatusMethodNotAllowed)
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

	info, statErr := os.Stat(target)
	if statErr != nil {
		if cfg.SPAFallback {
			fallback := filepath.Join(rootAbs, "index.html")
			info, statErr = os.Stat(fallback)
			if statErr == nil {
				target = fallback
			}
		}
		if statErr != nil {
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
		if _, err := os.Stat(target); err != nil {
			http.NotFound(w, r)
			return
		}
	}

	if shouldCountView(r, target) {
		if updated, ok := store.IncrementView(token); ok {
			site = updated
		}
		atomic.AddInt64(&totalViews, 1)
	}

	setStaticHeaders(w, target, site)
	http.ServeFile(w, r, target)
}

// ─────────────────────────────────────────────
// Password auth
// ─────────────────────────────────────────────

func handleSiteLogin(w http.ResponseWriter, r *http.Request, cfg Config, site HostedSite) {
	if err := r.ParseForm(); err != nil {
		writePasswordPageWithError(w, site, "Invalid form submission.")
		return
	}

	password := r.FormValue("password")
	if !verifyPassword(password, site.PasswordSalt, site.PasswordHash) {
		writePasswordPageWithError(w, site, "Incorrect password. Please try again.")
		return
	}

	cookie := &http.Cookie{
		Name:     "site_auth_" + site.Token,
		Value:    authCookieValue(site, cfg.CookieSecret),
		Path:     "/s/" + site.Token + "/",
		Expires:  site.ExpiresAt,
		MaxAge:   int(time.Until(site.ExpiresAt).Seconds()),
		HttpOnly: true,
		Secure:   requestIsHTTPS(r),
		SameSite: http.SameSiteLaxMode,
	}

	http.SetCookie(w, cookie)
	http.Redirect(w, r, "/s/"+site.Token+"/", http.StatusSeeOther)
}

func isPasswordAuthed(r *http.Request, cfg Config, site HostedSite) bool {
	cookie, err := r.Cookie("site_auth_" + site.Token)
	if err != nil {
		return false
	}
	expected := authCookieValue(site, cfg.CookieSecret)
	return subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(expected)) == 1
}

func authCookieValue(site HostedSite, secret string) string {
	sum := sha256.Sum256([]byte(secret + ":" + site.Token + ":" + site.PasswordHash + ":" + site.PasswordSalt))
	return hex.EncodeToString(sum[:])
}

func requestIsHTTPS(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

// ─────────────────────────────────────────────
// HTML pages
// ─────────────────────────────────────────────

func writePasswordPage(w http.ResponseWriter, site HostedSite) {
	writePasswordPageWithError(w, site, "")
}

func writePasswordPageWithError(w http.ResponseWriter, site HostedSite, errMsg string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	errHTML := ""
	if errMsg != "" {
		errHTML = `<div class="err">` + html.EscapeString(errMsg) + `</div>`
	}

	_, _ = fmt.Fprintf(w, `<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Password Required</title>
<style>
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;background:#0b1020;color:#e8eefc;margin:0;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px}
.card{width:100%%;max-width:400px;background:#121a33;border:1px solid #26345e;border-radius:20px;padding:32px;box-shadow:0 24px 60px rgba(0,0,0,.45)}
h1{margin:0 0 6px;font-size:20px}
.sub{color:#aebce3;font-size:14px;margin:0 0 24px}
label{display:block;font-size:13px;color:#8ab4ff;margin-bottom:6px}
input[type=password]{width:100%%;background:#0b1020;border:1px solid #344678;border-radius:10px;color:#e8eefc;padding:12px 14px;font-size:15px;outline:none;transition:border-color .15s}
input[type=password]:focus{border-color:#4776ff}
button{width:100%%;background:#4776ff;color:#fff;border:none;border-radius:10px;padding:13px;font-size:15px;font-weight:600;cursor:pointer;margin-top:14px;transition:background .15s}
button:hover{background:#3a68e8}
.err{background:#3d1620;border:1px solid #7a2630;border-radius:10px;padding:10px 14px;font-size:14px;color:#ff9aa8;margin-bottom:16px}
.lock{font-size:40px;text-align:center;margin-bottom:16px}
</style></head><body>
<form class="card" method="post" autocomplete="off">
<div class="lock">🔐</div>
<h1>Password Required</h1>
<p class="sub">%s</p>
%s
<label for="pw">Enter password to continue</label>
<input type="password" id="pw" name="password" placeholder="Password" autofocus required>
<button type="submit">Unlock Website</button>
</form>
</body></html>`,
		html.EscapeString(site.OriginalName),
		errHTML,
	)
}

func writeExpiredPage(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusGone)
	_, _ = fmt.Fprintf(w, `<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Site Expired</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;background:#0b1020;color:#e8eefc;margin:0;display:flex;align-items:center;justify-content:center;min-height:100vh;text-align:center;padding:20px}
.card{background:#121a33;border:1px solid #26345e;border-radius:20px;padding:40px 32px;max-width:380px}
h1{font-size:18px;margin:16px 0 8px}
p{color:#aebce3;font-size:14px;margin:0}
.icon{font-size:48px}
</style></head><body>
<div class="card">
<div class="icon">⏰</div>
<h1>%s</h1>
<p>This temporary website link is no longer available.</p>
</div>
</body></html>`,
		html.EscapeString(message),
	)
}

func writeHomePage(w http.ResponseWriter, cfg Config) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	adminStatus := "disabled"
	adminLink := ""
	if cfg.AdminPassword != "" {
		adminStatus = "enabled"
		adminLink = fmt.Sprintf(` — <a href="%s">Open Dashboard</a>`, html.EscapeString(cfg.AdminPath))
	}

	_, _ = fmt.Fprintf(w, `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Telegram Static Site Host Bot V2</title>
<style>
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;background:#0b1020;color:#e8eefc;margin:0;padding:32px 20px;min-height:100vh}
.card{max-width:860px;margin:auto;background:#121a33;border:1px solid #26345e;border-radius:20px;padding:32px;box-shadow:0 24px 60px rgba(0,0,0,.4)}
h1{margin:0 0 8px;font-size:24px}
.desc{color:#aebce3;margin:0 0 24px;font-size:15px;line-height:1.6}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:0 0 24px}
.metric{background:#1a2447;border:1px solid #344678;border-radius:14px;padding:16px}
.metric label{display:block;font-size:11px;color:#8ab4ff;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px}
.metric b{display:block;font-size:26px;font-weight:700}
.features{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:8px;margin-bottom:20px}
.feature{background:#131c38;border:1px solid #1e2e55;border-radius:10px;padding:10px 14px;font-size:13px;color:#c8d8f8}
.small{color:#aebce3;font-size:13px;line-height:1.7}
a{color:#8ab4ff}
code{background:#0b1020;border:1px solid #26345e;border-radius:6px;padding:2px 6px;font-size:13px}
</style>
</head><body>
<div class="card">
<h1>🌐 Telegram Static Site Host Bot V2</h1>
<p class="desc">Upload a <code>.zip</code> project (must contain <code>index.html</code>) to the Telegram bot to get a temporary public website URL and QR code instantly.</p>
<div class="grid">
  <div class="metric"><label>Link TTL</label><b>%s</b></div>
  <div class="metric"><label>Max Project</label><b>%d MB</b></div>
  <div class="metric"><label>Hosted Sites</label><b>%d</b></div>
  <div class="metric"><label>Total Views</label><b>%d</b></div>
</div>
<div class="features">
  <div class="feature">✅ QR Code</div>
  <div class="feature">✅ Admin Dashboard</div>
  <div class="feature">✅ Auto-detect project type</div>
  <div class="feature">✅ Password protection</div>
  <div class="feature">✅ User site manager</div>
  <div class="feature">✅ ZIP security scanner</div>
</div>
<p class="small">Admin dashboard: <strong>%s</strong>%s<br>Healthcheck: <a href="/healthz"><code>/healthz</code></a></p>
</div>
</body></html>`,
		html.EscapeString(humanDuration(cfg.LinkTTL)),
		cfg.MaxProjectMB,
		store.Count(),
		atomic.LoadInt64(&totalViews),
		html.EscapeString(adminStatus),
		adminLink,
	)
}

// ─────────────────────────────────────────────
// Cleanup goroutine
// ─────────────────────────────────────────────

func cleanupExpiredSites() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		for _, s := range store.Expired(now) {
			store.Delete(s.Token)
			if err := os.RemoveAll(s.BaseDir); err != nil {
				log.Printf("cleanup: failed to remove %s: %v", s.BaseDir, err)
			} else {
				log.Printf("cleanup: removed expired site %s", s.Token)
			}
		}
	}
}

// ─────────────────────────────────────────────
// HTTP helpers
// ─────────────────────────────────────────────

func shouldCountView(r *http.Request, target string) bool {
	if r.Method != http.MethodGet {
		return false
	}
	ext := strings.ToLower(filepath.Ext(target))
	return ext == ".html" || ext == ".htm"
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
		case ".svg":
			contentType = "image/svg+xml"
		case ".wasm":
			contentType = "application/wasm"
		default:
			contentType = "application/octet-stream"
		}
	}
	w.Header().Set("Content-Type", contentType)
	if ext == ".html" || ext == ".htm" {
		w.Header().Set("Cache-Control", "no-store")
	} else {
		seconds := int(time.Until(site.ExpiresAt).Seconds())
		if seconds < 0 {
			seconds = 0
		}
		if seconds > 3600 {
			seconds = 3600
		}
		w.Header().Set("Cache-Control", fmt.Sprintf("private, max-age=%d", seconds))
	}
	w.Header().Set("X-Link-Expires-At", site.ExpiresAt.Format(time.RFC3339))
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Robots-Tag", "noindex, nofollow")
		w.Header().Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		next.ServeHTTP(w, r)
	})
}

// ─────────────────────────────────────────────
// Store methods
// ─────────────────────────────────────────────

func (s *SiteStore) Add(site HostedSite) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sites[site.Token] = site
}

func (s *SiteStore) Update(site HostedSite) {
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

func (s *SiteStore) IncrementView(token string) (HostedSite, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	site, ok := s.sites[token]
	if !ok {
		return HostedSite{}, false
	}
	site.ViewCount++
	s.sites[token] = site
	return site, true
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

func (s *SiteStore) All() []HostedSite {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]HostedSite, 0, len(s.sites))
	for _, site := range s.sites {
		result = append(result, site)
	}
	return result
}

func (s *SiteStore) ByUser(userID int64) []HostedSite {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	var result []HostedSite
	for _, site := range s.sites {
		if site.UploadedBy == userID && now.Before(site.ExpiresAt) {
			result = append(result, site)
		}
	}
	return result
}

func (s *SiteStore) Expired(now time.Time) []HostedSite {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []HostedSite
	for _, site := range s.sites {
		if now.After(site.ExpiresAt) {
			result = append(result, site)
		}
	}
	return result
}

// ─────────────────────────────────────────────
// User store methods
// ─────────────────────────────────────────────

func (u *UserStore) Get(userID int64) UserSettings {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.settings[userID]
}

func (u *UserStore) SetPassword(userID int64, password string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	s := u.settings[userID]
	s.NextPassword = password
	u.settings[userID] = s
}

// ─────────────────────────────────────────────
// Telegram helpers
// ─────────────────────────────────────────────

// sendMD sends a MarkdownV2-formatted message and falls back to plain text
// if Telegram rejects the Markdown. This prevents parse errors from breaking UX.
func sendMD(bot *tgbotapi.BotAPI, chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = tgbotapi.ModeMarkdownV2
	msg.DisableWebPagePreview = true
	if _, err := bot.Send(msg); err != nil {
		log.Printf("telegram send markdown failed: %v", err)
		msg.ParseMode = ""
		msg.Text = plainFromMarkdownV2(text)
		if _, err := bot.Send(msg); err != nil {
			log.Printf("telegram send plain fallback failed: %v", err)
		}
	}
}

// sendWithButtons sends a MarkdownV2 message with inline keyboard and plain-text fallback.
func sendWithButtons(bot *tgbotapi.BotAPI, chatID int64, text string, buttons [][]tgbotapi.InlineKeyboardButton) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = tgbotapi.ModeMarkdownV2
	msg.DisableWebPagePreview = true
	msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(buttons...)
	if _, err := bot.Send(msg); err != nil {
		log.Printf("telegram send buttons markdown failed: %v", err)
		msg.ParseMode = ""
		msg.Text = plainFromMarkdownV2(text)
		if _, err := bot.Send(msg); err != nil {
			log.Printf("telegram send buttons plain fallback failed: %v", err)
		}
	}
}

// editMD edits an existing message with MarkdownV2 text and ignores harmless duplicate edits.
func editMD(bot *tgbotapi.BotAPI, chatID int64, messageID int, text string) {
	if messageID == 0 {
		return
	}
	edit := tgbotapi.NewEditMessageText(chatID, messageID, text)
	edit.ParseMode = tgbotapi.ModeMarkdownV2
	edit.DisableWebPagePreview = true
	if _, err := bot.Send(edit); err != nil {
		if isMessageNotModified(err) {
			return
		}
		log.Printf("telegram edit markdown failed: %v", err)
		edit.ParseMode = ""
		edit.Text = plainFromMarkdownV2(text)
		if _, err := bot.Send(edit); err != nil && !isMessageNotModified(err) {
			log.Printf("telegram edit plain fallback failed: %v", err)
		}
	}
}

// editMsgWithButtons edits an existing message with MarkdownV2 text and inline keyboard.
func editMsgWithButtons(bot *tgbotapi.BotAPI, chatID int64, messageID int, text string, buttons [][]tgbotapi.InlineKeyboardButton) {
	if messageID == 0 {
		return
	}
	edit := tgbotapi.NewEditMessageText(chatID, messageID, text)
	edit.ParseMode = tgbotapi.ModeMarkdownV2
	edit.DisableWebPagePreview = true
	markup := tgbotapi.NewInlineKeyboardMarkup(buttons...)
	edit.ReplyMarkup = &markup
	if _, err := bot.Send(edit); err != nil {
		if isMessageNotModified(err) {
			return
		}
		log.Printf("telegram edit buttons markdown failed: %v", err)
		edit.ParseMode = ""
		edit.Text = plainFromMarkdownV2(text)
		if _, err := bot.Send(edit); err != nil && !isMessageNotModified(err) {
			log.Printf("telegram edit buttons plain fallback failed: %v", err)
		}
	}
}

func isMessageNotModified(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "message is not modified")
}

func plainFromMarkdownV2(s string) string {
	replacer := strings.NewReplacer(
		"\\_", "_", "\\*", "*", "\\[", "[", "\\]", "]", "\\(", "(", "\\)", ")",
		"\\~", "~", "\\`", "`", "\\>", ">", "\\#", "#", "\\+", "+", "\\-", "-",
		"\\=", "=", "\\|", "|", "\\{", "{", "\\}", "}", "\\.", ".", "\\!", "!",
	)
	return replacer.Replace(s)
}

// escapeMarkdownV2 escapes all special characters required by Telegram MarkdownV2.
func escapeMarkdownV2(s string) string {
	// Characters that must be escaped in MarkdownV2 outside of code/pre spans
	special := `\_*[]()~` + "`" + `>#+-=|{}.!`
	var b strings.Builder
	b.Grow(len(s) + 8)
	for _, r := range s {
		if strings.ContainsRune(special, r) {
			b.WriteRune('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// ─────────────────────────────────────────────
// Path utilities
// ─────────────────────────────────────────────

func cleanZipName(name string) (string, error) {
	name = strings.ReplaceAll(name, "\\", "/")
	name = strings.TrimPrefix(name, "/")
	name = path.Clean(name)

	if name == "." || name == "" {
		return "", errors.New("empty file path in zip")
	}
	if strings.ContainsRune(name, 0) || name == ".." || strings.HasPrefix(name, "../") || strings.Contains(name, ":") {
		return "", fmt.Errorf("unsafe file path in zip: %s", name)
	}

	return name, nil
}

func cleanURLPath(urlPath string) (string, error) {
	urlPath = strings.ReplaceAll(urlPath, "\\", "/")
	urlPath = strings.TrimPrefix(urlPath, "/")
	urlPath = path.Clean(urlPath)

	if urlPath == "." || urlPath == "" {
		return "index.html", nil
	}
	if strings.ContainsRune(urlPath, 0) || urlPath == ".." || strings.HasPrefix(urlPath, "../") || strings.Contains(urlPath, ":") {
		return "", errors.New("unsafe URL path")
	}

	return filepath.FromSlash(urlPath), nil
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

// ─────────────────────────────────────────────
// IO utilities
// ─────────────────────────────────────────────

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

// ─────────────────────────────────────────────
// Crypto utilities
// ─────────────────────────────────────────────

func newToken() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

const passwordHashIterations = 120000

func hashPassword(password string) (string, string, error) {
	saltBytes := make([]byte, 16)
	if _, err := rand.Read(saltBytes); err != nil {
		return "", "", err
	}
	salt := hex.EncodeToString(saltBytes)
	derived := derivePasswordHash(password, salt, passwordHashIterations)
	return salt, fmt.Sprintf("v2$%d$%s", passwordHashIterations, derived), nil
}

func verifyPassword(password, salt, expectedHash string) bool {
	parts := strings.Split(expectedHash, "$")
	if len(parts) == 3 && parts[0] == "v2" {
		iterations, err := strconv.Atoi(parts[1])
		if err != nil || iterations < 1 || iterations > 1000000 {
			return false
		}
		got := derivePasswordHash(password, salt, iterations)
		return subtle.ConstantTimeCompare([]byte(got), []byte(parts[2])) == 1
	}

	// Legacy compatibility for older in-memory records created by previous builds.
	sum := sha256.Sum256([]byte(salt + ":" + password))
	got := hex.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(got), []byte(expectedHash)) == 1
}

func derivePasswordHash(password, salt string, iterations int) string {
	material := []byte(salt + ":" + password)
	sum := sha256.Sum256(material)
	for i := 1; i < iterations; i++ {
		next := make([]byte, 0, len(sum)+len(material))
		next = append(next, sum[:]...)
		next = append(next, material...)
		sum = sha256.Sum256(next)
	}
	return hex.EncodeToString(sum[:])
}

// ─────────────────────────────────────────────
// Format helpers
// ─────────────────────────────────────────────

func humanDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60

	switch {
	case h > 0 && m > 0:
		return fmt.Sprintf("%dh %dm", h, m)
	case h > 0:
		return fmt.Sprintf("%dh", h)
	case m > 0:
		return fmt.Sprintf("%dm", m)
	default:
		return fmt.Sprintf("%ds", s)
	}
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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

// ─────────────────────────────────────────────
// Env helpers
// ─────────────────────────────────────────────

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

func parseUserIDs(raw string) map[int64]bool {
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
			log.Printf("invalid user ID ignored: %q", part)
			continue
		}
		result[id] = true
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// ─────────────────────────────────────────────
// Message helpers
// ─────────────────────────────────────────────

func firstCommand(text string) string {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) == 0 {
		return ""
	}
	cmd := fields[0]
	if i := strings.Index(cmd, "@"); i >= 0 {
		cmd = cmd[:i]
	}
	return strings.ToLower(cmd)
}

func safeFromID(msg *tgbotapi.Message) int64 {
	if msg != nil && msg.From != nil {
		return msg.From.ID
	}
	return msg.Chat.ID
}

func usernameFromMessage(msg *tgbotapi.Message) string {
	if msg == nil || msg.From == nil {
		return "unknown"
	}
	if msg.From.UserName != "" {
		return "@" + msg.From.UserName
	}
	name := strings.TrimSpace(msg.From.FirstName + " " + msg.From.LastName)
	if name == "" {
		return strconv.FormatInt(msg.From.ID, 10)
	}
	return name
}

// isAdminUser returns true if userID is listed in ADMIN_USER_IDS.
// Falls back to ALLOWED_USER_IDS if no dedicated admin list is configured.
func isAdminUser(cfg Config, userID int64) bool {
	if cfg.AdminUserIDs != nil {
		return cfg.AdminUserIDs[userID]
	}
	// When no dedicated admin list, allowed users are treated as admins
	if cfg.AllowedUsers != nil {
		return cfg.AllowedUsers[userID]
	}
	return false
}
