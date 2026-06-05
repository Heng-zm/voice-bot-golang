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
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	qrcode "github.com/skip2/go-qrcode"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

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
	SPAFallback          bool
	KeepFilesOnStartup   bool
	AdminPassword        string
	AdminPath            string
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

var (
	startedAt     = time.Now()
	activeUploads int64
	totalSites    int64
	totalViews    int64
	store         = &SiteStore{sites: make(map[string]HostedSite)}
	users         = &UserStore{settings: make(map[int64]UserSettings)}
)

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

	adminPath := envString("ADMIN_PATH", "/admin")
	if !strings.HasPrefix(adminPath, "/") {
		adminPath = "/" + adminPath
	}

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
		AllowedUsers:         parseAllowedUsers(os.Getenv("ALLOWED_USER_IDS")),
		SPAFallback:          envBool("SPA_FALLBACK", true),
		KeepFilesOnStartup:   envBool("KEEP_FILES_ON_STARTUP", false),
		AdminPassword:        strings.TrimSpace(os.Getenv("ADMIN_PASSWORD")),
		AdminPath:            adminPath,
	}
}

func handleMessage(bot *tgbotapi.BotAPI, cfg Config, sem chan struct{}, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	userID := safeFromID(msg)

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

	if strings.HasPrefix(text, "/my_sites") {
		sendText(bot, chatID, mySitesText(cfg, userID))
		return
	}

	if strings.HasPrefix(text, "/delete_site") {
		handleDeleteSiteCommand(bot, cfg, chatID, userID, text)
		return
	}

	if strings.HasPrefix(text, "/extend_site") {
		handleExtendSiteCommand(bot, cfg, chatID, userID, text)
		return
	}

	if strings.HasPrefix(text, "/password") {
		handlePasswordCommand(bot, chatID, userID, text)
		return
	}

	if msg.Document == nil {
		sendText(bot, chatID, "📦 Please upload a static website project as .zip.\n\nZIP must contain index.html.\nCommands: /help, /password, /my_sites")
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

	editStatus(bot, chatID, status.MessageID, "🛡️ Scanning project security...")

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
	var projectType string
	var scan ScanResult

	if ext == ".zip" {
		rootDir, sizeBytes, fileCount, projectType, scan, err = extractZipProject(localFile, siteBaseDir, cfg)
	} else {
		rootDir, sizeBytes, fileCount, projectType, scan, err = installSingleHTML(localFile, siteBaseDir, cfg)
	}

	if err != nil {
		_ = os.RemoveAll(siteBaseDir)
		editStatus(bot, chatID, status.MessageID, "❌ Project rejected:\n"+truncate(err.Error(), 3000))
		return
	}

	userSettings := users.Get(userID)

	var salt, hashValue string
	if userSettings.NextPassword != "" {
		salt, hashValue, err = hashPassword(userSettings.NextPassword)
		if err != nil {
			_ = os.RemoveAll(siteBaseDir)
			editStatus(bot, chatID, status.MessageID, "❌ Cannot create password protection.")
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

	qrPath := filepath.Join(tempDir, "site_qr.png")
	qrOK := qrcode.WriteFile(publicURL, qrcode.Medium, 512, qrPath) == nil

	warnings := ""
	if len(scan.Warnings) > 0 {
		warnings = "\n\n⚠️ Warnings:\n- " + strings.Join(scan.Warnings, "\n- ")
	}

	passwordNote := "No"
	if site.PasswordHash != "" {
		passwordNote = "Yes"
	}

	reply := fmt.Sprintf(
		"✅ Website hosted successfully\n\nProject: %s\nType: %s\nFiles: %d\nSize: %.2fMB\nPassword: %s\nExpires in: %s\n\n🌐 Public URL:\n%s\n\nManage: /my_sites\nDelete: /delete_site %s%s",
		truncate(fileName, 120),
		projectType,
		fileCount,
		float64(sizeBytes)/(1024*1024),
		passwordNote,
		humanDuration(time.Until(site.ExpiresAt)),
		publicURL,
		token,
		warnings,
	)

	editStatus(bot, chatID, status.MessageID, reply)

	if qrOK {
		photo := tgbotapi.NewPhoto(chatID, tgbotapi.FilePath(qrPath))
		photo.Caption = "📱 QR Code for your website link"
		_, _ = bot.Send(photo)
	}
}

func handlePasswordCommand(bot *tgbotapi.BotAPI, chatID, userID int64, text string) {
	args := strings.TrimSpace(strings.TrimPrefix(text, "/password"))

	if args == "" {
		current := users.Get(userID)
		if current.NextPassword == "" {
			sendText(bot, chatID, "🔐 Password for next upload: OFF\n\nUse:\n/password 1234\n\nDisable:\n/password off")
		} else {
			sendText(bot, chatID, "🔐 Password for next upload: ON\n\nDisable:\n/password off")
		}
		return
	}

	if strings.EqualFold(args, "off") || strings.EqualFold(args, "none") || strings.EqualFold(args, "disable") {
		users.SetPassword(userID, "")
		sendText(bot, chatID, "✅ Password protection disabled for your next uploads.")
		return
	}

	if len(args) < 4 {
		sendText(bot, chatID, "❌ Password too short. Use at least 4 characters.")
		return
	}

	if len(args) > 64 {
		sendText(bot, chatID, "❌ Password too long. Max 64 characters.")
		return
	}

	users.SetPassword(userID, args)
	sendText(bot, chatID, "✅ Password protection enabled for your next uploads.\n\nYour next hosted website will require this password.")
}

func handleDeleteSiteCommand(bot *tgbotapi.BotAPI, cfg Config, chatID, userID int64, text string) {
	fields := strings.Fields(text)
	if len(fields) < 2 {
		sendText(bot, chatID, "Usage:\n/delete_site SITE_TOKEN\n\nUse /my_sites to see tokens.")
		return
	}

	token := fields[1]
	site, ok := store.Get(token)
	if !ok {
		sendText(bot, chatID, "❌ Site not found or already expired.")
		return
	}

	if site.UploadedBy != userID && !isAdminUser(cfg, userID) {
		sendText(bot, chatID, "⛔ You can delete only your own sites.")
		return
	}

	store.Delete(token)
	_ = os.RemoveAll(site.BaseDir)
	sendText(bot, chatID, "✅ Site deleted:\n"+token)
}

func handleExtendSiteCommand(bot *tgbotapi.BotAPI, cfg Config, chatID, userID int64, text string) {
	fields := strings.Fields(text)
	if len(fields) < 3 {
		sendText(bot, chatID, "Usage:\n/extend_site SITE_TOKEN MINUTES\n\nExample:\n/extend_site abc123 60")
		return
	}

	token := fields[1]
	minutes, err := strconv.Atoi(fields[2])
	if err != nil || minutes < 1 {
		sendText(bot, chatID, "❌ Invalid minutes.")
		return
	}

	site, ok := store.Get(token)
	if !ok {
		sendText(bot, chatID, "❌ Site not found or expired.")
		return
	}

	if site.UploadedBy != userID && !isAdminUser(cfg, userID) {
		sendText(bot, chatID, "⛔ You can extend only your own sites.")
		return
	}

	newExpiry := site.ExpiresAt.Add(time.Duration(minutes) * time.Minute)
	maxExpiry := site.CreatedAt.Add(cfg.MaxTTL)
	if newExpiry.After(maxExpiry) {
		newExpiry = maxExpiry
	}

	site.ExpiresAt = newExpiry
	store.Update(site)

	sendText(bot, chatID, fmt.Sprintf(
		"✅ Site extended.\n\nToken: %s\nExpires in: %s",
		token,
		humanDuration(time.Until(site.ExpiresAt)),
	))
}

func mySitesText(cfg Config, userID int64) string {
	sites := store.ByUser(userID)

	if len(sites) == 0 {
		return "📭 You have no active hosted sites.\n\nUpload a .zip project that contains index.html."
	}

	sort.Slice(sites, func(i, j int) bool {
		return sites[i].CreatedAt.After(sites[j].CreatedAt)
	})

	var b strings.Builder
	b.WriteString("🌐 Your active sites\n\n")

	for i, s := range sites {
		if i >= 10 {
			b.WriteString("\nOnly showing latest 10 sites.")
			break
		}

		url := cfg.PublicBaseURL + "/s/" + s.Token + "/"
		pwd := "No"
		if s.PasswordHash != "" {
			pwd = "Yes"
		}

		b.WriteString(fmt.Sprintf(
			"%d. %s\nType: %s\nURL: %s\nToken: %s\nViews: %d\nPassword: %s\nExpires in: %s\nDelete: /delete_site %s\nExtend: /extend_site %s 60\n\n",
			i+1,
			truncate(s.OriginalName, 80),
			s.ProjectType,
			url,
			s.Token,
			s.ViewCount,
			pwd,
			humanDuration(time.Until(s.ExpiresAt)),
			s.Token,
			s.Token,
		))
	}

	return b.String()
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

		target := filepath.Join(destClean, cleanName)
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
			return "", 0, 0, "", scan, fmt.Errorf("extracted project exceeds max size %dMB", cfg.MaxProjectMB)
		}
	}

	root, projectType, err := detectProjectRootAndType(destClean)
	if err != nil {
		return "", 0, 0, "", scan, err
	}

	return root, total, count, projectType, scan, nil
}

func scanZip(files []*zip.File, cfg Config) (ScanResult, error) {
	result := ScanResult{}

	if len(files) == 0 {
		return result, errors.New("zip is empty")
	}

	if len(files) > cfg.MaxZipEntries {
		return result, fmt.Errorf("too many files in zip: %d. Max: %d", len(files), cfg.MaxZipEntries)
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

		if f.FileInfo().Mode()&os.ModeSymlink != 0 {
			return result, fmt.Errorf("symlink not allowed in zip: %s", f.Name)
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
			return result, fmt.Errorf("single file too large: %s is %.2fMB. Max single file: %dMB",
				cleanName,
				float64(size)/(1024*1024),
				cfg.MaxSingleFileMB,
			)
		}

		if result.TotalBytes > cfg.MaxProjectBytes {
			return result, fmt.Errorf("project too large after unzip: %.2fMB. Max: %dMB",
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

	if len(result.BlockedFiles) > 0 {
		maxShow := result.BlockedFiles
		if len(maxShow) > 10 {
			maxShow = maxShow[:10]
		}
		return result, fmt.Errorf("blocked unsafe files found:\n- %s", strings.Join(maxShow, "\n- "))
	}

	if !indexFound {
		return result, errors.New("project must contain index.html")
	}

	if result.FileCount > 300 {
		result.Warnings = append(result.Warnings, fmt.Sprintf("Large project: %d files", result.FileCount))
	}

	return result, nil
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

func installSingleHTML(htmlPath string, destDir string, cfg Config) (string, int64, int, string, ScanResult, error) {
	info, err := os.Stat(htmlPath)
	if err != nil {
		return "", 0, 0, "", ScanResult{}, err
	}
	if info.Size() > cfg.MaxProjectBytes {
		return "", 0, 0, "", ScanResult{}, fmt.Errorf("html file exceeds max size %dMB", cfg.MaxProjectMB)
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

	scan := ScanResult{
		FileCount:  1,
		TotalBytes: n,
	}

	return destDir, n, 1, "Single HTML", scan, nil
}

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
			candidates = append(candidates, filepath.Join(destDir, e.Name()))
			candidates = append(candidates, filepath.Join(destDir, e.Name(), "dist"))
			candidates = append(candidates, filepath.Join(destDir, e.Name(), "build"))
			candidates = append(candidates, filepath.Join(destDir, e.Name(), "public"))
			candidates = append(candidates, filepath.Join(destDir, e.Name(), "out"))
		}
	}

	for _, c := range candidates {
		if fileExists(filepath.Join(c, "index.html")) {
			return c, detectProjectType(c, destDir), nil
		}
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

	if filepath.Base(rootDir) == "dist" {
		return "dist static build"
	}
	if filepath.Base(rootDir) == "build" {
		return "build static site"
	}
	if filepath.Base(rootDir) == "public" {
		return "public static site"
	}

	return "HTML static site"
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
	}

	log.Printf("static site host listening on :%s", port)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http server failed: %v", err)
	}
}

func handleAdmin(w http.ResponseWriter, r *http.Request, cfg Config) {
	if cfg.AdminPassword == "" {
		http.Error(w, "Admin dashboard disabled. Set ADMIN_PASSWORD.", http.StatusForbidden)
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
		site, ok := store.Get(token)
		if ok {
			store.Delete(token)
			_ = os.RemoveAll(site.BaseDir)
		}
		http.Redirect(w, r, cfg.AdminPath, http.StatusSeeOther)
		return
	}

	if strings.HasPrefix(path, "/extend/") {
		token := strings.TrimPrefix(path, "/extend/")
		site, ok := store.Get(token)
		if ok {
			site.ExpiresAt = time.Now().Add(cfg.LinkTTL)
			store.Update(site)
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

	adminUser := envString("ADMIN_USERNAME", "admin")

	userOK := subtle.ConstantTimeCompare([]byte(user), []byte(adminUser)) == 1
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
			pwd = "Yes"
		}

		rows.WriteString(fmt.Sprintf(`<tr>
<td><a href="%s" target="_blank">%s</a><br><small>%s</small></td>
<td>%s</td>
<td>%s</td>
<td>%d</td>
<td>%.2f MB</td>
<td>%d</td>
<td>%s</td>
<td>%s</td>
<td><a class="btn" href="%s/extend/%s">Extend</a> <a class="btn danger" href="%s/delete/%s" onclick="return confirm('Delete this site?')">Delete</a></td>
</tr>`,
			html.EscapeString(publicURL),
			html.EscapeString(truncate(s.OriginalName, 50)),
			html.EscapeString(s.Token),
			html.EscapeString(s.ProjectType),
			html.EscapeString(s.Username),
			s.FileCount,
			float64(s.SizeBytes)/(1024*1024),
			s.ViewCount,
			pwd,
			html.EscapeString(humanDuration(time.Until(s.ExpiresAt))),
			html.EscapeString(cfg.AdminPath),
			html.EscapeString(s.Token),
			html.EscapeString(cfg.AdminPath),
			html.EscapeString(s.Token),
		))
	}

	_, _ = fmt.Fprintf(w, `<!doctype html>
<html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Admin Dashboard</title>
<style>
body{font-family:Arial,sans-serif;background:#0b1020;color:#e8eefc;margin:0;padding:24px}
.card{background:#121a33;border:1px solid #26345e;border-radius:18px;padding:20px;box-shadow:0 20px 50px rgba(0,0,0,.35)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:18px}
.metric{background:#1b2547;border:1px solid #344678;border-radius:14px;padding:14px}
.metric b{display:block;font-size:24px}
table{width:100%%;border-collapse:collapse;margin-top:16px}
th,td{border-bottom:1px solid #26345e;padding:10px;text-align:left;vertical-align:top}
a{color:#8ab4ff}.btn{display:inline-block;background:#26345e;color:#fff;padding:6px 10px;border-radius:8px;text-decoration:none;margin:2px}.danger{background:#7a2630}
small{color:#aebce3}
</style>
</head><body>
<h1>🛠 Admin Dashboard</h1>
<div class="grid">
<div class="metric">Active uploads <b>%d</b></div>
<div class="metric">Hosted sites <b>%d</b></div>
<div class="metric">Total sites <b>%d</b></div>
<div class="metric">Total views <b>%d</b></div>
</div>
<div class="card">
<h2>Active Sites</h2>
<table>
<thead><tr><th>Site</th><th>Type</th><th>User</th><th>Files</th><th>Size</th><th>Views</th><th>Password</th><th>Expires</th><th>Actions</th></tr></thead>
<tbody>%s</tbody>
</table>
</div>
</body></html>`,
		atomic.LoadInt64(&activeUploads),
		store.Count(),
		atomic.LoadInt64(&totalSites),
		atomic.LoadInt64(&totalViews),
		rows.String(),
	)
}

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
		http.Error(w, "Site not found or expired.", http.StatusGone)
		return
	}

	if time.Now().After(site.ExpiresAt) {
		store.Delete(token)
		_ = os.RemoveAll(site.BaseDir)
		http.Error(w, "This site link has expired.", http.StatusGone)
		return
	}

	if site.PasswordHash != "" && !isPasswordAuthed(r, site) {
		if r.Method == http.MethodPost {
			handleSiteLogin(w, r, site)
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

	site.ViewCount++
	store.Update(site)
	atomic.AddInt64(&totalViews, 1)

	setStaticHeaders(w, target, site)
	http.ServeFile(w, r, target)
}

func handleSiteLogin(w http.ResponseWriter, r *http.Request, site HostedSite) {
	if err := r.ParseForm(); err != nil {
		writePasswordPageWithError(w, site, "Invalid form.")
		return
	}

	password := r.FormValue("password")
	if !verifyPassword(password, site.PasswordSalt, site.PasswordHash) {
		writePasswordPageWithError(w, site, "Wrong password.")
		return
	}

	cookieValue := authCookieValue(site)
	cookie := &http.Cookie{
		Name:     "site_auth_" + site.Token,
		Value:    cookieValue,
		Path:     "/s/" + site.Token + "/",
		Expires:  site.ExpiresAt,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	}

	http.SetCookie(w, cookie)
	http.Redirect(w, r, "/s/"+site.Token+"/", http.StatusSeeOther)
}

func isPasswordAuthed(r *http.Request, site HostedSite) bool {
	cookie, err := r.Cookie("site_auth_" + site.Token)
	if err != nil {
		return false
	}

	expected := authCookieValue(site)
	return subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(expected)) == 1
}

func authCookieValue(site HostedSite) string {
	sum := sha256.Sum256([]byte(site.Token + ":" + site.PasswordHash + ":" + site.PasswordSalt))
	return hex.EncodeToString(sum[:])
}

func writePasswordPage(w http.ResponseWriter, site HostedSite) {
	writePasswordPageWithError(w, site, "")
}

func writePasswordPageWithError(w http.ResponseWriter, site HostedSite, errMsg string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	errHTML := ""
	if errMsg != "" {
		errHTML = `<p class="err">` + html.EscapeString(errMsg) + `</p>`
	}

	_, _ = fmt.Fprintf(w, `<!doctype html><html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Password Required</title>
<style>
body{font-family:Arial,sans-serif;background:#0b1020;color:#e8eefc;margin:0;display:grid;place-items:center;min-height:100vh;padding:20px}
.card{width:100%%;max-width:420px;background:#121a33;border:1px solid #26345e;border-radius:18px;padding:24px;box-shadow:0 20px 50px rgba(0,0,0,.35)}
input,button{width:100%%;box-sizing:border-box;padding:12px;border-radius:10px;border:1px solid #344678;margin-top:10px}
input{background:#0b1020;color:#fff}button{background:#4776ff;color:#fff;font-weight:bold;cursor:pointer}
.err{color:#ff9aa8}
.small{color:#aebce3;font-size:14px}
</style></head><body>
<form class="card" method="post">
<h1>🔐 Password Required</h1>
<p class="small">%s</p>
%s
<input type="password" name="password" placeholder="Enter password" autofocus required>
<button type="submit">Open Website</button>
</form>
</body></html>`,
		html.EscapeString(site.OriginalName),
		errHTML,
	)
}

func writeHomePage(w http.ResponseWriter, cfg Config) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	adminStatus := "disabled"
	if cfg.AdminPassword != "" {
		adminStatus = "enabled"
	}

	_, _ = fmt.Fprintf(w, `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Telegram Static Site Host Bot V2</title>
<style>
body{font-family:Arial,sans-serif;background:#0b1020;color:#e8eefc;margin:0;padding:32px}
.card{max-width:950px;margin:auto;background:#121a33;border:1px solid #26345e;border-radius:18px;padding:24px;box-shadow:0 20px 50px rgba(0,0,0,.35)}
h1{margin-top:0}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin:18px 0}
.badge,.metric{background:#1f2a4d;border:1px solid #344678;border-radius:14px;padding:12px}
.metric b{display:block;font-size:24px}code{background:#0b1020;border:1px solid #26345e;border-radius:8px;padding:2px 6px}
.small{color:#aebce3;font-size:14px}
</style>
</head>
<body>
<div class="card">
<h1>🌐 Telegram Static Site Host Bot V2</h1>
<p>Upload a ZIP project to the Telegram bot. The ZIP must contain <code>index.html</code>. The bot hosts it as a temporary public website.</p>
<div class="grid">
<div class="metric">TTL <b>%s</b></div>
<div class="metric">Max project <b>%dMB</b></div>
<div class="metric">Hosted sites <b>%d</b></div>
<div class="metric">Total views <b>%d</b></div>
</div>
<p class="small">Features: QR code, admin dashboard, password protection, user site manager, project auto-detect, ZIP security scanner.</p>
<p class="small">Admin dashboard: %s at <code>%s</code></p>
</div>
</body>
</html>`,
		html.EscapeString(humanDuration(cfg.LinkTTL)),
		cfg.MaxProjectMB,
		store.Count(),
		atomic.LoadInt64(&totalViews),
		adminStatus,
		html.EscapeString(cfg.AdminPath),
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
		case ".svg":
			contentType = "image/svg+xml"
		case ".wasm":
			contentType = "application/wasm"
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

	sites := make([]HostedSite, 0, len(s.sites))
	for _, site := range s.sites {
		sites = append(sites, site)
	}
	return sites
}

func (s *SiteStore) ByUser(userID int64) []HostedSite {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sites := make([]HostedSite, 0)
	for _, site := range s.sites {
		if site.UploadedBy == userID && time.Now().Before(site.ExpiresAt) {
			sites = append(sites, site)
		}
	}
	return sites
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
	admin := "disabled"
	if cfg.AdminPassword != "" {
		admin = cfg.PublicBaseURL + cfg.AdminPath
	}

	return fmt.Sprintf(`🌐 Telegram Static Site Host Bot V2

របៀបប្រើ:
1. Compress HTML project to .zip
2. Make sure it contains index.html
3. Upload the .zip to this bot
4. Bot returns public website URL + QR Code
5. Link expires after %s and files auto delete

Commands:
/help - show help
/status - bot status
/my_sites - list your active websites
/delete_site TOKEN - delete your website
/extend_site TOKEN 60 - extend website 60 minutes
/password 1234 - set password for next uploads
/password off - disable password for next uploads

Features:
✅ QR Code
✅ Admin Dashboard
✅ Auto Detect Project Type
✅ Password Protected Website
✅ User Project Manager
✅ ZIP Security Scanner

Supported:
HTML, CSS, JS, images, fonts, JSON, static assets.

Not supported:
PHP, Python, Node backend, database, server-side execution.

Limits:
- Max project: %dMB
- Max single file: %dMB
- Max zip files: %d
- Link TTL: %s
- Max TTL: %s
- SPA fallback: %s
- Admin: %s`,
		humanDuration(cfg.LinkTTL),
		cfg.MaxProjectMB,
		cfg.MaxSingleFileMB,
		cfg.MaxZipEntries,
		humanDuration(cfg.LinkTTL),
		humanDuration(cfg.MaxTTL),
		yesNo(cfg.SPAFallback),
		admin,
	)
}

func statusText(cfg Config) string {
	return fmt.Sprintf(
		"📊 Bot status\n\nUptime: %s\nActive uploads: %d\nTotal hosted sites: %d\nHosted sites now: %d\nTotal views: %d\nMax project: %dMB\nMax single file: %dMB\nMax zip entries: %d\nLink TTL: %s\nPublic base URL: %s\nAdmin dashboard: %s",
		time.Since(startedAt).Round(time.Second),
		atomic.LoadInt64(&activeUploads),
		atomic.LoadInt64(&totalSites),
		store.Count(),
		atomic.LoadInt64(&totalViews),
		cfg.MaxProjectMB,
		cfg.MaxSingleFileMB,
		cfg.MaxZipEntries,
		humanDuration(cfg.LinkTTL),
		cfg.PublicBaseURL,
		cfg.AdminPath,
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

func hashPassword(password string) (string, string, error) {
	saltBytes := make([]byte, 16)
	if _, err := rand.Read(saltBytes); err != nil {
		return "", "", err
	}

	salt := hex.EncodeToString(saltBytes)
	sum := sha256.Sum256([]byte(salt + ":" + password))
	return salt, hex.EncodeToString(sum[:]), nil
}

func verifyPassword(password, salt, expectedHash string) bool {
	sum := sha256.Sum256([]byte(salt + ":" + password))
	got := hex.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(got), []byte(expectedHash)) == 1
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

func isAdminUser(cfg Config, userID int64) bool {
	if cfg.AllowedUsers == nil {
		return false
	}
	return cfg.AllowedUsers[userID]
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
