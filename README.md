# Telegram Video Downloader Bot - Go

A simple Telegram bot written in Go. Send a public video link to the bot, and it downloads the video using `yt-dlp`, then sends the file back to Telegram.

## Safety / legal note

Use this only for videos you own or have permission to download. This bot does not bypass DRM, paid/private videos, logins, or access controls.

## Requirements

- Go 1.22+
- Telegram bot token from `@BotFather`
- `yt-dlp`
- `ffmpeg` recommended for merging/remuxing video/audio into MP4

## Install dependencies

### Linux / macOS using pip

```bash
python3 -m pip install -U "yt-dlp[default]"
```

Install `ffmpeg`:

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install -y ffmpeg

# macOS Homebrew
brew install ffmpeg
```

### Run locally

```bash
git init
go mod tidy
```

Set env:

```bash
export TELEGRAM_BOT_TOKEN="YOUR_BOT_TOKEN"
export MAX_FILE_MB=48
export MAX_CONCURRENT_DOWNLOADS=2
```

Run:

```bash
go run .
```

## Windows PowerShell

```powershell
py -m pip install -U "yt-dlp[default]"
winget install Gyan.FFmpeg
$env:TELEGRAM_BOT_TOKEN="YOUR_BOT_TOKEN"
go run .
```

## User usage

Send a video URL to the bot:

```text
https://example.com/video
```

The bot will:
1. Validate the link
2. Download with `yt-dlp`
3. Limit to 720p and max file size
4. Upload to Telegram as video, or fallback as document

## Environment variables

| Variable | Default | Description |
|---|---:|---|
| `TELEGRAM_BOT_TOKEN` | required | Bot token from BotFather |
| `DOWNLOAD_DIR` | `downloads` | Temp download folder |
| `YTDLP_BIN` | `yt-dlp` | yt-dlp binary path |
| `MAX_FILE_MB` | `48` | Max upload file size, capped at 50 |
| `DOWNLOAD_TIMEOUT_MINUTES` | `10` | Max time per download |
| `MAX_CONCURRENT_DOWNLOADS` | `2` | Concurrent download jobs |
| `ALLOWED_USER_IDS` | empty | Comma-separated Telegram user IDs allowed to use bot |
| `ALLOW_PRIVATE_URLS` | `false` | Allow private/local network URLs; keep false for public bots |
| `BOT_DEBUG` | `false` | Telegram API debug logging |
