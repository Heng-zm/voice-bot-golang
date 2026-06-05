# Telegram Video Downloader Bot - Go

Telegram bot written in Go. Send a public video link to the bot, it downloads the video using `yt-dlp`, then sends the file back to Telegram.

## Important safety note

Use this only for videos you own or have permission to download. This bot does not bypass DRM, paid videos, private videos, or access controls.

## Requirements

- Telegram bot token from `@BotFather`
- Docker recommended for Render deploy
- `yt-dlp` and `ffmpeg` are installed inside Docker image

## Render deploy

Recommended Render service:

```text
Service Type: Background Worker
Runtime: Docker
Dockerfile Path: ./Dockerfile
Build Command: empty
Start Command: empty
```

Required environment variable:

```env
TELEGRAM_BOT_TOKEN=your_botfather_token
```

Optional:

```env
MAX_FILE_MB=48
MAX_CONCURRENT_DOWNLOADS=2
DOWNLOAD_TIMEOUT_MINUTES=10
ALLOWED_USER_IDS=1272791365
```

## YouTube "Sign in to confirm you're not a bot" fix

Export cookies from your local PC where YouTube is logged in:

```bash
yt-dlp --cookies-from-browser chrome --cookies cookies.txt "https://www.youtube.com/"
```

For Edge:

```bash
yt-dlp --cookies-from-browser edge --cookies cookies.txt "https://www.youtube.com/"
```

Do not commit cookies to GitHub.

On Render:

1. Go to service -> Environment -> Secret Files
2. Add Secret File:
   - Filename: `youtube_cookies.txt`
   - Contents: paste your `cookies.txt`
3. Add Environment Variable:

```env
YTDLP_COOKIES_FILE=/etc/secrets/youtube_cookies.txt
```

Redeploy.

Cookies can expire. If YouTube blocks again, export fresh cookies and update the Render Secret File.

## Local run

```bash
go mod tidy
export TELEGRAM_BOT_TOKEN="YOUR_BOT_TOKEN"
go run .
```

For local machine you also need:

```bash
python3 -m pip install -U "yt-dlp[default]"
```

and `ffmpeg`.

## Commands

- `/start`
- `/help`
- `/status`

## Files

```text
main.go
go.mod
Dockerfile
.env.example
README.md
```
