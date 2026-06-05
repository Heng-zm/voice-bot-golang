# Telegram Static Site Host Bot - Go

This version removes video downloading completely.

The bot accepts a user-uploaded `.zip` HTML project or a single `.html` file, hosts it as a temporary public website, and returns a public URL.

## Flow

1. User uploads `project.zip` to Telegram bot.
2. ZIP must contain `index.html`.
3. Bot downloads the ZIP from Telegram.
4. Bot safely extracts static files.
5. Bot hosts the project at:

```text
https://your-service-name.onrender.com/s/<secure-token>/
```

6. Link expires after 1 hour by default.
7. Files are auto-deleted after expiration.

## Supported

- HTML
- CSS
- JavaScript
- Images
- Fonts
- JSON
- Static assets
- Single `.html` file

## Not supported

- PHP
- Python backend
- Node backend
- Database server
- Server-side code execution

This is a static-site host only.

## Render deploy

Use **Web Service + Docker**.

```text
Service Type: Web Service
Runtime: Docker
Dockerfile Path: ./Dockerfile
Build Command: empty
Start Command: empty
```

Required environment variables:

```env
TELEGRAM_BOT_TOKEN=your_botfather_token
PUBLIC_BASE_URL=https://your-service-name.onrender.com
```

Recommended environment variables:

```env
LINK_TTL_MINUTES=60
MAX_PROJECT_MB=50
MAX_ZIP_ENTRIES=1000
MAX_CONCURRENT_UPLOADS=2
SPA_FALLBACK=true
KEEP_FILES_ON_STARTUP=false
```

Optional private bot:

```env
ALLOWED_USER_IDS=1272791365
```

## Local run

```bash
go mod tidy
export TELEGRAM_BOT_TOKEN="YOUR_BOT_TOKEN"
export PUBLIC_BASE_URL="http://localhost:8080"
go run .
```

Open:

```text
http://localhost:8080
http://localhost:8080/healthz
```

## Bot commands

- `/start`
- `/help`
- `/status`

## Security features

- Random token URL per hosted site
- Auto-expire links
- Auto-delete expired files
- Blocks unsafe ZIP paths like `../`
- Blocks symlinks in ZIP
- Limits project size
- Limits number of ZIP entries
- Serves static files only
- No server-side code execution

## Important limitation

Sites are stored on local service disk and registered in memory. If Render restarts, old links can stop working. For permanent hosting, upgrade to Cloudflare R2, S3, or Supabase Storage.
