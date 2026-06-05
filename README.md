# Telegram Static Site Host Bot V2 - Go

This bot accepts a user-uploaded `.zip` HTML project or single `.html` file, hosts it as a temporary public website, and returns a public URL + QR Code.

## New V2 features

- QR Code for Website Link
- Admin Dashboard
- Auto Detect Project Type
- Password Protected Website
- User Project Manager in Telegram
- ZIP Security Scanner

## Flow

1. User uploads `project.zip` to Telegram bot.
2. ZIP must contain `index.html`.
3. Bot scans ZIP for unsafe files and paths.
4. Bot auto-detects the best website root folder.
5. Bot hosts the project at:

```text
https://your-service-name.onrender.com/s/<secure-token>/
```

6. Bot sends the URL and a QR code.
7. Link expires after 1 hour by default.
8. Files are auto-deleted after expiration.

## Supported

- HTML
- CSS
- JavaScript
- Images
- Fonts
- JSON
- Static assets
- React/Vite/Vue/Angular/Next static exports
- Single `.html` file

## Not supported

- PHP
- Python backend
- Node backend
- Database server
- Server-side code execution

## Telegram commands

```text
/start
/help
/status
/my_sites
/delete_site TOKEN
/extend_site TOKEN 60
/password 1234
/password off
```

## Password protected website

Before uploading a ZIP, send:

```text
/password 1234
```

Then upload your project ZIP. The website will require password `1234`.

Disable password protection for next uploads:

```text
/password off
```

## Admin Dashboard

Set env:

```env
ADMIN_USERNAME=admin
ADMIN_PASSWORD=your-strong-password
ADMIN_PATH=/admin
```

Open:

```text
https://your-service-name.onrender.com/admin
```

Admin can:

- View active sites
- Open sites
- See views, size, files, project type
- Extend expiration
- Delete sites

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
ADMIN_PASSWORD=your-strong-password
```

Recommended environment variables:

```env
LINK_TTL_MINUTES=60
MAX_LINK_TTL_MINUTES=1440
MAX_PROJECT_MB=80
MAX_SINGLE_FILE_MB=25
MAX_ZIP_ENTRIES=1000
MAX_CONCURRENT_UPLOADS=2
SPA_FALLBACK=true
KEEP_FILES_ON_STARTUP=false
ADMIN_USERNAME=admin
ADMIN_PATH=/admin
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
export ADMIN_PASSWORD="admin1234"
go run .
```

Open:

```text
http://localhost:8080
http://localhost:8080/admin
http://localhost:8080/healthz
```

## ZIP Security Scanner

The scanner blocks:

- Unsafe paths like `../`
- Symlinks
- PHP, Python, shell, EXE, DLL, SQL/db files
- `.env`, private keys
- `.git`, `node_modules`, cache folders
- Too many files
- Files too large
- Project too large
- Too-deep folders

## Auto Detect Project Type

The bot checks common project folders:

```text
/
dist/
build/
public/
out/
www/
project-folder/dist/
project-folder/build/
```

Then detects:

```text
Vite static build
Vue static build
Angular static build
Next.js static export
Nuxt static export
Svelte static build
Astro static build
Tailwind static site
HTML static site
```

## Important limitation

Sites are stored on local service disk and registered in memory. If Render restarts, old links can stop working. For permanent hosting, upgrade to Cloudflare R2, S3, or Supabase Storage.
