# Telegram Static Site Host Bot V6

Button-first Telegram bot for temporary static website hosting.

## V6 update

This version adds:

- Cloudflare R2 storage backend for uploaded static site files
- Custom domain mapping by HTTP `Host` header
- Optional Cloudflare DNS automation for custom domains
- Supabase persistence for users, hosted sites, domains, and upload logs
- Strict normal-user/admin separation

Normal users can only:

- Upload `.zip`, `.html`, or `.htm` static project files
- Open their own hosted sites
- Extend/delete their own hosted sites
- Set password protection for their next upload

Only admins in `ADMIN_USER_IDS` can:

- See admin controls
- Open admin status/dashboard
- Add custom domains
- Trigger Cloudflare DNS automation

## Required setup

```env
TELEGRAM_BOT_TOKEN=123456:telegram-token
PUBLIC_BASE_URL=https://your-service.onrender.com
COOKIE_SECRET=generate-a-long-random-secret
ADMIN_USER_IDS=1272791365
```

## Cloudflare R2 setup

```env
STORAGE_DRIVER=r2
CLOUDFLARE_ACCOUNT_ID=your_cloudflare_account_id
R2_ACCESS_KEY_ID=your_r2_access_key_id
R2_SECRET_ACCESS_KEY=your_r2_secret_access_key
R2_BUCKET=telegram-sites
R2_REGION=auto
R2_KEY_PREFIX=sites
```

When `STORAGE_DRIVER=r2` is enabled, uploaded site files are copied to R2 after extraction and security scanning. The Go server still handles password protection, expiry checks, `/s/TOKEN/` routing, and custom-domain routing.

## Custom domain setup

For automatic DNS records:

```env
CLOUDFLARE_API_TOKEN=your_cloudflare_dns_edit_token
CLOUDFLARE_ZONE_ID=your_cloudflare_zone_id
CUSTOM_DOMAIN_TARGET=your-service.onrender.com
```

Admin flow:

1. Open **My Sites**
2. Tap **Add Domain** beside a site
3. Send a domain like `demo.example.com`
4. Bot stores the mapping and creates/updates a Cloudflare CNAME if API env vars are set

If Cloudflare DNS automation is disabled, manually add:

```text
CNAME demo.example.com -> your-service.onrender.com
```

## Supabase setup

Run `supabase_schema.sql` in Supabase SQL Editor, then set:

```env
SUPABASE_ENABLED=true
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
```

The bot stores:

- `bot_users` — Telegram user profile and admin/allowed flags
- `hosted_sites` — site metadata, expiry, storage info, view count
- `site_domains` — custom domain to site-token mapping
- `upload_logs` — upload success/failure history

Use the service role key only on your server. Do not expose it in frontend code.

## Build

```bash
go mod download
go build -o app .
```

## Run locally

```bash
cp .env.example .env
# edit .env
export $(grep -v '^#' .env | xargs)
go run .
```

## Docker

```bash
docker build -t static-site-host-bot:v6 .
docker run --env-file .env -p 8080:8080 static-site-host-bot:v6
```

## Notes

- Keep `ADMIN_USER_IDS` separate from `ALLOWED_USER_IDS`.
- `ALLOWED_USER_IDS` lets users use/upload; it does not grant admin access.
- Custom domains route through your Go server so password protection and expiry still work.
- R2 is used as durable object storage; local files may still be used as a cache during the current process lifetime.
