FROM golang:1.22-bookworm AS build

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -tags netgo -trimpath -ldflags="-s -w" -o /out/app .

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /out/app /app/app

RUN mkdir -p /app/uploads

ENV UPLOAD_DIR=/app/uploads
ENV LINK_TTL_MINUTES=60
ENV MAX_LINK_TTL_MINUTES=1440
ENV MAX_PROJECT_MB=80
ENV MAX_SINGLE_FILE_MB=25
ENV MAX_ZIP_ENTRIES=1000
ENV MAX_CONCURRENT_UPLOADS=2
ENV SPA_FALLBACK=true
ENV KEEP_FILES_ON_STARTUP=false
ENV ADMIN_PATH=/admin

CMD ["/app/app"]
