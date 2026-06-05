FROM golang:1.22-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app .

FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --no-cache-dir -U "yt-dlp[default]"

WORKDIR /app
COPY --from=build /out/app /app/app

RUN mkdir -p /app/downloads

ENV DOWNLOAD_DIR=/app/downloads
ENV YTDLP_BIN=yt-dlp
ENV MAX_FILE_MB=48
ENV DOWNLOAD_TIMEOUT_MINUTES=10
ENV MAX_CONCURRENT_DOWNLOADS=2
ENV ALLOW_PRIVATE_URLS=false

CMD ["/app/app"]
