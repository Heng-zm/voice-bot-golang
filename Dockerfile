FROM golang:1.22-alpine AS build
WORKDIR /app

COPY go.mod ./
COPY main.go ./

RUN apk add --no-cache ca-certificates git \
    && go mod tidy \
    && go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /app/server .

FROM alpine:3.20
WORKDIR /app
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -H -u 10001 appuser \
    && mkdir -p /app/uploads \
    && chown -R appuser:appuser /app

COPY --from=build /app/server /app/server
USER appuser

ENV PORT=8080 \
    UPLOAD_DIR=/app/uploads \
    LINK_TTL_MINUTES=60 \
    MAX_LINK_TTL_MINUTES=1440 \
    MAX_PROJECT_MB=80 \
    MAX_SINGLE_FILE_MB=25 \
    MAX_ZIP_ENTRIES=1000 \
    MAX_CONCURRENT_UPLOADS=2 \
    SPA_FALLBACK=true \
    KEEP_FILES_ON_STARTUP=false

EXPOSE 8080
CMD ["/app/server"]
