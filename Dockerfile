# Stage 1: Build static binaries
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Cache Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build statically linked binaries
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /bin/mtflux ./cmd/mtflux
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /bin/mtbench ./cmd/mtbench

# Stage 2: Minimal scratch runtime image
FROM scratch

# Copy SSL certificates and timezone data
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Copy binaries
COPY --from=builder /bin/mtflux /mtflux
COPY --from=builder /bin/mtbench /mtbench

# Persistent state volume mount
VOLUME ["/data"]

EXPOSE 8080

ENTRYPOINT ["/mtflux"]

