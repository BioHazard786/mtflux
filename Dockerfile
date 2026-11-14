# syntax=docker/dockerfile:1

# ---- build ------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /app
RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
# go.mod replaces github.com/gotd/ige with this local copy
COPY third_party/ ./third_party/
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
  --mount=type=cache,target=/root/.cache/go-build \
  CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
  go build -trimpath -ldflags="-s -w" -o /out/mtflux ./cmd/mtflux && \
  CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
  go build -trimpath -ldflags="-s -w" -o /out/mtbench ./cmd/mtbench

# scratch has no mkdir/chown: prepare the state dir here, owned by the runtime user
RUN mkdir -p /skel/data && chown -R 65532:65532 /skel/data

# ---- shared runtime base ----------------------------------------------------
FROM scratch AS base

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder --chown=65532:65532 /skel/data /data

ENV DATA_DIR=/data
USER 65532:65532
VOLUME ["/data"]

# ---- benchmark image (not the default target) ------------------------------
#   docker build --target bench -t mtflux-bench .
FROM base AS bench
COPY --from=builder /out/mtbench /mtbench
ENTRYPOINT ["/mtbench"]

# ---- production image (default: last stage) --------------------------------
FROM base AS mtflux
COPY --from=builder /out/mtflux /mtflux

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
  CMD ["/mtflux", "-healthcheck"]
ENTRYPOINT ["/mtflux"]
