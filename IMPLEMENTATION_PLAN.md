# mtflux — high-throughput Telegram → direct-link server (Go)

## Context

TG-FileStreamBot (EverythingSuckz, Go/gotgproto) is our reference. Reading its code (v3.2.0) shows why it tops out around 10 MB/s per bot and feels "sequential":

- **One bot per HTTP request**, picked round-robin. Health and load are ignored, so a bot that isn't admin of the log channel still gets every Nth request.
- **Files on a foreign DC share one connection**: gotd's sub-connection pool there has `max=1`, i.e. one `upload.getFile` in flight per bot.
- **Every foreign-DC chunk takes an extra round trip**: it goes to the home DC first, gets `FILE_MIGRATE`, then retries on the right DC.
- **Batch-barrier prefetch**: it fetches 4 chunks, waits for the slowest, then starts the next 4.
- **A 10 RPC/s rate limiter per worker**, which with 1 MB chunks is about 10 MB/s.
- **No recovery from `FILE_REFERENCE_EXPIRED`**: errors after headers are sent just truncate the body.
- **No sharing**: no chunk cache and no dedup between viewers or seeks.
- **HTTP mistakes**: 400 instead of 416, possible panic on a malformed Range, no ETag.
- **Guessable links**: the "hash" is unsalted MD5 truncated to 6 hex chars. Links have no expiry and can't be revoked.

How fast clients get their speed:
- **Telegram Desktop**: up to 8 sessions per DC with an adaptive window of up to 2 MB each (16 MB in flight), 2 MB socket buffers, media DCs.
- **Nekogram "extreme"**: 1 MB chunks × 12 requests in flight.
- **Official non-Premium level**: about 2 MB in flight per DC per account.

Two Telegram rules shape the design:
1. **Parallel sessions to non-media DCs can trigger `AUTH_KEY_DUPLICATED`**, which kills the auth key. **Media-only DCs** are exempt and "may always be opened in parallel", so the download path must use media DCs.
2. **`upload.getFile` alignment**: `offset % 4096 == 0`, `1 MiB % limit == 0`, and a request must not cross a 1 MiB boundary. Using 1 MiB-aligned 1 MiB requests is always valid.

**Goal:** one download should be spread across N worker bots × M media-DC connections, with a sliding window in place of batches, so a single link gets several times Nekogram's ~60 Mbit/s, limited by the VPS link and Telegram throttling rather than our code. On top of that: correct HTTP (Range/HEAD/ETag) so IDM, aria2 and players work, signed expiring links, access control and metrics.

**User decisions:**
- VPS + Docker.
- Bots only, no user sessions.
- Log channel is the only store (no DB).
- v1 includes access control, link expiry and revocation, and stats/metrics. No player page.

## Architecture

```
            Telegram user ──file──▶ main bot (updates) ──sendMedia copy──▶ LOG CHANNEL (the "DB")
                                        │ replies with signed link            ▲ channels.getMessages
                                        ▼                                     │ (per worker, cached)
HTTP client ─GET /dl/{token}/{name}─▶ httpapi ─▶ resolver ─(FileMeta: size,dc,docID,name,mime)
     (Range)                            │
                                        ▼
                               stream.Reader (ordered sliding window, slow-start)
                                        │ Fetch(file, chunkIdx)
                                        ▼
                     chunk cache (byte-budget LRU) + singleflight(docID,chunk)
                                        │ miss
                                        ▼
                     scheduler: pick least-loaded healthy (worker, dc) with a free slot
                                        │
             ┌──────────────┬───────────┴──────┬──────────────┐
          bot1: MediaOnly(dc,M)  bot2: MediaOnly(dc,M)  …  botN   (each conn = 1 in-flight getFile)
```

The core idea: **chunk-level striping across bots and connections**. The unit of work is one 1 MiB chunk, not one HTTP request. Each chunk goes to whichever (bot, DC connection) slot is free and healthy. On a flood wait we move the chunk to another bot right away instead of sleeping.

## Package layout (module `github.com/biohazard786/mtflux`)

```
cmd/mtflux/main.go        wiring, graceful shutdown (SIGTERM drains streams)
cmd/mtbench/main.go       throughput benchmark: sweep bots × conns × window against a log-channel msg
internal/config/          caarlos0/env + godotenv; validation; speed profiles
internal/tg/              gotd plumbing
  worker.go               one bot = telegram.Client (NoUpdates for workers), FileStorage session, health/EWMA stats
  dcpool.go               lazy per-(worker,dc) pools: MediaOnly → fallback; singleflight creation; rebuild on auth errors
  dialer.go               dcs.PlainOptions{Obfuscated, Dial w/ 2MB SO_RCVBUF/SNDBUF}; live dc_options from help.getConfig
  errors.go               classify tgerr → {FloodWait(d), FileRefExpired, Migrate(dc), AuthBroken, NoAccess, NotFound, Transient}
internal/resolver/        msgID → FileMeta + per-worker InputFileLocation; LRU + singleflight; refresh on FILE_REFERENCE_EXPIRED; negative cache
internal/fetch/           scheduler (slot accounting, cooldowns, retry/reassign) + chunk cache + singleflight dedup
internal/stream/          ordered Reader over [start,end]: window futures, slow-start, global buffer budget, cancellation
internal/link/            compact HMAC-SHA256 token: ver|msgID|expiry|ownerID|mac[10] → base64url (~30 chars)
internal/access/          owner/admins/allowlist/bans (env seed + /data/acl.json), x/time/rate limiters, per-IP stream caps
internal/bot/             main-bot handlers & commands (gotd updates.Manager + tg.UpdateDispatcher, message/markup helpers)
internal/httpapi/         net/http mux: /dl, /healthz, /metrics; RFC 7233 Range, ETag, If-Range, Content-Disposition (RFC 6266/5987)
internal/metrics/         Prometheus collectors + snapshot for /stats
deploy/                   Dockerfile (scratch, CGO_ENABLED=0), docker-compose.yml, sample .env
```

Dependencies:
- `gotd/td` (latest v0.16x), plus `gotd/contrib/middleware/floodwait`, used **only on main-bot control-plane calls**.
- `golang.org/x/sync/singleflight`, `golang.org/x/time/rate`.
- `prometheus/client_golang`, `zap` (gotd's logger), `caarlos0/env/v11`, `joho/godotenv`.

We use gotd directly instead of gotgproto, to keep control over pools and sessions. The byte-budget LRU is our own (`container/list`), about 100 lines.

## Key designs

### 1. Connections (`internal/tg`)
- **Per worker per DC, created lazily**:
  - First try `client.MediaOnly(ctx, dc, M)`.
  - If there's no media address, fall back to `client.Pool(M)` for the home DC or `client.DC(ctx, dc, M)` for a foreign one, capped at **2** connections, because non-media DCs carry the `AUTH_KEY_DUPLICATED` risk.
  - Pool creation goes through a singleflight and a global semaphore (staggered) so a cold start doesn't flood `auth.exportAuthorization`.
- **The DC address list comes from live `help.getConfig` dc_options**, which include the media-only IPs. Without that, gotd's built-in `dcs.Prod()` only has media IPs for DC2 and DC4.
- **The file's DC is taken from `Document.DCID`**, and each request is sent straight to that DC's pool, so there's no `FILE_MIGRATE` round trip.
- **Transport**: obfuscated Intermediate, mimicking Desktop, with a custom `Dial` setting 2 MB socket buffers.
- **Sessions are persisted** with `session.FileStorage` at `/data/sessions/<botID>.json`, so restarts don't re-run `auth.importBotAuthorization`.
- **Workers run `NoUpdates`.** Only the main bot handles updates; it can optionally also act as a worker.
- **Startup self-check**: every worker must be able to read the log channel, via `channels.getChannels` with access_hash 0 (to verify). A worker that can't is marked `NoAccess` with a clear log line and never gets scheduled. This fixes their #169/#172.

### 2. Chunk fetch scheduler (`internal/fetch`)
- **Chunks**: index `i` maps to `upload.getFile{offset: i·1MiB, limit: 1MiB}`, without `precise` and without `cdn_supported` (we stay off CDN DCs). The first and last chunk of a range are trimmed in memory.
- **Slots**: each (worker, DC) pool has M slots, since a gotd pool connection carries one RPC at a time. To acquire one, pick the healthy worker on the target DC with the lowest `inflight/M`, breaking ties by EWMA latency. If every slot is busy, wait in a FIFO queue.
- **How each error class is handled**:
  - `FLOOD_WAIT` / `FLOOD_PREMIUM_WAIT(d)`: put that worker in cooldown until now+d and requeue the chunk immediately on another worker.
  - `FILE_REFERENCE_EXPIRED`: invalidate that worker's location, re-resolve once (singleflight), retry.
  - `Migrate(dc)`: correct the DC in FileMeta and retry.
  - `AuthBroken` (`AUTH_KEY_*`): tear down and rebuild the pool. Repeated failures disable the worker.
  - Timeout (per attempt, about 10 s, measured *after* a slot is acquired): penalize the worker and retry elsewhere. Up to K attempts, then the stream fails.
- **Shared chunk cache**: an LRU keyed by `(docID, chunkIdx)` with a byte budget (default 256 MB), plus a singleflight so concurrent viewers, seeks and IDM's parallel connections share a single fetch. The shared fetch runs on a detached context with a timeout, so one viewer cancelling doesn't kill a fetch others are waiting on.

### 3. Ordered stream reader (`internal/stream`)
- **A true sliding window**: it keeps futures for chunks `[next, next+w)`. `Read()` blocks only on the head future; consuming it immediately launches chunk `next+w`. There is no batch barrier.
- **Slow-start**: `w` starts at 4 and doubles up to `STREAM_WINDOW_MAX` (default 16) as the client keeps reading. Short probe requests from players (e.g. reading the moov atom at the end of the file) therefore don't waste 16 MB of fetches.
- **Global buffer budget** (`MAX_BUFFER_MB`, default 1024) as a weighted semaphore: each stream always gets 2 chunks and must acquire budget for anything above that. This bounds memory under 100+ concurrent streams.
- **Cancellation**: the reader uses `r.Context()`, so a client disconnect stops new fetches. A stalled client is detected with `http.ResponseController.SetWriteDeadline`, extended on every write (60 s idle), which frees its buffers. This fixes their paused-player memory pinning.

### 4. Speed profiles (Nekogram-style, overridable per knob)

| profile | conns per bot per DC (M) | window max | notes |
|---|---|---|---|
| `safe` | 2 | 4 | ≈ official non-Premium in-flight level |
| `fast` (default) | 4 | 16 | |
| `extreme` | 8 | 32 | highest flood/throttle risk; cooldown/reassign absorbs it |

Each profile also sets `STREAM_MAX_BOTS`, which caps how many bots a single stream may stripe across. The default is all bots. The final defaults are set from Phase 0 measurements.

### 5. Links, expiry, revocation (`internal/link`, `internal/bot`)
- **Ingest**: the main bot copies the user's media to `LOG_CHANNEL` with `messages.sendMedia(InputMediaDocument/Photo{id, access_hash, file_reference})`, with caption `#u<ownerID>`. A copy rather than a forward avoids privacy-dependent `fwd_from`, and the log channel becomes the record of who owns what.
  - Control-plane calls go through the floodwait middleware plus a per-user rate limit, so a media group no longer hits FLOOD_WAIT. This fixes their #174.
- **Token**: `base64url(ver | uvarint msgID | uvarint expiryMinutes (0 = never) | uvarint ownerID | HMAC-SHA256(LINK_SECRET)[:10])`. Verified in constant time. It can't be enumerated, which fixes their #80.
  - URL: `https://HOST/dl/<token>/<urlescaped-filename>`. `?inline=1` switches to inline disposition.
- **Expiry**: `DEFAULT_LINK_TTL` (0 = never). Users can reply `/link 6h` to their file for a custom TTL. Expired links get 410.
- **Revocation**:
  - `/revoke` (reply to the link message, or pass the link) deletes the log message with `channels.deleteMessages` and evicts it from the resolver and chunk caches. The resolver then reports not-found, which returns 410 with a short negative cache.
  - Owners can revoke their own links; admins can revoke any link.
  - Rotating `LINK_SECRET` revokes all links at once.
  - A banned user's links stop working too, because the token carries ownerID and is checked against the ban list on every request.

### 6. Access control (`internal/access`)
- **Who can generate links**: `ACCESS_MODE=public|private`, `OWNER_ID`, `ADMIN_IDS`, `ALLOWED_USERS`.
- **Admin commands**: `/allow`, `/disallow`, `/ban`, `/unban`. They are persisted to `/data/acl.json` with atomic writes. `/data` already exists for sessions; this is the only local state and is not a database.
- **Limits**: per-user link-generation rate limit (token bucket); `MAX_STREAMS_PER_IP` (default 16, enough for IDM/aria2 `-x16`); global `MAX_ACTIVE_STREAMS`.

### 7. HTTP (`internal/httpapi`)
- Go 1.22+ `ServeMux` patterns (`GET /dl/{token}/{name...}`, HEAD handled), with no web framework.
- **Range**: parse a single range; for multiple ranges, serve the first. Unsatisfiable ranges return **416** with `Content-Range: bytes */size`. Suffix and open-ended ranges are supported. Fuzz-tested.
- **Headers**: `Accept-Ranges`, a strong `ETag` (`"<docID>"`), `Last-Modified` (message date), `If-Range`/`If-None-Match`, and `Cache-Control` bounded by link expiry.
  - `Content-Disposition` carries both an ASCII-fallback `filename` and `filename*=UTF-8''…`.
  - MIME type comes from the document, falling back to the file extension.
- **Errors before any body byte is sent** map to proper status codes. Mid-stream errors abort the connection rather than sending a short, silent body.
- **Server timeouts**: `ReadHeaderTimeout` and `IdleTimeout`; no global `WriteTimeout`, since the per-write deadline above covers stalls. TLS and HTTP/2 are terminated by Caddy in compose.

### 8. Metrics and stats (`internal/metrics`)
- **Prometheus at `/metrics`**, optionally protected by a bearer token:
  - `mtflux_bytes_served_total`
  - `mtflux_chunk_fetch_seconds{worker,dc}` (histogram)
  - `mtflux_chunk_errors_total{class}`
  - `mtflux_flood_wait_seconds_total{worker}`
  - `mtflux_inflight_chunks{worker}`
  - `mtflux_active_streams`
  - `mtflux_cache_{hits,misses}_total`, `mtflux_cache_bytes`
- **`/stats` admin command**: per-worker status (ok/cooldown/no-access), EWMA MB/s, in-flight count, flood-wait totals; plus overall throughput, active streams, cache hit rate, uptime.

## Phases

**Phase 0 — Spike and benchmark (do first; it decides the defaults).** Build `cmd/mtbench` using `internal/tg`, then measure with real bots:
1. `MediaOnly` works for both the home DC and foreign DCs. This includes checking whether the home DC hits `DC_ID_INVALID` on auth export, and that `help.getConfig` media IPs resolve.
2. Per-connection MB/s and how throughput scales with M = 1, 2, 4, 8 and with the number of bots.
3. Whether bots get `FLOOD_PREMIUM_WAIT` or are throttled under sustained load (about 20 GB).
4. Whether one bot's `file_reference` and `access_hash` work for the other bots. If yes, resolve once per file rather than once per bot.
5. `channels.getChannels` with access_hash 0 works for admin bots.

Output: a numbers table, then lock in the profile defaults.

**Phase 1 — Core path.** config → `internal/tg` (workers, dcpool, errors) → resolver → fetch scheduler (no cache yet) → stream.Reader → httpapi `/dl` with Range → link token → main bot ingest and reply. Milestone: send a file to the bot, `curl` the link, and the sha256 matches.

**Phase 2 — Speed and robustness.** Chunk cache and singleflight, slow-start, global buffer budget, cooldown/reassign, file-reference refresh, pool rebuild, write deadlines, graceful shutdown.

**Phase 3 — Features.** Access control and ACL persistence, `/link` TTL, `/revoke`, bans, metrics, `/stats`.

**Phase 4 — Ship.** Dockerfile, compose with Caddy, `.env.sample`, README with BotFather and log-channel setup (all worker bots must be channel admins), and a tuning guide built from the Phase 0 numbers.

## Verification
- **Unit tests** (`go test -race ./...`):
  - Chunk math: alignment, first/last trim, last partial chunk, ranges of 0 to 1 bytes.
  - Range parser, including a fuzz test.
  - Token sign/verify, expiry and tampering.
  - LRU byte budget.
  - ACL persistence.
- **Engine tests with a fake `ChunkSource`**: random latency, injected FLOOD_WAIT, FILE_REFERENCE_EXPIRED and timeouts. Assert ordered output equals the source bytes, chunks are reassigned on flood, memory stays within the budget, and fairness holds across 50 concurrent streams. No Telegram needed.
- **End to end with real bots** (a test log channel, 1 to 8 worker bots, a 2 GB file):
  - `curl -o /dev/null -w '%{speed_download}'` for single-connection speed, compared against Nekogram's 60 Mbit/s target.
  - `aria2c -x16 -s16` to check multi-connection behaviour, then `sha256sum` against the original.
  - `mpv`/VLC on the link, seeking repeatedly. `ffprobe` on the link to check moov-at-end probing.
  - `curl -I` and `curl -r 100-200 -v` to check headers, 206 and 416.
  - Revoke a link and confirm 410; let a link expire and confirm 410; ban a user and confirm their links die.
  - Watch Grafana or `curl /metrics` during a 10-stream load test for flood waits, per-worker spread and cache hit rate.
