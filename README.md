# mtflux ⚡

**mtflux** is a high-throughput Telegram-to-direct-link streaming and download server written in Go. It stripes 1 MiB chunks across **$N$ worker bots $\times$ $M$ Media-DC connections** with a sliding-window reader to achieve extreme download speeds.

---

## Why mtflux?

Existing solutions like TG-FileStreamBot top out around ~10 MB/s per bot because they assign one bot per HTTP request, serialize downloads across a single DC connection, lack prefetch windows, and use un-resilient batch barriers.

mtflux changes the architecture:
- **Chunk-level striping**: Each 1 MiB chunk is independently dispatched to the least-loaded healthy (bot, DC) connection slot.
- **Media-only DCs**: Download sessions run against parallel-safe Media DCs, avoiding Telegram's `AUTH_KEY_DUPLICATED` triggers.
- **Ordered sliding window with slow-start**: Reader window dynamically scales ($4 \to 16 \to 32$) without batch barriers. Probe requests (like MP4 `moov` atom seek) fetch only what is needed.
- **Zero-downtime flood handling**: Chunks experiencing `FLOOD_WAIT` are immediately reassigned to another bot.
- **Shared chunk cache**: Byte-budget LRU cache (`CHUNK_CACHE_MB`) deduplicates concurrent viewers, player seeks, and multi-connection downloaders (`aria2c`, IDM) using singleflight.
- **Compact cryptographically-signed URLs**: Tamper-proof HMAC-SHA256 tokens carrying message ID, owner ID, and expiry timestamp in ~32 URL-safe characters.
- **Full RFC 7233 compliance**: Range headers, strong ETags, 206 Partial Content, 416 Range Not Satisfiable, and RFC 6266/5987 UTF-8 `Content-Disposition`.
- **Pure log-channel architecture**: The log channel serves as the database; no external database required.

---

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

---

## Speed Profiles

| Profile | Conns/Bot/DC ($M$) | Window Max | Target Throughput / Notes |
|---|---|---|---|
| `safe` | 2 | 4 | ~Official Telegram desktop level |
| `fast` *(default)* | 4 | 16 | Optimal balance for VPS servers |
| `extreme` | 8 | 32 | Nekogram-style extreme throughput |

---

## Quick Start

### 1. Requirements & Telegram Setup
1. **API ID & API Hash**: Obtain from [my.telegram.org](https://my.telegram.org).
2. **Main Bot**: Create via [@BotFather](https://t.me/BotFather) (`/newbot`).
3. **Worker Bots**: Create 1 to 7 additional bots via [@BotFather].
4. **Log Channel**:
   - Create a private channel.
   - Add your **main bot AND all worker bots as Channel Administrators** with full read/write/post permissions.
   - Get the channel ID (usually formatted as `-100...`).

### 2. Deployment with Docker Compose

```bash
git clone https://github.com/mtflux/mtflux.git
cd mtflux/deploy

# Copy configuration
cp .env.sample .env

# Edit credentials in .env
nano .env

# Start with Caddy (automatic HTTPS)
docker compose up -d
```

### 3. Local Development Build

```bash
# Build mtflux and benchmark harness
go build -o bin/mtflux ./cmd/mtflux
go build -o bin/mtbench ./cmd/mtbench

# Run all unit tests with race detection
go test -v -race ./...
```

---

## Benchmark Harness (`mtbench`)

To benchmark your worker bots and tune your profile defaults against an actual file:

```bash
./bin/mtbench -msg <LOG_CHANNEL_MSG_ID> -conns 1,2,4,8 -chunks 32
```

Outputs live latency percentiles ($P_{50}, P_{90}$), throughput MB/s, and Media-DC connection diagnostics.

---

## Bot Commands

- `/start` — Welcome message and instructions.
- `/help` — Overview of commands and usage.
- `/link <duration>` — Generate custom TTL link (e.g. `/link 6h`, `/link 2d`).
- `/revoke <token_or_link>` — Delete the file from the log channel and revoke link.
- `/stats` — *(Admin)* View uptime, worker status, in-flight chunks, cache hit rates.
- `/allow <user_id>` — *(Admin)* Grant link generation access.
- `/disallow <user_id>` — *(Admin)* Revoke user link generation access.
- `/ban <user_id>` — *(Admin)* Ban user and invalidate all their links immediately.
- `/unban <user_id>` — *(Admin)* Unban user.

---

## Prometheus Metrics (`/metrics`)

- `mtflux_bytes_served_total`: Total bytes served to clients.
- `mtflux_chunk_fetch_seconds`: Histogram of chunk fetch latency by worker and DC.
- `mtflux_chunk_errors_total`: Counter of errors classified by type.
- `mtflux_flood_wait_seconds_total`: Total seconds of flood-wait per worker.
- `mtflux_inflight_chunks`: Live gauge of in-flight chunk requests.
- `mtflux_active_streams`: Current number of active client downloads.
- `mtflux_cache_hits_total`, `mtflux_cache_misses_total`: Cache efficiency counters.
- `mtflux_cache_bytes`: Current memory utilized by the LRU chunk cache.

---

## License

MIT
