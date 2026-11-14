# mtflux ⚡

**mtflux** is a high-throughput Telegram → direct-link streaming and download server written in Go. It stripes 1 MiB chunks across **N worker bots × 2 connections each** with a sliding-window reader, so a single download link is served by many connections at once.

---

## Why it's fast

Existing solutions like TG-FileStreamBot top out around ~10 MB/s per bot: one bot per request, one connection per foreign DC, batch-barrier prefetch, no sharing.

- **Chunk-level striping.** Each 1 MiB chunk goes to whichever (bot, DC connection) slot is free. Slots are accounted per (bot, DC): never more than M getFile RPCs in flight per bot, FIFO queue when full.
- **Two connections per bot per DC.** Every bot opens a pool of 2 connections to the file's DC (more per bot draws `AUTH_KEY_DUPLICATED`), and requests go straight to that DC (no `FILE_MIGRATE` round trip). Telegram's media-only addresses (DC 2 and DC 4), which would allow more, are not used: gotd v0.163.0 cannot complete a key exchange with them ("wrong DC").
- **Warm pools, saved keys.** With `PREWARM_DCS=true` (default) every bot connects to every DC in the background at startup. Each bot's auth key for each DC is saved to disk (gotd itself only keeps the home DC's), so only the very first start pays for key generation; after a restart 20 bots × 5 DCs reconnect in seconds. A chunk is never tied to a bot whose pool is still being created: it goes to whichever bot is ready first.
- **Sliding window with fast slow-start.** No batch barrier. The window doubles every `STREAM_WINDOW_MIN` chunks up to `STREAM_WINDOW_MAX`, which by default is 3 × *bots × connections* (at most 128). Chunks go out in order, so the window has to hold the requests in flight plus every finished chunk waiting behind a slower one; at 1 × only about half the slots stayed busy (measured on the A1: ~18 of 40). The window only grows for a client that reads as fast as chunks arrive, so a slow client doesn't make the server hold a full window for it.
- **Failover, not sleeping.** Flood-wait → that bot cools down and the chunk moves to another bot immediately. A getFile slower than `FETCH_ATTEMPT_TIMEOUT` is abandoned at the deadline and retried elsewhere, without waiting for the stalled connection to confirm the cancellation. Bots that keep failing are parked.
- **A stalled chunk is fetched twice.** When a getFile takes far longer than chunks from that DC normally do and the download is about to need it, a second bot fetches the same chunk and the first answer wins (`FETCH_HEDGE`). The slower request is left to finish: if it never answers by its deadline twice within two minutes, that bot's connections to the DC are replaced. Measured on the A1: one dead connection among 40 froze a 512 MiB download for 3 × 10 s and halved its speed (9.1 MB/s instead of ~20) before this existed; with it, the same file ran at 26 MB/s with about 2% of its chunks fetched twice.
- **Per-bot file references.** Telegram file references are bound to the account that fetched them (verified live: sharing one bot's reference made 13 of 20 bots fail with `FILE_REFERENCE_EXPIRED`). Each bot resolves the log message itself, caches its own location, and only that bot refreshes when Telegram rejects it.
- **Adaptive concurrency.** A delivery-rate controller (BBR-style) grows the number of RPCs in flight while throughput keeps rising and settles just above the point where it plateaus. On a link slower than the bot fleet this prevents congestion collapse (chunks queueing past their deadline and being downloaded twice); Telegram's heavy latency jitter doesn't fool it. Exposed as `mtflux_fetch_concurrency_limit`.
- **Urgent chunks first.** Queued chunks are served by distance from their reader's position, so every viewer's next-needed chunk beats anyone's deep read-ahead: a new viewer starts fast even when the server is busy.
- **Shared chunk cache + dedup.** Concurrent viewers, seeks, and IDM/aria2's parallel connections share one fetch. A fetch is cancelled once *every* waiter has gone.
- **Zero-copy path.** Cached chunk slices are written straight to the socket.
- **Lean MTProto path.** The obfuscation layer (an extra AES-CTR pass over every byte) is off by default, and AES-IGE decryption uses a register-chained loop on arm64 (+23% on an Ampere A1; vendored in [third_party/ige](third_party/ige)). Download cost on one A1 core: **7.9 ms CPU per MiB** (~127 MB/s per core).
- **Small-VPS friendly cold start.** Bots without a saved session log in a few at a time, and DC pools are created one per core at a time, so a 1-core box doesn't stall generating 20 auth keys in parallel and the first pool is ready as early as possible.

Measured in-process against a 100 ms-latency fake Telegram with 20 bots × 2 conns (`go test ./internal/stream -run x -bench SingleStream -benchtime 1x`, 128 MiB):

| stream window | single-link throughput |
|---|---|
| 4 | 39 MB/s |
| 16 | 114 MB/s |
| 32 | 156 MB/s |
| 64 (auto for 20 bots) | **156 MB/s** (bounded by ramp-up on a 128 MiB file; the 40 slots allow 400 MB/s on longer files) |

More simulations (`go test ./internal/fetch -run x -bench . -benchtime 1x`): on a link-limited host the adaptive limiter lifts goodput from 8.5 to 49.75 chunks/s (99.5% of the link) with zero re-downloads, and a second viewer's first byte drops from 2.9 s to 0.33 s.

Real numbers depend on Telegram and your network: measure them with `mtbench` (below).

### Live results

**Oracle Cloud Ampere A1, 1 OCPU / 6 GB, US East (Ashburn), ~840 Mbit/s link; 20 worker bots, all homed on DC 5** (`mtbench`, 276 MiB per run):

| File stored on | 1 connection | best raw sweep | **one download link** | what limits it |
|---|---|---|---|---|
| DC 1 (Miami, close) | 12.6 MB/s (78 ms per chunk) | 70 MB/s (5 bots × 2) | **95.7 MB/s** (~770 Mbit/s) | the VPS link; CPU at 0.64 cores |
| DC 5 (Singapore, far) | 2.4 MB/s (~400 ms per chunk) | 25–35 MB/s | 17–24 MB/s | Telegram / the route; flat beyond ~10 bots × 2 |

From a residential IP, the same DC 5 file gave a flat ~2–2.8 MB/s total whatever the bot count (an ISP-side ceiling; that run was also verified with SHA-256 across single-connection, `aria2c -x16` and cross-chunk range requests).

What this means for deployment:

- **The file's DC matters, the bots' DC doesn't.** A file lives on its uploader's DC; all 20 DC 5 bots pulled 96 MB/s from DC 1. Put the server near the DC your users upload to (India → DC 5 Singapore, Europe → DC 2/4 Amsterdam, Americas → DC 1/3 Miami).
- **Per-connection speed is latency-bound.** `upload.getFile` returns at most 1 MiB, so throughput ≈ requests in flight × 1 MiB ÷ round trip.
- **Each bot is rate-limited to roughly 20 getFile/s** (`FLOOD_WAIT_1` above ~20 MB/s from one bot). mtflux cools that bot down for the stated second and moves its chunks elsewhere; more bots spread the load.
- **`CONNS_PER_BOT_PER_DC` above 2 has no effect.** Pools hold 2 connections per bot on every DC.
- **Auth keys are generated once, then saved.** A bot needs one auth key per DC (~4 s on an A1 core, of which ~2.7 s is CPU). The first start generates them in the background, one per core at a time: 20 bots × 4 other DCs took 5.5 minutes on 1 OCPU, during which a download from a DC that isn't warm yet starts on the first ready bot and speeds up as the rest join (measured on the A1 with cold pools: 16.9 MB/s for the first DC 1 download, 66.9 MB/s a minute later). Keys live in `DATA_DIR/sessions/bot_<id>_dc<n>.json`; keep that volume and later starts skip all of this.

The same VPS through the HTTP server with every DC warm, downloaded from a second VPS in the same region (one run each):

| File stored on | `curl` (1 connection) | first byte | `aria2c -x16` |
|---|---|---|---|
| DC 1 (Miami) | 44–71 MB/s | 0.6–0.9 s | 81 MiB/s |
| DC 4 (Amsterdam) | 53 MB/s | 0.9–1.0 s | not measured |
| DC 5 (Singapore) | 26 MB/s | 1.9 s | 38 MiB/s average, 49 MiB/s peak |

These were taken with a 64-chunk window, before it was widened. mtflux used 77–83% of the single core at 54–81 MB/s, so on DC 1 and DC 4 this VPS is close to its CPU limit; DC 5 used 46%. A file that was almost entirely in the chunk cache went out at 106 MiB/s (~890 Mbit/s) over 16 connections, so the uplink is not the limit.

Run `mtbench` on your own VPS before tuning: if its raw sweep is flat as bots × connections grow, more bots won't help on that host.

## Correctness & safety

- **Chunk length is validated.** A chunk whose length isn't what the file size implies is retried (never cached, never forwarded), so a bad response can't silently corrupt a download.
- **Errors get real status codes.** The first chunk is fetched *before* headers are sent: 410 (gone), 503 + `Retry-After` (upstream busy), 502. A failure after the body has started aborts the connection instead of delivering a plausible-looking short file.
- **RFC 7233** Range (206/416), `If-Range`, strong `ETag`, `If-None-Match` (lists/weak), `HEAD`, RFC 6266/5987 `Content-Disposition`.
- **Signed links.** Compact HMAC-SHA256 tokens with expiry (rounded *up* to the minute) and owner id. Revocation deletes the log-channel message; bans kill all of a user's links.
- **Hardening.** `Cache-Control: private, no-cache` (revoked links can't be served from shared caches), `nosniff`, HTML/SVG/JS are always forced to download.
- **Fail-closed config.** Typos in `ACCESS_MODE`, `SPEED_PROFILE`, `CHUNK_SIZE`, `DEFAULT_LINK_TTL` etc. are startup errors, not silent defaults. `LINK_SECRET` must be ≥16 chars and not the sample value.

---

## Architecture

```
            Telegram user ──file──▶ main bot (updates) ──sendMedia copy──▶ LOG CHANNEL (the "DB")
                                        │ replies with signed link            ▲ channels.getMessages
                                        ▼                                     │ (any healthy worker, cached)
HTTP client ─GET /dl/{token}/{name}─▶ httpapi ─▶ resolver ─(FileMeta: size,dc,docID,name,mime)
     (Range)                            │
                                        ▼
                               stream.Reader (ordered sliding window, slow-start)
                                        │ FetchChunk(file, idx)
                                        ▼
                     chunk cache (byte-budget LRU) + in-flight dedup (ref-counted)
                                        │ miss
                                        ▼
                     scheduler: warm (bot, dc) with a free slot, lowest load, lowest latency
                                        │
             ┌──────────────┬───────────┴──────┬──────────────┐
          bot1: pool(dc, 2)      bot2: pool(dc, 2)      …  botN   (each conn = 1 in-flight getFile)
```

---

## Quick start

### 1. Telegram setup
1. **API ID & hash** from [my.telegram.org](https://my.telegram.org).
2. **Main bot** via [@BotFather](https://t.me/BotFather).
3. **Worker bots**: up to 48 (`WORKER_TOKEN_1` … `WORKER_TOKEN_48`). More bots means more parallelism and less flood-wait pressure per bot.
4. **Log channel**: a private channel with the **main bot and every worker bot as administrators**. Workers that can't read it are excluded at startup with a log line.

### 2. Deploy with Docker Compose

```bash
git clone https://github.com/biohazard786/mtflux.git
cd mtflux
cp .env.sample .env
nano .env        # fill in tokens, LINK_SECRET (openssl rand -hex 32), PUBLIC_URL, OWNER_ID
docker compose up -d
docker compose logs -f mtflux     # shows the effective tuning + worker readiness
```

On the very first start the bots have no saved sessions and log in a few at a time; on a 1-core VPS 20 bots take 1–2 minutes. A `some workers did not become ready in time` warning is expected then: late workers join as soon as they finish. Later restarts reuse the sessions in the `mtflux_data` volume and are ready in about a second.

The image is a 29 MB static binary running as a non-root user, with a built-in health check, a read-only root filesystem and a 45 s stop grace period (so SIGTERM can drain active downloads).

### 3. Reverse proxy (Caddy / nginx)

Set `BIND_ADDR=127.0.0.1` in `.env` so mtflux is reachable only through the proxy. mtflux trusts `X-Forwarded-For` / `X-Real-IP` only from `TRUSTED_PROXIES` (default: loopback and private ranges, which covers a host proxy and Docker's bridge), so `MAX_STREAMS_PER_IP` applies per real client.

**Caddy**: see [Caddyfile](Caddyfile). Don't add `encode` (payloads are already compressed).

**nginx:**
```nginx
server {
    server_name dl.yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_buffering off;
        proxy_request_buffering off;
        proxy_read_timeout 300s;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### 4. Local development

```bash
go build -o bin/mtflux ./cmd/mtflux
go build -o bin/mtbench ./cmd/mtbench
go test -race ./...
```

`github.com/gotd/ige` is replaced by the patched copy in [third_party/ige](third_party/ige) (see its `README.mtflux`).

---

## Tuning

Everything is configured by environment (see [.env.sample](.env.sample)). The startup log prints the **effective** values (`workers`, `slots_per_dc`, `stream_window_min/max`).

| Setting | Default | Notes |
|---|---|---|
| `SPEED_PROFILE` | `fast` | Sets the default stream window: `safe` (4), `fast` (16), `extreme` (32) |
| `CONNS_PER_BOT_PER_DC` | profile | Parallel getFile RPCs per bot per DC (**M**), capped at 2 |
| `STREAM_WINDOW_MAX` | auto | Auto = 3 × bots × 2, between the profile default and 128 |
| `STREAM_WINDOW_MIN` | auto | Initial window; auto = max/8 clamped to 4–8 |
| `STREAM_MAX_BOTS` | `0` (all) | Per-download cap on distinct bots; rotates across downloads |
| `FETCH_ATTEMPT_TIMEOUT` | `10s` | A slower getFile is retried on another bot (stretched automatically on slow links) |
| `FETCH_HEDGE` | `true` | Fetch a stalled chunk from a second bot when the download is about to need it |
| `PREWARM_DCS` | `true` | Connect every bot to every DC at startup (background). `false`: a DC is warmed by its first download |
| `MTPROTO_OBFUSCATION` | `false` | Obfuscated2 transport; enable only where Telegram is blocked (costs ~9% CPU) |
| `MAX_BUFFER_MB` | `1024` | Read-ahead budget across all downloads |
| `CHUNK_CACHE_MB` | `256` | Shared cache for seeks / parallel downloaders |
| `LOG_LEVEL` | `info` | mtflux's own messages: `debug`, `info`, `warn`, `error` |
| `TG_LOG_LEVEL` | `warn` | The Telegram client library's messages (several lines per connection at `info`) |
| `LOG_FORMAT` | `console` | `console` (readable lines) or `json` |

At `LOG_LEVEL=info` every download ends with one summary line, which is the first thing to look at when a download was slow or broke:

```
2026-11-14 12:00:41.512  INFO  download finished  {"outcome": "complete", "msg_id": 7718, "file": "movie.mkv", "dc": 5, "client": "203.0.113.7", "range_start": 0, "bytes_sent": 536870912, "bytes_wanted": 536870912, "first_byte": "640ms", "took": "24.1s", "avg_mb_per_s": "21.2", "cpu_pct": 38, "host_cpu_pct": 52, "fetch_limit": 40, "chunk_avg": "1.31s", "hedged": 11, "hedge_wins": 9}
```

The last six fields say what held the speed where it was; they cover the whole server while the download ran, not that download alone. `cpu_pct` is mtflux's own CPU use (100 = one core busy) and `host_cpu_pct` how busy the whole machine was, kernel network work included (100 = every core busy; near 100 means the VPS is the limit). `fetch_limit` is how many getFile requests the scheduler currently allows in flight (the ceiling is bots × 2), and `chunk_avg` how long one takes; a download can't go faster than about `fetch_limit ÷ chunk_avg` MiB/s. `hedged` counts chunks fetched a second time because the first request was slow, and `hedge_wins` how often the second copy arrived first.

`outcome` is `complete`, `client_gone` (the client disconnected or stalled) or `upstream_failed` (Telegram could not deliver a chunk; the connection is aborted). `chunk attempt failed, retrying on another bot` lines are routine in small numbers: a slow or failed `getFile` is moved to another bot. `bot got no answer twice on this DC, reconnecting it` means that bot's connections to the DC were replaced. A `chunk failed` error means every attempt was used up.

Total in-flight RPCs are additionally capped by the adaptive limiter, so the settings above are ceilings, not targets. Go's heap is soft-limited to `MAX_BUFFER_MB + CHUNK_CACHE_MB + 512` MiB (unless you set `GOMEMLIMIT`). **Rule of thumb for the window:** about 3 × bots × 2; at 1 × half the slots sit idle behind the slowest chunk. Total slots = bots × 2; more bots add slots and spread the per-bot rate limit.

### Benchmark with your own bots (`mtbench`)

Send a large file (≥ 500 MB) to your bot, note its message id in the log channel, then:

```bash
docker build --target bench -t mtflux-bench .
docker run --rm --env-file .env -v mtflux_bench:/data mtflux-bench \
  -msg <MESSAGE_ID> -nbots 1,5,10,20 -conns 1,2 -chunks 512 -windows 32,64
# or locally: ./bin/mtbench -msg <MESSAGE_ID> ...
```

What it reports:

1. **Login**: per bot, its home DC and login time, or the exact error.
2. **The file**: its DC, and how long the first bot takes to open a pool there.
3. **Warm-up**: how long it took for every bot to open a pool on the file's DC, and any bot that failed.
4. **Phase 1**: raw `upload.getFile` throughput over *bots × connections*, with p50/p90 per chunk and the full first error of any failing bot.
5. **Phase 2**: one download through the real scheduler and window, i.e. what a single link gets.
6. **CPU usage** over phases 1+2: CPU ms per MiB, MB/s one core can sustain, bytes allocated per payload byte, GC count.

| Flag | Default | Purpose |
|---|---|---|
| `-nbots` | `1,half,all` | Bot counts to sweep |
| `-conns` | `1,2,4,8` | Connections per bot to sweep |
| `-chunks` | `64` | MiB per run (use the whole file for steady-state numbers) |
| `-windows` | `16,32,64` | Phase 2 windows; `""` skips phase 2 |
| `-auth-timeout` | `5m` | Wait for logins (a first login on a small VPS is slow) |
| `-warm-all` | off | Open every bot's pool on every DC, as `PREWARM_DCS` does at server start, print how long it took, and exit (no `-msg` needed) |
| `-v` / `-vv` | off | Telegram connection logs at info / debug level |
| `-cpuprofile`, `-memprofile` | off | pprof files for phases 1+2 (mount a writable dir, e.g. `-v ~/prof:/prof`, then `go tool pprof -top prof/cpu.pprof`) |

`mtbench` keeps its own sessions under `DATA_DIR/bench`, so it is safe to run while the server is up.

---

## Bot commands

- `/start`, `/help`
- Send any file → direct link.
- `/link 6h`: custom lifetime. Use it as a **caption** on the file, as a **reply** to a file, or on its own (applies to your next file within 2 minutes). Units: `m`, `h`, `d`, `w`.
- `/revoke <link>`: deletes the log message and revokes the link. Reports failure honestly if the delete didn't go through.
- `/stats` *(admin)*: uptime, per-worker status/latency/throughput, cache hit rate.
- `/allow`, `/disallow`, `/ban`, `/unban` `<user_id>` *(admin)*

## HTTP endpoints

- `GET|HEAD /dl/{token}/{name}`: download (`?inline=1` for inline disposition where safe)
- `GET /healthz`: liveness. `GET /readyz`: 503 if no worker is healthy
- `GET /metrics`: served on the public port **only** if `METRICS_AUTH_TOKEN` is set (bearer auth); otherwise set `METRICS_ADDR` (e.g. `127.0.0.1:9100`) for an internal, unauthenticated listener

## Prometheus metrics

`mtflux_bytes_served_total`, `mtflux_chunk_fetch_seconds{worker,dc}`, `mtflux_chunk_errors_total{class}`, `mtflux_flood_wait_seconds_total{worker}`, `mtflux_inflight_chunks{worker}`, `mtflux_active_streams`, `mtflux_cache_{hits,misses}_total`, `mtflux_cache_bytes`.

---

## License

MIT
