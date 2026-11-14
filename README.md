# mtflux ⚡

Send a file to a Telegram bot. Get a download link back. Click it. Done.

No "wait 30 seconds", no ads, no sketchy buttons. Just a link that goes brrr.

## How it works

Telegram makes you download files slowly, one bite at a time. mtflux hires a whole **gang of bots** to fetch the bites at the same time and glues them together before they reach you. One bot is slow. Twenty bots arguing over who gets the next chunk are surprisingly quick.

On a small free VPS it hit **~95 MB/s** for a nearby file. Your mileage will vary with how far the file's Telegram datacenter is, and how much your server can handle.

Bonus features, because why not:

- Links expire when you want them to, and you can revoke them.
- Pause, resume and seek work (video players love this).
- If a bot gets stuck, another one picks up its work. Nobody gets left behind.
- Broken downloads fail loudly instead of quietly handing you a corrupted file.

## Setup

You'll need:

1. An **API ID and hash** from [my.telegram.org](https://my.telegram.org).
2. A **main bot** from [@BotFather](https://t.me/BotFather).
3. A few **worker bots** from the same place (more bots = more speed, up to 48).
4. A **private channel** where the main bot and all worker bots are admins. It's where files get stashed.
5. A server with Docker. A tiny one is fine.

Then:

```bash
git clone https://github.com/biohazard786/mtflux.git
cd mtflux
cp .env.sample .env
nano .env                  # fill in your tokens and a secret (openssl rand -hex 32)
docker compose up -d
docker compose logs -f mtflux
```

The first start is slow-ish while the bots log in (a minute or two). After that, restarts take a second. Keep the `mtflux_data` volume and it won't make you wait again.

Putting it behind Caddy or nginx? Set `BIND_ADDR=127.0.0.1` in `.env`. There's an example [Caddyfile](Caddyfile) in the repo.

## Using it

Message your main bot:

- Send any file to get a link.
- `/link 6h` makes the link last 6 hours (`m`, `h`, `d`, `w`). Add it as a caption or reply to a file.
- `/revoke <link>` kills a link.
- `/stats` shows how your bots are doing (admins only).
- `/allow`, `/disallow`, `/ban`, `/unban` `<user_id>` manage who gets in (admins only).

## Settings

Everything lives in [.env.sample](.env.sample), with comments. The defaults are good, so you can skip this section. The ones people actually touch:

| Setting | What it does |
|---|---|
| `PUBLIC_URL` | The address your links point to |
| `ACCESS_MODE` | `public` (anyone) or `private` (only you and friends) |
| `DEFAULT_LINK_TTL` | How long links live. `0` means forever |
| `LOG_LEVEL` | `info` is chatty enough. `debug` if something smells off |

## For the nerds

The speed tricks, benchmarks, architecture diagram, metrics and the `mtbench` tool are in [docs/TECHNICAL.md](docs/TECHNICAL.md).

```bash
go build -o bin/mtflux ./cmd/mtflux
go test -race ./...
```

## License

[MIT](LICENSE). Do whatever you want, just don't blame me if your bots get tired.
