package bot

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/log/logzap"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/mtflux/mtflux/internal/access"
	"github.com/mtflux/mtflux/internal/config"
	"github.com/mtflux/mtflux/internal/fetch"
	"github.com/mtflux/mtflux/internal/link"
	"github.com/mtflux/mtflux/internal/metrics"
	"github.com/mtflux/mtflux/internal/resolver"
	gotgtg "github.com/mtflux/mtflux/internal/tg"
	"go.uber.org/zap"
)

// Bot manages Telegram main bot updates, message ingest, commands, and link creation.
type Bot struct {
	cfg       *config.Config
	client    *telegram.Client
	access    *access.Manager
	resolver  *resolver.Resolver
	scheduler *fetch.Scheduler
	metrics   *metrics.Metrics
	log       *zap.Logger
}

// NewBot initializes the main Telegram control bot.
func NewBot(
	cfg *config.Config,
	accessMgr *access.Manager,
	res *resolver.Resolver,
	sched *fetch.Scheduler,
	m *metrics.Metrics,
	log *zap.Logger,
) (*Bot, error) {
	if log == nil {
		log = zap.NewNop()
	}
	botLog := log.With(zap.String("role", "main_bot"))

	sessionsDir := filepath.Join(cfg.DataDir, "sessions")
	if err := os.MkdirAll(sessionsDir, 0o700); err != nil {
		return nil, fmt.Errorf("create sessions dir: %w", err)
	}

	sessionPath := filepath.Join(sessionsDir, "main_bot.json")
	sessionStorage := &session.FileStorage{Path: sessionPath}

	b := &Bot{
		cfg:       cfg,
		access:    accessMgr,
		resolver:  res,
		scheduler: sched,
		metrics:   m,
		log:       botLog,
	}

	dispatcher := tg.NewUpdateDispatcher()
	dispatcher.OnNewMessage(b.handleNewMessage)

	// Control plane uses floodwait middleware
	waiter := floodwait.NewWaiter()

	opts := telegram.Options{
		Resolver:       gotgtg.CreateResolver(),
		SessionStorage: sessionStorage,
		Logger:         logzap.New(botLog),
		UpdateHandler:  dispatcher,
		Middlewares: []telegram.Middleware{
			waiter,
		},
	}

	b.client = telegram.NewClient(cfg.AppID, cfg.AppHash, opts)
	return b, nil
}

// Start connects and runs the main bot.
func (b *Bot) Start(ctx context.Context, ready chan<- struct{}) error {
	return b.client.Run(ctx, func(ctx context.Context) error {
		status, err := b.client.Auth().Status(ctx)
		if err != nil || !status.Authorized {
			b.log.Info("logging in main bot")
			if _, err := b.client.Auth().Bot(ctx, b.cfg.BotToken); err != nil {
				return fmt.Errorf("main bot login failed: %w", err)
			}
		}

		b.log.Info("main bot authenticated successfully")
		if ready != nil {
			close(ready)
		}

		<-ctx.Done()
		return ctx.Err()
	})
}

func (b *Bot) handleNewMessage(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
	msg, ok := u.Message.(*tg.Message)
	if !ok || msg.Out {
		return nil // Ignore outgoing or non-standard messages
	}

	senderID := b.extractSenderID(msg)
	if senderID == 0 {
		return nil
	}

	text := strings.TrimSpace(msg.Message)

	// Handle commands
	if strings.HasPrefix(text, "/") {
		return b.handleCommand(ctx, msg, senderID, text)
	}

	// Handle media file upload
	if msg.Media != nil {
		return b.handleMediaUpload(ctx, msg, senderID, b.cfg.DefaultLinkTTL)
	}

	return nil
}

func (b *Bot) handleCommand(ctx context.Context, msg *tg.Message, senderID int64, text string) error {
	parts := strings.Fields(text)
	cmd := strings.ToLower(parts[0])
	// Strip @botname if present
	if idx := strings.IndexByte(cmd, '@'); idx != -1 {
		cmd = cmd[:idx]
	}

	switch cmd {
	case "/start":
		return b.reply(ctx, msg, "👋 *Welcome to mtflux!*\n\nSend or forward any file or video to get a blazing-fast direct download and streaming link powered by parallel chunk striping across multiple connections.\n\nUse `/help` for commands.")

	case "/help":
		helpText := "📖 *mtflux Commands:*\n\n" +
			"• Send any media: generate direct download link\n" +
			"• `/link <duration>`: customize TTL (e.g., `/link 6h`, `/link 2d`)\n" +
			"• `/revoke <link/token>`: revoke and delete link immediately\n"
		if b.access.IsAdmin(senderID) {
			helpText += "\n👮 *Admin Commands:*\n" +
				"• `/stats`: view live worker and download metrics\n" +
				"• `/allow <user_id>`: grant user link access\n" +
				"• `/disallow <user_id>`: remove user access\n" +
				"• `/ban <user_id>`: ban user and revoke all their links\n" +
				"• `/unban <user_id>`: unban user\n"
		}
		return b.reply(ctx, msg, helpText)

	case "/stats":
		if !b.access.IsAdmin(senderID) {
			return b.reply(ctx, msg, "⛔ Access denied.")
		}
		stats := b.metrics.FormatStats(b.access.ActiveStreamsCount())
		return b.reply(ctx, msg, stats)

	case "/allow":
		return b.handleAdminACL(ctx, msg, senderID, parts, b.access.Allow, "allowed")
	case "/disallow":
		return b.handleAdminACL(ctx, msg, senderID, parts, b.access.Disallow, "disallowed")
	case "/ban":
		return b.handleAdminACL(ctx, msg, senderID, parts, b.access.Ban, "banned")
	case "/unban":
		return b.handleAdminACL(ctx, msg, senderID, parts, b.access.Unban, "unbanned")

	case "/revoke":
		return b.handleRevoke(ctx, msg, senderID, parts)

	case "/link":
		// Custom TTL link request (e.g. /link 6h or /link 2d in reply to media)
		if len(parts) < 2 {
			return b.reply(ctx, msg, "Usage: `/link <duration>` (e.g. `/link 6h`, `/link 24h`, `/link 7d`)")
		}
		ttl, err := time.ParseDuration(parts[1])
		if err != nil {
			return b.reply(ctx, msg, "Invalid duration format. Use e.g. `2h`, `24h`, `7d`.")
		}
		// If replying to media, ingest that media with this TTL
		if msg.ReplyTo != nil {
			return b.reply(ctx, msg, "Please send or forward the file directly along with the command.")
		}
		return b.reply(ctx, msg, fmt.Sprintf("TTL set to `%v`. Now send your file.", ttl))
	}

	return nil
}

func (b *Bot) handleAdminACL(ctx context.Context, msg *tg.Message, senderID int64, parts []string, fn func(int64) error, action string) error {
	if !b.access.IsAdmin(senderID) {
		return b.reply(ctx, msg, "⛔ Admin access required.")
	}
	if len(parts) < 2 {
		return b.reply(ctx, msg, fmt.Sprintf("Usage: `%s <user_id>`", parts[0]))
	}
	targetID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return b.reply(ctx, msg, "Invalid user ID.")
	}
	if err := fn(targetID); err != nil {
		return b.reply(ctx, msg, fmt.Sprintf("Error: %v", err))
	}
	return b.reply(ctx, msg, fmt.Sprintf("✅ User `%d` has been %s.", targetID, action))
}

func (b *Bot) handleRevoke(ctx context.Context, msg *tg.Message, senderID int64, parts []string) error {
	var tokenStr string
	if len(parts) >= 2 {
		tokenStr = extractTokenFromText(parts[1])
	}

	if tokenStr == "" {
		return b.reply(ctx, msg, "Usage: `/revoke <token_or_link>`")
	}

	tok, err := link.Parse([]byte(b.cfg.LinkSecret), tokenStr)
	if err != nil && !errors.Is(err, link.ErrTokenExpired) {
		return b.reply(ctx, msg, "❌ Invalid link token.")
	}

	// Only owner or admin can revoke
	if tok.OwnerID != senderID && !b.access.IsAdmin(senderID) {
		return b.reply(ctx, msg, "⛔ You are not authorized to revoke this link.")
	}

	// Delete from log channel
	cleanChannelID := gotgtg.ParseChannelID(b.cfg.LogChannelID)
	_, err = b.client.API().ChannelsDeleteMessages(ctx, &tg.ChannelsDeleteMessagesRequest{
		Channel: &tg.InputChannel{ChannelID: cleanChannelID, AccessHash: 0},
		ID:      []int{tok.MsgID},
	})
	if err != nil {
		b.log.Warn("failed to delete message from log channel", zap.Int("msg_id", tok.MsgID), zap.Error(err))
	}

	// Invalidate resolver and chunk cache
	b.resolver.Invalidate(tok.MsgID)

	return b.reply(ctx, msg, "🗑️ *Link revoked.* The file and link have been removed.")
}

func (b *Bot) handleMediaUpload(ctx context.Context, msg *tg.Message, senderID int64, ttl time.Duration) error {
	if !b.access.CanGenerateLink(senderID) {
		return b.reply(ctx, msg, "⛔ You do not have permission to generate download links.")
	}

	if !b.access.CheckRateLimit(senderID) {
		return b.reply(ctx, msg, "⚠️ Rate limit reached. Please wait a few seconds before uploading another file.")
	}

	var inputMedia tg.InputMediaClass
	var filename string
	var fileSize int64

	switch media := msg.Media.(type) {
	case *tg.MessageMediaDocument:
		doc, ok := media.Document.AsNotEmpty()
		if !ok {
			return b.reply(ctx, msg, "❌ Unsupported document format.")
		}
		inputMedia = &tg.InputMediaDocument{
			ID: &tg.InputDocument{
				ID:            doc.ID,
				AccessHash:    doc.AccessHash,
				FileReference: doc.FileReference,
			},
		}
		fileSize = doc.Size
		for _, attr := range doc.Attributes {
			if f, ok := attr.(*tg.DocumentAttributeFilename); ok {
				filename = f.FileName
				break
			}
		}
		if filename == "" {
			filename = fmt.Sprintf("file_%d", doc.ID)
		}

	case *tg.MessageMediaPhoto:
		photo, ok := media.Photo.AsNotEmpty()
		if !ok {
			return b.reply(ctx, msg, "❌ Unsupported photo format.")
		}
		inputMedia = &tg.InputMediaPhoto{
			ID: &tg.InputPhoto{
				ID:            photo.ID,
				AccessHash:    photo.AccessHash,
				FileReference: photo.FileReference,
			},
		}
		filename = fmt.Sprintf("photo_%d.jpg", photo.ID)

	default:
		return b.reply(ctx, msg, "❌ Please send a valid document, audio, video, or photo.")
	}

	// Send copy of media to log channel with caption #u<senderID>
	cleanChannelID := gotgtg.ParseChannelID(b.cfg.LogChannelID)
	logPeer := &tg.InputPeerChannel{ChannelID: cleanChannelID, AccessHash: 0}

	randID, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	updates, err := b.client.API().MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
		Peer:     logPeer,
		Media:    inputMedia,
		Message:  fmt.Sprintf("#u%d", senderID),
		RandomID: randID.Int64(),
	})

	if err != nil {
		b.log.Error("failed to copy media to log channel", zap.Error(err))
		return b.reply(ctx, msg, "❌ Failed to store media into log channel. Please make sure the bot is an admin in the log channel.")
	}

	logMsgID := extractMsgIDFromUpdates(updates)
	if logMsgID == 0 {
		return b.reply(ctx, msg, "❌ Failed to retrieve message ID from log channel.")
	}

	var expiry time.Time
	if ttl > 0 {
		expiry = time.Now().Add(ttl)
	}

	token := link.Generate([]byte(b.cfg.LinkSecret), logMsgID, senderID, expiry)
	encodedName := url.PathEscape(filename)
	dlURL := fmt.Sprintf("%s/dl/%s/%s", b.cfg.PublicURL, token, encodedName)
	streamURL := fmt.Sprintf("%s?inline=1", dlURL)

	var sb strings.Builder
	sb.WriteString("⚡ *Download Link Generated!*\n\n")
	sb.WriteString(fmt.Sprintf("📁 *Name:* `%s`\n", filename))
	if fileSize > 0 {
		sb.WriteString(fmt.Sprintf("📦 *Size:* `%.2f MB`\n", float64(fileSize)/(1024*1024)))
	}
	if !expiry.IsZero() {
		sb.WriteString(fmt.Sprintf("⏱️ *Expires:* `%s`\n", expiry.UTC().Format("2006-01-02 15:04 UTC")))
	} else {
		sb.WriteString("⏱️ *Expires:* `Never`\n")
	}

	sb.WriteString("\n🚀 *Direct Links:*\n")
	sb.WriteString(fmt.Sprintf("📥 [Fast Download](%s)\n", dlURL))
	sb.WriteString(fmt.Sprintf("▶️ [Direct Stream / Web Player](%s)\n\n", streamURL))
	sb.WriteString(fmt.Sprintf("`%s`\n", dlURL))

	return b.reply(ctx, msg, sb.String())
}

func (b *Bot) reply(ctx context.Context, msg *tg.Message, text string) error {
	peer := b.getInputPeer(msg)
	randID, _ := rand.Int(rand.Reader, big.NewInt(1<<62))

	_, err := b.client.API().MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
		Peer:     peer,
		Message:  text,
		ReplyTo:  &tg.InputReplyToMessage{ReplyToMsgID: msg.ID},
		RandomID: randID.Int64(),
	})
	return err
}

func (b *Bot) getInputPeer(msg *tg.Message) tg.InputPeerClass {
	switch p := msg.PeerID.(type) {
	case *tg.PeerUser:
		return &tg.InputPeerUser{UserID: p.UserID, AccessHash: 0}
	case *tg.PeerChannel:
		return &tg.InputPeerChannel{ChannelID: p.ChannelID, AccessHash: 0}
	case *tg.PeerChat:
		return &tg.InputPeerChat{ChatID: p.ChatID}
	default:
		return &tg.InputPeerSelf{}
	}
}

func (b *Bot) extractSenderID(msg *tg.Message) int64 {
	if msg.FromID != nil {
		if u, ok := msg.FromID.(*tg.PeerUser); ok {
			return u.UserID
		}
	}
	if p, ok := msg.PeerID.(*tg.PeerUser); ok {
		return p.UserID
	}
	return 0
}

func extractTokenFromText(text string) string {
	text = strings.TrimSpace(text)
	if strings.Contains(text, "/dl/") {
		parts := strings.Split(text, "/dl/")
		if len(parts) > 1 {
			tokenPart := parts[1]
			if slash := strings.IndexByte(tokenPart, '/'); slash != -1 {
				return tokenPart[:slash]
			}
			return tokenPart
		}
	}
	return text
}

func extractMsgIDFromUpdates(u tg.UpdatesClass) int {
	switch v := u.(type) {
	case *tg.Updates:
		for _, upd := range v.Updates {
			if m, ok := upd.(*tg.UpdateNewChannelMessage); ok {
				if msg, ok := m.Message.(*tg.Message); ok {
					return msg.ID
				}
			}
			if m, ok := upd.(*tg.UpdateNewMessage); ok {
				if msg, ok := m.Message.(*tg.Message); ok {
					return msg.ID
				}
			}
		}
	case *tg.UpdatesCombined:
		for _, upd := range v.Updates {
			if m, ok := upd.(*tg.UpdateNewChannelMessage); ok {
				if msg, ok := m.Message.(*tg.Message); ok {
					return msg.ID
				}
			}
		}
	case *tg.UpdateShortSentMessage:
		return v.ID
	}
	return 0
}
