package bot

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	stdhtml "html"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/biohazard786/mtflux/internal/access"
	"github.com/biohazard786/mtflux/internal/config"
	"github.com/biohazard786/mtflux/internal/fetch"
	"github.com/biohazard786/mtflux/internal/link"
	"github.com/biohazard786/mtflux/internal/metrics"
	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/log/logzap"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/message/html"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"
)

const (
	// maxConcurrentHandlers bounds how many messages are processed at once so a
	// burst (e.g. an album) is handled in parallel without unbounded goroutines.
	maxConcurrentHandlers = 8
	handlerTimeout        = 60 * time.Second
	pendingTTLWindow      = 2 * time.Minute
)

type pendingTTL struct {
	ttl   time.Duration
	until time.Time
}

// Bot manages Telegram main bot updates, message ingest, commands, and link creation.
type Bot struct {
	cfg       *config.Config
	client    *telegram.Client
	waiter    *floodwait.Waiter
	sender    *message.Sender
	access    *access.Manager
	resolver  *resolver.Resolver
	scheduler *fetch.Scheduler
	metrics   *metrics.Metrics
	log       *zap.Logger

	runCtx atomic.Pointer[context.Context]
	sem    chan struct{}

	logChannelID   atomic.Int64
	logChannelHash atomic.Int64

	pendingMu sync.Mutex
	pending   map[int64]pendingTTL
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
	sessionStorage := &session.FileStorage{Path: filepath.Join(sessionsDir, "main_bot.json")}

	b := &Bot{
		cfg:       cfg,
		access:    accessMgr,
		resolver:  res,
		scheduler: sched,
		metrics:   m,
		log:       botLog,
		sem:       make(chan struct{}, maxConcurrentHandlers),
		pending:   make(map[int64]pendingTTL),
	}
	b.logChannelID.Store(gotgtg.ParseChannelID(cfg.LogChannelID))

	dispatcher := tg.NewUpdateDispatcher()
	dispatcher.OnNewMessage(b.handleNewMessage)

	// Control plane rides the flood-wait middleware. The Waiter only works while
	// its Run loop is active, so Start wraps the client in waiter.Run.
	b.waiter = floodwait.NewWaiter()

	b.client = telegram.NewClient(cfg.AppID, cfg.AppHash, telegram.Options{
		Resolver:       gotgtg.CreateResolver(),
		SessionStorage: sessionStorage,
		Logger:         logzap.New(gotgtg.LibraryLogger(log).With(zap.String("role", "main_bot"))),
		UpdateHandler:  dispatcher,
		Middlewares:    []telegram.Middleware{b.waiter},
	})
	b.sender = message.NewSender(b.client.API())
	return b, nil
}

// Start connects and runs the main bot, restarting with backoff if the client
// loop dies. It returns when ctx is cancelled or the token is rejected.
func (b *Bot) Start(ctx context.Context, ready chan<- struct{}) error {
	var once sync.Once
	signalReady := func() {
		if ready != nil {
			once.Do(func() { close(ready) })
		}
	}

	backoff := time.Second
	for {
		err := b.waiter.Run(ctx, func(ctx context.Context) error {
			return b.client.Run(ctx, func(ctx context.Context) error {
				status, err := b.client.Auth().Status(ctx)
				if err != nil || !status.Authorized {
					b.log.Info("logging in main bot")
					if _, err := b.client.Auth().Bot(ctx, b.cfg.BotToken); err != nil {
						return &fatalError{err: fmt.Errorf("main bot login failed: %w", err)}
					}
				}
				b.runCtx.Store(&ctx)
				b.resolveLogChannel(ctx)

				// Ask for the update state once so Telegram starts pushing
				// updates to this session right away.
				if _, err := b.client.API().UpdatesGetState(ctx); err != nil {
					b.log.Warn("updates.getState failed", zap.Error(err))
				}

				username := ""
				if self, err := b.client.Self(ctx); err == nil {
					username = self.Username
				}
				b.log.Info("main bot authenticated successfully", zap.String("username", "@"+username))
				backoff = time.Second
				signalReady()

				<-ctx.Done()
				return ctx.Err()
			})
		})

		if ctx.Err() != nil {
			return ctx.Err()
		}
		var fe *fatalError
		if errors.As(err, &fe) {
			signalReady()
			return fe.err
		}

		b.log.Warn("main bot stopped, restarting", zap.Error(err), zap.Duration("in", backoff))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

type fatalError struct{ err error }

func (e *fatalError) Error() string { return e.err.Error() }
func (e *fatalError) Unwrap() error { return e.err }

// resolveLogChannel learns this bot's access hash for the log channel; hashes
// are per-account, so it can't be shared with the workers.
func (b *Bot) resolveLogChannel(ctx context.Context) {
	id := b.logChannelID.Load()
	res, err := b.client.API().ChannelsGetChannels(ctx, []tg.InputChannelClass{
		&tg.InputChannel{ChannelID: id},
	})
	if err != nil {
		b.log.Warn("could not resolve log channel access hash (is the main bot an admin there?)", zap.Error(err))
		return
	}
	for _, chat := range res.GetChats() {
		if c, ok := chat.(*tg.Channel); ok && c.ID == id {
			b.logChannelHash.Store(c.AccessHash)
			b.log.Info("log channel resolved", zap.Int64("channel_id", id))
			return
		}
	}
	b.log.Warn("log channel not returned for the main bot; check it is a channel admin", zap.Int64("channel_id", id))
}

func (b *Bot) logChannel() *tg.InputChannel {
	return &tg.InputChannel{ChannelID: b.logChannelID.Load(), AccessHash: b.logChannelHash.Load()}
}

func (b *Bot) logPeer() *tg.InputPeerChannel {
	return &tg.InputPeerChannel{ChannelID: b.logChannelID.Load(), AccessHash: b.logChannelHash.Load()}
}

// ---- update handling -------------------------------------------------------

func (b *Bot) handleNewMessage(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
	msg, ok := u.Message.(*tg.Message)
	if !ok || msg.Out {
		return nil // Ignore outgoing or non-standard messages
	}

	senderID := b.extractSenderID(msg)
	if senderID == 0 {
		return nil
	}

	// Process off the update loop so a slow ingest never stalls other updates.
	b.async(func(ctx context.Context) {
		if err := b.dispatch(ctx, e, msg, senderID); err != nil {
			b.log.Warn("message handling failed", zap.Int64("sender", senderID), zap.Error(err))
		}
	})
	return nil
}

func (b *Bot) async(fn func(ctx context.Context)) {
	parent := context.Background()
	if p := b.runCtx.Load(); p != nil {
		parent = *p
	}
	go func() {
		b.sem <- struct{}{}
		defer func() { <-b.sem }()
		ctx, cancel := context.WithTimeout(parent, handlerTimeout)
		defer cancel()
		fn(ctx)
	}()
}

func (b *Bot) dispatch(ctx context.Context, e tg.Entities, msg *tg.Message, senderID int64) error {
	text := strings.TrimSpace(msg.Message)
	cmd, args := parseCommand(text)

	if hasDownloadableMedia(msg.Media) {
		// A caption of "/link 6h" sets the TTL for this very file.
		ttl := b.cfg.DefaultLinkTTL
		if cmd == "/link" {
			if len(args) == 0 {
				return b.reply(ctx, e, msg, "Usage: send a file with the caption <code>/link 6h</code>")
			}
			parsed, err := config.ParseTTL(args[0])
			if err != nil {
				return b.reply(ctx, e, msg, "❌ "+stdhtml.EscapeString(err.Error()))
			}
			ttl = parsed
		} else if p, ok := b.takePending(senderID); ok {
			ttl = p
		}
		return b.ingest(ctx, e, msg, msg.Media, senderID, ttl)
	}

	if cmd != "" {
		return b.handleCommand(ctx, e, msg, senderID, cmd, args)
	}
	return nil
}

func (b *Bot) handleCommand(ctx context.Context, e tg.Entities, msg *tg.Message, senderID int64, cmd string, args []string) error {
	switch cmd {
	case "/start":
		return b.reply(ctx, e, msg, "👋 <b>Welcome to mtflux!</b>\n\nSend or forward any file or video to get a fast direct download and streaming link, served in parallel across many connections.\n\nUse /help for commands.")

	case "/help":
		helpText := "📖 <b>mtflux commands</b>\n\n" +
			"• Send any file: get a direct download link\n" +
			"• <code>/link 6h</code> as a caption, or as a reply to a file: custom link lifetime (<code>30m</code>, <code>6h</code>, <code>7d</code>, <code>2w</code>)\n" +
			"• <code>/revoke &lt;link&gt;</code>: revoke and delete a link immediately\n"
		if b.access.IsAdmin(senderID) {
			helpText += "\n👮 <b>Admin</b>\n" +
				"• /stats: live worker and download metrics\n" +
				"• <code>/allow &lt;user_id&gt;</code> / <code>/disallow &lt;user_id&gt;</code>\n" +
				"• <code>/ban &lt;user_id&gt;</code> / <code>/unban &lt;user_id&gt;</code>: banning also kills all of that user's links\n"
		}
		return b.reply(ctx, e, msg, helpText)

	case "/stats":
		if !b.access.IsAdmin(senderID) {
			return b.reply(ctx, e, msg, "⛔ Access denied.")
		}
		return b.reply(ctx, e, msg, b.metrics.FormatStats(b.access.ActiveStreamsCount()))

	case "/allow":
		return b.handleAdminACL(ctx, e, msg, senderID, cmd, args, b.access.Allow, "allowed")
	case "/disallow":
		return b.handleAdminACL(ctx, e, msg, senderID, cmd, args, b.access.Disallow, "disallowed")
	case "/ban":
		return b.handleAdminACL(ctx, e, msg, senderID, cmd, args, b.access.Ban, "banned")
	case "/unban":
		return b.handleAdminACL(ctx, e, msg, senderID, cmd, args, b.access.Unban, "unbanned")

	case "/revoke":
		return b.handleRevoke(ctx, e, msg, senderID, args)

	case "/link":
		return b.handleLinkCommand(ctx, e, msg, senderID, args)
	}
	return nil
}

// handleLinkCommand implements "/link <ttl>" sent as a reply to a file, or on
// its own (applies to the next file the user sends within a couple of minutes).
func (b *Bot) handleLinkCommand(ctx context.Context, e tg.Entities, msg *tg.Message, senderID int64, args []string) error {
	if len(args) == 0 {
		return b.reply(ctx, e, msg, "Usage: <code>/link 6h</code> (reply to a file, or send it before the file). Units: <code>m h d w</code>")
	}
	ttl, err := config.ParseTTL(args[0])
	if err != nil {
		return b.reply(ctx, e, msg, "❌ "+stdhtml.EscapeString(err.Error()))
	}

	if hdr, ok := msg.ReplyTo.(*tg.MessageReplyHeader); ok && hdr.ReplyToMsgID != 0 {
		target, err := b.fetchReplied(ctx, e, msg, hdr.ReplyToMsgID)
		if err != nil || target == nil || !hasDownloadableMedia(target.Media) {
			return b.reply(ctx, e, msg, "❌ The message you replied to has no file.")
		}
		return b.ingest(ctx, e, msg, target.Media, senderID, ttl)
	}

	b.setPending(senderID, ttl)
	return b.reply(ctx, e, msg, fmt.Sprintf("⏱ OK. The next file you send within %d minutes gets a link valid for <code>%s</code>.",
		int(pendingTTLWindow.Minutes()), stdhtml.EscapeString(args[0])))
}

func (b *Bot) setPending(userID int64, ttl time.Duration) {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	now := time.Now()
	for id, p := range b.pending {
		if now.After(p.until) {
			delete(b.pending, id)
		}
	}
	b.pending[userID] = pendingTTL{ttl: ttl, until: now.Add(pendingTTLWindow)}
}

func (b *Bot) takePending(userID int64) (time.Duration, bool) {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	p, ok := b.pending[userID]
	if !ok {
		return 0, false
	}
	delete(b.pending, userID)
	if time.Now().After(p.until) {
		return 0, false
	}
	return p.ttl, true
}

func (b *Bot) fetchReplied(ctx context.Context, e tg.Entities, msg *tg.Message, id int) (*tg.Message, error) {
	ids := []tg.InputMessageClass{&tg.InputMessageID{ID: id}}

	var res tg.MessagesMessagesClass
	var err error
	if p, ok := msg.PeerID.(*tg.PeerChannel); ok {
		ch, found := e.Channels[p.ChannelID]
		if !found {
			return nil, errors.New("channel not in update entities")
		}
		res, err = b.client.API().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
			Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash},
			ID:      ids,
		})
	} else {
		res, err = b.client.API().MessagesGetMessages(ctx, ids)
	}
	if err != nil {
		return nil, err
	}
	for _, m := range gotgtg.MessagesFrom(res) {
		if mm, ok := m.(*tg.Message); ok {
			return mm, nil
		}
	}
	return nil, nil
}

func (b *Bot) handleAdminACL(ctx context.Context, e tg.Entities, msg *tg.Message, senderID int64, cmd string, args []string, fn func(int64) error, action string) error {
	if !b.access.IsAdmin(senderID) {
		return b.reply(ctx, e, msg, "⛔ Admin access required.")
	}
	if len(args) < 1 {
		return b.reply(ctx, e, msg, fmt.Sprintf("Usage: <code>%s &lt;user_id&gt;</code>", cmd))
	}
	targetID, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return b.reply(ctx, e, msg, "Invalid user ID.")
	}
	if cmd == "/ban" && (b.access.IsOwner(targetID) || targetID == senderID) {
		return b.reply(ctx, e, msg, "⛔ You can't ban the owner or yourself.")
	}
	if err := fn(targetID); err != nil {
		return b.reply(ctx, e, msg, "Error: "+stdhtml.EscapeString(err.Error()))
	}
	return b.reply(ctx, e, msg, fmt.Sprintf("✅ User <code>%d</code> has been %s.", targetID, action))
}

func (b *Bot) handleRevoke(ctx context.Context, e tg.Entities, msg *tg.Message, senderID int64, args []string) error {
	var tokenStr string
	if len(args) >= 1 {
		tokenStr = extractTokenFromText(args[0])
	}
	if tokenStr == "" {
		return b.reply(ctx, e, msg, "Usage: <code>/revoke &lt;link&gt;</code>")
	}

	tok, err := link.Parse([]byte(b.cfg.LinkSecret), tokenStr)
	if tok == nil || (err != nil && !errors.Is(err, link.ErrTokenExpired)) {
		return b.reply(ctx, e, msg, "❌ Invalid link.")
	}

	// Only the owner of the link or an admin can revoke
	if tok.OwnerID != senderID && !b.access.IsAdmin(senderID) {
		return b.reply(ctx, e, msg, "⛔ You are not authorized to revoke this link.")
	}

	// The log channel message *is* the record: deleting it is the revocation.
	_, err = b.client.API().ChannelsDeleteMessages(ctx, &tg.ChannelsDeleteMessagesRequest{
		Channel: b.logChannel(),
		ID:      []int{tok.MsgID},
	})
	if err != nil && !tgerr.Is(err, "MESSAGE_ID_INVALID") {
		// Don't claim success: the link would come back once the negative cache expires.
		b.log.Error("failed to delete message from log channel", zap.Int("msg_id", tok.MsgID), zap.Error(err))
		return b.reply(ctx, e, msg, "❌ Could not revoke the link right now (the log channel refused the delete). Please try again.")
	}

	if docID, ok := b.resolver.Invalidate(tok.MsgID); ok {
		b.scheduler.EvictDoc(docID)
	}
	return b.reply(ctx, e, msg, "🗑️ <b>Link revoked.</b> The file and its link have been removed.")
}

// ---- ingest ----------------------------------------------------------------

func (b *Bot) ingest(ctx context.Context, e tg.Entities, cmdMsg *tg.Message, media tg.MessageMediaClass, senderID int64, ttl time.Duration) error {
	if !b.access.CanGenerateLink(senderID) {
		return b.reply(ctx, e, cmdMsg, "⛔ You do not have permission to generate download links.")
	}
	if !b.access.CheckRateLimit(senderID) {
		return b.reply(ctx, e, cmdMsg, "⚠️ Rate limit reached. Please wait a few seconds before sending more files.")
	}

	var (
		inputMedia tg.InputMediaClass
		filename   string
		fileSize   int64
	)

	switch m := media.(type) {
	case *tg.MessageMediaDocument:
		doc, ok := m.Document.AsNotEmpty()
		if !ok {
			return b.reply(ctx, e, cmdMsg, "❌ Unsupported document format.")
		}
		inputMedia = &tg.InputMediaDocument{
			ID: &tg.InputDocument{ID: doc.ID, AccessHash: doc.AccessHash, FileReference: doc.FileReference},
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
		photo, ok := m.Photo.AsNotEmpty()
		if !ok {
			return b.reply(ctx, e, cmdMsg, "❌ Unsupported photo format.")
		}
		inputMedia = &tg.InputMediaPhoto{
			ID: &tg.InputPhoto{ID: photo.ID, AccessHash: photo.AccessHash, FileReference: photo.FileReference},
		}
		filename = fmt.Sprintf("photo_%d.jpg", photo.ID)

	default:
		return b.reply(ctx, e, cmdMsg, "❌ Please send a document, audio, video, or photo.")
	}

	// Copy (not forward) into the log channel, tagged with the owner.
	randID, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	updates, err := b.client.API().MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
		Peer:     b.logPeer(),
		Media:    inputMedia,
		Message:  fmt.Sprintf("#u%d", senderID),
		RandomID: randID.Int64(),
	})
	if err != nil {
		b.log.Error("failed to copy media to log channel", zap.Error(err))
		return b.reply(ctx, e, cmdMsg, "❌ Failed to store the file. Please make sure the bot is an admin of the log channel.")
	}

	logMsgID := extractMsgIDFromUpdates(updates)
	if logMsgID == 0 {
		return b.reply(ctx, e, cmdMsg, "❌ Failed to retrieve the stored message ID from the log channel.")
	}

	var expiry time.Time
	if ttl > 0 {
		expiry = time.Now().Add(ttl)
	}

	token := link.Generate([]byte(b.cfg.LinkSecret), logMsgID, senderID, expiry)
	b.log.Info("link created",
		zap.Int("log_msg_id", logMsgID), zap.Int64("owner", senderID),
		zap.String("file", filename), zap.Int64("size", fileSize), zap.Duration("ttl", ttl))
	dlURL := fmt.Sprintf("%s/dl/%s/%s", b.cfg.PublicURL, token, url.PathEscape(filename))
	streamURL := dlURL + "?inline=1"

	var sb strings.Builder
	sb.WriteString("⚡ <b>Download link ready</b>\n\n")
	fmt.Fprintf(&sb, "📁 <b>Name:</b> <code>%s</code>\n", stdhtml.EscapeString(filename))
	if fileSize > 0 {
		fmt.Fprintf(&sb, "📦 <b>Size:</b> <code>%s</code>\n", humanSize(fileSize))
	}
	if !expiry.IsZero() {
		fmt.Fprintf(&sb, "⏱ <b>Expires:</b> <code>%s</code>\n", expiry.UTC().Format("2006-01-02 15:04 UTC"))
	} else {
		sb.WriteString("⏱ <b>Expires:</b> <code>never</code>\n")
	}
	sb.WriteString("\n")
	fmt.Fprintf(&sb, "📥 <a href=\"%s\">Download</a>\n", stdhtml.EscapeString(dlURL))
	fmt.Fprintf(&sb, "▶️ <a href=\"%s\">Stream</a>\n\n", stdhtml.EscapeString(streamURL))
	fmt.Fprintf(&sb, "<code>%s</code>", stdhtml.EscapeString(dlURL))

	return b.reply(ctx, e, cmdMsg, sb.String())
}

// ---- helpers ---------------------------------------------------------------

// reply sends an HTML-formatted reply (no link preview).
func (b *Bot) reply(ctx context.Context, e tg.Entities, msg *tg.Message, htmlText string) error {
	_, err := b.sender.To(b.inputPeer(e, msg)).Reply(msg.ID).NoWebpage().StyledText(ctx, html.String(nil, htmlText))
	return err
}

// inputPeer addresses the chat of msg with the access hash Telegram gave us in
// the update, falling back to 0 only when the entity is missing.
func (b *Bot) inputPeer(e tg.Entities, msg *tg.Message) tg.InputPeerClass {
	switch p := msg.PeerID.(type) {
	case *tg.PeerUser:
		var hash int64
		if u, ok := e.Users[p.UserID]; ok {
			hash = u.AccessHash
		}
		return &tg.InputPeerUser{UserID: p.UserID, AccessHash: hash}
	case *tg.PeerChannel:
		var hash int64
		if c, ok := e.Channels[p.ChannelID]; ok {
			hash = c.AccessHash
		}
		return &tg.InputPeerChannel{ChannelID: p.ChannelID, AccessHash: hash}
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

func hasDownloadableMedia(m tg.MessageMediaClass) bool {
	switch m.(type) {
	case *tg.MessageMediaDocument, *tg.MessageMediaPhoto:
		return true
	}
	return false
}

// parseCommand splits "/cmd@botname arg1 arg2" into ("/cmd", [arg1 arg2]).
func parseCommand(text string) (string, []string) {
	if !strings.HasPrefix(text, "/") {
		return "", nil
	}
	parts := strings.Fields(text)
	cmd := strings.ToLower(parts[0])
	if idx := strings.IndexByte(cmd, '@'); idx != -1 {
		cmd = cmd[:idx]
	}
	return cmd, parts[1:]
}

func humanSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
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
			if q := strings.IndexByte(tokenPart, '?'); q != -1 {
				return tokenPart[:q]
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
