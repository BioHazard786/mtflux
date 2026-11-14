package fetch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"github.com/gotd/td/tg"
	"golang.org/x/sync/singleflight"
)

const (
	// File references last hours; refetch well before and on any rejection.
	locationTTL        = 30 * time.Minute
	maxLocations       = 50000
	locationFetchLimit = 20 * time.Second
)

// Warmer is optionally implemented by a ChunkSource that can prepare a worker
// (connections, file location) ahead of the first chunk request.
type Warmer interface {
	Warm(ctx context.Context, worker *gotgtg.Worker, meta *resolver.FileMeta) error
}

// LocationInvalidator is implemented by sources that keep per-worker file
// locations; the scheduler calls it when Telegram rejects a worker's file reference.
type LocationInvalidator interface {
	InvalidateLocation(worker *gotgtg.Worker, meta *resolver.FileMeta)
}

type locKey struct {
	worker string
	msgID  int
}

type locEntry struct {
	loc     tg.InputFileLocationClass
	expires time.Time
}

// TelegramChunkSource issues upload.getFile RPCs via gotd.
//
// Telegram file references are bound to the account that fetched the
// message, so every worker resolves the log-channel message itself and keeps
// its own location; sharing one bot's reference makes the others fail with
// FILE_REFERENCE_EXPIRED.
type TelegramChunkSource struct {
	channelID int64

	mu   sync.Mutex
	locs map[locKey]locEntry
	sf   singleflight.Group
}

// NewTelegramChunkSource creates a source that resolves per-worker file
// locations from the given log channel.
func NewTelegramChunkSource(logChannelID int64) *TelegramChunkSource {
	return &TelegramChunkSource{channelID: logChannelID, locs: make(map[locKey]locEntry)}
}

func (s *TelegramChunkSource) FetchFromWorker(ctx context.Context, worker *gotgtg.Worker, meta *resolver.FileMeta, chunkIdx int, offset, limit int64) ([]byte, error) {
	loc, err := s.location(ctx, worker, meta)
	if err != nil {
		return nil, err
	}

	pool, err := worker.GetPool(ctx, meta.DC)
	if err != nil {
		return nil, err
	}

	res, err := tg.NewClient(pool.Invoker).UploadGetFile(ctx, &tg.UploadGetFileRequest{
		Location: loc,
		Offset:   offset,
		Limit:    int(limit),
	})
	if err != nil {
		return nil, err
	}

	switch f := res.(type) {
	case *tg.UploadFile:
		return f.Bytes, nil
	default:
		return nil, fmt.Errorf("unexpected file response type: %T", res)
	}
}

// Warm opens the worker's pool for the file's DC and resolves its own file location.
func (s *TelegramChunkSource) Warm(ctx context.Context, worker *gotgtg.Worker, meta *resolver.FileMeta) error {
	if _, err := s.location(ctx, worker, meta); err != nil {
		return err
	}
	_, err := worker.GetPool(ctx, meta.DC)
	return err
}

// InvalidateLocation drops a worker's cached location so the next attempt refetches it.
func (s *TelegramChunkSource) InvalidateLocation(worker *gotgtg.Worker, meta *resolver.FileMeta) {
	s.mu.Lock()
	delete(s.locs, locKey{worker: worker.ID(), msgID: meta.MsgID})
	s.mu.Unlock()
}

func (s *TelegramChunkSource) location(ctx context.Context, w *gotgtg.Worker, meta *resolver.FileMeta) (tg.InputFileLocationClass, error) {
	if s.channelID == 0 || meta.MsgID == 0 {
		return meta.Location, nil // no log channel to resolve from (tests, tools)
	}

	key := locKey{worker: w.ID(), msgID: meta.MsgID}
	s.mu.Lock()
	if e, ok := s.locs[key]; ok && time.Now().Before(e.expires) {
		s.mu.Unlock()
		return e.loc, nil
	}
	s.mu.Unlock()

	ch := s.sf.DoChan(fmt.Sprintf("%s:%d", key.worker, key.msgID), func() (interface{}, error) {
		fctx, cancel := context.WithTimeout(context.Background(), locationFetchLimit)
		defer cancel()

		res, err := w.Client().API().ChannelsGetMessages(fctx, &tg.ChannelsGetMessagesRequest{
			Channel: w.LogChannel(s.channelID),
			ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: meta.MsgID}},
		})
		if err != nil {
			return nil, err
		}
		for _, m := range gotgtg.MessagesFrom(res) {
			msg, ok := m.(*tg.Message)
			if !ok {
				continue
			}
			fm, err := resolver.ExtractFileMeta(meta.MsgID, msg)
			if err != nil {
				return nil, err
			}
			if fm.DocID != meta.DocID {
				return nil, fmt.Errorf("log message %d now holds a different file: %w", meta.MsgID, resolver.ErrNotFound)
			}
			s.store(key, fm.Location)
			return fm.Location, nil
		}
		return nil, fmt.Errorf("log message %d: %w", meta.MsgID, resolver.ErrNotFound)
	})

	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.(tg.InputFileLocationClass), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *TelegramChunkSource) store(key locKey, loc tg.InputFileLocationClass) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.locs) >= maxLocations {
		now := time.Now()
		for k, e := range s.locs {
			if now.After(e.expires) {
				delete(s.locs, k)
			}
		}
		if len(s.locs) >= maxLocations {
			s.locs = make(map[locKey]locEntry) // pathological: start over rather than grow
		}
	}
	s.locs[key] = locEntry{loc: loc, expires: time.Now().Add(locationTTL)}
}
