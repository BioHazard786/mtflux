package resolver

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"golang.org/x/sync/singleflight"
)

const (
	defaultCacheTTL     = 10 * time.Minute
	defaultNegativeTTL  = 30 * time.Second
	defaultMaxEntries   = 8192
	maxNegativeEntries  = 4096
	resolveFetchTimeout = 20 * time.Second
)

// MessageFetcher retrieves a raw Telegram message from the log channel.
// It must return ErrNotFound only when the message definitively doesn't exist
// (deleted/empty); transient failures must be returned as other errors.
type MessageFetcher interface {
	GetMessage(ctx context.Context, msgID int) (*tg.Message, error)
}

type cacheEntry struct {
	msgID   int
	meta    *FileMeta
	expires time.Time
}

// Resolver resolves message IDs from the log channel into downloadable FileMeta.
// Entries expire after a TTL (so out-of-band deletions eventually take effect)
// and the cache is bounded.
type Resolver struct {
	fetcher MessageFetcher
	sf      singleflight.Group

	mu            sync.Mutex
	cache         map[int]*list.Element
	lru           *list.List
	negativeCache map[int]time.Time

	ttl         time.Duration
	negativeTTL time.Duration
	maxEntries  int
}

// New creates a Resolver instance.
func New(fetcher MessageFetcher) *Resolver {
	return &Resolver{
		fetcher:       fetcher,
		cache:         make(map[int]*list.Element),
		lru:           list.New(),
		negativeCache: make(map[int]time.Time),
		ttl:           defaultCacheTTL,
		negativeTTL:   defaultNegativeTTL,
		maxEntries:    defaultMaxEntries,
	}
}

// lookupLocked returns a cached result: (meta, nil) on hit, (nil, ErrNotFound)
// on negative hit, (nil, nil) on miss.
func (r *Resolver) lookupLocked(msgID int, now time.Time) (*FileMeta, error) {
	if exp, ok := r.negativeCache[msgID]; ok {
		if now.Before(exp) {
			return nil, ErrNotFound
		}
		delete(r.negativeCache, msgID)
	}
	if elem, ok := r.cache[msgID]; ok {
		e := elem.Value.(*cacheEntry)
		if now.Before(e.expires) {
			r.lru.MoveToFront(elem)
			return e.meta, nil
		}
		r.removeLocked(elem)
	}
	return nil, nil
}

func (r *Resolver) removeLocked(elem *list.Element) {
	e := elem.Value.(*cacheEntry)
	delete(r.cache, e.msgID)
	r.lru.Remove(elem)
}

func (r *Resolver) storeLocked(msgID int, meta *FileMeta) {
	if elem, ok := r.cache[msgID]; ok {
		r.removeLocked(elem)
	}
	r.cache[msgID] = r.lru.PushFront(&cacheEntry{msgID: msgID, meta: meta, expires: time.Now().Add(r.ttl)})
	delete(r.negativeCache, msgID)
	for r.lru.Len() > r.maxEntries {
		r.removeLocked(r.lru.Back())
	}
}

func (r *Resolver) markNegativeLocked(msgID int) {
	if len(r.negativeCache) >= maxNegativeEntries {
		now := time.Now()
		for id, exp := range r.negativeCache {
			if !now.Before(exp) {
				delete(r.negativeCache, id)
			}
		}
		if len(r.negativeCache) >= maxNegativeEntries {
			return // under a flood of bogus IDs, stop remembering rather than grow
		}
	}
	r.negativeCache[msgID] = time.Now().Add(r.negativeTTL)
}

// Resolve looks up the file metadata for the given msgID, using caching and singleflight.
// The shared lookup runs on a detached context, so a caller going away neither
// fails other waiters nor gets recorded as "file missing".
func (r *Resolver) Resolve(ctx context.Context, msgID int) (*FileMeta, error) {
	r.mu.Lock()
	meta, err := r.lookupLocked(msgID, time.Now())
	r.mu.Unlock()
	if meta != nil || err != nil {
		return meta, err
	}

	ch := r.sf.DoChan(fmt.Sprintf("msg:%d", msgID), func() (interface{}, error) {
		fctx, cancel := context.WithTimeout(context.Background(), resolveFetchTimeout)
		defer cancel()
		return r.load(fctx, msgID)
	})

	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*FileMeta), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *Resolver) load(ctx context.Context, msgID int) (*FileMeta, error) {
	r.mu.Lock()
	if meta, _ := r.lookupLocked(msgID, time.Now()); meta != nil {
		r.mu.Unlock()
		return meta, nil
	}
	r.mu.Unlock()

	msg, err := r.fetcher.GetMessage(ctx, msgID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			r.mu.Lock()
			r.markNegativeLocked(msgID)
			r.mu.Unlock()
		}
		// Anything else (network, flood, cancellation) is transient: don't cache it.
		return nil, err
	}
	if msg == nil {
		r.mu.Lock()
		r.markNegativeLocked(msgID)
		r.mu.Unlock()
		return nil, ErrNotFound
	}

	meta, err := ExtractFileMeta(msgID, msg)
	if err != nil {
		if errors.Is(err, ErrUnsupportedMedia) {
			r.mu.Lock()
			r.markNegativeLocked(msgID)
			r.mu.Unlock()
		}
		return nil, err
	}

	r.mu.Lock()
	r.storeLocked(msgID, meta)
	r.mu.Unlock()
	return meta, nil
}

// Evict removes a message from both cache and negative cache.
func (r *Resolver) Evict(msgID int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if elem, ok := r.cache[msgID]; ok {
		r.removeLocked(elem)
	}
	delete(r.negativeCache, msgID)
}

// Refresh invalidates the existing cached location and forces re-fetching the message from Telegram.
func (r *Resolver) Refresh(ctx context.Context, msgID int) (*FileMeta, error) {
	r.Evict(msgID)
	return r.Resolve(ctx, msgID)
}

// RefreshIfStale re-fetches the message only if the cached file reference is
// still the one that Telegram rejected. When many chunks fail with
// FILE_REFERENCE_EXPIRED at once, only the first triggers a network lookup.
func (r *Resolver) RefreshIfStale(ctx context.Context, msgID int, staleRef []byte) (*FileMeta, error) {
	r.mu.Lock()
	if elem, ok := r.cache[msgID]; ok {
		e := elem.Value.(*cacheEntry)
		if !bytes.Equal(e.meta.FileReference, staleRef) {
			meta := e.meta
			r.mu.Unlock()
			return meta, nil
		}
		r.removeLocked(elem)
	}
	r.mu.Unlock()
	return r.Resolve(ctx, msgID)
}

// SetDC records a corrected datacenter for an already-resolved message.
func (r *Resolver) SetDC(msgID, dc int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if elem, ok := r.cache[msgID]; ok {
		e := elem.Value.(*cacheEntry)
		updated := *e.meta
		updated.DC = dc
		e.meta = &updated
	}
}

// Invalidate removes a message from cache and marks it as missing. It returns
// the document ID that was cached, if any, so callers can drop its chunks.
func (r *Resolver) Invalidate(msgID int) (docID int64, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if elem, found := r.cache[msgID]; found {
		docID, ok = elem.Value.(*cacheEntry).meta.DocID, true
		r.removeLocked(elem)
	}
	r.markNegativeLocked(msgID)
	return docID, ok
}

// ExtractFileMeta inspects a tg.Message and extracts FileMeta for Document or Photo media.
func ExtractFileMeta(msgID int, msg *tg.Message) (*FileMeta, error) {
	if msg == nil || msg.Media == nil {
		return nil, ErrUnsupportedMedia
	}

	switch media := msg.Media.(type) {
	case *tg.MessageMediaDocument:
		doc, ok := media.Document.AsNotEmpty()
		if !ok {
			return nil, ErrUnsupportedMedia
		}

		var filename string
		for _, attr := range doc.Attributes {
			if f, ok := attr.(*tg.DocumentAttributeFilename); ok {
				filename = f.FileName
				break
			}
		}
		if filename == "" {
			filename = fmt.Sprintf("file_%d", doc.ID)
		}

		mime := doc.MimeType
		if mime == "" {
			mime = "application/octet-stream"
		}

		return &FileMeta{
			MsgID:         msgID,
			DocID:         doc.ID,
			AccessHash:    doc.AccessHash,
			FileReference: doc.FileReference,
			DC:            doc.DCID,
			Size:          doc.Size,
			Filename:      filename,
			MimeType:      mime,
			Date:          time.Unix(int64(doc.Date), 0),
			Location: &tg.InputDocumentFileLocation{
				ID:            doc.ID,
				AccessHash:    doc.AccessHash,
				FileReference: doc.FileReference,
			},
		}, nil

	case *tg.MessageMediaPhoto:
		photo, ok := media.Photo.AsNotEmpty()
		if !ok {
			return nil, ErrUnsupportedMedia
		}

		var largestSize int64
		var thumbType string
		for _, sz := range photo.Sizes {
			switch s := sz.(type) {
			case *tg.PhotoSize:
				if int64(s.Size) > largestSize {
					largestSize = int64(s.Size)
					thumbType = s.Type
				}
			case *tg.PhotoSizeProgressive:
				for _, w := range s.Sizes {
					if int64(w) > largestSize {
						largestSize = int64(w)
						thumbType = s.Type
					}
				}
			}
		}

		return &FileMeta{
			MsgID:         msgID,
			DocID:         photo.ID,
			AccessHash:    photo.AccessHash,
			FileReference: photo.FileReference,
			DC:            photo.DCID,
			Size:          largestSize,
			Filename:      fmt.Sprintf("photo_%d.jpg", photo.ID),
			MimeType:      "image/jpeg",
			Date:          time.Unix(int64(photo.Date), 0),
			Location: &tg.InputPhotoFileLocation{
				ID:            photo.ID,
				AccessHash:    photo.AccessHash,
				FileReference: photo.FileReference,
				ThumbSize:     thumbType,
			},
		}, nil

	default:
		return nil, ErrUnsupportedMedia
	}
}
