package resolver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"golang.org/x/sync/singleflight"
)

// MessageFetcher retrieves a raw Telegram message from the log channel.
type MessageFetcher interface {
	GetMessage(ctx context.Context, msgID int) (*tg.Message, error)
}

// Resolver resolves message IDs from the log channel into downloadable FileMeta.
type Resolver struct {
	fetcher MessageFetcher
	sf      singleflight.Group

	mu            sync.RWMutex
	cache         map[int]*FileMeta
	negativeCache map[int]time.Time
	negativeTTL   time.Duration
}

// New creates a Resolver instance.
func New(fetcher MessageFetcher) *Resolver {
	return &Resolver{
		fetcher:       fetcher,
		cache:         make(map[int]*FileMeta),
		negativeCache: make(map[int]time.Time),
		negativeTTL:   60 * time.Second,
	}
}

// Resolve looks up the file metadata for the given msgID, using caching and singleflight.
func (r *Resolver) Resolve(ctx context.Context, msgID int) (*FileMeta, error) {
	// 1. Check negative cache
	r.mu.RLock()
	if exp, ok := r.negativeCache[msgID]; ok {
		if time.Now().Before(exp) {
			r.mu.RUnlock()
			return nil, ErrNotFound
		}
	}

	// 2. Check LRU/memory cache
	if meta, ok := r.cache[msgID]; ok {
		r.mu.RUnlock()
		return meta, nil
	}
	r.mu.RUnlock()

	// 3. Singleflight fetch
	key := fmt.Sprintf("msg:%d", msgID)
	val, err, _ := r.sf.Do(key, func() (interface{}, error) {
		// Double check cache under lock
		r.mu.RLock()
		if meta, ok := r.cache[msgID]; ok {
			r.mu.RUnlock()
			return meta, nil
		}
		r.mu.RUnlock()

		msg, err := r.fetcher.GetMessage(ctx, msgID)
		if err != nil {
			r.mu.Lock()
			r.negativeCache[msgID] = time.Now().Add(r.negativeTTL)
			r.mu.Unlock()
			return nil, err
		}

		if msg == nil {
			r.mu.Lock()
			r.negativeCache[msgID] = time.Now().Add(r.negativeTTL)
			r.mu.Unlock()
			return nil, ErrNotFound
		}

		meta, err := ExtractFileMeta(msgID, msg)
		if err != nil {
			return nil, err
		}

		r.mu.Lock()
		r.cache[msgID] = meta
		delete(r.negativeCache, msgID)
		r.mu.Unlock()

		return meta, nil
	})

	if err != nil {
		return nil, err
	}
	return val.(*FileMeta), nil
}

// Evict removes a message from both cache and negative cache.
func (r *Resolver) Evict(msgID int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.cache, msgID)
	delete(r.negativeCache, msgID)
}

// Refresh invalidates existing cached location and forces re-fetching the message from Telegram.
// Useful when Telegram responds with FILE_REFERENCE_EXPIRED.
func (r *Resolver) Refresh(ctx context.Context, msgID int) (*FileMeta, error) {
	r.Evict(msgID)
	return r.Resolve(ctx, msgID)
}

// Invalidate removes a message from cache and marks it in the negative cache.
func (r *Resolver) Invalidate(msgID int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.cache, msgID)
	r.negativeCache[msgID] = time.Now().Add(r.negativeTTL)
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
