package resolver

import (
	"errors"
	"time"

	"github.com/gotd/td/tg"
)

var (
	ErrNotFound         = errors.New("file not found or message was deleted")
	ErrUnsupportedMedia = errors.New("message does not contain downloadable document or photo media")
)

// FileMeta represents metadata and location of a downloadable file stored in Telegram.
type FileMeta struct {
	MsgID         int
	DocID         int64
	AccessHash    int64
	FileReference []byte
	DC            int
	Size          int64
	Filename      string
	MimeType      string
	Date          time.Time
	Location      tg.InputFileLocationClass
}
