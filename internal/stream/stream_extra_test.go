package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/biohazard786/mtflux/internal/fetch"
	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

const testChunk = 1 << 20

type shortAt struct {
	payload []byte
	short   int // chunk index that comes back 100 bytes short
}

func (s *shortAt) FetchFromWorker(ctx context.Context, w *gotgtg.Worker, m *resolver.FileMeta, i int, off, lim int64) ([]byte, error) {
	end := off + lim
	if end > int64(len(s.payload)) {
		end = int64(len(s.payload))
	}
	if off >= end {
		return []byte{}, nil
	}
	d := append([]byte(nil), s.payload[off:end]...)
	if i == s.short {
		d = d[:len(d)-100]
	}
	return d, nil
}

func newTestReader(t *testing.T, src fetch.ChunkSource, size int64, known bool) (*Reader, []byte) {
	t.Helper()
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	if s, ok := src.(*shortAt); ok {
		s.payload = payload
	}
	w, _ := gotgtg.NewWorker(1, "h", "1:t", t.TempDir(), 4, zap.NewNop())
	sched := fetch.NewScheduler([]*gotgtg.Worker{w}, testChunk, 0, 0, nil, nil, zap.NewNop())
	sched.SetSource(src)
	meta := &resolver.FileMeta{DocID: 1, DC: 4}
	if known {
		meta.Size = size
	}
	return NewReader(context.Background(), meta, sched, nil, ByteRange{0, size - 1}, testChunk, 4, 16), payload
}

// A short middle chunk used to shift every following byte and still end in a clean EOF.
func TestReader_ShortMiddleChunkIsAnErrorNotCorruption(t *testing.T) {
	r, _ := newTestReader(t, &shortAt{short: 1}, 3*testChunk, true)
	defer r.Close()

	_, err := io.ReadAll(r)
	if err == nil {
		t.Fatal("expected an error for a short chunk, got a silently truncated/corrupted stream")
	}
}

// Even if the size isn't known to the scheduler, running out of chunks while
// bytes are still owed must not look like success.
func TestReader_PrematureEndIsUnexpectedEOF(t *testing.T) {
	r, _ := newTestReader(t, &shortAt{short: 2}, 3*testChunk, false)
	defer r.Close()

	_, err := io.ReadAll(r)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestReader_WriteToMatchesPayload(t *testing.T) {
	r, payload := newTestReader(t, &shortAt{short: -1}, 5*testChunk+1234, true)
	defer r.Close()

	if err := r.WaitFirst(); err != nil {
		t.Fatalf("WaitFirst: %v", err)
	}
	var out bytes.Buffer
	n, err := r.WriteTo(&out)
	if err != nil || n != int64(len(payload)) {
		t.Fatalf("WriteTo n=%d err=%v want %d", n, err, len(payload))
	}
	if !bytes.Equal(out.Bytes(), payload) {
		t.Fatal("WriteTo output differs from payload")
	}
}

func TestReader_WaitFirstSurfacesUpstreamFailure(t *testing.T) {
	r, _ := newTestReader(t, &shortAt{short: 0}, 2*testChunk, true)
	defer r.Close()

	if err := r.WaitFirst(); err == nil {
		t.Fatal("WaitFirst should report the first chunk failure so the HTTP layer can pick a status")
	}
}
