package stream

import (
	"errors"
	"testing"
)

func TestParseRange(t *testing.T) {
	totalSize := int64(1000)

	tests := []struct {
		header    string
		wantStart int64
		wantEnd   int64
		wantErr   error
	}{
		{"bytes=0-499", 0, 499, nil},
		{"bytes=500-999", 500, 999, nil},
		{"bytes=500-", 500, 999, nil},
		{"bytes=-500", 500, 999, nil},
		{"bytes=-1000", 0, 999, nil},
		{"bytes=-1500", 0, 999, nil}, // suffix larger than file
		{"bytes=0-0", 0, 0, nil},
		{"bytes=999-999", 999, 999, nil},
		{"bytes=0-2000", 0, 999, nil},             // clamp to totalSize-1
		{"bytes=100-200, 300-400", 100, 200, nil}, // multi-range: first range served
		{"", 0, 999, ErrNoRangeHeader},
		{"bytes=1000-", 0, 0, ErrRangeUnsatisfiable}, // start >= total
		{"bytes=1500-1600", 0, 0, ErrRangeUnsatisfiable},
		{"bytes=-0", 0, 0, ErrInvalidRange},
		{"bytes=200-100", 0, 0, ErrInvalidRange}, // end < start
		{"bytes=abc-def", 0, 0, ErrInvalidRange},
		{"invalid", 0, 0, ErrInvalidRange},
	}

	for _, tt := range tests {
		t.Run(tt.header, func(t *testing.T) {
			rng, err := ParseRange(tt.header, totalSize)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rng.Start != tt.wantStart || rng.End != tt.wantEnd {
				t.Errorf("expected [%d-%d], got [%d-%d]", tt.wantStart, tt.wantEnd, rng.Start, rng.End)
			}
		})
	}
}

func TestChunkBoundsMath(t *testing.T) {
	chunkSize := int64(1048576) // 1 MiB

	// Exact chunk boundary: [0, 1048575]
	startChunk, endChunk, firstOff, lastEnd := ChunkBounds(0, chunkSize-1, chunkSize)
	if startChunk != 0 || endChunk != 0 || firstOff != 0 || lastEnd != chunkSize-1 {
		t.Errorf("chunk 0 mismatch: %d, %d, %d, %d", startChunk, endChunk, firstOff, lastEnd)
	}

	// Range spanning 2 chunks: [500000, 1500000]
	startChunk, endChunk, firstOff, lastEnd = ChunkBounds(500000, 1500000, chunkSize)
	if startChunk != 0 || endChunk != 1 || firstOff != 500000 || lastEnd != (1500000-chunkSize) {
		t.Errorf("span 2 chunks mismatch: %d, %d, %d, %d", startChunk, endChunk, firstOff, lastEnd)
	}

	// Sub-chunk range within chunk 1: [1100000, 1200000]
	startChunk, endChunk, firstOff, lastEnd = ChunkBounds(1100000, 1200000, chunkSize)
	if startChunk != 1 || endChunk != 1 || firstOff != (1100000-chunkSize) || lastEnd != (1200000-chunkSize) {
		t.Errorf("within chunk 1 mismatch: %d, %d, %d, %d", startChunk, endChunk, firstOff, lastEnd)
	}
}

func FuzzParseRange(f *testing.F) {
	f.Add("bytes=0-100", int64(1000))
	f.Add("bytes=-500", int64(1000))
	f.Add("bytes=500-", int64(1000))
	f.Add("bytes=100-200, 300-400", int64(1000))
	f.Add("invalid", int64(500))

	f.Fuzz(func(t *testing.T, header string, size int64) {
		if size < 0 || size > 1000000000000 {
			return
		}
		rng, err := ParseRange(header, size)
		if err == nil {
			if rng.Start < 0 || rng.End >= size || rng.Start > rng.End {
				t.Fatalf("fuzz invariant failed for size %d: [%d-%d]", size, rng.Start, rng.End)
			}
		}
	})
}
