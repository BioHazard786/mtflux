package stream

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrNoRangeHeader      = errors.New("no Range header present")
	ErrInvalidRange       = errors.New("invalid Range header format")
	ErrRangeUnsatisfiable = errors.New("range not satisfiable")
)

// ByteRange represents a parsed, normalized HTTP byte range [Start, End] inclusive.
type ByteRange struct {
	Start int64
	End   int64
}

// Length returns the number of bytes in this range.
func (r ByteRange) Length() int64 {
	return r.End - r.Start + 1
}

// ContentRangeHeader returns RFC 7233 format: "bytes <start>-<end>/<total>".
func (r ByteRange) ContentRangeHeader(totalSize int64) string {
	return fmt.Sprintf("bytes %d-%d/%d", r.Start, r.End, totalSize)
}

// ParseRange parses an HTTP Range header string (RFC 7233) against total file size.
// Supports:
//   - "bytes=100-200"
//   - "bytes=100-"
//   - "bytes=-500" (suffix)
//   - Multi-ranges: parses the first range (as specified in implementation plan)
func ParseRange(rangeHeader string, totalSize int64) (ByteRange, error) {
	if rangeHeader == "" {
		return ByteRange{Start: 0, End: totalSize - 1}, ErrNoRangeHeader
	}

	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return ByteRange{}, ErrInvalidRange
	}

	spec := strings.TrimPrefix(rangeHeader, "bytes=")
	spec = strings.TrimSpace(spec)

	// If multiple ranges comma-separated, take the first one
	if idx := strings.IndexByte(spec, ','); idx != -1 {
		spec = strings.TrimSpace(spec[:idx])
	}

	hyphen := strings.IndexByte(spec, '-')
	if hyphen == -1 {
		return ByteRange{}, ErrInvalidRange
	}

	startStr := strings.TrimSpace(spec[:hyphen])
	endStr := strings.TrimSpace(spec[hyphen+1:])

	if startStr == "" && endStr == "" {
		return ByteRange{}, ErrInvalidRange
	}

	// Suffix range: "bytes=-500"
	if startStr == "" {
		suffixLen, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || suffixLen <= 0 {
			return ByteRange{}, ErrInvalidRange
		}
		if suffixLen > totalSize {
			suffixLen = totalSize
		}
		if totalSize == 0 {
			return ByteRange{}, ErrRangeUnsatisfiable
		}
		return ByteRange{
			Start: totalSize - suffixLen,
			End:   totalSize - 1,
		}, nil
	}

	// Normal or open-ended range: "bytes=100-" or "bytes=100-200"
	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		return ByteRange{}, ErrInvalidRange
	}

	if start >= totalSize {
		return ByteRange{}, ErrRangeUnsatisfiable
	}

	var end int64
	if endStr == "" {
		end = totalSize - 1
	} else {
		end, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil || end < start {
			return ByteRange{}, ErrInvalidRange
		}
		if end >= totalSize {
			end = totalSize - 1
		}
	}

	return ByteRange{Start: start, End: end}, nil
}

// ChunkBounds calculates the chunk indices and offsets required to satisfy [start, end].
func ChunkBounds(start, end, chunkSize int64) (startChunk, endChunk int, firstChunkOffset, lastChunkEnd int64) {
	startChunk = int(start / chunkSize)
	endChunk = int(end / chunkSize)
	firstChunkOffset = start % chunkSize
	lastChunkEnd = end % chunkSize
	return
}
