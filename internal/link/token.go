package link

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const (
	tokenVersion byte = 1
	macLen            = 10
)

var (
	ErrInvalidToken  = errors.New("invalid link token")
	ErrTokenExpired  = errors.New("link has expired")
	ErrTamperedToken = errors.New("token verification failed")
)

// Token represents the decoded contents of a link token.
type Token struct {
	MsgID   int
	OwnerID int64
	Expiry  time.Time // Zero value means never expires
}

// Generate creates a signed, compact URL-safe token.
// If expiry is zero or in the past, expiry is treated as never expiring (0) or expired respectively.
func Generate(secret []byte, msgID int, ownerID int64, expiry time.Time) string {
	buf := make([]byte, 1, 32)
	buf[0] = tokenVersion

	// Expiry has minute resolution. Round up so a link is never cut short
	// (floor would let a "1m" link die after as little as one second).
	var expMin uint64
	if !expiry.IsZero() {
		expMin = uint64((expiry.Unix() + 59) / 60)
	}

	var tmp [binary.MaxVarintLen64]byte

	n := binary.PutUvarint(tmp[:], uint64(msgID))
	buf = append(buf, tmp[:n]...)

	n = binary.PutUvarint(tmp[:], expMin)
	buf = append(buf, tmp[:n]...)

	n = binary.PutUvarint(tmp[:], uint64(ownerID))
	buf = append(buf, tmp[:n]...)

	// Compute HMAC over payload
	mac := computeMAC(secret, buf)
	buf = append(buf, mac...)

	return base64.RawURLEncoding.EncodeToString(buf)
}

// Parse verifies and extracts data from a link token.
func Parse(secret []byte, tokenStr string) (*Token, error) {
	data, err := base64.RawURLEncoding.DecodeString(tokenStr)
	if err != nil {
		return nil, fmt.Errorf("%w: base64 decode: %v", ErrInvalidToken, err)
	}

	if len(data) < 1+1+1+1+macLen { // version + 3 varints (min 1 byte each) + mac
		return nil, ErrInvalidToken
	}

	if data[0] != tokenVersion {
		return nil, fmt.Errorf("%w: unknown version %d", ErrInvalidToken, data[0])
	}

	payloadLen := len(data) - macLen
	payload := data[:payloadLen]
	givenMAC := data[payloadLen:]

	expectedMAC := computeMAC(secret, payload)
	if !hmac.Equal(givenMAC, expectedMAC) {
		return nil, ErrTamperedToken
	}

	reader := bytes.NewReader(payload[1:]) // skip version byte

	msgIDUint, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("%w: msgID read: %v", ErrInvalidToken, err)
	}

	expMinUint, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("%w: expiry read: %v", ErrInvalidToken, err)
	}

	ownerIDUint, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("%w: ownerID read: %v", ErrInvalidToken, err)
	}

	var expiry time.Time
	if expMinUint > 0 {
		expiry = time.Unix(int64(expMinUint*60), 0)
		if time.Now().After(expiry) {
			return &Token{
				MsgID:   int(msgIDUint),
				OwnerID: int64(ownerIDUint),
				Expiry:  expiry,
			}, ErrTokenExpired
		}
	}

	return &Token{
		MsgID:   int(msgIDUint),
		OwnerID: int64(ownerIDUint),
		Expiry:  expiry,
	}, nil
}

func computeMAC(secret, payload []byte) []byte {
	h := hmac.New(sha256.New, secret)
	h.Write(payload)
	sum := h.Sum(nil)
	return sum[:macLen]
}
