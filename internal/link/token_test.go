package link

import (
	"crypto/rand"
	"errors"
	"testing"
	"time"
)

func TestTokenRoundTrip(t *testing.T) {
	secret := []byte("test-link-secret-key-12345")
	msgID := 48192
	ownerID := int64(987654321)
	expiry := time.Now().Add(2 * time.Hour).Truncate(time.Minute)

	tokenStr := Generate(secret, msgID, ownerID, expiry)
	if len(tokenStr) == 0 {
		t.Fatal("empty token generated")
	}

	tok, err := Parse(secret, tokenStr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if tok.MsgID != msgID {
		t.Errorf("expected msgID %d, got %d", msgID, tok.MsgID)
	}
	if tok.OwnerID != ownerID {
		t.Errorf("expected ownerID %d, got %d", ownerID, tok.OwnerID)
	}
	if tok.Expiry.Unix() != expiry.Unix() {
		t.Errorf("expected expiry %v, got %v", expiry, tok.Expiry)
	}
}

func TestTokenNeverExpires(t *testing.T) {
	secret := []byte("secret")
	msgID := 123
	ownerID := int64(456)

	tokenStr := Generate(secret, msgID, ownerID, time.Time{})
	tok, err := Parse(secret, tokenStr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !tok.Expiry.IsZero() {
		t.Errorf("expected zero expiry, got %v", tok.Expiry)
	}
}

func TestTokenExpired(t *testing.T) {
	secret := []byte("secret")
	msgID := 123
	ownerID := int64(456)
	// Expired 5 minutes ago
	pastExpiry := time.Now().Add(-5 * time.Minute)

	tokenStr := Generate(secret, msgID, ownerID, pastExpiry)
	tok, err := Parse(secret, tokenStr)
	if !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("expected ErrTokenExpired, got %v", err)
	}
	if tok == nil || tok.MsgID != msgID {
		t.Errorf("expected token data to still be extracted on expiry, got %+v", tok)
	}
}

func TestTokenTampering(t *testing.T) {
	secret := []byte("secret")
	tokenStr := Generate(secret, 100, 200, time.Time{})

	// Tamper single character
	tampered := []byte(tokenStr)
	if tampered[5] == 'a' {
		tampered[5] = 'b'
	} else {
		tampered[5] = 'a'
	}

	_, err := Parse(secret, string(tampered))
	if err == nil {
		t.Fatal("expected error on tampered token, got nil")
	}

	// Different secret
	wrongSecret := []byte("wrong-secret")
	_, err = Parse(wrongSecret, tokenStr)
	if !errors.Is(err, ErrTamperedToken) {
		t.Fatalf("expected ErrTamperedToken with wrong secret, got %v", err)
	}
}

func TestTokenInvalidFormats(t *testing.T) {
	secret := []byte("secret")
	tests := []string{
		"",
		"not-valid-base64-!!!",
		"AQ==", // too short
	}
	for _, tt := range tests {
		_, err := Parse(secret, tt)
		if !errors.Is(err, ErrInvalidToken) {
			t.Errorf("for input %q, expected ErrInvalidToken, got %v", tt, err)
		}
	}
}

func FuzzTokenParse(f *testing.F) {
	secret := make([]byte, 16)
	_, _ = rand.Read(secret)

	f.Add("AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRo=")
	f.Add("")
	f.Add("invalid")

	f.Fuzz(func(t *testing.T, data string) {
		_, _ = Parse(secret, data)
	})
}

func TestTokenExpiryNeverEndsEarly(t *testing.T) {
	secret := []byte("s3cret-for-expiry-rounding")
	want := time.Unix((time.Now().Unix()/60+10)*60+59, 0) // 59s into a future minute
	tok, err := Parse(secret, Generate(secret, 1, 1, want))
	if err != nil {
		t.Fatal(err)
	}
	if tok.Expiry.Before(want) {
		t.Fatalf("link expires %v before the requested %v", want.Sub(tok.Expiry), want)
	}
	if tok.Expiry.Sub(want) >= time.Minute {
		t.Fatalf("rounded more than a minute late: %v", tok.Expiry.Sub(want))
	}
}
