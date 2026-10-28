package bot

import (
	"testing"

	"github.com/gotd/td/tg"
)

func TestExtractTokenFromText(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"https://example.com/dl/AbCdEf123/movie.mp4", "AbCdEf123"},
		{"http://localhost:8080/dl/xyz789", "xyz789"},
		{"/revoke AbCdEf123", "/revoke AbCdEf123"},
		{"tokenOnly12345", "tokenOnly12345"},
	}

	for _, tt := range tests {
		got := extractTokenFromText(tt.input)
		if got != tt.want {
			t.Errorf("for input %q: expected %q, got %q", tt.input, tt.want, got)
		}
	}
}

func TestExtractMsgIDFromUpdates(t *testing.T) {
	// 1. Updates struct
	upd := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewChannelMessage{
				Message: &tg.Message{ID: 1001},
			},
		},
	}
	if id := extractMsgIDFromUpdates(upd); id != 1001 {
		t.Errorf("expected 1001, got %d", id)
	}

	// 2. Short sent message
	short := &tg.UpdateShortSentMessage{
		ID: 2002,
	}
	if id := extractMsgIDFromUpdates(short); id != 2002 {
		t.Errorf("expected 2002, got %d", id)
	}
}

