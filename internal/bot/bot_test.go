package bot

import (
	"testing"
	"time"

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

func TestParseCommand(t *testing.T) {
	cases := []struct {
		in   string
		cmd  string
		args []string
	}{
		{"/link 6h", "/link", []string{"6h"}},
		{"/Link@MyBot 2d", "/link", []string{"2d"}},
		{"/revoke  abc ", "/revoke", []string{"abc"}},
		{"/start", "/start", nil},
		{"hello /link", "", nil},
		{"", "", nil},
	}
	for _, c := range cases {
		cmd, args := parseCommand(c.in)
		if cmd != c.cmd || len(args) != len(c.args) {
			t.Errorf("parseCommand(%q) = %q %v; want %q %v", c.in, cmd, args, c.cmd, c.args)
			continue
		}
		for i := range args {
			if args[i] != c.args[i] {
				t.Errorf("parseCommand(%q) args = %v", c.in, args)
			}
		}
	}
}

func TestExtractTokenIgnoresQueryString(t *testing.T) {
	if got := extractTokenFromText("https://dl.example.com/dl/TOKEN123/movie.mp4?inline=1"); got != "TOKEN123" {
		t.Errorf("got %q", got)
	}
	if got := extractTokenFromText("https://dl.example.com/dl/TOKEN123?inline=1"); got != "TOKEN123" {
		t.Errorf("got %q", got)
	}
}

func TestPendingTTLIsConsumedOnce(t *testing.T) {
	b := &Bot{pending: map[int64]pendingTTL{}}
	b.setPending(42, 6*time.Hour)

	if ttl, ok := b.takePending(42); !ok || ttl != 6*time.Hour {
		t.Fatalf("takePending = %v, %v", ttl, ok)
	}
	if _, ok := b.takePending(42); ok {
		t.Fatal("a pending TTL must apply to one file only")
	}

	b.pending[7] = pendingTTL{ttl: time.Hour, until: time.Now().Add(-time.Second)}
	if _, ok := b.takePending(7); ok {
		t.Fatal("expired pending TTL must not apply")
	}
}

func TestHumanSize(t *testing.T) {
	for in, want := range map[int64]string{512: "512 B", 1536: "1.50 KiB", 5 << 20: "5.00 MiB", 3 << 30: "3.00 GiB"} {
		if got := humanSize(in); got != want {
			t.Errorf("humanSize(%d) = %q want %q", in, got, want)
		}
	}
}
