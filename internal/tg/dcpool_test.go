package tg

import (
	"testing"

	"github.com/gotd/td/tg"
)

func TestHasMediaAddr(t *testing.T) {
	opts := []tg.DCOption{
		{ID: 1, IPAddress: "149.154.175.53"},
		{ID: 1, IPAddress: "2001:b28:f23d:f001::a", Ipv6: true, MediaOnly: true},
		{ID: 2, IPAddress: "149.154.167.51"},
		{ID: 2, IPAddress: "149.154.167.151", MediaOnly: true},
		{ID: 3, IPAddress: "149.154.175.100", MediaOnly: true, CDN: true},
	}
	cases := map[int]bool{
		1: false, // only an IPv6 media address: we dial IPv4
		2: true,
		3: false, // CDN, not a media DC
		5: false, // not listed at all
	}
	for dc, want := range cases {
		if got := hasMediaAddr(opts, dc); got != want {
			t.Errorf("hasMediaAddr(dc=%d) = %v, want %v", dc, got, want)
		}
	}
}
