package tg

import (
	"context"
	"net"
	"syscall"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"
)

// NewDialFunc creates a net dialer that configures 2 MB SO_RCVBUF and SO_SNDBUF socket buffers.
func NewDialFunc() dcs.DialFunc {
	dialer := &net.Dialer{
		Timeout:   15 * time.Second,
		KeepAlive: 30 * time.Second,
		Control: func(network, address string, c syscall.RawConn) error {
			var operr error
			err := c.Control(func(fd uintptr) {
				const bufSize = 2 * 1024 * 1024 // 2 MB socket buffer
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, bufSize)
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, bufSize)
			})
			if err != nil {
				return err
			}
			return operr
		},
	}
	return dialer.DialContext
}

// CreateResolver creates a gotd dcs.Resolver using obfuscated intermediate transport and custom dialer.
func CreateResolver() dcs.Resolver {
	return dcs.Plain(dcs.PlainOptions{
		Dial:       NewDialFunc(),
		Protocol:   transport.Intermediate,
		Obfuscated: true,
		Network:    "tcp",
		PreferIPv6: false,
	})
}

// FetchLiveDCOptions queries help.getConfig to retrieve the current, complete DC list including media-only IPs.
func FetchLiveDCOptions(ctx context.Context, client *telegram.Client) ([]tg.DCOption, error) {
	cfg, err := client.API().HelpGetConfig(ctx)
	if err != nil {
		return nil, err
	}
	return cfg.DCOptions, nil
}

