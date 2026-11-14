package tg

import (
	"net"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/transport"
)

// socketBufferSize mirrors Telegram Desktop's 2 MB socket buffers.
const socketBufferSize = 2 * 1024 * 1024

// NewDialFunc creates a net dialer that configures 2 MB SO_RCVBUF and SO_SNDBUF socket buffers.
func NewDialFunc() dcs.DialFunc {
	dialer := &net.Dialer{
		Timeout:   15 * time.Second,
		KeepAlive: 30 * time.Second,
		Control: func(network, address string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, socketBufferSize)
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, socketBufferSize)
			})
		},
	}
	return dialer.DialContext
}

// obfuscated selects the obfuscated2 transport for new connections.
var obfuscated atomic.Bool

// SetObfuscation turns the obfuscated2 transport on or off for connections
// created afterwards. Call it once at startup, before creating clients.
func SetObfuscation(on bool) { obfuscated.Store(on) }

// CreateResolver creates a gotd dcs.Resolver using the intermediate transport
// (optionally obfuscated, see SetObfuscation) and the custom dialer.
// gotd keeps its DC list (including media-only addresses) up to date from help.getConfig.
func CreateResolver() dcs.Resolver {
	return dcs.Plain(dcs.PlainOptions{
		Dial:       NewDialFunc(),
		Protocol:   transport.Intermediate,
		Obfuscated: obfuscated.Load(),
		Network:    "tcp",
		PreferIPv6: false,
	})
}
