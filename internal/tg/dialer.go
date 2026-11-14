package tg

import (
	"net"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/transport"
	"go.uber.org/zap"
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

// libraryLog, when set, receives the Telegram client library's own log output
// instead of the application logger.
var libraryLog atomic.Pointer[zap.Logger]

// SetLibraryLogger routes gotd's internal logging (connections, key exchange,
// retries) to l, so it can run at a different level from mtflux's own
// messages. Call it once at startup, before creating clients.
func SetLibraryLogger(l *zap.Logger) { libraryLog.Store(l) }

// LibraryLogger returns the logger gotd clients should use: the one given to
// SetLibraryLogger, or fallback when none was set.
func LibraryLogger(fallback *zap.Logger) *zap.Logger {
	if l := libraryLog.Load(); l != nil {
		return l
	}
	return fallback
}
