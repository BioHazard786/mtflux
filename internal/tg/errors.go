package tg

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gotd/td/tgerr"
)

type ErrorClass int

const (
	ClassUnknown ErrorClass = iota
	ClassFloodWait
	ClassFileRefExpired
	ClassMigrate
	ClassAuthBroken
	ClassNoAccess
	ClassNotFound
	ClassTransient
)

func (c ErrorClass) String() string {
	switch c {
	case ClassFloodWait:
		return "flood_wait"
	case ClassFileRefExpired:
		return "file_ref_expired"
	case ClassMigrate:
		return "migrate"
	case ClassAuthBroken:
		return "auth_broken"
	case ClassNoAccess:
		return "no_access"
	case ClassNotFound:
		return "not_found"
	case ClassTransient:
		return "transient"
	default:
		return "unknown"
	}
}

// ClassifiedError provides structured metadata on an error from Telegram or network.
type ClassifiedError struct {
	Class     ErrorClass
	Err       error
	Wait      time.Duration // for FloodWait
	MigrateDC int           // for Migrate
}

func (e *ClassifiedError) Error() string {
	return e.Err.Error()
}

func (e *ClassifiedError) Unwrap() error {
	return e.Err
}

// Classify inspects an error and categorizes it into a ClassifiedError.
func Classify(err error) *ClassifiedError {
	if err == nil {
		return nil
	}

	res := &ClassifiedError{
		Class: ClassUnknown,
		Err:   err,
	}

	// 1. Check transient network errors
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		res.Class = ClassTransient
		return res
	}

	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || strings.Contains(err.Error(), "connection reset") || strings.Contains(err.Error(), "broken pipe")) {
		res.Class = ClassTransient
		return res
	}

	// 2. Check TG RPC Error
	var rpcErr *tgerr.Error
	if errors.As(err, &rpcErr) {
		msg := rpcErr.Message

		// Flood Wait
		if strings.HasPrefix(msg, "FLOOD_WAIT_") || strings.HasPrefix(msg, "FLOOD_PREMIUM_WAIT_") || strings.HasPrefix(msg, "SLOWMODE_WAIT_") {
			res.Class = ClassFloodWait
			secs := extractNumber(msg)
			if secs <= 0 {
				secs = 1
			}
			res.Wait = time.Duration(secs) * time.Second
			return res
		}

		// File Reference Expired / Invalid
		if strings.HasPrefix(msg, "FILE_REFERENCE_") {
			res.Class = ClassFileRefExpired
			return res
		}

		// Migrate
		if strings.HasPrefix(msg, "FILE_MIGRATE_") || strings.HasPrefix(msg, "PHONE_MIGRATE_") || strings.HasPrefix(msg, "NETWORK_MIGRATE_") {
			res.Class = ClassMigrate
			res.MigrateDC = extractNumber(msg)
			return res
		}

		// Auth broken
		if strings.HasPrefix(msg, "AUTH_KEY_") ||
			msg == "SESSION_REVOKED" ||
			msg == "SESSION_EXPIRED" ||
			msg == "USER_DEACTIVATED" ||
			msg == "USER_DEACTIVATED_BAN" {
			res.Class = ClassAuthBroken
			return res
		}

		// No Access
		if msg == "CHANNEL_PRIVATE" ||
			msg == "CHAT_ADMIN_REQUIRED" ||
			msg == "CHAT_WRITE_FORBIDDEN" ||
			msg == "USER_NOT_PARTICIPANT" ||
			msg == "CHAT_INVALID" ||
			msg == "CHANNEL_INVALID" {
			res.Class = ClassNoAccess
			return res
		}

		// Not Found
		if msg == "MESSAGE_ID_INVALID" || msg == "FILE_ID_INVALID" || msg == "DOCUMENT_INVALID" {
			res.Class = ClassNotFound
			return res
		}
	}

	// Fallback to error string matching
	errStr := strings.ToUpper(err.Error())
	if strings.Contains(errStr, "FLOOD_WAIT") || strings.Contains(errStr, "FLOOD_PREMIUM_WAIT") {
		res.Class = ClassFloodWait
		secs := extractNumber(errStr)
		if secs <= 0 {
			secs = 1
		}
		res.Wait = time.Duration(secs) * time.Second
		return res
	}

	if strings.Contains(errStr, "FILE_REFERENCE") {
		res.Class = ClassFileRefExpired
		return res
	}

	if strings.Contains(errStr, "MIGRATE_") {
		res.Class = ClassMigrate
		res.MigrateDC = extractNumber(errStr)
		return res
	}

	if strings.Contains(errStr, "AUTH_KEY_") || strings.Contains(errStr, "SESSION_REVOKED") {
		res.Class = ClassAuthBroken
		return res
	}

	if strings.Contains(errStr, "TIMEOUT") || strings.Contains(errStr, "RESET BY PEER") {
		res.Class = ClassTransient
		return res
	}

	return res
}

func extractNumber(s string) int {
	var numStr strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= '0' && c <= '9' {
			numStr.WriteByte(c)
		} else if numStr.Len() > 0 {
			break
		}
	}
	if numStr.Len() == 0 {
		return 0
	}
	val, _ := strconv.Atoi(numStr.String())
	return val
}

