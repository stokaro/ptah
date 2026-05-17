// Package dbcli holds small helpers shared by the CLI subcommands that connect
// to a database. Centralising the connect-timeout flag, context construction
// and the close-error log avoids inconsistent behaviour across commands.
package dbcli

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-extras/cobraflags"
)

// ConnectTimeoutFlagName is the CLI flag name exposed by [NewConnectTimeoutFlag].
const ConnectTimeoutFlagName = "connect-timeout"

// DefaultConnectTimeout is the default value for [ConnectTimeoutFlagName]. It
// matches the value suggested by issue #139.
const DefaultConnectTimeout = 10 * time.Second

// NewConnectTimeoutFlag returns a string-valued flag definition that accepts a
// [time.Duration] literal (for example "5s" or "2m"). The flag is intentionally
// a string so a value of "0" disables the timeout, while still supporting the
// usual duration suffixes.
func NewConnectTimeoutFlag() cobraflags.Flag {
	return &cobraflags.StringFlag{
		Name:  ConnectTimeoutFlagName,
		Value: DefaultConnectTimeout.String(),
		Usage: "Maximum time to wait when establishing the initial database connection (for example 5s or 1m). Use 0 to disable the timeout.",
	}
}

// ParseConnectTimeout parses the raw string value returned by the
// [ConnectTimeoutFlagName] flag. A zero duration is accepted and signals that
// callers should not wrap the parent context with a deadline.
func ParseConnectTimeout(raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid --%s value %q: %w", ConnectTimeoutFlagName, raw, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("invalid --%s value %q: must not be negative", ConnectTimeoutFlagName, raw)
	}
	return d, nil
}

// ConnectContext derives a context for the initial database connection from
// the supplied parent. When timeout is zero, the parent is returned along with
// a no-op cancel so callers can defer cancel unconditionally.
func ConnectContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, timeout)
}

// WarnOnClose closes the given resource and logs a warning if Close returns an
// error. It is intended to be used inside a deferred call so that CLI handlers
// no longer silently discard errors from database connections.
//
//	defer dbcli.WarnOnClose("database connection", conn)
func WarnOnClose(resource string, closer interface{ Close() error }) {
	if closer == nil {
		return
	}
	if err := closer.Close(); err != nil {
		slog.Warn("failed to close "+resource, "error", err)
	}
}
