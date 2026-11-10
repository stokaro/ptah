package dburldisplay

import (
	"context"
	"slices"
	"strings"
	"sync"
)

type errorRedactionKey struct{}

// errorAddresses holds the addresses a command opened. A mutex protects the
// list because a command may open several databases concurrently.
type errorAddresses struct {
	mu   sync.Mutex
	urls []string
}

// WithErrorRedaction gives one command a private set of database addresses to
// redact from its diagnostics. The set lives only as long as the command's
// context and is never shared between runs.
func WithErrorRedaction(ctx context.Context) context.Context {
	return context.WithValue(ctx, errorRedactionKey{}, &errorAddresses{})
}

// RecordErrorAddress records an address before a connection is opened, so an
// error from any later statement can be redacted at the command boundary.
func RecordErrorAddress(ctx context.Context, dbURL string) {
	if ctx == nil || dbURL == "" {
		return
	}
	addresses, ok := ctx.Value(errorRedactionKey{}).(*errorAddresses)
	if !ok {
		return
	}
	addresses.mu.Lock()
	addresses.urls = append(addresses.urls, dbURL)
	addresses.mu.Unlock()
}

// RedactRecordedError removes every secret from the addresses opened under
// ctx, preserving the original error for errors.Is and errors.As.
func RedactRecordedError(ctx context.Context, err error) error {
	if err == nil || ctx == nil {
		return err
	}
	addresses, ok := ctx.Value(errorRedactionKey{}).(*errorAddresses)
	if !ok {
		return err
	}
	addresses.mu.Lock()
	urls := slices.Clone(addresses.urls)
	addresses.mu.Unlock()
	var parts []string
	for _, dbURL := range urls {
		parts = append(parts, secretParts(dbURL)...)
	}
	parts = longestFirst(parts)
	message := err.Error()
	redacted := message
	for _, part := range parts {
		redacted = strings.ReplaceAll(redacted, part, redactedPassword)
	}
	if redacted == message {
		return err
	}
	return redactedError{text: redacted, err: err}
}
