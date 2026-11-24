package dburldisplay

import (
	"context"
	"slices"
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
	message := err.Error()
	redacted := RedactRecordedText(ctx, message)
	if redacted == message {
		return err
	}
	return redactedError{text: redacted, err: err}
}

// RedactRecordedText is [RedactRecordedError] for a text that is not an
// error's message, such as a field a database server answered with: it takes
// the secrets of the addresses opened under ctx out of text, where [RedactError]
// would take them out.
func RedactRecordedText(ctx context.Context, text string) string {
	if ctx == nil {
		return text
	}
	addresses, ok := ctx.Value(errorRedactionKey{}).(*errorAddresses)
	if !ok {
		return text
	}
	addresses.mu.Lock()
	urls := slices.Clone(addresses.urls)
	addresses.mu.Unlock()
	var parts []secretText
	for _, dbURL := range urls {
		parts = append(parts, secretParts(dbURL)...)
	}
	return maskText(text, parts, redactedPassword)
}
