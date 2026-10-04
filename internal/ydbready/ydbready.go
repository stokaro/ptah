// Package ydbready waits out the moment a freshly started YDB database answers
// queries and still refuses to create a table.
//
// Measured on local-ydb 26.2.1.14, started three times: for about a second
// after `SELECT Version()` first answers, a CREATE TABLE fails with `database
// doesn't have storage pools at all`, and no read-only query tells that second
// apart from the ones after it. A connection that opens is therefore not yet a
// database that takes DDL. The capability probe's first schema change and the
// readiness wait of a `docker://ydb` dev database both retry that one refusal
// through [Until], so the two cannot disagree about what a fresh server says or
// about how long it is given.
package ydbready

import (
	"context"
	"strings"
	"time"
)

// StorageNotReadyText is the part of the refusal a YDB database answers a
// CREATE TABLE with before its storage is bound.
const StorageNotReadyText = "doesn't have storage pools"

// Retries and Interval bound the wait [Until] spends on a database that is
// still binding its storage: one try, then up to Retries more, one Interval
// apart.
const (
	Retries  = 60
	Interval = time.Second
)

// StorageNotReady reports whether err is the refusal of a database that has
// not bound its storage yet.
func StorageNotReady(err error) bool {
	return err != nil && strings.Contains(err.Error(), StorageNotReadyText)
}

// Until runs try until it succeeds, fails with anything other than the
// storage refusal, has run Retries+1 times, or ctx ends, and returns the last
// error try returned. A refusal other than the storage one is the answer and
// ends the wait at once. When ctx ends during the wait, the last refusal is
// returned rather than the context's error, because it is what a caller needs
// to say why the database was not ready.
func Until(ctx context.Context, try func(context.Context) error) error {
	for attempt := 0; ; attempt++ {
		err := try(ctx)
		if err == nil || !StorageNotReady(err) || attempt == Retries {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(Interval):
		}
	}
}
