// Package statementtimeout carries a migration's statement timeout from the
// migrator to an executor whose owner stops each statement itself, as an
// executable provider does when it offers units.statement-timeout. The
// migrator sets it on the context the migration's statements run under, and
// the executor reads it there; no other package sets one.
package statementtimeout

import (
	"context"
	"time"
)

type key struct{}

// With returns a copy of ctx that bounds each statement run under it by
// timeout. A timeout that is not positive bounds nothing.
func With(ctx context.Context, timeout time.Duration) context.Context {
	if timeout <= 0 {
		return ctx
	}
	return context.WithValue(ctx, key{}, timeout)
}

// From returns the statement timeout ctx carries, and whether it carries one.
func From(ctx context.Context) (time.Duration, bool) {
	timeout, bounded := ctx.Value(key{}).(time.Duration)
	return timeout, bounded
}
