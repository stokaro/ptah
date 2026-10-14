// Package isolators lists the engines that give a test case a database of
// its own in a way of their own, for migration/internal/scratchdb to look up
// by dialect. An engine whose server creates databases states it in its
// descriptor instead and needs no entry. The package holds no rule: each
// isolation lives in its engine's package and states the dialect it serves.
package isolators

import (
	"context"

	"ptah.run/migration/internal/scratchdb/sqlitescratch"
	"ptah.run/migration/internal/scratchdb/ydbscratch"
)

// Service gives a case of one dialect a database of its own.
type Service struct {
	// Dialect is the canonical dialect name, as platform.NormalizeDialect
	// returns it.
	Dialect string
	// Isolate provisions the database next to the one baseURL names, and
	// returns its URL and the function that removes it.
	Isolate func(ctx context.Context, baseURL string) (url string, release func() error, err error)
}

// All returns every built-in isolation. Each call returns a new slice.
func All() []Service {
	return []Service{
		{Dialect: sqlitescratch.Dialect, Isolate: sqlitescratch.Isolate},
		{Dialect: ydbscratch.Dialect, Isolate: ydbscratch.Isolate},
	}
}
