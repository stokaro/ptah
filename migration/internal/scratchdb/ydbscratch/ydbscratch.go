// Package ydbscratch gives a YDB test case a database of its own. YDB has no
// SQL that creates a database, so a case gets a dev realm instead: a directory
// in the database the URL names, which a connection opened from the returned
// URL treats as its whole database (see ptah.run/internal/ydbrealm).
// migration/internal/scratchdb provisions through it.
package ydbscratch

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/internal/ydbrealm"
)

// Dialect is the dialect this package isolates cases for.
const Dialect = platform.YDB

// Isolate creates a dev realm in the database baseURL names and returns its
// URL, and the function that removes the realm with everything in it. A realm
// it could not remove is reported in a warning that names the directory, and
// the function returns nil.
func Isolate(ctx context.Context, baseURL string) (url string, release func() error, err error) {
	realmURL, leave, err := ydbrealm.Enter(ctx, baseURL)
	if err != nil {
		return "", nil, fmt.Errorf("create a scratch dev realm: %w", err)
	}
	return realmURL, func() error {
		leave()
		return nil
	}, nil
}
