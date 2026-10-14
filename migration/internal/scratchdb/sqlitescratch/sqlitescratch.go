// Package sqlitescratch gives a SQLite test case a database of its own: a
// file in a temporary directory nothing else opens. A SQLite URL names one
// database rather than a server, so there is no server to create one beside
// it. migration/internal/scratchdb provisions through it.
package sqlitescratch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"ptah.run/core/platform"
	"ptah.run/internal/atlasurl"
)

// Dialect is the dialect this package isolates cases for.
const Dialect = platform.SQLite

// Isolate creates the database file and returns its URL, and the function
// that removes the directory holding it. baseURL is not read: the new file
// shares nothing with the one it names.
func Isolate(context.Context, string) (url string, release func() error, err error) {
	dir, err := os.MkdirTemp("", "ptah-scratch-*")
	if err != nil {
		return "", nil, fmt.Errorf("create scratch directory: %w", err)
	}
	return atlasurl.SQLiteURLFromPath(filepath.Join(dir, "scratch.db")), func() error {
		return os.RemoveAll(dir)
	}, nil
}
