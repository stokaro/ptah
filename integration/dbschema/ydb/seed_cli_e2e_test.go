//go:build integration

package ydb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestYDBBinary_SeedsOnceAndSkipsTheSecondRun drives `ptah seed` against a live
// YDB database: the first run applies the seed and records it, the second finds
// it recorded and runs nothing, and the rows the seed wrote are read back.
func TestYDBBinary_SeedsOnceAndSkipsTheSecondRun(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			ownSeeds(c, conn)
			seeds := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(seeds, "001_regions.all.sql"), []byte(regionsSeed), 0o600), qt.IsNil)
			args := []string{"seed", "--db-url", url, "--env", "dev", "--seeds-dir", seeds}

			first, firstErr := runBinary(ctx, binary, args...)
			second, secondErr := runBinary(ctx, binary, args...)

			c.Assert(firstErr, qt.IsNil, qt.Commentf("first seed run:\n%s", first))
			c.Assert(first, qt.Contains, "Applied seeds: 1\n")
			c.Assert(first, qt.Contains, "Seeds completed successfully.")
			c.Assert(secondErr, qt.IsNil, qt.Commentf("second seed run:\n%s", second))
			c.Assert(second, qt.Contains, "Applied seeds: 0\nSkipped seeds: 1\n")
			c.Assert(second, qt.Contains, "Database seed data is already up to date.")
			c.Assert(readSorted(c, conn, seedSchema, "regions", "code", "name"), qt.DeepEquals, []map[string]any{
				{"code": "CZ", "name": "Czechia"},
				{"code": "SK", "name": "Slovakia"},
			})
		})
	}
}
