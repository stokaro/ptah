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
	"ptah.run/internal/ydburl"
)

// TestYDBBinary_CheckpointCarriesReferenceRows writes a checkpoint whose
// reference table carries its rows, with the test database as the shadow
// database, and bootstraps a fresh database from it. YQL types every literal,
// so each value is written in the type the replayed table gives its column:
// without those types the checkpoint is refused with `the diff names no type
// for this column`. The rows the fresh database reads back are the rows the
// history produced, a Decimal and a Timestamp included, on both lines.
func TestYDBBinary_CheckpointCarriesReferenceRows(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			migrations := filepath.Join(c.TempDir(), "migrations")
			writeFiles(c, migrations, map[string]string{
				"0000000001_colors.up.sql": "CREATE TABLE `ref/colors` (`id` Int32 NOT NULL, `name` Utf8, " +
					"`weight` Decimal(22,9), `created` Timestamp, PRIMARY KEY (`id`));\n",
				"0000000001_colors.down.sql": "DROP TABLE `ref/colors`;\n",
				"0000000002_rows.up.sql": "UPSERT INTO `ref/colors` (`id`, `name`, `weight`, `created`) VALUES " +
					"(2, 'blue'u, Decimal('1.5', 22, 9), Timestamp('2026-01-02T03:04:05Z')), " +
					"(1, 'red'u, Decimal('0.25', 22, 9), Timestamp('2025-12-31T00:00:00Z'));\n",
				"0000000002_rows.down.sql": "DELETE FROM `ref/colors`;\n",
			})
			hashed, hashErr := runBinary(ctx, binary, "migrations", "hash", "--dir", migrations)
			c.Assert(hashErr, qt.IsNil, qt.Commentf("%s", hashed))
			fresh := enterRealm(c, line)

			written, writeErr := runBinary(ctx, binary, "migrations", "checkpoint", "--migrations-dir", migrations,
				"--shadow-db", url, "--data-table", "ref.colors")
			c.Assert(writeErr, qt.IsNil, qt.Commentf("%s", written))
			applied, upErr := runBinary(ctx, binary, "migrations", "up", "--db-url", fresh, "--migrations-dir", migrations)
			c.Assert(upErr, qt.IsNil, qt.Commentf("%s", applied))
			conn := connect(c, fresh)
			rows, err := conn.QueryContext(ctx,
				"SELECT `id`, `name`, CAST(`weight` AS Utf8), CAST(`created` AS Utf8) FROM `ref/colors` ORDER BY `id`")
			c.Assert(err, qt.IsNil)
			defer func() { c.Check(rows.Close(), qt.IsNil) }()
			var read []string
			for rows.Next() {
				var id int32
				var name, weight, created string
				c.Assert(rows.Scan(&id, &name, &weight, &created), qt.IsNil)
				read = append(read, name+" "+weight+" "+created)
			}
			c.Assert(rows.Err(), qt.IsNil)
			checkpoints, globErr := filepath.Glob(filepath.Join(migrations, "*.checkpoint.up.sql"))
			c.Assert(globErr, qt.IsNil)
			c.Assert(checkpoints, qt.HasLen, 1)
			body, readErr := os.ReadFile(checkpoints[0])

			c.Assert(readErr, qt.IsNil)
			c.Assert(string(body), qt.Contains, "-- ptah:checkpoint-data ref.colors rows=2")
			c.Assert(read, qt.DeepEquals, []string{
				"red 0.25 2025-12-31T00:00:00Z",
				"blue 1.5 2026-01-02T03:04:05Z",
			})
			c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), "ref")
			c.Assert(directoryNames(c, ctx, line, ydburl.RealmDirectory), qt.HasLen, 1)
		})
	}
}
