//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestClickHouseIndexKeyComparisonE2E_HappyPath applies a data-skipping index
// to ClickHouse, changes the key its declaration indexes, and applies again.
//
// Measured on master against 26.9.14, each change below reported "Schema is
// synced" and left the old index in place: the comparison read a skipping
// index's type and granularity and nothing of its key. The plan has to
// replace the index, and the second apply of the same schema has to find
// nothing left to do, which it does only if ClickHouse's own formatting of
// the key, such as `(b * 2) + 1`, reads as the key declared.
func TestClickHouseIndexKeyComparisonE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		from     string
		to       string
		wantPlan string
	}{
		{
			name:     "a column",
			from:     "s TYPE minmax GRANULARITY 1",
			to:       "b TYPE minmax GRANULARITY 1",
			wantPlan: "ALTER TABLE `t` DROP INDEX `i_k`;\nALTER TABLE `t` ADD INDEX `i_k` b TYPE minmax GRANULARITY 1;\n",
		},
		{
			name:     "a column against an expression of it",
			from:     "s TYPE minmax GRANULARITY 1",
			to:       "lower(s) TYPE minmax GRANULARITY 1",
			wantPlan: "ALTER TABLE `t` DROP INDEX `i_k`;\nALTER TABLE `t` ADD INDEX `i_k` lower(s) TYPE minmax GRANULARITY 1;\n",
		},
		{
			name:     "the order of a tuple",
			from:     "(s, b) TYPE minmax GRANULARITY 1",
			to:       "(b, s) TYPE minmax GRANULARITY 1",
			wantPlan: "ALTER TABLE `t` DROP INDEX `i_k`;\nALTER TABLE `t` ADD INDEX `i_k` (b, s) TYPE minmax GRANULARITY 1;\n",
		},
		{
			name:     "an expression the server regroups",
			from:     "b * 2 + 1 TYPE minmax GRANULARITY 1",
			to:       "b * 3 + 1 TYPE minmax GRANULARITY 1",
			wantPlan: "ALTER TABLE `t` DROP INDEX `i_k`;\nALTER TABLE `t` ADD INDEX `i_k` b * 3 + 1 TYPE minmax GRANULARITY 1;\n",
		},
		{
			name:     "the key and the settings together",
			from:     "s TYPE minmax GRANULARITY 1",
			to:       "lower(s) TYPE bloom_filter GRANULARITY 4",
			wantPlan: "ALTER TABLE `t` DROP INDEX `i_k`;\nALTER TABLE `t` ADD INDEX `i_k` lower(s) TYPE bloom_filter GRANULARITY 4;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			dbURL := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s", renameInPath)
			from := writeClickHouseIndexSchema(c, "from.sql", test.from)
			to := writeClickHouseIndexSchema(c, "to.sql", test.to)

			created, createdErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", from, "--db-url", dbURL, "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", created, createdErr))

			changed, changedErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", to, "--db-url", dbURL, "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", changed, changedErr))
			c.Assert(changed, qt.Contains, test.wantPlan)

			reapplied, reappliedErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", to, "--db-url", dbURL, "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", reapplied, reappliedErr))
			c.Assert(reapplied, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// writeClickHouseIndexSchema writes a schema file with one MergeTree table
// carrying the skipping index i_k over the key and settings given.
func writeClickHouseIndexSchema(c *qt.C, name, index string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	content := fmt.Sprintf("CREATE TABLE t (id UInt64, s String, b Int64, INDEX i_k %s) ENGINE = MergeTree ORDER BY id;\n", index)
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}

// TestClickHouseIndexKeyComparisonE2E_FailurePath declares a key that names a
// column the table does not declare. The replacement would drop the index and
// then fail to add it, so the apply is refused before it touches the server,
// and the index the server holds stays.
func TestClickHouseIndexKeyComparisonE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	dbURL := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s", renameInPath)
	from := writeClickHouseIndexSchema(c, "from.sql", "s TYPE minmax GRANULARITY 1")
	to := writeClickHouseIndexSchema(c, "to.sql", "sx TYPE minmax GRANULARITY 1")
	created, createdErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", from, "--db-url", dbURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", created, createdErr))

	changed, changedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", to, "--db-url", dbURL, "--auto-approve",
	})
	c.Assert(err, qt.ErrorMatches, `.*index "i_k" reads column "sx", which table t does not declare; .*`,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", changed, changedErr))

	kept, keptErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", from, "--db-url", dbURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", kept, keptErr))
	c.Assert(kept, qt.Equals, "Schema is synced, no changes to be made.\n")
}
