//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
)

// TestNativeMigrationsLintReadsAnEarlierPartitionedParentE2E drives PG108
// through the shipped binary against PostgreSQL (stokaro/ptah#3588).
//
// The parent is created by the first migration, so nothing in the second says
// it is partitioned; only the catalog the replay reads before the second
// version can. Each row moves one fact away from the reporting case:
//
//   - the same table left ordinary keeps PG101 and its CONCURRENTLY advice;
//   - the remedy PG108 names, run as the second migration, replays, and
//     neither rule reports a step of it.
func TestNativeMigrationsLintReadsAnEarlierPartitionedParentE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	nativeBinary := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, nativeBinary)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	const partitioned = "CREATE TABLE measurements (id bigint NOT NULL, taken_on date NOT NULL, value int) PARTITION BY RANGE (taken_on);\n" +
		"CREATE TABLE measurements_2025 PARTITION OF measurements FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');\n" +
		"CREATE TABLE measurements_2026 PARTITION OF measurements FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');\n"
	tests := []struct {
		name    string
		base    string
		change  string
		want    []string
		wantNot []string
	}{
		{
			name:   "an index on the parent",
			base:   partitioned,
			change: "CREATE INDEX m_value ON measurements (value);\n",
			want: []string{
				"] PG108: building this index takes a SHARE lock on measurements and on every one of its partitions",
				"attach it to a parent index created with ON ONLY",
			},
			wantNot: []string{"] PG101: "},
		},
		{
			name:    "control: the same index on the table left ordinary",
			base:    "CREATE TABLE measurements (id bigint NOT NULL, taken_on date NOT NULL, value int);\n",
			change:  "CREATE INDEX m_value ON measurements (value);\n",
			want:    []string{"] PG101: CREATE INDEX without CONCURRENTLY blocks writes"},
			wantNot: []string{"] PG108: "},
		},
		{
			name: "the remedy PG108 names",
			base: partitioned,
			change: "-- atlas:txmode none\n\n" +
				"CREATE INDEX m_value ON ONLY measurements (value);\n" +
				"CREATE INDEX CONCURRENTLY m_value_2025 ON measurements_2025 (value);\n" +
				"ALTER INDEX m_value ATTACH PARTITION m_value_2025;\n" +
				"CREATE INDEX CONCURRENTLY m_value_2026 ON measurements_2026 (value);\n" +
				"ALTER INDEX m_value ATTACH PARTITION m_value_2026;\n",
			wantNot: []string{"] PG101: ", "] PG108: "},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			testDBName := fmt.Sprintf("ptah_lint_partitioned_e2e_%d", time.Now().UnixNano())
			createE2EDatabase(c, ctx, adminDB, testDBName)
			defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

			migrationsDir := c.TempDir()
			writeLintE2EFile(c, migrationsDir, "1.sql", test.base)
			writeLintE2EFile(c, migrationsDir, "2.sql", test.change)

			stdout, stderr, err := runLintE2EBinary(ctx, nativeBinary,
				"migrations", "lint",
				"--dir", migrationsDir,
				"--dir-format", "atlas",
				"--dev-url", replaceDatabaseName(c, dbURL, testDBName),
				"--latest", "1",
				"--fail-on", "none",
			)

			c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			// The dev database was read, so no rule went without the state it asked for.
			c.Assert(stderr, qt.Not(qt.Contains), "ran without the")
			for _, want := range test.want {
				c.Assert(stdout, qt.Contains, want)
			}
			for _, wantNot := range test.wantNot {
				c.Assert(stdout, qt.Not(qt.Contains), wantNot)
			}
		})
	}
}
