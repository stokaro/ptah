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

// TestNativeMigrationsLintReadsIndexesAndCollationsFromTheDevDatabaseE2E
// drives the two facts stokaro/ptah#2957 added to the dev-database read
// through a live PostgreSQL server: the indexes on a column and the
// column's declared collation. Neither is in the migration text, and neither
// reaches the rules through any path but the catalog read the replay makes
// before each analyzed version.
//
// Each row is a directory whose second version the rules can judge only with
// the fact in hand, beside a control that holds the fact the other way:
//
//   - a collation change on an indexed column is the index rebuild PG301
//     names; the same change on a column no index reads is a catalog edit
//     and reports nothing under PG301. The tokens are `] CODE: `, the shape
//     the native report prints a finding's code in, since a bare code is a
//     prefix of the info rule PG301P a later change added.
//   - a unique index built under a new name over the columns a dropped index
//     covered is the replacement MF102 names; the same build after dropping
//     an index over other columns is the plain MF101.
func TestNativeMigrationsLintReadsIndexesAndCollationsFromTheDevDatabaseE2E(t *testing.T) {
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

	tests := []struct {
		name    string
		base    string
		change  string
		want    []string
		wantNot []string
	}{
		{
			name:   "a collation change on an indexed column rebuilds the index",
			base:   "CREATE TABLE notes (id int, body text);\nCREATE INDEX notes_body ON notes (body);\n",
			change: "ALTER TABLE notes ALTER COLUMN body TYPE text COLLATE \"C\";\n",
			want: []string{
				"] PG301: ",
				`changes the collation from the database default to "C"`,
				"rebuilds every index on the column (notes_body)",
			},
		},
		{
			name:    "control: the same change on a column no index reads is a catalog edit",
			base:    "CREATE TABLE notes (id int, body text);\nCREATE INDEX notes_id ON notes (id);\n",
			change:  "ALTER TABLE notes ALTER COLUMN body TYPE text COLLATE \"C\";\n",
			wantNot: []string{"] PG301: "},
		},
		{
			name:    "control: the collation restated on an indexed column changes nothing",
			base:    "CREATE TABLE notes (id int, body text COLLATE \"C\");\nCREATE INDEX notes_body ON notes (body);\n",
			change:  "ALTER TABLE notes ALTER COLUMN body TYPE text COLLATE \"C\";\n",
			wantNot: []string{"] PG301: "},
		},
		{
			name:   "a unique index rebuilt under a new name over a dropped index's columns",
			base:   "CREATE TABLE notes (id int, body text);\nCREATE INDEX notes_body_idx ON notes (body);\n",
			change: "DROP INDEX notes_body_idx;\nCREATE UNIQUE INDEX notes_body_uq ON notes (body);\n",
			want: []string{
				"] MF102: ",
				"replaces the index notes_body_idx dropped earlier, which covered the same columns, with a unique one under a new name",
			},
		},
		{
			name:    "control: a dropped index over other columns leaves the plain MF101",
			base:    "CREATE TABLE notes (id int, body text);\nCREATE INDEX notes_id_idx ON notes (id);\n",
			change:  "DROP INDEX notes_id_idx;\nCREATE UNIQUE INDEX notes_body_uq ON notes (body);\n",
			want:    []string{"] MF101: "},
			wantNot: []string{"] MF102: "},
		},
		{
			name:    "control: a unique index the state already holds proves the rows unique",
			base:    "CREATE TABLE notes (id int, body text);\nCREATE UNIQUE INDEX notes_body_old ON notes (body);\n",
			change:  "CREATE UNIQUE INDEX notes_body_uq ON notes (body);\n",
			wantNot: []string{"] MF101: ", "] MF102: "},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			testDBName := fmt.Sprintf("ptah_lint_baseline_indexes_e2e_%d", time.Now().UnixNano())
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
			// The native report goes to stderr, so both streams are read.
			report := stdout + stderr

			c.Assert(exitStatusOf(c, err), qt.Equals, 0)
			for _, want := range test.want {
				c.Assert(report, qt.Contains, want)
			}
			for _, wantNot := range test.wantNot {
				c.Assert(report, qt.Not(qt.Contains), wantNot)
			}
		})
	}
}
