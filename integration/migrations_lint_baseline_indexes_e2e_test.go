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
			// The column inherits C from its domain, so it declares no collation
			// and the restatement compares against the one it has.
			name:    "control: a domain's collation restated on an indexed column changes nothing",
			base:    "CREATE DOMAIN dc AS text COLLATE \"C\";\nCREATE TABLE notes (id int, body dc);\nCREATE INDEX notes_body ON notes (body);\n",
			change:  "ALTER TABLE notes ALTER COLUMN body TYPE dc COLLATE \"C\";\n",
			wantNot: []string{"] PG301: "},
		},
		{
			// A clause that names no COLLATE gives the column its type's default,
			// which for dc is C, the collation the column has.
			name:    "control: a domain column takes the domain's collation when the clause names none",
			base:    "CREATE DOMAIN dc AS text COLLATE \"C\";\nCREATE TABLE notes (id int, body dc);\nCREATE INDEX notes_body ON notes (body);\n",
			change:  "ALTER TABLE notes ALTER COLUMN body TYPE dc;\n",
			wantNot: []string{"] PG301: "},
		},
		{
			name:   "a domain column's own collation is reset when the clause names none",
			base:   "CREATE DOMAIN dc AS text COLLATE \"C\";\nCREATE TABLE notes (id int, body dc COLLATE \"POSIX\");\nCREATE INDEX notes_body ON notes (body);\n",
			change: "ALTER TABLE notes ALTER COLUMN body TYPE dc;\n",
			want: []string{
				"] PG301: ",
				`changes the collation from "POSIX" to the default collation of dc`,
				"rebuilds every index on the column (notes_body)",
			},
		},
		{
			name:    "control: a name column keeps C when the clause names none",
			base:    "CREATE TABLE notes (id int, body name);\nCREATE INDEX notes_body ON notes (body);\n",
			change:  "ALTER TABLE notes ALTER COLUMN body TYPE name;\n",
			wantNot: []string{"] PG301: "},
		},
		{
			name:    "control: a name[] column keeps C when the clause names none",
			base:    "CREATE TABLE notes (id int, body name[]);\nCREATE INDEX notes_body ON notes (body);\n",
			change:  "ALTER TABLE notes ALTER COLUMN body TYPE name[];\n",
			wantNot: []string{"] PG301: "},
		},
		{
			name:    "control: a dc[] column keeps the domain's collation when the clause names none",
			base:    "CREATE DOMAIN dc AS text COLLATE \"C\";\nCREATE TABLE notes (id int, body dc[]);\nCREATE INDEX notes_body ON notes (body);\n",
			change:  "ALTER TABLE notes ALTER COLUMN body TYPE dc[];\n",
			wantNot: []string{"] PG301: "},
		},
		{
			name:   "default named on a domain column that takes the domain's C rebuilds the index",
			base:   "CREATE DOMAIN dc AS text COLLATE \"C\";\nCREATE TABLE notes (id int, body dc);\nCREATE INDEX notes_body ON notes (body);\n",
			change: "ALTER TABLE notes ALTER COLUMN body TYPE dc COLLATE \"default\";\n",
			want: []string{
				"] PG301: ",
				`changes the collation from "C" to the database default`,
				"rebuilds every index on the column (notes_body)",
			},
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

// TestNativeMigrationsLintReadsAnInheritedCollationFromTheDevDatabaseE2E
// changes the collation of a column that takes its table's. The column
// declares none, so the read reports no collation for it, and the lint
// compares against the one it has rather than against none: without that
// the change reads as a column gaining a collation, which costs nothing.
func TestNativeMigrationsLintReadsAnInheritedCollationFromTheDevDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	ptahBinary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, t.Context(), e2eRepoRoot(t), ptahBinary)
	const base = "CREATE TABLE notes (id int, body varchar(20), KEY notes_body (body)) " +
		"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n"
	tests := []struct {
		name   string
		engine dbtarget.Engine
		change string
		want   string
	}{
		{
			name:   "MySQL copies the table",
			engine: dbtarget.MySQLAdmin,
			change: "ALTER TABLE notes MODIFY body varchar(20) COLLATE utf8mb4_bin;\n",
			want:   "[warning] MY130: MODIFY COLUMN notes.body changes the collation of varchar(20) from utf8mb4_unicode_ci to utf8mb4_bin on a column a key covers",
		},
		{
			name:   "MariaDB changes it in place",
			engine: dbtarget.MariaDBAdmin,
			change: "ALTER TABLE notes MODIFY body varchar(20) COLLATE utf8mb4_bin;\n",
			want:   "[info] MY130P: MODIFY COLUMN notes.body changes the collation of varchar(20) from utf8mb4_unicode_ci to utf8mb4_bin, which MariaDB applies in place",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
			defer cancel()
			scratch := newMySQLFamilyScratch(c, test.engine)
			_, dev := scratch.database(c, "lint_coll")
			dir := c.TempDir()
			writeLintE2EFile(c, dir, "1.sql", base)
			writeLintE2EFile(c, dir, "2.sql", test.change)

			stdout, stderr, err := runLintE2EBinary(ctx, ptahBinary,
				"migrations", "lint", "--dir", dir, "--dir-format", "atlas", "--dev-url", dev,
				"--latest", "1", "--fail-on", "none")

			c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout+stderr, qt.Contains, test.want)
		})
	}
}
