//go:build integration

package atlas_test

import (
	"context"
	"database/sql"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// `ptah-compat migrate apply` runs every file of one run on one session, so a
// setting a file leaves holds for the files after it (stokaro/ptah#5034). The
// directories below set search_path in their second file and create t3
// without a schema in their third. Measured on PostgreSQL 18.6, one run of the
// three files puts t3 in s1 on golang-migrate v4.20.1, through postgres:// and
// pgx5://, and on the pinned community binary v1.3.0, over the golang-migrate
// and the Atlas layouts under --tx-mode file, none and all. A later run
// starts on a new session: `migrate apply 2` followed by `migrate apply`, and
// golang-migrate's `up 2` followed by `up`, put t3 in public on both.
//
// Where t3 lands is read back from the catalog, because the run exits 0
// either way.

// golangMigrateSearchPathFiles is the directory in golang-migrate's layout.
var golangMigrateSearchPathFiles = map[string]string{
	"1_init.up.sql":   "CREATE SCHEMA s1;\n",
	"1_init.down.sql": "DROP SCHEMA s1 CASCADE;\n",
	"2_set.up.sql":    "SET search_path = s1;\nCREATE TABLE t2 (id int);\n",
	"2_set.down.sql":  "DROP TABLE s1.t2;\n",
	"3_next.up.sql":   "CREATE TABLE t3 (id int);\n",
	"3_next.down.sql": "DROP TABLE t3;\n",
}

// atlasSearchPathFiles is the same directory in Atlas's layout.
var atlasSearchPathFiles = map[string]string{
	"1_init.sql": "CREATE SCHEMA s1;\n",
	"2_set.sql":  "SET search_path = s1;\nCREATE TABLE t2 (id int);\n",
	"3_next.sql": "CREATE TABLE t3 (id int);\n",
}

// applySearchPathDirectory hashes files as layout, creates a fresh database
// and returns it with a function that applies the directory to it with the
// arguments given after `migrate apply`.
func applySearchPathDirectory(
	c *qt.C,
	layout string,
	files map[string]string,
) (*sql.DB, func(args ...string) (string, error)) {
	c.Helper()
	dir := writeHashedLayoutDirectory(c, layout, files)
	dbURL := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), nil)
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	apply := func(args ...string) (string, error) {
		return runCompat(c, slices.Concat(
			[]string{"migrate", "apply", "--dir", "file://" + dir + "?format=" + layout, "--url", dbURL},
			args,
		)...)
	}
	return db, apply
}

// schemaOfTable returns the schema of the table called name, or the empty
// string when there is none.
func schemaOfTable(c *qt.C, db *sql.DB, name string) string {
	c.Helper()
	var schema string
	c.Assert(db.QueryRowContext(context.Background(),
		"SELECT COALESCE(max(schemaname), '') FROM pg_tables WHERE tablename = $1", name,
	).Scan(&schema), qt.IsNil)
	return schema
}

func TestMigrateApplySessionStateAcrossFiles_HappyPathLive(t *testing.T) {
	tests := []struct {
		name   string
		layout string
		files  map[string]string
		args   []string
	}{
		{name: "golang-migrate", layout: "golang-migrate", files: golangMigrateSearchPathFiles},
		{
			name:   "golang-migrate under --tx-mode none",
			layout: "golang-migrate",
			files:  golangMigrateSearchPathFiles,
			args:   []string{"--tx-mode", "none"},
		},
		{name: "atlas", layout: "atlas", files: atlasSearchPathFiles},
		// The control: one transaction holds every file, so one session does.
		{
			name:   "golang-migrate under --tx-mode all",
			layout: "golang-migrate",
			files:  golangMigrateSearchPathFiles,
			args:   []string{"--tx-mode", "all"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, apply := applySearchPathDirectory(c, test.layout, test.files)

			out, err := apply(test.args...)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
			c.Assert(schemaOfTable(c, db, "t2"), qt.Equals, "s1")
			c.Assert(schemaOfTable(c, db, "t3"), qt.Equals, "s1")
		})
	}
}

// The session ends with the run, so the run that applies the third file
// alone starts with the connection's own search_path.
func TestMigrateApplySessionStateEndsWithTheRun_HappyPathLive(t *testing.T) {
	c := qt.New(t)
	db, apply := applySearchPathDirectory(c, "golang-migrate", golangMigrateSearchPathFiles)

	out, err := apply("2")
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	out, err = apply()

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	c.Assert(schemaOfTable(c, db, "t2"), qt.Equals, "s1")
	c.Assert(schemaOfTable(c, db, "t3"), qt.Equals, "public")
}
