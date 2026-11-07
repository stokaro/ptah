package importer_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/importer"
	"ptah.run/migration/migrationfile"
)

// A rollback the source states it does not have is written so that a rollback
// through it runs nothing. A rollback the source does not mention is written as a
// placeholder a rollback refuses: the import cannot tell a change with nothing to
// undo from a rollback nobody wrote.
func TestLiquibaseImport_KeepsWhetherTheSourceStatesItHasNoRollback(t *testing.T) {
	tests := []struct {
		name     string
		files    fstest.MapFS
		wantDown string
		// wantDeclared is whether the down file reads as stating it has none.
		wantDeclared bool
	}{
		{
			name:         "a formatted changeset with no rollback line",
			files:        fstest.MapFS{"changelog.sql": {Data: []byte("--liquibase formatted sql\n--changeset s:1\nSELECT 1;\n")}},
			wantDown:     "-- No rollback was provided by the source migration.\n",
			wantDeclared: false,
		},
		{
			name:         "a formatted --rollback empty",
			files:        fstest.MapFS{"changelog.sql": {Data: []byte("--liquibase formatted sql\n--changeset s:1\nSELECT 1;\n--rollback empty\n")}},
			wantDown:     "-- +ptah empty_rollback\n-- The source migration states that it has no rollback.\n",
			wantDeclared: true,
		},
		{
			name:         "a formatted --rollback not required",
			files:        fstest.MapFS{"changelog.sql": {Data: []byte("--liquibase formatted sql\n--changeset s:1\nSELECT 1;\n--rollback not required\n")}},
			wantDown:     "-- +ptah empty_rollback\n-- The source migration states that it has no rollback.\n",
			wantDeclared: true,
		},
		{
			name:         "an XML sql change with no rollback",
			files:        liquibaseXMLOneChangeSet(`<sql>SELECT 1</sql>`),
			wantDown:     "-- No rollback was provided by the source migration.\n",
			wantDeclared: false,
		},
		{
			name:         "an XML sql change with an empty rollback",
			files:        liquibaseXMLOneChangeSet(`<sql>SELECT 1</sql><rollback/>`),
			wantDown:     "-- +ptah empty_rollback\n-- The source migration states that it has no rollback.\n",
			wantDeclared: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out := c.TempDir()

			result, err := importer.Import(c.Context(), test.files, nil, out, importer.Options{})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Files, qt.HasLen, 2)
			down, err := os.ReadFile(filepath.Join(out, result.Files[1]))
			c.Assert(err, qt.IsNil)
			c.Assert(string(down), qt.Equals, test.wantDown)
			declared, err := migrationfile.DeclaresEmptyRollback(string(down), "postgres")
			c.Assert(err, qt.IsNil)
			c.Assert(declared, qt.Equals, test.wantDeclared)
		})
	}
}

// TestLiquibaseImport_AStatementRollbackIsNotAStatement is the control: an empty
// flag on a rollback that holds SQL would run neither.
func TestLiquibaseImport_AStatementRollbackIsNotAStatement(t *testing.T) {
	c := qt.New(t)
	out := c.TempDir()

	result, err := importer.Import(c.Context(), fstest.MapFS{
		"changelog.sql": {Data: []byte("--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t (id int);\n--rollback DROP TABLE t;\n")},
	}, nil, out, importer.Options{})

	c.Assert(err, qt.IsNil)
	down, err := os.ReadFile(filepath.Join(out, result.Files[1]))
	c.Assert(err, qt.IsNil)
	c.Assert(string(down), qt.Equals, "DROP TABLE t;\n")
}

// TestLiquibaseImport_ANoTransactionChangesetStatesBoth keeps the directive the
// importer adds to a no-transaction changeset beside the statement. Both sit in the
// header, so the file loads and both take effect.
func TestLiquibaseImport_ANoTransactionChangesetStatesBoth(t *testing.T) {
	c := qt.New(t)
	out := c.TempDir()

	result, err := importer.Import(c.Context(), fstest.MapFS{
		"changelog.sql": {Data: []byte("--liquibase formatted sql\n--changeset s:1 runInTransaction:false\nSELECT 1;\n--rollback empty\n")},
	}, nil, out, importer.Options{})

	c.Assert(err, qt.IsNil)
	down, err := os.ReadFile(filepath.Join(out, result.Files[1]))
	c.Assert(err, qt.IsNil)
	declared, err := migrationfile.DeclaresEmptyRollback(string(down), "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(declared, qt.IsTrue)
	c.Assert(migrationfile.ParseDirectives(string(down))[migrationfile.DirectiveNoTransaction], qt.Equals, "true")
	_, err = migrationfile.ParseTimeouts(string(down))
	c.Assert(err, qt.IsNil)
}

// TestLiquibaseParse_AnEmptyXMLRollbackStatesIt pins the difference an empty DownSQL
// cannot carry, on the parse a typed changeset takes. A rollback holding SQL states
// nothing of the kind.
func TestLiquibaseParse_AnEmptyXMLRollbackStatesIt(t *testing.T) {
	tests := []struct {
		name     string
		rollback string
		wantDown string
		want     bool
	}{
		{name: "an empty rollback", rollback: `<rollback/>`, want: true},
		{name: "a rollback holding SQL", rollback: `<rollback>DROP TABLE t;</rollback>`, wantDown: "DROP TABLE t;", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := parseLiquibaseFor(c, "postgres", liquibaseXMLOneChangeSet(
				`<createTable tableName="t"><column name="a" type="int"/></createTable>`+test.rollback,
			))

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Migrations[0].DownSQL, qt.Equals, test.wantDown)
			c.Assert(parsed.Migrations[0].EmptyRollback, qt.Equals, test.want)
		})
	}
}
