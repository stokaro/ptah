package atlasmigrate

// White-box testing required: the rule is enforced before publication by an
// unexported validator, and the property under test — that nothing is written
// when it refuses — cannot be stated from the artifacts, which do not exist.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasmigrateimport"
)

// golangMigrateRow is one migration shape and whether golang-migrate can run it
// outside a transaction.
type golangMigrateRow struct {
	name    string
	content MigrationFileContent
	wantErr string
}

func TestGolangMigrateAcceptsOnlyTheShapeItCanRun(t *testing.T) {
	rows := []golangMigrateRow{{
		// Measured against golang-migrate v4.19.0: a migration holding one
		// statement runs unwrapped, and the CREATE INDEX CONCURRENTLY in it
		// applies.
		name: "one forward statement is what the format can run",
		content: MigrationFileContent{
			NoTransaction: true,
			Statements:    []string{"CREATE INDEX CONCURRENTLY idx ON widgets (id)"},
		},
	}, {
		// The same statement beside another is sent as one multi-statement
		// query, which PostgreSQL treats as an implicit transaction block.
		name: "two forward statements cannot be run outside one",
		content: MigrationFileContent{
			NoTransaction: true,
			Statements: []string{
				"CREATE TABLE widgets (id INTEGER)",
				"CREATE INDEX CONCURRENTLY idx ON widgets (id)",
			},
		},
		wantErr: `.*holds one statement, and this forward migration holds 2.*`,
	}, {
		// The two directions are separate files, so each is judged alone.
		name: "one rollback statement is judged on its own file",
		content: MigrationFileContent{
			ReverseNoTransaction: true,
			Statements:           []string{"a", "b", "c"},
			ReverseStatements:    []string{"DROP INDEX CONCURRENTLY idx"},
		},
	}, {
		name: "two rollback statements cannot be run outside one",
		content: MigrationFileContent{
			ReverseNoTransaction: true,
			ReverseStatements:    []string{"DROP INDEX CONCURRENTLY idx", "DROP TABLE widgets"},
		},
		wantErr: `.*holds one statement, and this rollback holds 2.*`,
	}, {
		// A migration with no requirement is not judged at all: an ordinary
		// multi-statement migration is exactly what this format usually holds.
		name: "no requirement leaves an ordinary migration alone",
		content: MigrationFileContent{
			Statements: []string{"a", "b", "c"},
		},
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			err := validateForeignTransactionMode(atlasmigrateimport.FormatGolangMigrate, row.content)

			assertTransactionModeError(c, err, row.wantErr)
		})
	}
}

// assertTransactionModeError keeps the conditional out of the table loop.
func assertTransactionModeError(c *qt.C, err error, want string) {
	c.Helper()
	if want == "" {
		c.Assert(err, qt.IsNil)
		return
	}
	c.Assert(err, qt.ErrorMatches, want)
}

// TestGolangMigrateWritesEachNoTransactionStatementAsAMigration pins the shape
// that makes the one-statement rule workable: a file that must run outside a
// transaction and holds several statements becomes one migration per statement,
// in order, and every other file is left alone.
func TestGolangMigrateWritesEachNoTransactionStatementAsAMigration(t *testing.T) {
	c := qt.New(t)
	contents := []MigrationFileContent{
		{NameSuffix: "_transactional", SQL: "a;\nb;\n", Statements: []string{"a;", "b;"}},
		{
			NameSuffix: "_constraint_validations", NoTransaction: true,
			Statements: []string{`ALTER TABLE "t" VALIDATE CONSTRAINT "c1";`, `ALTER TABLE "t" VALIDATE CONSTRAINT "c2";`},
		},
	}

	split, err := splitNoTransactionStatements(contents, "postgres", "{{ sql . }}")

	c.Assert(err, qt.IsNil)
	c.Assert(split, qt.HasLen, 3)
	c.Assert(split[0], qt.DeepEquals, contents[0])
	c.Assert(split[1].NameSuffix, qt.Equals, "_constraint_validations_1")
	c.Assert(split[1].Statements, qt.DeepEquals, []string{`ALTER TABLE "t" VALIDATE CONSTRAINT "c1";`})
	c.Assert(split[1].SQL, qt.Contains, `VALIDATE CONSTRAINT "c1"`)
	c.Assert(split[1].SQL, qt.Not(qt.Contains), `"c2"`)
	c.Assert(split[2].NameSuffix, qt.Equals, "_constraint_validations_2")
	c.Assert(split[2].NoTransaction, qt.IsTrue)
	for _, content := range split {
		c.Assert(validateForeignTransactionMode(atlasmigrateimport.FormatGolangMigrate, content), qt.IsNil)
	}
}

// TestReverseStaysOnTheLastFileThatCanUndoSomething pins where the rollback of
// a run lands when validations end it. A validation has no rollback, so the
// reverse of the run belongs to the file before them: `down` of one step would
// otherwise run the whole reverse while the migrator still records the files
// before it as applied.
func TestReverseStaysOnTheLastFileThatCanUndoSomething(t *testing.T) {
	c := qt.New(t)
	contents := []MigrationFileContent{
		{NameSuffix: "_transactional"},
		{NameSuffix: validationSuffix + "_1"},
		{NameSuffix: validationSuffix + "_2"},
	}

	attached, err := attachReversePlan(contents, "postgres", []string{`ALTER TABLE "c" DROP COLUMN "note";`}, "{{ sql . }}", false)

	c.Assert(err, qt.IsNil)
	c.Assert(attached[0].DownSQL, qt.Contains, "DROP COLUMN")
	c.Assert(attached[1].DownSQL, qt.Equals, "")
	c.Assert(attached[2].DownSQL, qt.Equals, "")
}
