package atlasmigrate

// White-box testing required: the rule is enforced before publication by an
// unexported validator, and the property under test — that nothing is written
// when it refuses — cannot be stated from the artifacts, which do not exist.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
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
