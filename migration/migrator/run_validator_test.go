package migrator_test

import (
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// errRunValidatorRefusal is what [statementValidatorRecorder] answers for the
// statement it rejects in the tests below.
var errRunValidatorRefusal = errors.New("refused by the run's validator")

// refusingAttach is a validator that rejects the ATTACH the histories below
// carry and records every statement it is asked about.
func refusingAttach() *statementValidatorRecorder {
	return &statementValidatorRecorder{reject: "ATTACH DATABASE ':memory:' AS aux", err: errRunValidatorRefusal}
}

// attachHistory creates a at version 1 and b at version 2, whose body then
// attaches another database, in the Atlas layout.
var attachHistory = fstest.MapFS{
	"1_a.sql": {Data: []byte("CREATE TABLE a (id INTEGER PRIMARY KEY);\n")},
	"2_b.sql": {Data: []byte("CREATE TABLE b (id INTEGER PRIMARY KEY);\nATTACH DATABASE ':memory:' AS aux;\n")},
}

// A validator installed on the migrator refuses a run before its first
// statement: the history's first migration, which it accepts, is not applied
// either.
func TestMigratorWithStatementValidator_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "validated.db")
	mig, err := migrator.NewFSMigrator(conn, attachHistory, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	validator := refusingAttach()

	err = mig.WithStatementValidator(validator).MigrateUp(c.Context())

	c.Assert(err, qt.ErrorIs, errRunValidatorRefusal)
	c.Assert(err, qt.ErrorMatches, `(?s).*failed to validate migration SQL in 2_b\.sql: refused by the run's validator.*`)
	c.Assert(sqliteTableNames(c, conn), qt.Not(qt.Contains), "a")
}

// The validator reads a migration built from SQL in code, which no provider
// option reaches, as a run asks it of a migration read from a file.
func TestMigratorWithStatementValidator_FailurePathForMigrationsBuiltInCode(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "built.db")
	provider := migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "a", "CREATE TABLE a (id INTEGER PRIMARY KEY);", "DROP TABLE a;"),
		migrator.CreateMigrationFromSQL(2, "b", "CREATE TABLE b (id INTEGER PRIMARY KEY);\nATTACH DATABASE ':memory:' AS aux;", "DROP TABLE b;"),
	)
	validator := refusingAttach()

	err := migrator.NewMigrator(conn, provider).WithStatementValidator(validator).MigrateUp(c.Context())

	c.Assert(err, qt.ErrorIs, errRunValidatorRefusal)
	c.Assert(err, qt.ErrorMatches, `(?s).*failed to validate migration SQL in b: refused by the run's validator.*`)
	c.Assert(sqliteTableNames(c, conn), qt.Not(qt.Contains), "a")
}

// A rollback asks the validator of the down bodies it runs, before the first
// of them: version 2's down body, which the validator accepts, does not run
// either.
func TestMigratorWithStatementValidator_FailurePathForDownBodies(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "down.db")
	history := fstest.MapFS{
		"0000000001_a.up.sql":   {Data: []byte("CREATE TABLE a (id INTEGER PRIMARY KEY);\n")},
		"0000000001_a.down.sql": {Data: []byte("ATTACH DATABASE ':memory:' AS aux;\nDROP TABLE a;\n")},
		"0000000002_b.up.sql":   {Data: []byte("CREATE TABLE b (id INTEGER PRIMARY KEY);\n")},
		"0000000002_b.down.sql": {Data: []byte("DROP TABLE b;\n")},
	}
	mig, err := migrator.NewFSMigrator(conn, history, migrator.WithMigrationDirFormat(migrationfile.DirFormatPtah))
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	validator := refusingAttach()

	err = mig.WithStatementValidator(validator).MigrateDownTo(c.Context(), 0)

	c.Assert(err, qt.ErrorIs, errRunValidatorRefusal)
	c.Assert(sqliteTableNames(c, conn), qt.Contains, "b")
}

// A run asks the validator of the repeatable migrations it selected before it
// applies a versioned one.
func TestMigratorWithStatementValidator_FailurePathForRepeatables(t *testing.T) {
	c := qt.New(t)
	history := repeatableFiles(map[string]string{"R__aux.sql": "ATTACH DATABASE ':memory:' AS aux;\n"})
	conn, mig := openRepeatableMigrator(c, filepath.Join(c.TempDir(), "repeatable.db"), history)
	validator := refusingAttach()

	err := mig.WithStatementValidator(validator).MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{})

	c.Assert(err, qt.ErrorIs, errRunValidatorRefusal)
	c.Assert(sqliteTableNames(c, conn), qt.Not(qt.Contains), "users")
}

// The control: a validator that accepts every statement is asked about each
// one of every body the run executes, and the run applies them. Installing
// nil removes it, and the history then runs without being asked about.
func TestMigratorWithStatementValidator_HappyPath(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "accepted.db")
	history := fstest.MapFS{
		"1_a.sql": {Data: []byte("CREATE TABLE a (id INTEGER PRIMARY KEY);\n")},
		"2_b.sql": {Data: []byte("CREATE TABLE b (id INTEGER PRIMARY KEY);\nINSERT INTO b (id) VALUES (1);\n")},
	}
	mig, err := migrator.NewFSMigrator(conn, history, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	validator := &statementValidatorRecorder{}
	removed := &statementValidatorRecorder{}

	err = mig.WithStatementValidator(removed).WithStatementValidator(nil).WithStatementValidator(validator).MigrateUp(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(sqliteTableNames(c, conn), qt.Contains, "b")
	c.Assert(validator.statements, qt.Contains, "INSERT INTO b (id) VALUES (1)")
	c.Assert(removed.statements, qt.HasLen, 0)
}

// boundaryNamingValidator names the first statement of every body as the
// body's own transaction control, and records what it validates.
type boundaryNamingValidator struct {
	statementValidatorRecorder
}

func (v *boundaryNamingValidator) TransactionBoundaries([]string) ([]int, error) {
	return []int{0}, nil
}

// A body the run wraps in a transaction of its own has no transaction control
// the validator may name as the body's: a validator that names one is still
// asked about that statement.
func TestMigratorWithStatementValidator_ReadsAWrappedBodyStatementByStatement(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "wrapped.db")
	history := fstest.MapFS{"1_a.sql": {Data: []byte("CREATE TABLE a (id INTEGER PRIMARY KEY);\n")}}
	mig, err := migrator.NewFSMigrator(conn, history, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	validator := &boundaryNamingValidator{}

	err = mig.WithStatementValidator(validator).MigrateUp(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(validator.statements, qt.Contains, "CREATE TABLE a (id INTEGER PRIMARY KEY)")
}
