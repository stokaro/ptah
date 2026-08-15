package atlas_test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/migration/migrator"
)

// This file is the DROP half of stokaro/ptah#1013's coverage. Every other
// fixture in this package's `migrate diff` tests is CREATE-only, and the
// reverse of a create is a DROP TABLE against either schema — so none of them
// can tell a reverse planned against the pre-change state from one planned
// against the desired state, and none of them exercises what a rollback has to
// REBUILD.
//
// The fixture here starts from a directory that already created two tables and
// asks for one of them to go away. That makes the forward half a DROP and the
// rollback half a CREATE, which is the only shape where the two ways of getting
// the reverse wrong become visible:
//
//   - planned against `desired`, the rollback has nothing to re-create and the
//     `.down.sql` is written EMPTY; and
//   - planned without the rule that a re-created table brings its own
//     constraints, the rollback repeats them and the server refuses it.

// compatDroppedTableFixture writes a hashed golang-migrate directory that
// already created `widgets` and `gadgets` — the latter with a primary key and a
// foreign key — and returns it with a desired-state file that no longer
// declares `gadgets`.
func compatDroppedTableFixture(tb testing.TB) (dir, target string) {
	c := qt.New(tb)
	c.Helper()
	dir = filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "1_init.up.sql"),
		[]byte(compatDroppedTableSchema),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "1_init.down.sql"),
		[]byte("DROP TABLE gadgets;\nDROP TABLE widgets;\n"),
		0o600,
	), qt.IsNil)
	_, _, err := runCompat("migrate", "hash", "--dir", "file://"+dir+"?format=golang-migrate")
	c.Assert(err, qt.IsNil)

	target = filepath.Join(c.TempDir(), "target.sql")
	c.Assert(os.WriteFile(
		target,
		[]byte("CREATE TABLE widgets (id INTEGER NOT NULL PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	return dir, target
}

const compatDroppedTableSchema = "CREATE TABLE widgets (id INTEGER NOT NULL PRIMARY KEY);\n" +
	"CREATE TABLE gadgets (\n" +
	"  id INTEGER NOT NULL PRIMARY KEY,\n" +
	"  widget_id INTEGER CONSTRAINT gadgets_widget_fk REFERENCES widgets (id)\n" +
	");\n"

// TestCompatMigrateDiff_ForeignLayoutRollbackRebuildsTheDroppedTable is the
// completion criterion for the rollback half of `migrate diff` on a foreign
// layout, and it is applied rather than read.
//
// The forward half drops `gadgets`. The rollback half therefore has to CREATE
// it, and the assertion is that a database taken through both halves ends up
// holding the table again, with the foreign key it had. A rollback planned
// against the desired state leaves an empty `.down.sql` here, which the
// execution step reports as the table never coming back.
func TestCompatMigrateDiff_ForeignLayoutRollbackRebuildsTheDroppedTable(t *testing.T) {
	c := qt.New(t)
	dir, target := compatDroppedTableFixture(c.TB)

	_, _, err := runCompat("migrate", "diff", "drop",
		"--dir", "file://"+dir+"?format=golang-migrate",
		"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
		"--to", "file://"+target)
	c.Assert(err, qt.IsNil)

	names := atlasDirEntryNames(c.TB, dir)
	forward := compatReadMigrationFile(c.TB, dir, compatNewestNameWithSuffix(c.TB, names, ".up.sql"))
	c.Assert(strings.ToUpper(forward), qt.Contains, "DROP TABLE",
		qt.Commentf("the forward half must be the drop this fixture asks for"))

	rollbackName := compatNewestNameWithSuffix(c.TB, names, ".down.sql")
	rollback := compatReadMigrationFile(c.TB, dir, rollbackName)
	c.Assert(strings.ToUpper(rollback), qt.Contains, "CREATE TABLE",
		qt.Commentf("%s carries no rebuild; a reverse planned against the desired "+
			"state has nothing to re-create:\n%s", rollbackName, rollback))

	// Rendered is not applied. Take a database through the whole history and
	// then through the rollback, and read the catalog back.
	dbPath := filepath.Join(c.TempDir(), "target.db")
	compatExecSQL(c.TB, dbPath, compatDroppedTableSchema, "SEED")
	compatExecSQL(c.TB, dbPath, forward, "FORWARD")
	c.Assert(compatTableNames(c.TB, dbPath), qt.Not(qt.Contains), "gadgets",
		qt.Commentf("the forward half must really drop the table, or the rollback proves nothing"))

	compatExecSQL(c.TB, dbPath, rollback, "ROLLBACK")
	c.Assert(compatTableNames(c.TB, dbPath), qt.Contains, "gadgets",
		qt.Commentf("rollback SQL:\n%s", rollback))
	c.Assert(compatForeignKeyNames(c.TB, dbPath, "gadgets"), qt.Contains, "gadgets_widget_fk",
		qt.Commentf("the rollback must restore the table's foreign key:\n%s", rollback))
}

func compatReadMigrationFile(tb testing.TB, dir, name string) string {
	c := qt.New(tb)
	c.Helper()
	contents, err := os.ReadFile(filepath.Join(dir, name))
	c.Assert(err, qt.IsNil)
	return string(contents)
}

// compatExecSQL applies every statement of a migration half to a SQLite
// database, failing on the first one the engine refuses. A half that renders
// and does not apply is the failure this test exists to catch.
func compatExecSQL(tb testing.TB, dbPath, sqlText, label string) {
	c := qt.New(tb)
	c.Helper()
	conn := compatConnect(c.TB, dbPath)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range migrator.SplitSQLStatements(sqlText) {
		compatExecStatement(c.TB, conn, statement, label)
	}
}

func compatExecStatement(tb testing.TB, conn *dbschema.DatabaseConnection, statement, label string) {
	c := qt.New(tb)
	c.Helper()
	trimmed := strings.TrimSpace(statement)
	c.Assert(compatExecNonEmpty(conn, trimmed), qt.IsNil,
		qt.Commentf("%s statement must apply cleanly:\n%s", label, trimmed))
}

func compatExecNonEmpty(conn *dbschema.DatabaseConnection, statement string) error {
	if statement == "" {
		return nil
	}
	_, err := conn.Exec(statement)
	return err
}

func compatConnect(tb testing.TB, dbPath string) *dbschema.DatabaseConnection {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), atlasurl.SQLiteURLFromPath(dbPath))
	c.Assert(err, qt.IsNil)
	return conn
}

func compatForeignKeyNames(tb testing.TB, dbPath, table string) []string {
	c := qt.New(tb)
	c.Helper()
	var names []string
	for _, constraint := range compatReadSchema(c.TB, dbPath).Constraints {
		names = append(names, compatForeignKeyName(constraint, table)...)
	}
	slices.Sort(names)
	return names
}

func compatForeignKeyName(constraint dbschematypes.DBConstraint, table string) []string {
	if constraint.Type != "FOREIGN KEY" || constraint.TableName != table {
		return nil
	}
	return []string{constraint.Name}
}

func compatReadSchema(tb testing.TB, dbPath string) *dbschematypes.DBSchema {
	c := qt.New(tb)
	c.Helper()
	conn := compatConnect(c.TB, dbPath)
	defer dbschema.CloseAndWarn(conn)
	schema, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	return schema
}
