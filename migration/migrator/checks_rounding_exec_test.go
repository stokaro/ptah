package migrator_test

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// roundedValuesCheck is the shape `migrations generate` writes before a type change
// that rounds, guarding a body that drops users; it holds while users is empty, so
// a seeded row stands for a value the change would round.
const roundedValuesCheck = `-- +ptah check kind=rounded-values name="users.id" assert="SELECT count(*) = 0 FROM users"` + "\n"

// ordinaryCheck is a check of no kind beside it, which holds while audit is empty.
const ordinaryCheck = `-- +ptah check name="audit_empty" assert="SELECT count(*) = 0 FROM audit"` + "\n"

// roundingFixture is a SQLite database holding users and audit, the rows each
// seeded as the test says, with one migration whose up body is up and whose down
// body is down.
type roundingFixture struct {
	usersRows, auditRows int
	up, down             string
}

func (f roundingFixture) open(c *qt.C) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(c.TempDir(), "rounding.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	for _, statement := range []string{"CREATE TABLE users (id INTEGER PRIMARY KEY)", "CREATE TABLE audit (id INTEGER PRIMARY KEY)"} {
		_, err = conn.Exec(statement)
		c.Assert(err, qt.IsNil)
	}
	for i := range f.usersRows {
		_, err = conn.Exec("INSERT INTO users (id) VALUES (?)", i+1)
		c.Assert(err, qt.IsNil)
	}
	for i := range f.auditRows {
		_, err = conn.Exec("INSERT INTO audit (id) VALUES (?)", i+1)
		c.Assert(err, qt.IsNil)
	}
	migration := migrator.CreateMigrationFromSQL(1, "convert", f.up, f.down)
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))
	c.Assert(m.Initialize(ctx), qt.IsNil)
	return conn, m
}

// TestMigrateUp_RoundedValuesCheckRefuses holds a failed check of kind rounded-values to
// the refusal that says what the rows hold and how the run asks for the change, when the
// caller names a way to ask, and leaves the body unrun.
func TestMigrateUp_RoundedValuesCheckRefuses(t *testing.T) {
	tests := []struct {
		name    string
		request string
		want    string
	}{
		{"a way to ask", "--allow-value-rounding",
			`pre-migration check users\.id for migration 1 was not satisfied: the migration converts a column to a type ` +
				`that keeps fewer digits after the point while the table holds a value with more digits, which the server ` +
				`rounds or cuts without an error; store values the new type keeps, or ask for the change with ` +
				`--allow-value-rounding \(assert: SELECT count\(\*\) = 0 FROM users\)`},
		{"no way to ask", "",
			`pre-migration check users\.id for migration 1 was not satisfied: .*without an error; store values the new ` +
				`type keeps \(assert: SELECT count\(\*\) = 0 FROM users\)`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, m := roundingFixture{usersRows: 1, up: roundedValuesCheck + "DROP TABLE users;\n"}.open(c)

			err := m.WithValueRounding(migrator.ValueRounding{Request: test.request}).MigrateUp(context.Background())

			var checkErr *migrator.CheckFailedError
			c.Assert(err, qt.ErrorAs, &checkErr)
			c.Assert(checkErr.Kind, qt.Equals, migrator.CheckKindRoundedValues)
			c.Assert(checkErr, qt.ErrorMatches, test.want)
			c.Assert(tableExists(c, conn, "users"), qt.IsTrue)
		})
	}
}

// TestMigrateUp_AllowedValueRoundingSkipsTheRoundedValuesCheck applies the body a failing
// check of kind rounded-values guards once the run allows the rounding, warns of the
// skipped check, and still evaluates the ordinary check beside it, which holds here.
func TestMigrateUp_AllowedValueRoundingSkipsTheRoundedValuesCheck(t *testing.T) {
	c := qt.New(t)
	conn, m := roundingFixture{usersRows: 1, up: roundedValuesCheck + ordinaryCheck + "DROP TABLE users;\n"}.open(c)
	var log bytes.Buffer

	err := m.WithValueRounding(migrator.ValueRounding{Allowed: true, Request: "--allow-value-rounding"}).
		WithLogger(slog.New(slog.NewTextHandler(&log, nil))).
		MigrateUp(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(tableExists(c, conn, "users"), qt.IsFalse)
	c.Assert(log.String(), qt.Contains, "pre-migration check users.id of migration 1 is skipped, so migration 1 "+
		"converts a column to a type that keeps fewer digits after the point without a scan of the values the table holds")
}

// TestMigrateUp_AllowedValueRoundingKeepsOrdinaryChecks is the control on the waiver: a
// run that allows the rounding still refuses on an ordinary check that fails, so the
// waiver reaches the kind and nothing else.
func TestMigrateUp_AllowedValueRoundingKeepsOrdinaryChecks(t *testing.T) {
	c := qt.New(t)
	conn, m := roundingFixture{auditRows: 1, up: roundedValuesCheck + ordinaryCheck + "DROP TABLE users;\n"}.open(c)

	err := m.WithValueRounding(migrator.ValueRounding{Allowed: true}).MigrateUp(context.Background())

	var checkErr *migrator.CheckFailedError
	c.Assert(err, qt.ErrorAs, &checkErr)
	c.Assert(checkErr.Name, qt.Equals, "audit_empty")
	c.Assert(checkErr.Kind, qt.Equals, migrator.CheckKind(""))
	c.Assert(tableExists(c, conn, "users"), qt.IsTrue)
}

// TestMigrateDown_AllowedValueRoundingSkipsTheRoundedValuesCheck holds a rollback to the
// same waiver: a down body whose check of kind rounded-values fails runs once the run
// allows the rounding, with the same warning.
func TestMigrateDown_AllowedValueRoundingSkipsTheRoundedValuesCheck(t *testing.T) {
	c := qt.New(t)
	conn, m := roundingFixture{
		up:   "CREATE TABLE converted (id INTEGER PRIMARY KEY);\n",
		down: roundedValuesCheck + "DROP TABLE converted;\n",
	}.open(c)
	c.Assert(m.MigrateUp(context.Background()), qt.IsNil)
	_, err := conn.Exec("INSERT INTO users (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	var log bytes.Buffer

	err = m.WithValueRounding(migrator.ValueRounding{Allowed: true}).
		WithLogger(slog.New(slog.NewTextHandler(&log, nil))).
		MigrateDown(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(tableExists(c, conn, "converted"), qt.IsFalse)
	c.Assert(log.String(), qt.Contains, "pre-migration check users.id of migration 1 is skipped")
}

// TestMigrateDown_RoundedValuesCheckRefuses is the control on the rollback waiver: the
// same rollback without it refuses on the check.
func TestMigrateDown_RoundedValuesCheckRefuses(t *testing.T) {
	c := qt.New(t)
	conn, m := roundingFixture{
		up:   "CREATE TABLE converted (id INTEGER PRIMARY KEY);\n",
		down: roundedValuesCheck + "DROP TABLE converted;\n",
	}.open(c)
	c.Assert(m.MigrateUp(context.Background()), qt.IsNil)
	_, err := conn.Exec("INSERT INTO users (id) VALUES (1)")
	c.Assert(err, qt.IsNil)

	err = m.MigrateDown(context.Background())

	var checkErr *migrator.CheckFailedError
	c.Assert(err, qt.ErrorAs, &checkErr)
	c.Assert(checkErr.Kind, qt.Equals, migrator.CheckKindRoundedValues)
	c.Assert(tableExists(c, conn, "converted"), qt.IsTrue)
}
