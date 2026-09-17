//go:build integration

package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/shadow"
)

// A MySQL shadow database is a different database, and on this engine the
// default schema IS the database.
//
// The shadow semantics check compared that field, so it refused every shadow a
// caller could give it: a differently named one failed here, and a same-named
// one on another host failed the distinctness guard instead. Between them they
// covered every possibility, and `migrations baseline --shadow-db` could not
// succeed against MySQL at all. PostgreSQL never showed it, because both sides
// answer `public` there (stokaro/ptah#3375).
//
// A live server rather than a fixture because the value under test is what the
// connection reports, and only a server reports it.

// mysqlShadowMigration is the one migration both databases are checked against.
// Its content is not the subject; whether the check gets far enough to compare
// the replayed schema at all is.
const mysqlShadowMigration = "CREATE TABLE ptah_shadow_semantics (id INT NOT NULL, PRIMARY KEY (id));\n"

// TestShadowIdentifierSemantics_MySQLAcceptsAnotherDatabase drives the baseline
// verification the adopt path runs, on the shape a caller has: a target holding
// the tables the migration produces, and a shadow database of another name on
// the same server.
func TestShadowIdentifierSemantics_MySQLAcceptsAnotherDatabase(t *testing.T) {
	c := qt.New(t)

	targetURL := mySQLScratchDatabaseURL(c, "ptah_3375_target")
	shadowURL := mySQLScratchDatabaseURL(c, "ptah_3375_shadow")

	target, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })

	_, err = target.ExecContext(c.Context(), mysqlShadowMigration)
	c.Assert(err, qt.IsNil)

	migrationsDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_init.up.sql"),
		[]byte(mysqlShadowMigration),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE ptah_shadow_semantics;\n"),
		0o600,
	), qt.IsNil)

	info := target.Info()

	err = shadow.VerifyBaseline(c.Context(), shadow.BaselineVerifyOptions{
		ShadowDatabaseURL: shadowURL,
		TargetConn:        target,
		MigrationsDir:     migrationsDir,
		Version:           1,
		Dialect:           info.Dialect,
		Capabilities:      info.Capabilities,
	})

	c.Assert(err, qt.IsNil)
}

// TestShadowIdentifierSemantics_MySQLStillRefusesAMismatch is the control. The
// check must still refuse a shadow whose rules differ, or the case above would
// be satisfied by a check that compares nothing.
//
// The difference is the schema the migration produces rather than a collation:
// a shadow the migrations do not fill the same way is what the verification
// exists to catch, and it is the half that must survive.
func TestShadowIdentifierSemantics_MySQLStillRefusesAMismatch(t *testing.T) {
	c := qt.New(t)

	targetURL := mySQLScratchDatabaseURL(c, "ptah_3375_ctl_target")
	shadowURL := mySQLScratchDatabaseURL(c, "ptah_3375_ctl_shadow")

	target, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })

	// The target carries a column the migration never creates, so the replay
	// produces a different schema.
	_, err = target.ExecContext(c.Context(),
		"CREATE TABLE ptah_shadow_semantics (id INT NOT NULL, extra INT NULL, PRIMARY KEY (id));")
	c.Assert(err, qt.IsNil)

	migrationsDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_init.up.sql"),
		[]byte(mysqlShadowMigration),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE ptah_shadow_semantics;\n"),
		0o600,
	), qt.IsNil)

	info := target.Info()

	err = shadow.VerifyBaseline(c.Context(), shadow.BaselineVerifyOptions{
		ShadowDatabaseURL: shadowURL,
		TargetConn:        target,
		MigrationsDir:     migrationsDir,
		Version:           1,
		Dialect:           info.Dialect,
		Capabilities:      info.Capabilities,
	})

	c.Assert(err, qt.IsNotNil)
	var shadowErr *shadow.VerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Not(qt.Equals), "identifier-semantics-check")
}
