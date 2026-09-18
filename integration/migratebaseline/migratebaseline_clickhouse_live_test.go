//go:build integration

package migratebaseline_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/shadow"
)

// A ClickHouse database is a schema, so a shadow database is a different
// database and carries a different name. Without the ClickHouse arm in
// semanticsAgree the identifier semantics check compares the database each
// connection selected and refuses this shadow, while a shadow named like the
// target is refused by the distinctness guard as possibly the target itself:
// between them no shadow reaches the replay (stokaro/ptah#3389).
//
// The second half is the property that arm must not cost: a target whose schema
// the migrations do not produce is still refused, by the schema comparison that
// follows the replay.
func TestVerifyBaselineClickHouseAcceptsAShadowDatabaseOfItsOwn(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminURL, admin := requireClickHouseBaselineConnection(t, c, ctx)
	defer dbschema.CloseAndWarn(admin)

	suffix := time.Now().UnixNano()
	targetName := fmt.Sprintf("ptah_3389_target_%d", suffix)
	shadowName := fmt.Sprintf("ptah_3389_shadow_%d", suffix)
	for _, name := range []string{targetName, shadowName} {
		_, err := admin.ExecContext(ctx, "CREATE DATABASE "+quoteClickHouseIdent(name))
		c.Assert(err, qt.IsNil)
		defer func(name string) {
			_, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+quoteClickHouseIdent(name))
		}(name)
	}

	target, err := dbschema.ConnectToDatabase(ctx, addressNamingDatabase(c, adminURL, targetName))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	// The schema exists and no history does: the case baseline is for.
	_, err = target.ExecContext(ctx, clickHouseWidgetsTable)
	c.Assert(err, qt.IsNil)

	migrationsDir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_init.up.sql"),
		[]byte(clickHouseWidgetsTable+";\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE widgets;\n"), 0o600), qt.IsNil)

	info := target.Info()
	options := shadow.BaselineVerifyOptions{
		ShadowDatabaseURL: addressNamingDatabase(c, adminURL, shadowName),
		TargetConn:        target,
		MigrationsDir:     migrationsDir,
		Version:           1,
		Dialect:           info.Dialect,
		Capabilities:      info.Capabilities,
	}
	c.Assert(shadow.VerifyBaseline(ctx, options), qt.IsNil)

	// A column the migrations never create makes the target a schema they do not
	// produce, and that is still a refusal -- from the comparison, not from the
	// semantics check.
	_, err = target.ExecContext(ctx, "ALTER TABLE widgets ADD COLUMN color String")
	c.Assert(err, qt.IsNil)
	err = shadow.VerifyBaseline(ctx, options)
	c.Assert(err, qt.ErrorMatches, `baseline shadow check failed: .*`)
	var shadowErr *shadow.VerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Equals, "schema-match")
}

// clickHouseWidgetsTable is written once and read twice, by the target and by
// the migration that has to reproduce it: two spellings of one table is the
// difference this test cannot measure.
const clickHouseWidgetsTable = `CREATE TABLE widgets (
	id UInt64,
	name String
)
ENGINE = MergeTree
ORDER BY id`

// requireClickHouseBaselineConnection is requireMySQLFamilyAdminConnection for
// ClickHouse: an unset address or an unreachable server skips, because the
// contour that runs this test is the one that starts the server.
func requireClickHouseBaselineConnection(
	t *testing.T,
	c *qt.C,
	ctx context.Context,
) (string, *dbschema.DatabaseConnection) {
	t.Helper()

	adminURL, err := dbtarget.Lookup(dbtarget.ClickHouse)
	c.Assert(err, qt.IsNil)
	if adminURL == "" {
		t.Skip("ClickHouse test database URL is not set")
	}
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	if err != nil {
		t.Skipf("test database is not available: %v", err)
	}
	return adminURL, admin
}

func quoteClickHouseIdent(value string) string {
	return "`" + value + "`"
}
