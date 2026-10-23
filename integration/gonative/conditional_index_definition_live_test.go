//go:build integration

package gonative_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// TestPostgreSQLFirstAttemptRefusesOverIndexWithAnotherDefinitionIntegration
// runs CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS where a usable index with
// another key already holds the name. IF NOT EXISTS would skip it, record the
// migration applied, and leave email unconstrained, so the run refuses and
// records nothing.
func TestPostgreSQLFirstAttemptRefusesOverIndexWithAnotherDefinitionIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRetryInvalidIndexDB(c, dsn)
	seedRetryInvalidIndexTable(c, db, "'user' || g || '@example.com'")
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE UNIQUE INDEX %q ON %q (id)", retryInvalidUniqueIndex, retryInvalidIndexTable))
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := retryInvalidUniqueIndexMigrator(conn)

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `(?s).*cannot be applied.*"public"."`+retryInvalidUniqueIndex+`".*has a different definition.*`)
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)
}

// TestPostgreSQLFirstAttemptOverIndexWithTheSameDefinitionStillAppliesIntegration
// is the control, in a transactional migration: the index already present is
// the declared one, spelled differently, so skipping it is correct and the
// migration applies.
func TestPostgreSQLFirstAttemptOverIndexWithTheSameDefinitionStillAppliesIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openRetryInvalidIndexDB(c, dsn)
	seedRetryInvalidIndexTable(c, db, "'user' || g || '@example.com'")
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"CREATE UNIQUE INDEX %q ON public.%q USING btree (email ASC)", retryInvalidUniqueIndex, retryInvalidIndexTable,
	))
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	up := fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS %q ON %q (%q);", retryInvalidUniqueIndex, retryInvalidIndexTable, "email")
	mig := retryInvalidIndexMigrator(conn, up, fmt.Sprintf("DROP INDEX IF EXISTS %q;", retryInvalidUniqueIndex))

	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{retryInvalidIndexVersion})
}
