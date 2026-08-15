//go:build integration

package gonative_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	indexRecoveryIdentityTable    = "ptah_issue1101_identity_members"
	indexRecoveryIdentityLegacy   = "ptah_issue1101_identity_legacy"
	indexRecoveryIdentityName     = "idx_ptah_issue1101_identity_email"
	indexRecoveryIdentitySchema   = "ptah_issue1101_identity_app"
	indexRecoveryIdentityTrackerA = "schema_migrations_issue_1101_identity_a"
	indexRecoveryIdentityTrackerB = "schema_migrations_issue_1101_identity_b"
	indexRecoveryIdentityTrackerC = "schema_migrations_issue_1101_identity_c"
	indexRecoveryIdentityTrackerD = "schema_migrations_issue_1101_identity_d"
	indexRecoveryIdentityTrackerE = "schema_migrations_issue_1101_identity_e"
	indexRecoveryIdentityTrackerF = "schema_migrations_issue_1101_identity_f"
	indexRecoveryIdentityTrackerG = "schema_migrations_issue_1101_identity_g"
	indexRecoveryIdentityTrackerH = "schema_migrations_issue_1101_identity_h"
	indexRecoveryIdentityTrackerI = "schema_migrations_issue_1101_identity_i"
	indexRecoveryIdentityTrackerJ = "schema_migrations_issue_1101_identity_j"
	indexRecoveryIdentityTrackerK = "schema_migrations_issue_1101_identity_k"
	indexRecoveryIdentityTrackerL = "schema_migrations_issue_1101_identity_l"
	indexRecoveryIdentityTrackerM = "schema_migrations_issue_1101_identity_m"
	indexRecoveryIdentityTrackerN = "schema_migrations_issue_1101_identity_n"
	indexRecoveryIdentityPathA    = "ptah_issue1101_identity_path_a"
	indexRecoveryIdentityPathB    = "ptah_issue1101_identity_path_b"
	indexRecoveryIdentityVersion  = int64(1785756330)
)

func TestPostgreSQLCreateIndexRefusesIndexNameOwnedByAnotherTableIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	db := openIndexRecoveryIdentityDB(c, dsn)
	seedIndexRecoveryIdentityPublicTables(c, db)

	_, err := db.Exec(fmt.Sprintf(
		`CREATE INDEX %q ON %q (email)`,
		indexRecoveryIdentityName,
		indexRecoveryIdentityLegacy,
	))
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(c.Context(), dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := indexRecoveryIdentityMigrator(conn, indexRecoveryIdentityTrackerA, fmt.Sprintf(
		`CREATE UNIQUE INDEX IF NOT EXISTS %q ON %q (email);`,
		indexRecoveryIdentityName,
		indexRecoveryIdentityTable,
	))

	err = mig.MigrateUp(c.Context())
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `"public"."`+indexRecoveryIdentityName+`"`)
	c.Assert(err.Error(), qt.Contains, `is an index on "public"."`+indexRecoveryIdentityLegacy+`"`)
	c.Assert(err.Error(), qt.Contains, `instead of target table "public"."`+indexRecoveryIdentityTable+`"`)
	assertIndexRecoveryIdentityNeverStarted(c, mig)
	err = mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{
		Version: indexRecoveryIdentityVersion,
		Force:   true,
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "cannot be repaired")
	c.Assert(err.Error(), qt.Contains, `is an index on "public"."`+indexRecoveryIdentityLegacy+`"`)
}

func TestPostgreSQLCreateIndexRefusesNameOwnedByNonIndexRelationIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	db := openIndexRecoveryIdentityDB(c, dsn)
	seedIndexRecoveryIdentityPublicTables(c, db)

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %q (id INTEGER PRIMARY KEY)`, indexRecoveryIdentityName))
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(c.Context(), dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := indexRecoveryIdentityMigrator(conn, indexRecoveryIdentityTrackerB, fmt.Sprintf(
		`CREATE UNIQUE INDEX IF NOT EXISTS %q ON %q (email);`,
		indexRecoveryIdentityName,
		indexRecoveryIdentityTable,
	))

	err = mig.MigrateUp(c.Context())
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `relation "public"."`+indexRecoveryIdentityName+`" has relkind="r"`)
	c.Assert(err.Error(), qt.Contains, `instead of being an index on target table "public"."`+indexRecoveryIdentityTable+`"`)
	assertIndexRecoveryIdentityNeverStarted(c, mig)
	err = mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{
		Version: indexRecoveryIdentityVersion,
		Force:   true,
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "cannot be repaired")
	c.Assert(err.Error(), qt.Contains, `has relkind="r"`)
}

func TestPostgreSQLDropCreateResolvesMixedQualificationIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	tests := []struct {
		name    string
		tracker string
		up      string
	}{
		{
			name:    "unqualified drop and qualified create",
			tracker: indexRecoveryIdentityTrackerC,
			up: fmt.Sprintf(
				"-- +ptah no_transaction\nDROP INDEX CONCURRENTLY IF EXISTS %q;\n"+
					"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q.%q (email);",
				indexRecoveryIdentityName,
				indexRecoveryIdentityName,
				indexRecoveryIdentitySchema,
				indexRecoveryIdentityTable,
			),
		},
		{
			name:    "qualified drop and unqualified create",
			tracker: indexRecoveryIdentityTrackerD,
			up: fmt.Sprintf(
				"-- +ptah no_transaction\nDROP INDEX CONCURRENTLY IF EXISTS %q.%q;\n"+
					"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q (email);",
				indexRecoveryIdentitySchema,
				indexRecoveryIdentityName,
				indexRecoveryIdentityName,
				indexRecoveryIdentityTable,
			),
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			db := openIndexRecoveryIdentityDB(c, dsn)
			seedIndexRecoveryIdentitySchema(c, db, test.tracker)
			leaveIndexRecoveryIdentityInvalid(c, db)
			_, err := db.Exec(fmt.Sprintf(
				`DELETE FROM %q.%q WHERE id > 1`,
				indexRecoveryIdentitySchema,
				indexRecoveryIdentityTable,
			))
			c.Assert(err, qt.IsNil)

			conn, err := dbschema.ConnectToDatabase(c.Context(), indexRecoveryIdentitySearchPathDSN(c, dsn))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			mig := indexRecoveryIdentityMigrator(conn, test.tracker, test.up)

			c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
			valid, ready := indexRecoveryIdentityFlags(c, db)
			c.Assert(valid, qt.IsTrue)
			c.Assert(ready, qt.IsTrue)
			status, err := mig.GetMigrationStatus(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(status.DirtyRevision, qt.IsNil)
			c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{indexRecoveryIdentityVersion})
		})
	}
}

func TestPostgreSQLPostCheckRequiresCreatedIndexOnTargetIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	db := openIndexRecoveryIdentityDB(c, dsn)
	seedIndexRecoveryIdentityPublicTables(c, db)

	conn, err := dbschema.ConnectToDatabase(c.Context(), dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	up := fmt.Sprintf(
		"-- +ptah no_transaction\nCREATE UNIQUE INDEX IF NOT EXISTS %q ON %q (email);\n"+
			"DROP INDEX %q;\nCREATE TABLE %q (id INTEGER PRIMARY KEY);",
		indexRecoveryIdentityName,
		indexRecoveryIdentityTable,
		indexRecoveryIdentityName,
		indexRecoveryIdentityName,
	)
	mig := indexRecoveryIdentityMigrator(conn, indexRecoveryIdentityTrackerE, up)

	err = mig.MigrateUp(c.Context())
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "failed to verify migration")
	c.Assert(err.Error(), qt.Contains, `relation "public"."`+indexRecoveryIdentityName+`" has relkind="r"`)
	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 3)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 3)
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)
	err = mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: indexRecoveryIdentityVersion})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "cannot be repaired")
	c.Assert(err.Error(), qt.Contains, `has relkind="r"`)
}

func TestPostgreSQLUnconditionalCreateMayBeDroppedLaterIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	db := openIndexRecoveryIdentityDB(c, dsn)
	seedIndexRecoveryIdentityPublicTables(c, db)

	conn, err := dbschema.ConnectToDatabase(c.Context(), dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	up := fmt.Sprintf(
		"CREATE INDEX %q ON %q (email);\nDROP INDEX %q;",
		indexRecoveryIdentityName,
		indexRecoveryIdentityTable,
		indexRecoveryIdentityName,
	)
	mig := indexRecoveryIdentityMigrator(conn, indexRecoveryIdentityTrackerF, up)

	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{indexRecoveryIdentityVersion})
}

func TestPostgreSQLRollbackChecksConditionalIndexResultIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	tests := []struct {
		name       string
		tracker    string
		directive  string
		concurrent string
		applied    int
	}{
		{
			name:    "transactional rollback",
			tracker: indexRecoveryIdentityTrackerG,
			applied: 0,
		},
		{
			name:       "no-transaction rollback",
			tracker:    indexRecoveryIdentityTrackerH,
			directive:  "-- +ptah no_transaction\n",
			concurrent: "CONCURRENTLY ",
			applied:    1,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			db := openIndexRecoveryIdentityDB(c, dsn)
			seedIndexRecoveryIdentityPublicTables(c, db)
			conn, err := dbschema.ConnectToDatabase(c.Context(), dsn)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			down := fmt.Sprintf(
				"%sCREATE UNIQUE INDEX %sIF NOT EXISTS %q ON %q (email);",
				test.directive,
				test.concurrent,
				indexRecoveryIdentityName,
				indexRecoveryIdentityTable,
			)
			migration := migrator.CreateMigrationFromSQL(indexRecoveryIdentityVersion, "restore index", "SELECT 1;", down)
			mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
				WithMigrationsTable("", test.tracker)
			c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
			_, err = db.Exec(fmt.Sprintf(
				`CREATE INDEX %q ON %q (email)`,
				indexRecoveryIdentityName,
				indexRecoveryIdentityLegacy,
			))
			c.Assert(err, qt.IsNil)

			err = mig.MigrateDownTo(c.Context(), 0)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "failed to verify rollback")
			c.Assert(err.Error(), qt.Contains, `is an index on "public"."`+indexRecoveryIdentityLegacy+`"`)
			status, err := mig.GetMigrationStatus(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(status.DirtyRevision, qt.IsNotNil)
			c.Assert(status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
			c.Assert(status.DirtyRevision.Applied, qt.Equals, test.applied)
			c.Assert(status.DirtyRevision.Total, qt.Equals, 1)
			c.Assert(status.AppliedMigrations, qt.HasLen, 0)
		})
	}
}

func TestPostgreSQLRollbackResolvesEachConditionalIndexAtStatementSearchPathIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	tests := []struct {
		name       string
		tracker    string
		directive  string
		concurrent string
		applied    int
	}{
		{
			name:    "transactional rollback",
			tracker: indexRecoveryIdentityTrackerK,
		},
		{
			name:       "no-transaction rollback",
			tracker:    indexRecoveryIdentityTrackerL,
			directive:  "-- +ptah no_transaction\n",
			concurrent: "CONCURRENTLY ",
			applied:    4,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			db := openIndexRecoveryIdentityDB(c, dsn)
			seedIndexRecoveryIdentitySearchPathSchemas(c, db, test.tracker)
			conn, err := dbschema.ConnectToDatabase(c.Context(), dsn)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			down := fmt.Sprintf(
				"%sSET search_path = %s;\n"+
					"CREATE INDEX %sIF NOT EXISTS %q ON %q (email);\n"+
					"SET search_path = %s;\n"+
					"CREATE INDEX %sIF NOT EXISTS %q ON %q (email);",
				test.directive,
				indexRecoveryIdentityPathA,
				test.concurrent,
				indexRecoveryIdentityName,
				indexRecoveryIdentityTable,
				indexRecoveryIdentityPathB,
				test.concurrent,
				indexRecoveryIdentityName,
				indexRecoveryIdentityTable,
			)
			migration := migrator.CreateMigrationFromSQL(indexRecoveryIdentityVersion, "search path index identity", "SELECT 1;", down)
			mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
				WithMigrationsTable("", test.tracker)
			c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)

			err = mig.MigrateDownTo(c.Context(), 0)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "failed to verify rollback")
			c.Assert(err.Error(), qt.Contains, `relation "`+indexRecoveryIdentityPathA+`"."`+indexRecoveryIdentityName+`" has relkind="r"`)
			c.Assert(err.Error(), qt.Contains, `target table "`+indexRecoveryIdentityPathA+`"."`+indexRecoveryIdentityTable+`"`)
			status, err := mig.GetMigrationStatus(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(status.DirtyRevision, qt.IsNotNil)
			c.Assert(status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
			c.Assert(status.DirtyRevision.Applied, qt.Equals, test.applied)
			c.Assert(status.DirtyRevision.Total, qt.Equals, 4)
			c.Assert(status.AppliedMigrations, qt.HasLen, 0)
		})
	}
}

func TestPostgreSQLRepairChecksAllAmbientSearchPathCandidatesIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	db := openIndexRecoveryIdentityDB(c, dsn)
	seedIndexRecoveryIdentitySearchPathSchemas(c, db, indexRecoveryIdentityTrackerM)
	_, err := db.Exec(fmt.Sprintf(
		`DROP TABLE %q.%q`,
		indexRecoveryIdentityPathA,
		indexRecoveryIdentityName,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE INDEX %q ON %q.%q (email)`,
		indexRecoveryIdentityName,
		indexRecoveryIdentityPathB,
		indexRecoveryIdentityTable,
	))
	c.Assert(err, qt.IsNil)
	fsys := fstest.MapFS{
		"000001_index.up.sql": {Data: fmt.Appendf(nil,
			"-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q (email);",
			indexRecoveryIdentityName,
			indexRecoveryIdentityTable,
		)},
		"000001_index.down.sql": {Data: fmt.Appendf(nil,
			"-- +ptah no_transaction\nDROP INDEX CONCURRENTLY IF EXISTS %q;",
			indexRecoveryIdentityName,
		)},
	}
	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithStatementObserver(
		migrator.StatementObserverFunc(func(context.Context, migrator.StatementEvent) error {
			return errors.New("post-execution observation failed")
		}),
	))
	c.Assert(err, qt.IsNil)
	pathAConn, err := dbschema.ConnectToDatabase(c.Context(), indexRecoveryIdentityDSNWithSearchPath(c, dsn, indexRecoveryIdentityPathA))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(pathAConn.Close(), qt.IsNil) })
	initial := migrator.NewMigrator(pathAConn, provider).
		WithMigrationsTable("public", indexRecoveryIdentityTrackerM)
	c.Assert(initial.MigrateUp(c.Context()), qt.IsNotNil)
	_, err = db.Exec(fmt.Sprintf(
		`DROP INDEX %q.%q`,
		indexRecoveryIdentityPathA,
		indexRecoveryIdentityName,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE %q.%q (id BIGINT PRIMARY KEY)`,
		indexRecoveryIdentityPathA,
		indexRecoveryIdentityName,
	))
	c.Assert(err, qt.IsNil)

	repairProvider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNil)
	pathBConn, err := dbschema.ConnectToDatabase(c.Context(), indexRecoveryIdentityDSNWithSearchPath(c, dsn, indexRecoveryIdentityPathB))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(pathBConn.Close(), qt.IsNil) })
	repair := migrator.NewMigrator(pathBConn, repairProvider).
		WithMigrationsTable("public", indexRecoveryIdentityTrackerM)
	err = repair.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `relation "`+indexRecoveryIdentityPathA+`"."`+indexRecoveryIdentityName+`" has relkind="r"`)

	_, err = db.Exec(fmt.Sprintf(
		`DROP TABLE %q.%q`,
		indexRecoveryIdentityPathA,
		indexRecoveryIdentityName,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE INDEX %q ON %q.%q (email)`,
		indexRecoveryIdentityName,
		indexRecoveryIdentityPathA,
		indexRecoveryIdentityTable,
	))
	c.Assert(err, qt.IsNil)
	c.Assert(repair.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1}), qt.IsNil)
	status, err := repair.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{1})
}

func TestPostgreSQLIndexPreflightReplaysStatementSearchPathIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	db := openIndexRecoveryIdentityDB(c, dsn)
	seedIndexRecoveryIdentitySearchPathSchemas(c, db, indexRecoveryIdentityTrackerN)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	up := fmt.Sprintf(
		"SET search_path = %s;\n"+
			"CREATE INDEX IF NOT EXISTS %q ON %q (email);\n"+
			"SET search_path = %s;\n"+
			"CREATE INDEX IF NOT EXISTS %q ON %q (email);",
		indexRecoveryIdentityPathA,
		indexRecoveryIdentityName,
		indexRecoveryIdentityTable,
		indexRecoveryIdentityPathB,
		indexRecoveryIdentityName,
		indexRecoveryIdentityTable,
	)
	mig := indexRecoveryIdentityMigrator(conn, indexRecoveryIdentityTrackerN, up)

	err = mig.MigrateUp(c.Context())
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `relation "`+indexRecoveryIdentityPathA+`"."`+indexRecoveryIdentityName+`" has relkind="r"`)
	c.Assert(err.Error(), qt.Contains, `target table "`+indexRecoveryIdentityPathA+`"."`+indexRecoveryIdentityTable+`"`)
	assertIndexRecoveryIdentityNeverStarted(c, mig)
}

func TestPostgreSQLCompletedUpRepairRestoresSessionPrefixIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	db := openIndexRecoveryIdentityDB(c, dsn)
	seedIndexRecoveryIdentityPublicTables(c, db)
	seedIndexRecoveryIdentitySchema(c, db, indexRecoveryIdentityTrackerI)
	removeIndexRecoveryIdentityDuplicate(c, db)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	up := fmt.Sprintf(
		"-- +ptah no_transaction\nSET search_path = %s;\n"+
			"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q (email);\n"+
			"DROP INDEX %q.%q;\nCREATE TABLE %q.%q (id BIGINT);",
		indexRecoveryIdentitySchema,
		indexRecoveryIdentityName,
		indexRecoveryIdentityTable,
		indexRecoveryIdentitySchema,
		indexRecoveryIdentityName,
		indexRecoveryIdentitySchema,
		indexRecoveryIdentityName,
	)
	mig := indexRecoveryIdentityMigrator(conn, indexRecoveryIdentityTrackerI, up)

	c.Assert(mig.MigrateUp(c.Context()), qt.IsNotNil)
	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 4)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 4)
	repairIndexRecoveryIdentityAppRelation(c, db)
	createIndexRecoveryIdentityPublicConflict(c, db)

	c.Assert(mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: indexRecoveryIdentityVersion}), qt.IsNil)
	status, err = mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{indexRecoveryIdentityVersion})
}

func TestPostgreSQLCompletedDownRepairRestoresSessionPrefixIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	db := openIndexRecoveryIdentityDB(c, dsn)
	seedIndexRecoveryIdentityPublicTables(c, db)
	seedIndexRecoveryIdentitySchema(c, db, indexRecoveryIdentityTrackerJ)
	removeIndexRecoveryIdentityDuplicate(c, db)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	down := fmt.Sprintf(
		"-- +ptah no_transaction\nSET search_path = %s;\n"+
			"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %q ON %q (email);\n"+
			"DROP INDEX %q.%q;\nCREATE TABLE %q.%q (id BIGINT);",
		indexRecoveryIdentitySchema,
		indexRecoveryIdentityName,
		indexRecoveryIdentityTable,
		indexRecoveryIdentitySchema,
		indexRecoveryIdentityName,
		indexRecoveryIdentitySchema,
		indexRecoveryIdentityName,
	)
	migration := migrator.CreateMigrationFromSQL(indexRecoveryIdentityVersion, "restore index", "SELECT 1;", down)
	mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("", indexRecoveryIdentityTrackerJ)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)

	c.Assert(mig.MigrateDownTo(c.Context(), 0), qt.IsNotNil)
	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 4)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 4)
	repairIndexRecoveryIdentityAppRelation(c, db)
	createIndexRecoveryIdentityPublicConflict(c, db)

	c.Assert(mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: indexRecoveryIdentityVersion}), qt.IsNil)
	status, err = mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)
}

func openIndexRecoveryIdentityDB(c *qt.C, dsn string) *sql.DB {
	c.Helper()
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return db
}

func seedIndexRecoveryIdentityPublicTables(c *qt.C, db *sql.DB) {
	c.Helper()
	cleanup := func() {
		_, err := db.Exec("DROP TABLE IF EXISTS " + indexRecoveryIdentityTable + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + indexRecoveryIdentityLegacy + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec(`DROP TABLE IF EXISTS "` + indexRecoveryIdentityName + `" CASCADE`)
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + indexRecoveryIdentityTrackerA + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + indexRecoveryIdentityTrackerB + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + indexRecoveryIdentityTrackerE + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + indexRecoveryIdentityTrackerF + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + indexRecoveryIdentityTrackerG + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + indexRecoveryIdentityTrackerH + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + indexRecoveryIdentityTrackerI + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + indexRecoveryIdentityTrackerJ + " CASCADE")
		c.Check(err, qt.IsNil)
	}
	cleanup()
	c.Cleanup(cleanup)

	_, err := db.Exec("CREATE TABLE " + indexRecoveryIdentityTable + " (id INTEGER PRIMARY KEY, email TEXT NOT NULL)")
	c.Assert(err, qt.IsNil)
	_, err = db.Exec("CREATE TABLE " + indexRecoveryIdentityLegacy + " (id INTEGER PRIMARY KEY, email TEXT NOT NULL)")
	c.Assert(err, qt.IsNil)
}

func seedIndexRecoveryIdentitySearchPathSchemas(c *qt.C, db *sql.DB, tracker string) {
	c.Helper()
	c.Cleanup(func() {
		_, err := db.Exec("DROP SCHEMA IF EXISTS " + indexRecoveryIdentityPathA + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP SCHEMA IF EXISTS " + indexRecoveryIdentityPathB + " CASCADE")
		c.Check(err, qt.IsNil)
		_, err = db.Exec(`DROP TABLE IF EXISTS "` + tracker + `"`)
		c.Check(err, qt.IsNil)
	})
	_, err := db.Exec("DROP SCHEMA IF EXISTS " + indexRecoveryIdentityPathA + " CASCADE")
	c.Assert(err, qt.IsNil)
	_, err = db.Exec("DROP SCHEMA IF EXISTS " + indexRecoveryIdentityPathB + " CASCADE")
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`DROP TABLE IF EXISTS "` + tracker + `"`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec("CREATE SCHEMA " + indexRecoveryIdentityPathA)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec("CREATE SCHEMA " + indexRecoveryIdentityPathB)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE %q.%q (id BIGINT PRIMARY KEY, email TEXT NOT NULL)`,
		indexRecoveryIdentityPathA,
		indexRecoveryIdentityTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE %q.%q (id BIGINT PRIMARY KEY)`,
		indexRecoveryIdentityPathA,
		indexRecoveryIdentityName,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE %q.%q (id BIGINT PRIMARY KEY, email TEXT NOT NULL)`,
		indexRecoveryIdentityPathB,
		indexRecoveryIdentityTable,
	))
	c.Assert(err, qt.IsNil)
}

func removeIndexRecoveryIdentityDuplicate(c *qt.C, db *sql.DB) {
	c.Helper()
	_, err := db.Exec(fmt.Sprintf(
		`DELETE FROM %q.%q WHERE id > 1`,
		indexRecoveryIdentitySchema,
		indexRecoveryIdentityTable,
	))
	c.Assert(err, qt.IsNil)
}

func repairIndexRecoveryIdentityAppRelation(c *qt.C, db *sql.DB) {
	c.Helper()
	_, err := db.Exec(fmt.Sprintf(
		`DROP TABLE %q.%q`,
		indexRecoveryIdentitySchema,
		indexRecoveryIdentityName,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE UNIQUE INDEX %q ON %q.%q (email)`,
		indexRecoveryIdentityName,
		indexRecoveryIdentitySchema,
		indexRecoveryIdentityTable,
	))
	c.Assert(err, qt.IsNil)
}

func createIndexRecoveryIdentityPublicConflict(c *qt.C, db *sql.DB) {
	c.Helper()
	_, err := db.Exec(fmt.Sprintf(
		`CREATE INDEX %q ON %q (email)`,
		indexRecoveryIdentityName,
		indexRecoveryIdentityLegacy,
	))
	c.Assert(err, qt.IsNil)
}

func seedIndexRecoveryIdentitySchema(c *qt.C, db *sql.DB, tracker string) {
	c.Helper()
	cleanup := func() {
		_, err := db.Exec(`DROP SCHEMA IF EXISTS "` + indexRecoveryIdentitySchema + `" CASCADE`)
		c.Check(err, qt.IsNil)
		_, err = db.Exec("DROP TABLE IF EXISTS " + tracker + " CASCADE")
		c.Check(err, qt.IsNil)
	}
	cleanup()
	c.Cleanup(cleanup)

	_, err := db.Exec(`CREATE SCHEMA "` + indexRecoveryIdentitySchema + `"`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE %q.%q (id INTEGER PRIMARY KEY, email TEXT NOT NULL)`,
		indexRecoveryIdentitySchema,
		indexRecoveryIdentityTable,
	))
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(fmt.Sprintf(
		`INSERT INTO %q.%q VALUES (1, 'shared@example.com'), (2, 'shared@example.com')`,
		indexRecoveryIdentitySchema,
		indexRecoveryIdentityTable,
	))
	c.Assert(err, qt.IsNil)
}

func leaveIndexRecoveryIdentityInvalid(c *qt.C, db *sql.DB) {
	c.Helper()
	_, err := db.Exec(fmt.Sprintf(
		`CREATE UNIQUE INDEX CONCURRENTLY %q ON %q.%q (email)`,
		indexRecoveryIdentityName,
		indexRecoveryIdentitySchema,
		indexRecoveryIdentityTable,
	))
	c.Assert(err, qt.IsNotNil)
	valid, ready := indexRecoveryIdentityFlags(c, db)
	c.Assert(valid, qt.IsFalse)
	c.Assert(ready, qt.IsFalse)
}

func indexRecoveryIdentityFlags(c *qt.C, db *sql.DB) (valid, ready bool) {
	c.Helper()
	err := db.QueryRow(`
		SELECT ix.indisvalid, ix.indisready
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = i.relnamespace
		WHERE n.nspname = $1 AND i.relname = $2`,
		indexRecoveryIdentitySchema,
		indexRecoveryIdentityName,
	).Scan(&valid, &ready)
	c.Assert(err, qt.IsNil)
	return valid, ready
}

func indexRecoveryIdentitySearchPathDSN(c *qt.C, dsn string) string {
	return indexRecoveryIdentityDSNWithSearchPath(c, dsn, indexRecoveryIdentitySchema+",public")
}

func indexRecoveryIdentityDSNWithSearchPath(c *qt.C, dsn, searchPath string) string {
	c.Helper()
	parsed, err := url.Parse(dsn)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", searchPath)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func indexRecoveryIdentityMigrator(
	conn *dbschema.DatabaseConnection,
	tracker string,
	up string,
) *migrator.Migrator {
	down := fmt.Sprintf("DROP INDEX IF EXISTS %q.%q;", indexRecoveryIdentitySchema, indexRecoveryIdentityName)
	migration := migrator.CreateMigrationFromSQL(indexRecoveryIdentityVersion, "verify index identity", up, down)
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).WithMigrationsTable("", tracker)
}

func assertIndexRecoveryIdentityNeverStarted(c *qt.C, mig *migrator.Migrator) {
	c.Helper()
	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)
}
