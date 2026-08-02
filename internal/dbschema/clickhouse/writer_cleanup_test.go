package clickhouse_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/clickhouse"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

const databaseRealmObjectsQuery = `
	SELECT name, engine, create_table_query
	FROM system.tables
	WHERE database = ?
	  AND is_temporary = 0
	ORDER BY name
`

const databaseRealmEngineQuery = `
	SELECT engine
	FROM system.databases
	WHERE name = ?
`

const databaseRealmVersionQuery = `SELECT version()`

const databaseRealmTemporaryObjectsQuery = `
	SELECT name
	FROM system.tables
	WHERE is_temporary = 1
	ORDER BY name
`

const databaseRealmGlobalPrivilegesQuery = `
	CHECK GRANT SHOW DATABASES, SHOW TABLES ON *.*
`

const databaseRealmExternalDependenciesQuery = `
	SELECT database, name, engine
	FROM system.tables
	WHERE database != ?
	  AND database NOT IN (
	    'INFORMATION_SCHEMA',
	    '_temporary_and_external_tables',
	    'information_schema',
	    'system'
	  )
	  AND engine IN (
	    'Buffer',
	    'Dictionary',
	    'Distributed',
	    'LiveView',
	    'MaterializedView',
	    'Merge',
	    'View',
	    'WindowView'
	  )
	ORDER BY database, name
	LIMIT 1
`

type sqlMockQuery struct {
	sql    string
	args   []driver.NamedValue
	result dbtest.QueryResult
	err    error
}

type sqlMockExec struct {
	sql  string
	args []driver.NamedValue
	err  error
}

func TestWriterDropDatabaseRealm_DropsPersistentObjectsInDependencyOrder(t *testing.T) {
	c := qt.New(t)
	database := "analytics`realm"
	privilegesQuery := "CHECK GRANT SHOW DATABASES, SHOW TABLES, DROP TABLE, DROP VIEW, " +
		"DROP DICTIONARY ON `analytics``realm`.*"
	queryArgs := []driver.NamedValue{{Ordinal: 1, Value: database}}
	queries := append(
		databaseRealmPreflightQueries(database, privilegesQuery, "Atomic"),
		sqlMockQuery{
			sql:  databaseRealmObjectsQuery,
			args: queryArgs,
			result: dbtest.QueryResult{
				Columns: []string{"name", "engine", "create_table_query"},
				Rows: [][]driver.Value{
					{".inner_id.mv", "MergeTree", "CREATE TABLE `analytics``realm`.`.inner_id.mv` (id UInt64) ENGINE = MergeTree ORDER BY id"},
					{"dictionary_table", "Dictionary", "CREATE TABLE `analytics``realm`.dictionary_table (id UInt64) ENGINE = Dictionary(lookup)"},
					{"events`daily", "MergeTree", "CREATE OR REPLACE TABLE `analytics``realm`.`events``daily` (id UInt64) ENGINE = MergeTree ORDER BY id"},
					{"events_mv", "MaterializedView", "CREATE OR REPLACE MATERIALIZED VIEW `analytics``realm`.events_mv TO `analytics``realm`.events_rollup AS SELECT 1"},
					{"events_view", "View", "CREATE OR REPLACE VIEW `analytics``realm`.events_view AS SELECT 1"},
					{"legacy_live", "LiveView", "CREATE LIVE VIEW `analytics``realm`.legacy_live AS SELECT 1"},
					{"lookup", "Dictionary", "CREATE OR REPLACE DICTIONARY `analytics``realm`.lookup (id UInt64) PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT())"},
					{"queue", "Kafka", "CREATE TABLE `analytics``realm`.queue (payload String) ENGINE = Kafka"},
					{"windowed", "WindowView", "CREATE WINDOW VIEW `analytics``realm`.windowed AS SELECT 1"},
				},
			},
		},
		databaseRealmGrantedQuery(privilegesQuery),
		databaseRealmGlobalGrantedQuery(),
		databaseRealmNoExternalDependencyQuery(database),
		sqlMockQuery{
			sql:    databaseRealmObjectsQuery,
			args:   queryArgs,
			result: dbtest.QueryResult{Columns: []string{"name", "engine", "create_table_query"}},
		},
	)
	execs := []sqlMockExec{
		{sql: "DROP VIEW IF EXISTS `analytics``realm`.`events_mv` SYNC"},
		{sql: "DROP VIEW IF EXISTS `analytics``realm`.`events_view` SYNC"},
		{sql: "DROP VIEW IF EXISTS `analytics``realm`.`legacy_live` SYNC"},
		{sql: "DROP VIEW IF EXISTS `analytics``realm`.`windowed` SYNC"},
		{sql: "DROP TABLE IF EXISTS `analytics``realm`.`dictionary_table` SYNC"},
		{sql: "DROP DICTIONARY IF EXISTS `analytics``realm`.`lookup` SYNC"},
		{sql: "DROP TABLE IF EXISTS `analytics``realm`.`.inner_id.mv` SYNC"},
		{sql: "DROP TABLE IF EXISTS `analytics``realm`.`events``daily` SYNC"},
		{sql: "DROP TABLE IF EXISTS `analytics``realm`.`queue` SYNC"},
	}
	db := openClickHouseSQLMock(t, c, queries, execs)
	writer := clickhouse.NewClickHouseWriter(db.SQL, database)

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
	assertClickHouseSQLMockComplete(c, db, queries, execs)
}

func TestWriterDropDatabaseRealm_UnknownObjectFailsBeforeMutation(t *testing.T) {
	c := qt.New(t)
	database := "analytics"
	privilegesQuery := databaseRealmPrivilegesQuery(database)
	queries := append(
		databaseRealmPreflightQueries(database, privilegesQuery, "Atomic"),
		sqlMockQuery{
			sql:  databaseRealmObjectsQuery,
			args: []driver.NamedValue{{Ordinal: 1, Value: database}},
			result: dbtest.QueryResult{
				Columns: []string{"name", "engine", "create_table_query"},
				Rows: [][]driver.Value{
					{"events", "MergeTree", "CREATE TABLE analytics.events (id UInt64) ENGINE = MergeTree ORDER BY id"},
					{"future_view", "FutureView", "CREATE FUTURE VIEW analytics.future_view AS SELECT 1"},
				},
			},
		},
	)
	db := openClickHouseSQLMock(t, c, queries, nil)
	writer := clickhouse.NewClickHouseWriter(db.SQL, database)

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches, `clickhouse: refusing to clean database "analytics": .*`)
	c.Assert(err.Error(), qt.Contains, `persistent object "future_view" with engine "FutureView"`)
	assertClickHouseSQLMockComplete(c, db, queries, nil)
}

func TestWriterDropDatabaseRealm_VerifiesPersistentObjectsAreGone(t *testing.T) {
	c := qt.New(t)
	database := "analytics"
	privilegesQuery := databaseRealmPrivilegesQuery(database)
	queryArgs := []driver.NamedValue{{Ordinal: 1, Value: database}}
	objectRows := dbtest.QueryResult{
		Columns: []string{"name", "engine", "create_table_query"},
		Rows: [][]driver.Value{
			{"events", "MergeTree", "CREATE TABLE analytics.events (id UInt64) ENGINE = MergeTree ORDER BY id"},
		},
	}
	queries := append(
		databaseRealmPreflightQueries(database, privilegesQuery, "Atomic"),
		sqlMockQuery{sql: databaseRealmObjectsQuery, args: queryArgs, result: objectRows},
		databaseRealmGrantedQuery(privilegesQuery),
		databaseRealmGlobalGrantedQuery(),
		databaseRealmNoExternalDependencyQuery(database),
		sqlMockQuery{sql: databaseRealmObjectsQuery, args: queryArgs, result: objectRows},
	)
	execs := []sqlMockExec{
		{sql: "DROP TABLE IF EXISTS `analytics`.`events` SYNC"},
	}
	db := openClickHouseSQLMock(t, c, queries, execs)
	writer := clickhouse.NewClickHouseWriter(db.SQL, database)

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`clickhouse: database-realm cleanup left persistent objects in "analytics": events \(MergeTree\)`,
	)
	assertClickHouseSQLMockComplete(c, db, queries, execs)
}

func TestWriterDropDatabaseRealm_CatalogErrorDoesNotMutate(t *testing.T) {
	c := qt.New(t)
	catalogErr := errors.New("catalog unavailable")
	database := "analytics"
	queries := append(
		databaseRealmPreflightQueries(
			database,
			databaseRealmPrivilegesQuery(database),
			"Atomic",
		),
		sqlMockQuery{
			sql:  databaseRealmObjectsQuery,
			args: []driver.NamedValue{{Ordinal: 1, Value: database}},
			err:  catalogErr,
		},
	)
	db := openClickHouseSQLMock(t, c, queries, nil)
	writer := clickhouse.NewClickHouseWriter(db.SQL, database)

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorIs, catalogErr)
	assertClickHouseSQLMockComplete(c, db, queries, nil)
}

func TestWriterDropDatabaseRealm_MissingPrivilegesFailBeforeCatalogRead(t *testing.T) {
	c := qt.New(t)
	database := "analytics"
	privilegesQuery := databaseRealmPrivilegesQuery(database)
	queries := []sqlMockQuery{
		databaseRealmVersionResult("26.2.0.0"),
		{
			sql: privilegesQuery,
			result: dbtest.QueryResult{
				Columns: []string{"result"},
				Rows:    [][]driver.Value{{int64(0)}},
			},
		},
	}
	db := openClickHouseSQLMock(t, c, queries, nil)
	writer := clickhouse.NewClickHouseWriter(db.SQL, database)

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches, `clickhouse: database-realm cleanup requires SHOW DATABASES, .*`)
	assertClickHouseSQLMockComplete(c, db, queries, nil)
}

func TestWriterDropDatabaseRealm_MissingGlobalVisibilityFailsBeforeCatalogRead(
	t *testing.T,
) {
	c := qt.New(t)
	database := "analytics"
	privilegesQuery := databaseRealmPrivilegesQuery(database)
	queries := []sqlMockQuery{
		databaseRealmVersionResult("26.2.0.0"),
		databaseRealmGrantedQuery(privilegesQuery),
		{
			sql: databaseRealmGlobalPrivilegesQuery,
			result: dbtest.QueryResult{
				Columns: []string{"result"},
				Rows:    [][]driver.Value{{int64(0)}},
			},
		},
	}
	db := openClickHouseSQLMock(t, c, queries, nil)
	writer := clickhouse.NewClickHouseWriter(db.SQL, database)

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`clickhouse: database-realm cleanup requires SHOW DATABASES and SHOW TABLES on \*\.\* `+
			`to prove that external dependencies are absent`,
	)
	assertClickHouseSQLMockComplete(c, db, queries, nil)
}

func TestWriterDropDatabaseRealm_UnsupportedDatabaseEngineFailsBeforeMutation(t *testing.T) {
	c := qt.New(t)
	engines := []string{"DataLakeCatalog", "Replicated", "Shared"}

	for _, engine := range engines {
		c.Run(engine, func(c *qt.C) {
			database := "analytics"
			privilegesQuery := databaseRealmPrivilegesQuery(database)
			queries := []sqlMockQuery{
				databaseRealmVersionResult("26.2.0.0"),
				databaseRealmGrantedQuery(privilegesQuery),
				databaseRealmGlobalGrantedQuery(),
				{
					sql:  databaseRealmEngineQuery,
					args: []driver.NamedValue{{Ordinal: 1, Value: database}},
					result: dbtest.QueryResult{
						Columns: []string{"engine"},
						Rows:    [][]driver.Value{{engine}},
					},
				},
			}
			db := openClickHouseSQLMock(t, c, queries, nil)
			writer := clickhouse.NewClickHouseWriter(db.SQL, database)

			err := writer.DropDatabaseRealm(t.Context())

			c.Assert(
				err,
				qt.ErrorMatches,
				`clickhouse: refusing to clean database "analytics" with unsupported engine "`+
					engine+`"`,
			)
			assertClickHouseSQLMockComplete(c, db, queries, nil)
		})
	}
}

func TestWriterDropDatabaseRealm_ExternalDependencyFailsBeforeMutation(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name       string
		objectName string
		engine     string
	}{
		{
			name:       "buffer table",
			objectName: "events_buffer",
			engine:     "Buffer",
		},
		{
			name:       "materialized view",
			objectName: "events_mv",
			engine:     "MaterializedView",
		},
		{
			name:       "merge table",
			objectName: "events_merge",
			engine:     "Merge",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			database := "analytics"
			privilegesQuery := databaseRealmPrivilegesQuery(database)
			externalArgs := []driver.NamedValue{
				{Ordinal: 1, Value: database},
			}
			queries := append(
				databaseRealmTargetPreflightQueries(database, privilegesQuery, "Atomic"),
				sqlMockQuery{
					sql:  databaseRealmExternalDependenciesQuery,
					args: externalArgs,
					result: dbtest.QueryResult{
						Columns: []string{"database", "name", "engine"},
						Rows:    [][]driver.Value{{"reporting", test.objectName, test.engine}},
					},
				},
			)
			db := openClickHouseSQLMock(t, c, queries, nil)
			writer := clickhouse.NewClickHouseWriter(db.SQL, database)

			err := writer.DropDatabaseRealm(t.Context())

			c.Assert(
				err,
				qt.ErrorMatches,
				`clickhouse: refusing to clean database "analytics": external object `+
					"`reporting`.`"+test.objectName+"`"+` with engine "`+test.engine+
					`" may depend on the cleanup realm`,
			)
			assertClickHouseSQLMockComplete(c, db, queries, nil)
		})
	}
}

func TestWriterDropDatabaseRealm_ProtectedDatabasesFailWithoutIO(t *testing.T) {
	c := qt.New(t)
	databases := []string{
		"INFORMATION_SCHEMA",
		"_temporary_and_external_tables",
		"information_schema",
		"system",
	}

	for _, database := range databases {
		c.Run(database, func(c *qt.C) {
			db := openClickHouseSQLMock(t, c, nil, nil)
			writer := clickhouse.NewClickHouseWriter(db.SQL, database)

			err := writer.DropDatabaseRealm(t.Context())

			c.Assert(err, qt.ErrorMatches, `clickhouse: refusing database-realm cleanup of protected database .*`)
			assertClickHouseSQLMockComplete(c, db, nil, nil)
		})
	}
}

func TestWriterDropDatabaseRealm_TemporaryObjectsFailBeforePersistentCatalogRead(t *testing.T) {
	c := qt.New(t)
	database := "analytics"
	privilegesQuery := databaseRealmPrivilegesQuery(database)
	queries := []sqlMockQuery{
		databaseRealmVersionResult("26.2.0.0"),
		databaseRealmGrantedQuery(privilegesQuery),
		databaseRealmGlobalGrantedQuery(),
		{
			sql:  databaseRealmEngineQuery,
			args: []driver.NamedValue{{Ordinal: 1, Value: database}},
			result: dbtest.QueryResult{
				Columns: []string{"engine"},
				Rows:    [][]driver.Value{{"Atomic"}},
			},
		},
		{
			sql: databaseRealmTemporaryObjectsQuery,
			result: dbtest.QueryResult{
				Columns: []string{"name"},
				Rows:    [][]driver.Value{{"shadow_events"}},
			},
		},
	}
	db := openClickHouseSQLMock(t, c, queries, nil)
	writer := clickhouse.NewClickHouseWriter(db.SQL, database)

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`clickhouse: refusing database-realm cleanup while session-temporary objects exist: \["shadow_events"\]`,
	)
	assertClickHouseSQLMockComplete(c, db, queries, nil)
}

func TestWriterDropDatabaseRealm_PostCleanupVisibilityLossFailsClosed(t *testing.T) {
	c := qt.New(t)
	database := "analytics"
	privilegesQuery := databaseRealmPrivilegesQuery(database)
	queryArgs := []driver.NamedValue{{Ordinal: 1, Value: database}}
	queries := append(
		databaseRealmPreflightQueries(database, privilegesQuery, "Atomic"),
		sqlMockQuery{
			sql:  databaseRealmObjectsQuery,
			args: queryArgs,
			result: dbtest.QueryResult{
				Columns: []string{"name", "engine", "create_table_query"},
				Rows: [][]driver.Value{
					{"events", "MergeTree", "CREATE TABLE analytics.events (id UInt64) ENGINE = MergeTree ORDER BY id"},
				},
			},
		},
		sqlMockQuery{
			sql: privilegesQuery,
			result: dbtest.QueryResult{
				Columns: []string{"result"},
				Rows:    [][]driver.Value{{int64(0)}},
			},
		},
	)
	execs := []sqlMockExec{
		{sql: "DROP TABLE IF EXISTS `analytics`.`events` SYNC"},
	}
	db := openClickHouseSQLMock(t, c, queries, execs)
	writer := clickhouse.NewClickHouseWriter(db.SQL, database)

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches, `clickhouse: verify database "analytics" remains fully visible: .*`)
	assertClickHouseSQLMockComplete(c, db, queries, execs)
}

func TestWriterDropDatabaseRealm_DropFailureStopsBeforeResidualVerification(t *testing.T) {
	c := qt.New(t)
	database := "analytics"
	dropErr := errors.New("drop failed")
	privilegesQuery := databaseRealmPrivilegesQuery(database)
	queries := append(
		databaseRealmPreflightQueries(database, privilegesQuery, "Atomic"),
		sqlMockQuery{
			sql:  databaseRealmObjectsQuery,
			args: []driver.NamedValue{{Ordinal: 1, Value: database}},
			result: dbtest.QueryResult{
				Columns: []string{"name", "engine", "create_table_query"},
				Rows: [][]driver.Value{
					{"events", "MergeTree", "CREATE TABLE analytics.events (id UInt64) ENGINE = MergeTree ORDER BY id"},
				},
			},
		},
	)
	execs := []sqlMockExec{
		{sql: "DROP TABLE IF EXISTS `analytics`.`events` SYNC", err: dropErr},
	}
	db := openClickHouseSQLMock(t, c, queries, execs)
	writer := clickhouse.NewClickHouseWriter(db.SQL, database)

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorIs, dropErr)
	assertClickHouseSQLMockComplete(c, db, queries, execs)
}

func TestWriterDropDatabaseRealm_IsIdempotentWhenDatabaseIsEmpty(t *testing.T) {
	c := qt.New(t)
	database := "analytics"
	privilegesQuery := databaseRealmPrivilegesQuery(database)
	queryArgs := []driver.NamedValue{{Ordinal: 1, Value: database}}
	emptyObjectsQuery := sqlMockQuery{
		sql:    databaseRealmObjectsQuery,
		args:   queryArgs,
		result: dbtest.QueryResult{Columns: []string{"name", "engine", "create_table_query"}},
	}
	oneRun := append(
		databaseRealmPreflightQueries(database, privilegesQuery, "Atomic"),
		emptyObjectsQuery,
		databaseRealmGrantedQuery(privilegesQuery),
		databaseRealmGlobalGrantedQuery(),
		databaseRealmNoExternalDependencyQuery(database),
		emptyObjectsQuery,
	)
	queries := append(append([]sqlMockQuery{}, oneRun...), oneRun...)
	db := openClickHouseSQLMock(t, c, queries, nil)
	writer := clickhouse.NewClickHouseWriter(db.SQL, database)

	firstErr := writer.DropDatabaseRealm(t.Context())
	secondErr := writer.DropDatabaseRealm(t.Context())

	c.Assert(firstErr, qt.IsNil)
	c.Assert(secondErr, qt.IsNil)
	assertClickHouseSQLMockComplete(c, db, queries, nil)
}

func TestWriterDropDatabaseRealm_ClickHouse2410FailsClosedBeforeCatalogRead(t *testing.T) {
	c := qt.New(t)
	queries := []sqlMockQuery{
		databaseRealmVersionResult("24.10.2.80"),
	}
	db := openClickHouseSQLMock(t, c, queries, nil)
	writer := clickhouse.NewClickHouseWriter(db.SQL, "analytics")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`clickhouse: refusing database-realm cleanup on server version "24\.10\.2\.80": `+
			`ClickHouse 24\.11 or newer is required to prove complete catalog visibility with CHECK GRANT`,
	)
	assertClickHouseSQLMockComplete(c, db, queries, nil)
}

func TestWriterDropDatabaseRealm_UnparseableServerVersionFailsBeforeMutation(t *testing.T) {
	c := qt.New(t)
	queries := []sqlMockQuery{
		databaseRealmVersionResult("development"),
	}
	db := openClickHouseSQLMock(t, c, queries, nil)
	writer := clickhouse.NewClickHouseWriter(db.SQL, "analytics")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches, `clickhouse: cannot parse server version "development"`)
	assertClickHouseSQLMockComplete(c, db, queries, nil)
}

func TestWriterDropDatabaseRealm_CanceledContext(t *testing.T) {
	c := qt.New(t)
	db := openClickHouseSQLMock(t, c, nil, nil)
	writer := clickhouse.NewClickHouseWriter(db.SQL, "analytics")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := writer.DropDatabaseRealm(ctx)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	assertClickHouseSQLMockComplete(c, db, nil, nil)
}

func TestWriterDropDatabaseRealm_DryRunNeedsNoDatabase(t *testing.T) {
	c := qt.New(t)
	writer := clickhouse.NewClickHouseWriter(nil, "")
	writer.SetDryRun(true)

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
}

func databaseRealmPrivilegesQuery(database string) string {
	return "CHECK GRANT SHOW DATABASES, SHOW TABLES, DROP TABLE, DROP VIEW, " +
		"DROP DICTIONARY ON `" + database + "`.*"
}

func databaseRealmGrantedQuery(privilegesQuery string) sqlMockQuery {
	return sqlMockQuery{
		sql: privilegesQuery,
		result: dbtest.QueryResult{
			Columns: []string{"result"},
			Rows:    [][]driver.Value{{int64(1)}},
		},
	}
}

func databaseRealmGlobalGrantedQuery() sqlMockQuery {
	return sqlMockQuery{
		sql: databaseRealmGlobalPrivilegesQuery,
		result: dbtest.QueryResult{
			Columns: []string{"result"},
			Rows:    [][]driver.Value{{int64(1)}},
		},
	}
}

func databaseRealmNoExternalDependencyQuery(database string) sqlMockQuery {
	args := []driver.NamedValue{
		{Ordinal: 1, Value: database},
	}
	return sqlMockQuery{
		sql:    databaseRealmExternalDependenciesQuery,
		args:   args,
		result: dbtest.QueryResult{Columns: []string{"database", "name", "engine"}},
	}
}

func databaseRealmVersionResult(version string) sqlMockQuery {
	return sqlMockQuery{
		sql: databaseRealmVersionQuery,
		result: dbtest.QueryResult{
			Columns: []string{"version()"},
			Rows:    [][]driver.Value{{version}},
		},
	}
}

func databaseRealmPreflightQueries(
	database string,
	privilegesQuery string,
	engine string,
) []sqlMockQuery {
	return append(
		databaseRealmTargetPreflightQueries(database, privilegesQuery, engine),
		databaseRealmNoExternalDependencyQuery(database),
	)
}

func databaseRealmTargetPreflightQueries(
	database string,
	privilegesQuery string,
	engine string,
) []sqlMockQuery {
	return []sqlMockQuery{
		databaseRealmVersionResult("26.2.0.0"),
		databaseRealmGrantedQuery(privilegesQuery),
		databaseRealmGlobalGrantedQuery(),
		{
			sql:  databaseRealmEngineQuery,
			args: []driver.NamedValue{{Ordinal: 1, Value: database}},
			result: dbtest.QueryResult{
				Columns: []string{"engine"},
				Rows:    [][]driver.Value{{engine}},
			},
		},
		{
			sql:    databaseRealmTemporaryObjectsQuery,
			result: dbtest.QueryResult{Columns: []string{"name"}},
		},
	}
}

func openClickHouseSQLMock(
	t *testing.T,
	c *qt.C,
	queries []sqlMockQuery,
	execs []sqlMockExec,
) *dbtest.DB {
	var queryIndex atomic.Int64
	var execIndex atomic.Int64
	return dbtest.OpenWithExec(
		t,
		func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
			expected := queries[queryIndex.Add(1)-1]
			c.Check(normalizeSQL(query), qt.Equals, normalizeSQL(expected.sql))
			c.Check(normalizeSQLArgs(args), qt.DeepEquals, normalizeSQLArgs(expected.args))
			return expected.result, expected.err
		},
		func(query string, args []driver.NamedValue) (driver.Result, error) {
			expected := execs[execIndex.Add(1)-1]
			c.Check(normalizeSQL(query), qt.Equals, normalizeSQL(expected.sql))
			c.Check(normalizeSQLArgs(args), qt.DeepEquals, normalizeSQLArgs(expected.args))
			return driver.RowsAffected(0), expected.err
		},
	)
}

func assertClickHouseSQLMockComplete(
	c *qt.C,
	db *dbtest.DB,
	queries []sqlMockQuery,
	execs []sqlMockExec,
) {
	c.Check(db.QueryCount(), qt.Equals, len(queries))
	c.Check(db.ExecCount(), qt.Equals, len(execs))
}

func normalizeSQL(query string) string {
	return strings.Join(strings.Fields(query), " ")
}

func normalizeSQLArgs(args []driver.NamedValue) []driver.NamedValue {
	return append([]driver.NamedValue{}, args...)
}
