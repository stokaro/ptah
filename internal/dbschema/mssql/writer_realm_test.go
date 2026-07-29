package mssql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/microsoft/go-mssqldb"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/dbschema/dbtest"
	"github.com/stokaro/ptah/internal/dbschema/mssql"
	"github.com/stokaro/ptah/internal/sqlident"
)

func TestWriterDropDatabaseRealm_CleansCrossSchemaGraph(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
			),
			queryExpectation(
				"COALESCE(t.temporal_type, 0)",
				nil,
				realmObjectColumns(),
				[]driver.Value{int64(1), int64(0), "a]pp", "row_filter", "SP", "SECURITY_POLICY", int64(0), int64(0), int64(0)},
				[]driver.Value{int64(2), int64(0), "a]pp", "order]view", "V ", "VIEW", int64(0), int64(0), int64(0)},
				[]driver.Value{int64(3), int64(0), "z_data", "child", "U ", "USER_TABLE", int64(0), int64(0), int64(0)},
				[]driver.Value{int64(4), int64(0), "a]pp", "normalize_id", "FN", "SQL_SCALAR_FUNCTION", int64(0), int64(0), int64(0)},
				[]driver.Value{int64(5), int64(0), "m_util", "order_seq", "SO", "SEQUENCE_OBJECT", int64(0), int64(0), int64(0)},
				[]driver.Value{int64(6), int64(0), "dbo", "orders_alias", "SN", "SYNONYM", int64(0), int64(0), int64(0)},
				[]driver.Value{int64(7), int64(3), "z_data", "df_child_id", "D ", "DEFAULT_CONSTRAINT", int64(0), int64(0), int64(0)},
				[]driver.Value{int64(8), int64(0), "types", "id_list", "TT", "TYPE_TABLE", int64(0), int64(0), int64(0)},
				[]driver.Value{int64(9), int64(0), "external", "event]feed", "ET", "EXTERNAL_TABLE", int64(0), int64(0), int64(0)},
			),
			queryExpectation(
				"FROM sys.foreign_keys AS fk",
				nil,
				[]string{"parent_schema", "parent_table", "name", "referenced_schema", "referenced_table"},
				[]driver.Value{"z_data", "child", "fk]child_parent", "m_util", "parent"},
			),
			queryExpectation(
				"FROM sys.sql_expression_dependencies AS dependency",
				nil,
				[]string{"referencing_id", "referenced_id"},
				[]driver.Value{int64(2), int64(3)},
				[]driver.Value{int64(3), int64(4)},
			),
			queryExpectation(
				"FROM sys.types AS t",
				nil,
				[]string{"schema", "name"},
				[]driver.Value{"types", "id_list"},
			),
			queryExpectation(
				"FROM sys.xml_schema_collections AS x",
				nil,
				[]string{"schema", "name"},
				[]driver.Value{"types", "payload]xml"},
			),
			queryExpectation(
				"SELECT s.name\n\t\tFROM sys.schemas AS s",
				[]any{"dbo"},
				[]string{"schema"},
				[]driver.Value{"a]pp"},
				[]driver.Value{"external"},
				[]driver.Value{"m_util"},
				[]driver.Value{"types"},
				[]driver.Value{"z_data"},
			),
			queryExpectation(
				"SELECT TOP (1) residual.category",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
			),
		},
		[]writerExecExpectation{
			execExpectation("ALTER TABLE [z_data].[child] DROP CONSTRAINT [fk]]child_parent]", nil),
			execExpectation("DROP SECURITY POLICY IF EXISTS [a]]pp].[row_filter]", nil),
			execExpectation("DROP VIEW IF EXISTS [a]]pp].[order]]view]", nil),
			execExpectation("DROP SYNONYM IF EXISTS [dbo].[orders_alias]", nil),
			execExpectation("DROP EXTERNAL TABLE IF EXISTS [external].[event]]feed]", nil),
			execExpectation("DROP SEQUENCE IF EXISTS [m_util].[order_seq]", nil),
			execExpectation("DROP TABLE IF EXISTS [z_data].[child]", nil),
			execExpectation("DROP FUNCTION IF EXISTS [a]]pp].[normalize_id]", nil),
			execExpectation("DROP TYPE IF EXISTS [types].[id_list]", nil),
			execExpectation("DROP XML SCHEMA COLLECTION [types].[payload]]xml]", nil),
			execExpectation("DROP SCHEMA IF EXISTS [a]]pp]", nil),
			execExpectation("DROP SCHEMA IF EXISTS [external]", nil),
			execExpectation("DROP SCHEMA IF EXISTS [m_util]", nil),
			execExpectation("DROP SCHEMA IF EXISTS [types]", nil),
			execExpectation("DROP SCHEMA IF EXISTS [z_data]", nil),
		},
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

func TestWriterDropDatabaseRealm_RequiresCompleteMetadataVisibility(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", false, true},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm without CONTROL permission on the current database`,
	)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RequiresViewDefinitionMetadataVisibility(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, false},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm without VIEW DEFINITION permission on the current database`,
	)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RejectsUnknownObjectAndRollsBack(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
			),
			queryExpectation(
				"COALESCE(t.temporal_type, 0)",
				nil,
				realmObjectColumns(),
				[]driver.Value{
					int64(1), int64(0), "jobs", "work_queue", "SQ", "SERVICE_QUEUE",
					int64(0), int64(0), int64(0),
				},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm with unsupported user objects: `+
			`\[jobs\]\.\[work_queue\] \(SERVICE_QUEUE\)`,
	)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RejectsResidualObjectAndRollsBack(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
			),
			queryExpectation(
				"COALESCE(t.temporal_type, 0)",
				nil,
				realmObjectColumns(),
			),
			queryExpectation(
				"FROM sys.foreign_keys AS fk",
				nil,
				[]string{"parent_schema", "parent_table", "name", "referenced_schema", "referenced_table"},
			),
			queryExpectation(
				"FROM sys.sql_expression_dependencies AS dependency",
				nil,
				[]string{"referencing_id", "referenced_id"},
			),
			queryExpectation(
				"FROM sys.types AS t",
				nil,
				[]string{"schema", "name"},
			),
			queryExpectation(
				"FROM sys.xml_schema_collections AS x",
				nil,
				[]string{"schema", "name"},
			),
			queryExpectation(
				"SELECT s.name\n\t\tFROM sys.schemas AS s",
				[]any{"dbo"},
				[]string{"schema"},
			),
			queryExpectation(
				"SELECT TOP (1) residual.category",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
				[]driver.Value{"object", "rogue]", "queue]", "SERVICE_QUEUE"},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNotNil)
	c.Assert(
		err.Error(),
		qt.Contains,
		"refusing to commit database realm cleanup: residual object [rogue]]].[queue]]] (SERVICE_QUEUE)",
	)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_DDLFailureRollsBack(t *testing.T) {
	c := qt.New(t)
	dropErr := errors.New("table is still referenced")
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
			),
			queryExpectation(
				"COALESCE(t.temporal_type, 0)",
				nil,
				realmObjectColumns(),
				[]driver.Value{
					int64(1), int64(0), "dbo", "users", "U", "USER_TABLE",
					int64(0), int64(0), int64(0),
				},
			),
			queryExpectation(
				"FROM sys.foreign_keys AS fk",
				nil,
				[]string{"parent_schema", "parent_table", "name", "referenced_schema", "referenced_table"},
			),
			queryExpectation(
				"FROM sys.sql_expression_dependencies AS dependency",
				nil,
				[]string{"referencing_id", "referenced_id"},
			),
			queryExpectation(
				"FROM sys.types AS t",
				nil,
				[]string{"schema", "name"},
			),
			queryExpectation(
				"FROM sys.xml_schema_collections AS x",
				nil,
				[]string{"schema", "name"},
			),
			queryExpectation(
				"SELECT s.name\n\t\tFROM sys.schemas AS s",
				[]any{"dbo"},
				[]string{"schema"},
			),
		},
		[]writerExecExpectation{
			execExpectation("DROP TABLE IF EXISTS [dbo].[users]", dropErr),
		},
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorIs, dropErr)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_DisablesTemporalTableBeforeDroppingHistory(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
			),
			queryExpectation(
				"COALESCE(t.temporal_type, 0)",
				nil,
				realmObjectColumns(),
				[]driver.Value{
					int64(2), int64(0), "audit", "a_history", "U ", "USER_TABLE",
					int64(1), int64(0), int64(0),
				},
				[]driver.Value{
					int64(1), int64(0), "app", "z_current", "U ", "USER_TABLE",
					int64(2), int64(2), int64(0),
				},
			),
			queryExpectation(
				"FROM sys.foreign_keys AS fk",
				nil,
				[]string{"parent_schema", "parent_table", "name", "referenced_schema", "referenced_table"},
			),
			queryExpectation(
				"FROM sys.sql_expression_dependencies AS dependency",
				nil,
				[]string{"referencing_id", "referenced_id"},
			),
			queryExpectation(
				"FROM sys.types AS t",
				nil,
				[]string{"schema", "name"},
			),
			queryExpectation(
				"FROM sys.xml_schema_collections AS x",
				nil,
				[]string{"schema", "name"},
			),
			queryExpectation(
				"SELECT s.name\n\t\tFROM sys.schemas AS s",
				[]any{"dbo"},
				[]string{"schema"},
			),
			queryExpectation(
				"SELECT TOP (1) residual.category",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
			),
		},
		[]writerExecExpectation{
			execExpectation(
				"ALTER TABLE [app].[z_current] SET (SYSTEM_VERSIONING = OFF)",
				nil,
			),
			execExpectation("DROP TABLE IF EXISTS [app].[z_current]", nil),
			execExpectation("DROP TABLE IF EXISTS [audit].[a_history]", nil),
		},
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

func TestWriterDropDatabaseRealm_RejectsLedgerTableBeforeDDL(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
			),
			queryExpectation(
				"COALESCE(t.temporal_type, 0)",
				nil,
				realmObjectColumns(),
				[]driver.Value{
					int64(1), int64(0), "dbo", "ledger_events", "U ", "USER_TABLE",
					int64(0), int64(0), int64(1),
				},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm with unsupported user objects: `+
			`\[dbo\]\.\[ledger_events\] \(LEDGER_TABLE\)`,
	)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RejectsSystemDatabase(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"master", true, true},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorMatches, `sqlserver: refusing to clean system database \[master\]`)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RejectsUnsupportedDatabaseArtifactBeforeDDL(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
				[]driver.Value{"database DDL trigger", "", "deny]drop", "SQL_TRIGGER"},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm with unsupported database-scoped `+
			`database DDL trigger \[deny\]\]drop\] \(SQL_TRIGGER\)`,
	)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RejectsReplicationEnabledDatabaseBeforeDDL(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
				[]driver.Value{
					"replication-enabled database",
					"",
					"ptah_dev",
					"is_distributor=1, is_published=0, is_merge_published=0",
				},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm with unsupported database-scoped `+
			`replication-enabled database \[ptah_dev\] `+
			`\(is_distributor=1, is_published=0, is_merge_published=0\)`,
	)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RejectsSubscriberDatabaseBeforeDDL(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			queryExpectation(
				"DATABASEPROPERTYEX(DB_NAME(), N'IsSubscribed')",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
				[]driver.Value{
					"subscriber database",
					"",
					"ptah_dev",
					"IsSubscribed=1",
				},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm with unsupported database-scoped `+
			`subscriber database \[ptah_dev\] \(IsSubscribed=1\)`,
	)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RejectsUnknownSubscriptionStateBeforeDDL(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			queryExpectation(
				"COALESCE(TRY_CONVERT(int, subscription.raw_value), -1) <> 0",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
				[]driver.Value{
					"unknown database subscription state",
					"",
					"ptah_dev",
					"IsSubscribed=<unavailable>",
				},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm with unsupported database-scoped `+
			`unknown database subscription state \[ptah_dev\] `+
			`\(IsSubscribed=<unavailable>\)`,
	)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RejectsUnexpectedSubscriptionStateBeforeDDL(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			queryExpectation(
				"COALESCE(TRY_CONVERT(int, subscription.raw_value), -1) <> 0",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
				[]driver.Value{
					"unknown database subscription state",
					"",
					"ptah_dev",
					"IsSubscribed=2",
				},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm with unsupported database-scoped `+
			`unknown database subscription state \[ptah_dev\] \(IsSubscribed=2\)`,
	)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RejectsReplicatedTableBeforeDDL(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
				[]driver.Value{
					"replicated table",
					"",
					"app.orders",
					"is_replicated=1, is_merge_published=0, is_sync_tran_subscribed=0",
				},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm with unsupported database-scoped `+
			`replicated table \[app\.orders\] `+
			`\(is_replicated=1, is_merge_published=0, is_sync_tran_subscribed=0\)`,
	)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RejectsPreservedSchemaPermissionBeforeDDL(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
				[]driver.Value{
					"schema permission",
					"",
					"dbo: GRANT SELECT TO public",
					"SCHEMA_PERMISSION",
				},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm with unsupported database-scoped `+
			`schema permission \[dbo: GRANT SELECT TO public\] \(SCHEMA_PERMISSION\)`,
	)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_RejectsLateDatabaseArtifactAndRollsBack(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		append(
			emptyRealmPlanQueryExpectations(),
			queryExpectation(
				"SELECT TOP (1) residual.category",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
				[]driver.Value{"database-scoped credential", "", "late]credential", "DATABASE_SCOPED_CREDENTIAL"},
			),
		),
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to commit database realm cleanup: residual database-scoped credential `+
			`\[late\]\]credential\] \(DATABASE_SCOPED_CREDENTIAL\)`,
	)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_IsIdempotentWhenRealmIsEmpty(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		append(
			emptyRealmPlanQueryExpectations(),
			queryExpectation(
				"SELECT TOP (1) residual.category",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
			),
			queryExpectation(
				"FROM sys.triggers AS t",
				[]any{"dbo"},
				[]string{"category", "schema", "name", "detail"},
			),
		),
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

func TestWriterDropDatabaseRealm_Live(t *testing.T) {
	c := qt.New(t)
	db := openLiveSQLServerRealmDatabase(t)
	writer := mssql.NewSQLServerWriter(db, "dbo")
	t.Cleanup(func() {
		qt.Check(t, writer.DropDatabaseRealm(context.Background()), qt.IsNil)
	})
	err := writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)

	statements := []string{
		"CREATE SCHEMA [app]",
		"CREATE SCHEMA [audit]",
		"CREATE TABLE [app].[parent] ([id] bigint NOT NULL PRIMARY KEY)",
		"CREATE TABLE [audit].[child] (" +
			"[id] bigint NOT NULL PRIMARY KEY, " +
			"[parent_id] bigint NOT NULL, " +
			"CONSTRAINT [fk_child_parent] FOREIGN KEY ([parent_id]) REFERENCES [app].[parent] ([id]))",
		"CREATE TABLE [app].[z_temporal] (" +
			"[id] bigint NOT NULL PRIMARY KEY, " +
			"[valid_from] datetime2 GENERATED ALWAYS AS ROW START NOT NULL, " +
			"[valid_to] datetime2 GENERATED ALWAYS AS ROW END NOT NULL, " +
			"PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) " +
			"WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [audit].[a_temporal_history]))",
		"CREATE FUNCTION [app].[normalize_id] (@value bigint) RETURNS bigint AS BEGIN RETURN @value END",
		"CREATE VIEW [audit].[child_view] AS SELECT [id], [parent_id] FROM [audit].[child]",
		"CREATE VIEW [audit].[temporal_view] AS " +
			"SELECT [id] FROM [app].[z_temporal] FOR SYSTEM_TIME ALL",
		"CREATE SEQUENCE [app].[order_seq] AS bigint START WITH 1 INCREMENT BY 1",
		"CREATE SYNONYM [dbo].[parent_alias] FOR [app].[parent]",
		"CREATE TYPE [app].[identifier] FROM bigint NOT NULL",
		"EXEC sys.sp_addextendedproperty " +
			"@name = N'realm_note', @value = N'test', " +
			"@level0type = N'SCHEMA', @level0name = N'app'",
	}
	for _, statement := range statements {
		_, err := db.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("execute SQL Server fixture statement: %s", statement))
	}

	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)
	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)

	var objectCount int
	err = db.QueryRowContext(t.Context(), `
		SELECT COUNT(*)
		FROM sys.objects AS o
		JOIN sys.schemas AS s ON s.schema_id = o.schema_id
		WHERE o.is_ms_shipped = 0
		  AND s.name NOT IN (
			  N'sys', N'INFORMATION_SCHEMA', N'guest',
			  N'db_owner', N'db_accessadmin', N'db_securityadmin',
			  N'db_ddladmin', N'db_backupoperator', N'db_datareader',
			  N'db_datawriter', N'db_denydatareader', N'db_denydatawriter'
		  )
	`).Scan(&objectCount)
	c.Assert(err, qt.IsNil)
	c.Assert(objectCount, qt.Equals, 0)

	var typeCount int
	err = db.QueryRowContext(t.Context(), `
		SELECT COUNT(*)
		FROM sys.types AS t
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		WHERE t.is_user_defined = 1
		  AND s.name NOT IN (N'sys', N'INFORMATION_SCHEMA')
	`).Scan(&typeCount)
	c.Assert(err, qt.IsNil)
	c.Assert(typeCount, qt.Equals, 0)

	var schemaCount int
	err = db.QueryRowContext(t.Context(), `
		SELECT COUNT(*)
		FROM sys.schemas AS s
		WHERE s.name NOT IN (
			  N'dbo', N'sys', N'INFORMATION_SCHEMA', N'guest',
			  N'db_owner', N'db_accessadmin', N'db_securityadmin',
			  N'db_ddladmin', N'db_backupoperator', N'db_datareader',
			  N'db_datawriter', N'db_denydatareader', N'db_denydatawriter'
		  )
	`).Scan(&schemaCount)
	c.Assert(err, qt.IsNil)
	c.Assert(schemaCount, qt.Equals, 0)
}

func TestWriterDropDatabaseRealm_RejectsDatabaseScopedArtifactLive(t *testing.T) {
	c := qt.New(t)
	db := openLiveSQLServerRealmDatabase(t)
	writer := mssql.NewSQLServerWriter(db, "dbo")
	err := writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		_, cleanupErr := db.ExecContext(
			context.Background(),
			"DROP TRIGGER IF EXISTS [realm]]guard] ON DATABASE",
		)
		qt.Check(t, cleanupErr, qt.IsNil)
		qt.Check(t, writer.DropDatabaseRealm(context.Background()), qt.IsNil)
	})

	_, err = db.ExecContext(t.Context(), "CREATE TABLE [dbo].[kept_until_safe] ([id] bigint NOT NULL)")
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(
		t.Context(),
		"CREATE TRIGGER [realm]]guard] ON DATABASE FOR CREATE_TABLE AS RETURN",
	)
	c.Assert(err, qt.IsNil)

	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm with unsupported database-scoped `+
			`database DDL trigger \[realm\]\]guard\] \(SQL_TRIGGER\)`,
	)

	var tableCount int
	err = db.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM sys.tables WHERE object_id = OBJECT_ID(N'dbo.kept_until_safe')",
	).Scan(&tableCount)
	c.Assert(err, qt.IsNil)
	c.Assert(tableCount, qt.Equals, 1)

	_, err = db.ExecContext(t.Context(), "DROP TRIGGER IF EXISTS [realm]]guard] ON DATABASE")
	c.Assert(err, qt.IsNil)
	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)
}

func TestWriterDropDatabaseRealm_RejectsPreservedSchemaPermissionLive(t *testing.T) {
	c := qt.New(t)
	db := openLiveSQLServerRealmDatabase(t)
	writer := mssql.NewSQLServerWriter(db, "dbo")
	err := writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		_, cleanupErr := db.ExecContext(
			context.Background(),
			"REVOKE SELECT ON SCHEMA::[dbo] FROM [public]",
		)
		qt.Check(t, cleanupErr, qt.IsNil)
		qt.Check(t, writer.DropDatabaseRealm(context.Background()), qt.IsNil)
	})

	_, err = db.ExecContext(t.Context(), "GRANT SELECT ON SCHEMA::[dbo] TO [public]")
	c.Assert(err, qt.IsNil)

	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlserver: refusing to clean database realm with unsupported database-scoped `+
			`schema permission \[dbo: GRANT SELECT TO public\] \(SCHEMA_PERMISSION\)`,
	)

	_, err = db.ExecContext(t.Context(), "REVOKE SELECT ON SCHEMA::[dbo] FROM [public]")
	c.Assert(err, qt.IsNil)
	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)
}

func TestWriterDropDatabaseRealm_CancellationAfterBeginRollsBack(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation(
				"HAS_PERMS_BY_NAME",
				nil,
				realmAccessColumns(),
				[]driver.Value{"ptah_dev", true, true},
			),
			{
				queryContains: "FROM sys.triggers AS t",
				args:          []any{"dbo"},
				err:           context.Canceled,
			},
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

func TestWriterDropDatabaseRealm_PreCanceledContextDoesNotBeginTransaction(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	db := dbtest.OpenWithExec(t, nil, nil)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealm(ctx)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(db.BeginCount(), qt.Equals, 0)
	c.Assert(db.QueryCount(), qt.Equals, 0)
	c.Assert(db.ExecCount(), qt.Equals, 0)
}

func openLiveSQLServerRealmDatabase(t *testing.T) *sql.DB {
	t.Helper()
	adminURL, configured := os.LookupEnv("PTAH_SQLSERVER_REALM_TEST_URL")
	if !configured {
		t.Skip("PTAH_SQLSERVER_REALM_TEST_URL is not configured")
	}
	admin, err := sql.Open("sqlserver", adminURL)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, admin.PingContext(t.Context()), qt.IsNil)

	databaseName := fmt.Sprintf("ptah_realm_%d", time.Now().UnixNano())
	quotedDatabase := sqlident.Quote(platform.SQLServer, databaseName)
	_, err = admin.ExecContext(t.Context(), "CREATE DATABASE "+quotedDatabase)
	qt.Assert(t, err, qt.IsNil)

	parsedURL, err := url.Parse(adminURL)
	qt.Assert(t, err, qt.IsNil)
	query := parsedURL.Query()
	query.Set("database", databaseName)
	parsedURL.RawQuery = query.Encode()
	db, err := sql.Open("sqlserver", parsedURL.String())
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, db.PingContext(t.Context()), qt.IsNil)

	t.Cleanup(func() {
		qt.Check(t, db.Close(), qt.IsNil)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, cleanupErr := admin.ExecContext(
			cleanupCtx,
			"ALTER DATABASE "+quotedDatabase+
				" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+quotedDatabase,
		)
		qt.Check(t, cleanupErr, qt.IsNil)
		qt.Check(t, admin.Close(), qt.IsNil)
	})
	return db
}

func emptyRealmPlanQueryExpectations() []writerQueryExpectation {
	return []writerQueryExpectation{
		queryExpectation(
			"HAS_PERMS_BY_NAME",
			nil,
			realmAccessColumns(),
			[]driver.Value{"ptah_dev", true, true},
		),
		queryExpectation(
			"FROM sys.triggers AS t",
			[]any{"dbo"},
			[]string{"category", "schema", "name", "detail"},
		),
		queryExpectation(
			"COALESCE(t.temporal_type, 0)",
			nil,
			realmObjectColumns(),
		),
		queryExpectation(
			"FROM sys.foreign_keys AS fk",
			nil,
			[]string{"parent_schema", "parent_table", "name", "referenced_schema", "referenced_table"},
		),
		queryExpectation(
			"FROM sys.sql_expression_dependencies AS dependency",
			nil,
			[]string{"referencing_id", "referenced_id"},
		),
		queryExpectation(
			"FROM sys.types AS t",
			nil,
			[]string{"schema", "name"},
		),
		queryExpectation(
			"FROM sys.xml_schema_collections AS x",
			nil,
			[]string{"schema", "name"},
		),
		queryExpectation(
			"SELECT s.name\n\t\tFROM sys.schemas AS s",
			[]any{"dbo"},
			[]string{"schema"},
		),
	}
}

func realmAccessColumns() []string {
	return []string{"database_name", "has_control", "has_view_definition"}
}

func realmObjectColumns() []string {
	return []string{
		"object_id",
		"parent_object_id",
		"schema",
		"name",
		"type",
		"type_desc",
		"temporal_type",
		"history_table_id",
		"ledger_type",
	}
}

type writerQueryExpectation struct {
	queryContains string
	args          []any
	result        dbtest.QueryResult
	err           error
}

type writerExecExpectation struct {
	sql string
	err error
}

type writerSQLMock struct {
	c                  *qt.C
	queryExpectations  []writerQueryExpectation
	execExpectations   []writerExecExpectation
	queryExpectationID int
	execExpectationID  int
}

func newWriterSQLMock(
	t *testing.T,
	queryExpectations []writerQueryExpectation,
	execExpectations []writerExecExpectation,
) *writerSQLMock {
	mock := &writerSQLMock{
		c:                 qt.New(t),
		queryExpectations: queryExpectations,
		execExpectations:  execExpectations,
	}
	t.Cleanup(mock.checkExpectations)
	return mock
}

func queryExpectation(
	queryContains string,
	args []any,
	columns []string,
	rows ...[]driver.Value,
) writerQueryExpectation {
	return writerQueryExpectation{
		queryContains: queryContains,
		args:          args,
		result: dbtest.QueryResult{
			Columns: columns,
			Rows:    rows,
		},
	}
}

func execExpectation(statement string, err error) writerExecExpectation {
	return writerExecExpectation{sql: statement, err: err}
}

func (m *writerSQLMock) query(
	query string,
	args []driver.NamedValue,
) (dbtest.QueryResult, error) {
	if m.queryExpectationID >= len(m.queryExpectations) {
		return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
	}
	expectation := m.queryExpectations[m.queryExpectationID]
	m.queryExpectationID++
	m.c.Check(query, qt.Contains, expectation.queryContains)
	m.c.Check(namedValues(args), qt.DeepEquals, expectation.args)
	return expectation.result, expectation.err
}

func (m *writerSQLMock) exec(
	statement string,
	args []driver.NamedValue,
) (driver.Result, error) {
	if m.execExpectationID >= len(m.execExpectations) {
		return nil, fmt.Errorf("unexpected SQL execution: %s", statement)
	}
	expectation := m.execExpectations[m.execExpectationID]
	m.execExpectationID++
	m.c.Check(statement, qt.Equals, expectation.sql)
	m.c.Check(namedValues(args), qt.DeepEquals, []any(nil))
	return driver.RowsAffected(0), expectation.err
}

func (m *writerSQLMock) checkExpectations() {
	m.c.Check(m.queryExpectationID, qt.Equals, len(m.queryExpectations))
	m.c.Check(m.execExpectationID, qt.Equals, len(m.execExpectations))
}

func namedValues(values []driver.NamedValue) []any {
	if len(values) == 0 {
		return nil
	}
	result := make([]any, 0, len(values))
	for _, value := range values {
		result = append(result, value.Value)
	}
	return result
}
