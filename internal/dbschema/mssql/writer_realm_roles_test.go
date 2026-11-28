package mssql_test

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mssql"
)

// emptyRealmQueries are the queries a realm cleanup asks of a database that
// holds nothing it removes, after its access check and the roles it drops.
func emptyRealmQueries() []writerQueryExpectation {
	return []writerQueryExpectation{
		queryExpectation("FROM sys.triggers AS t", []any{"dbo"}, []string{"category", "schema", "name", "detail"}),
		queryExpectation("COALESCE(t.temporal_type, 0)", nil, realmObjectColumns()),
		queryExpectation("FROM sys.foreign_keys AS fk", nil,
			[]string{"parent_schema", "parent_table", "name", "referenced_schema", "referenced_table"}),
		queryExpectation("FROM sys.sql_expression_dependencies AS dependency", nil, []string{"referencing_id", "referenced_id"}),
		queryExpectation("FROM sys.types AS t", nil, []string{"schema", "name"}),
		queryExpectation("FROM sys.xml_schema_collections AS x", nil, []string{"schema", "name"}),
		queryExpectation("SELECT s.name\n\t\tFROM sys.schemas AS s", []any{"dbo"}, []string{"schema"}),
		queryExpectation("SELECT TOP (1) residual.category", []any{"dbo"}, []string{"category", "schema", "name", "detail"}),
		queryExpectation("FROM sys.triggers AS t", []any{"dbo"}, []string{"category", "schema", "name", "detail"}),
	}
}

// TestWriterDropDatabaseRealmKeeping_DropsTheRolesTheRunCreated pins that a
// realm cleanup after a claim that recorded the database roles drops, in its
// transaction and before anything else, each role the claim did not find:
// first the memberships and the ownerships the role holds, then the role. The
// names compare exactly, so a role whose name differs from a kept one only in
// case is the run's.
func TestWriterDropDatabaseRealmKeeping_DropsTheRolesTheRunCreated(t *testing.T) {
	c := qt.New(t)
	queries := []writerQueryExpectation{
		queryExpectation("HAS_PERMS_BY_NAME", nil, realmAccessColumns(), []driver.Value{"ptah_dev", true, true}),
		queryExpectation("FROM sys.database_principals AS p\n\t\tWHERE p.type = N'R'", nil, []string{"principal_id", "name"},
			[]driver.Value{int64(5), "Kept Role"},
			[]driver.Value{int64(6), "a]pp"},
			[]driver.Value{int64(7), "Kept role"},
		),
		queryExpectation("WHERE drm.role_principal_id IN (6, 7) OR drm.member_principal_id IN (6, 7)", nil, []string{"statement"},
			[]driver.Value{"ALTER ROLE [a]]pp] DROP MEMBER [Kept Role]"},
			[]driver.Value{"ALTER AUTHORIZATION ON SCHEMA::[reports] TO [dbo]"},
		),
	}
	sqlMock := newWriterSQLMock(t,
		append(queries, emptyRealmQueries()...),
		[]writerExecExpectation{
			execExpectation("ALTER ROLE [a]]pp] DROP MEMBER [Kept Role]", nil),
			execExpectation("ALTER AUTHORIZATION ON SCHEMA::[reports] TO [dbo]", nil),
			execExpectation("DROP ROLE [a]]pp]", nil),
			execExpectation("DROP ROLE [Kept role]", nil),
			execExpectation(dropKeptSchemaComment, nil),
		},
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealmKeeping(t.Context(), dbreset.Kept{DatabaseRoles: []string{"Kept Role"}, DatabaseRolesRecorded: true})

	c.Assert(err, qt.IsNil)
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.CommitCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 0)
}

// TestWriterDatabaseRoles_HappyPath pins what a claim records: the database
// roles beyond the fixed ones, sorted by name.
func TestWriterDatabaseRoles_HappyPath(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation("FROM sys.database_principals AS p\n\t\tWHERE p.type = N'R'", nil, []string{"principal_id", "name"},
				[]driver.Value{int64(5), "reporting"},
				[]driver.Value{int64(6), "App"},
			),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	roles, err := writer.DatabaseRoles(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(roles, qt.DeepEquals, []string{"App", "reporting"})
}

// TestWriterDropDatabaseRealmKeeping_RefusesWhatItCannotKeep pins that the
// cleanup refuses, before it opens a transaction, a Kept naming what SQL
// Server's realm cleanup has no way to keep.
func TestWriterDropDatabaseRealmKeeping_RefusesWhatItCannotKeep(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t, nil, nil)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealmKeeping(t.Context(), dbreset.Kept{Schemas: []string{"audit"}, DatabaseRolesRecorded: true})

	c.Assert(err, qt.ErrorMatches, `sqlserver: clean dev database realm: this writer cannot keep the extensions, schemas, default privileges or database-scoped objects the database held before the run`)
	c.Assert(db.BeginCount(), qt.Equals, 0)
}

// TestWriterDropDatabaseRealmKeeping_RefusesARoleWithoutARecord pins that a
// cleanup whose claim did not record the roles drops none, and refuses the
// role it finds as [mssql.Writer.DropDatabaseRealm] does, since nothing says
// whose it is.
func TestWriterDropDatabaseRealmKeeping_RefusesARoleWithoutARecord(t *testing.T) {
	c := qt.New(t)
	sqlMock := newWriterSQLMock(t,
		[]writerQueryExpectation{
			queryExpectation("HAS_PERMS_BY_NAME", nil, realmAccessColumns(), []driver.Value{"ptah_dev", true, true}),
			queryExpectation("FROM sys.triggers AS t", []any{"dbo"}, []string{"category", "schema", "name", "detail"},
				[]driver.Value{"database principal", "", "APP", "DATABASE_ROLE"}),
		},
		nil,
	)
	db := dbtest.OpenWithExec(t, sqlMock.query, sqlMock.exec)
	writer := mssql.NewSQLServerWriter(db.SQL, "dbo")

	err := writer.DropDatabaseRealmKeeping(t.Context(), dbreset.Kept{})

	c.Assert(err, qt.ErrorMatches, `sqlserver: refusing to clean database realm with unsupported database-scoped database principal \[APP\] \(DATABASE_ROLE\)`)
	c.Assert(db.ExecCount(), qt.Equals, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}
