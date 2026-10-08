package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// serverDatabases are the user databases of a whole server, as its reader
// describes them.
func serverDatabases() *catalog.Database {
	return &catalog.Database{Schemas: []catalog.Schema{
		{Name: "r1", Charset: "utf8mb4", Collate: "utf8mb4_0900_ai_ci"},
		{Name: "r3", Charset: "utf8mb4", Collate: "utf8mb4_0900_ai_ci"},
	}}
}

// declaredDatabases is a declaration of r1, with the attributes given, and of
// r9 with a collation of its own.
func declaredDatabases(r1 schemamodel.Schema) *schemamodel.Database {
	r1.Name = "r1"
	return &schemamodel.Database{Schemas: []schemamodel.Schema{
		r1,
		{Name: "r9", Charset: "utf8mb4", Collate: "utf8mb4_bin"},
		{Name: "mysql"},
	}}
}

// TestCompareWithDatabaseInfo_ComparesTheDatabasesOfAWholeServer_HappyPath
// compares the databases of a connection to a whole MySQL-family server, as
// the pinned community binary v1.3.0 does (stokaro/ptah#3789): measured on
// MySQL 8.4.11 and MariaDB 11.8.9, a database only the file declares is
// created, one only the server holds is dropped, and a declared character set
// or collation that differs is changed. A database the server owns is never
// planned, and an attribute the declaration leaves out is the server's.
func TestCompareWithDatabaseInfo_ComparesTheDatabasesOfAWholeServer_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		dialect      string
		r1           schemamodel.Schema
		wantModified []difftypes.SchemaChange
	}{
		{
			name:    "MySQL, a collation that differs",
			dialect: platform.MySQL,
			r1:      schemamodel.Schema{Charset: "latin1", Collate: "latin1_swedish_ci"},
			wantModified: []difftypes.SchemaChange{{
				Name: "r1", Charset: "latin1", Collate: "latin1_swedish_ci",
				CurrentCharset: "utf8mb4", CurrentCollate: "utf8mb4_0900_ai_ci",
			}},
		},
		{
			name:    "MariaDB, the same attributes in another case",
			dialect: platform.MariaDB,
			r1:      schemamodel.Schema{Charset: "UTF8MB4", Collate: "UTF8MB4_0900_AI_CI"},
		},
		{
			name:    "MySQL, no attributes declared",
			dialect: platform.MySQL,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), declaredDatabases(test.r1), serverDatabases(),
				catalog.ServerInfo{Dialect: test.dialect, WholeServer: true}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.SchemasAdded, qt.DeepEquals, []schemamodel.Schema{{Name: "r9", Charset: "utf8mb4", Collate: "utf8mb4_bin"}})
			c.Assert(diff.SchemasRemoved, qt.DeepEquals, []string{"r3"})
			c.Assert(diff.SchemasModified, qt.DeepEquals, test.wantModified)
			c.Assert(diff.HasChanges(), qt.IsTrue)
		})
	}
}

// TestCompareWithDatabaseInfo_ComparesNoDatabasesWithoutAWholeServer is the
// control: a connection to one database, a PostgreSQL connection, and a
// server described without a connection plan no database, whatever the two
// sides declare.
func TestCompareWithDatabaseInfo_ComparesNoDatabasesWithoutAWholeServer(t *testing.T) {
	tests := []struct {
		name string
		info catalog.ServerInfo
	}{
		{name: "a connection to one MySQL database", info: catalog.ServerInfo{Dialect: platform.MySQL, Schema: "r1"}},
		{name: "a MySQL server described without a connection", info: catalog.ServerInfo{Dialect: platform.MySQL}},
		{name: "a PostgreSQL connection", info: catalog.ServerInfo{Dialect: platform.Postgres, WholeServer: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), declaredDatabases(schemamodel.Schema{}), serverDatabases(), test.info, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.SchemasAdded, qt.IsNil)
			c.Assert(diff.SchemasRemoved, qt.IsNil)
			c.Assert(diff.SchemasModified, qt.IsNil)
		})
	}
}

// TestCompareWithDatabaseInfo_DeclaresTheDatabaseOfADesiredTable reads a
// database a desired table is in as declared, with no schema block naming it:
// r1, which the server holds, is kept, and r4, which it does not, is created
// before the table in it. Dropped instead, r1 would take the table with it.
func TestCompareWithDatabaseInfo_DeclaresTheDatabaseOfADesiredTable(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Tables: []schemamodel.Table{
		{Name: "t", Schema: "r1"},
		{Name: "v", Schema: "r4"},
	}}

	diff, err := schemadiff.CompareWithDatabaseInfo(
		t.Context(), desired, serverDatabases(), catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true}, nil, must.Must(builtin.New()),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.SchemasAdded, qt.DeepEquals, []schemamodel.Schema{{Name: "r4"}})
	c.Assert(diff.SchemasRemoved, qt.DeepEquals, []string{"r3"})
	c.Assert(diff.SchemasModified, qt.IsNil)
}

// TestCompareWithDatabaseInfo_ComparesTheDatabasesOfAWholeServer_FailurePath
// refuses a desired object that names no database. Compared with a whole
// server it would be created where no database is selected, which the server
// refuses, and the object it stands for would be dropped; the pinned community
// binary v1.3.0 refuses an HCL table without a schema, measured on MySQL
// 8.4.11.
func TestCompareWithDatabaseInfo_ComparesTheDatabasesOfAWholeServer_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		wantErr string
	}{
		{
			name:    "a table",
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "r1"}}, Tables: []schemamodel.Table{{Name: "t"}}},
			wantErr: `the desired table "t" names no database, and a MySQL or MariaDB URL naming no database is compared as a whole server, .*`,
		},
		{
			name:    "a sequence",
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "r1"}}, Sequences: []schemamodel.Sequence{{Name: "s"}}},
			wantErr: `the desired sequence "s" names no database, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), test.desired, serverDatabases(), catalog.ServerInfo{Dialect: platform.MariaDB, WholeServer: true}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(diff, qt.IsNil)
		})
	}
}
