package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// autoIncrementColumn is a desired schema whose table t holds the key column
// id, declared auto-increment as field says.
func autoIncrementColumn(field schemamodel.Field) *schemamodel.Database {
	field.StructName, field.Name, field.Primary = "T", "id", true
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{field},
	}
}

// builtAutoIncrementColumn is a read of t whose key column id the server built
// with the type dataType, auto-increment or not.
func builtAutoIncrementColumn(dataType string, autoIncrement bool) *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name: "t",
		Type: "BASE TABLE",
		Columns: []catalog.Column{{
			Name: "id", DataType: dataType, ColumnType: dataType, UDTName: dataType,
			IsNullable: "NO", IsPrimaryKey: true, IsAutoIncrement: autoIncrement,
		}},
	}}}
}

// TestCompareWithDatabaseInfo_AutoIncrement_ReportsAChange compares a
// declared auto-increment with the one the server reports: AUTO_INCREMENT on
// the MySQL family, AUTOINCREMENT on SQLite, IDENTITY on SQL Server. Measured
// on master against MySQL 26.7, MariaDB 12.3, SQLite and SQL Server 2025, a
// declaration that added one compared synced on each, and the column never got
// it.
func TestCompareWithDatabaseInfo_AutoIncrement_ReportsAChange(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		declared schemamodel.Field
		dataType string
		built    bool
		want     string
	}{
		{name: "MySQL gains it", dialect: "mysql", declared: schemamodel.Field{Type: "INT", AutoInc: true}, dataType: "int", built: false, want: "false -> true"},
		{name: "MySQL loses it", dialect: "mysql", declared: schemamodel.Field{Type: "INT"}, dataType: "int", built: true, want: "true -> false"},
		{name: "MariaDB gains it", dialect: "mariadb", declared: schemamodel.Field{Type: "INT", AutoInc: true}, dataType: "int", built: false, want: "false -> true"},
		{
			name: "MySQL, from an identity generation", dialect: "mysql",
			declared: schemamodel.Field{Type: "BIGINT", IdentityGeneration: "BY DEFAULT"}, dataType: "bigint", built: false, want: "false -> true",
		},
		{name: "SQLite gains it", dialect: "sqlite", declared: schemamodel.Field{Type: "INTEGER", AutoInc: true}, dataType: "integer", built: false, want: "false -> true"},
		{name: "SQLite loses it", dialect: "sqlite", declared: schemamodel.Field{Type: "INTEGER"}, dataType: "integer", built: true, want: "true -> false"},
		{
			// Written as INT AUTO_INCREMENT, so a SERIAL key a server holds
			// as a plain INT is missing it.
			name: "MySQL, a SERIAL key held as a plain integer", dialect: "mysql",
			declared: schemamodel.Field{Type: "SERIAL"}, dataType: "int", built: false, want: "false -> true",
		},
		{name: "SQL Server gains it", dialect: "sqlserver", declared: schemamodel.Field{Type: "INT", AutoInc: true}, dataType: "int", built: false, want: "false -> true"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), autoIncrementColumn(test.declared), builtAutoIncrementColumn(test.dataType, test.built),
				catalog.ServerInfo{Dialect: test.dialect}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes, qt.DeepEquals, map[string]string{"auto_increment": test.want})
		})
	}
}

// TestCompareWithDatabaseInfo_AutoIncrement_AgreesWhenEqual is the control
// for the rows above, and holds the targets whose reader means something
// else by the flag: on PostgreSQL it is a serial column's owned sequence,
// which the type and the default compare, so an identity column reported
// without it is no change.
func TestCompareWithDatabaseInfo_AutoIncrement_AgreesWhenEqual(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		declared schemamodel.Field
		dataType string
		built    bool
	}{
		{name: "MySQL with it", dialect: "mysql", declared: schemamodel.Field{Type: "INT", AutoInc: true}, dataType: "int", built: true},
		{name: "MySQL without it", dialect: "mysql", declared: schemamodel.Field{Type: "INT"}, dataType: "int", built: false},
		{name: "SQLite with it", dialect: "sqlite", declared: schemamodel.Field{Type: "INTEGER", AutoInc: true}, dataType: "integer", built: true},
		{name: "SQL Server with it", dialect: "sqlserver", declared: schemamodel.Field{Type: "INT", AutoInc: true}, dataType: "int", built: true},
		{name: "MySQL, a SERIAL key", dialect: "mysql", declared: schemamodel.Field{Type: "SERIAL"}, dataType: "int", built: true},
		{name: "SQL Server, a SERIAL key", dialect: "sqlserver", declared: schemamodel.Field{Type: "SERIAL"}, dataType: "int", built: true},
		{
			// SQLite writes it as the rowid, which numbers rows without
			// AUTOINCREMENT, so the declaration does not say which.
			name: "SQLite, a SERIAL key some tool made AUTOINCREMENT", dialect: "sqlite",
			declared: schemamodel.Field{Type: "SERIAL"}, dataType: "integer", built: true,
		},
		{
			name: "PostgreSQL, an identity column", dialect: "postgres",
			declared: schemamodel.Field{Type: "bigint", IdentityGeneration: "BY DEFAULT"}, dataType: "int8", built: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current := builtAutoIncrementColumn(test.dataType, test.built)
			current.Tables[0].Columns[0].IdentityGeneration = test.declared.IdentityGeneration

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), autoIncrementColumn(test.declared), current,
				catalog.ServerInfo{Dialect: test.dialect}, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}
