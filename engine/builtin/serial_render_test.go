package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// serialTable declares the table things with the given fields.
func serialTable(fields ...schemamodel.Field) *schemamodel.Database {
	for position := range fields {
		fields[position].StructName = "Thing"
	}
	return &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Thing", Name: "things"}}, Fields: fields}
}

// TestGetOrderedCreateStatements_SerialKey_HappyPath writes a serial key as each
// target's own auto-increment integer. Measured on master against MySQL 26.7,
// MariaDB 12.3 and SQL Server 2025, a SERIAL key was written as a plain INT, so
// a row that named no key failed for want of a value.
func TestGetOrderedCreateStatements_SerialKey_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		field   schemamodel.Field
		want    string
	}{
		{name: "MySQL SERIAL", dialect: "mysql", field: schemamodel.Field{Name: "id", Type: "SERIAL", Primary: true}, want: "`id` INT PRIMARY KEY AUTO_INCREMENT"},
		{name: "MariaDB BIGSERIAL", dialect: "mariadb", field: schemamodel.Field{Name: "id", Type: "BIGSERIAL", Primary: true}, want: "`id` BIGINT PRIMARY KEY AUTO_INCREMENT"},
		{name: "MySQL SMALLSERIAL", dialect: "mysql", field: schemamodel.Field{Name: "id", Type: "SMALLSERIAL", Primary: true}, want: "`id` SMALLINT PRIMARY KEY AUTO_INCREMENT"},
		{name: "SQL Server SERIAL", dialect: "sqlserver", field: schemamodel.Field{Name: "id", Type: "SERIAL", Primary: true}, want: "[id] INT IDENTITY(1,1) PRIMARY KEY"},
		{name: "SQL Server SERIAL outside a key", dialect: "sqlserver", field: schemamodel.Field{Name: "id", Type: "SERIAL"}, want: "[id] INT IDENTITY(1,1) NOT NULL"},
		{
			name: "MySQL SERIAL with a type of its own", dialect: "mysql",
			field: schemamodel.Field{Name: "id", Type: "SERIAL", Primary: true, Overrides: map[string]map[string]string{"mysql": {"type": "INT AUTO_INCREMENT"}}},
			want:  "`id` INT AUTO_INCREMENT PRIMARY KEY,",
		},
		{name: "PostgreSQL keeps SERIAL", dialect: "postgres", field: schemamodel.Field{Name: "id", Type: "SERIAL", Primary: true}, want: `"id" SERIAL PRIMARY KEY`},
		{name: "SQLite keeps the rowid", dialect: "sqlite", field: schemamodel.Field{Name: "id", Type: "SERIAL", Primary: true}, want: `"id" INTEGER NOT NULL PRIMARY KEY,`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(
				serialTable(test.field, schemamodel.Field{Name: "n", Type: "INT", Nullable: true}), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// TestGetOrderedCreateStatements_SerialKey_FailurePath refuses a serial column
// the target refuses as an auto-increment one: MySQL and MariaDB answer an
// AUTO_INCREMENT column no key starts with, and a second one, with ERROR 1075,
// and SQL Server a second IDENTITY with Msg 2744.
func TestGetOrderedCreateStatements_SerialKey_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		fields  []schemamodel.Field
		wantErr string
	}{
		{
			name: "MySQL SERIAL outside a key", dialect: "mysql",
			fields:  []schemamodel.Field{{Name: "id", Type: "INT", Primary: true}, {Name: "seq", Type: "SERIAL"}},
			wantErr: `column things\.seq is a serial type, which mysql writes as AUTO_INCREMENT, and no key of the table starts with it; .*`,
		},
		{
			name: "MariaDB SERIAL as a key's second column", dialect: "mariadb",
			fields:  []schemamodel.Field{{Name: "a", Type: "INT", Primary: true}, {Name: "seq", Type: "SERIAL", Primary: true}},
			wantErr: `column things\.seq is a serial type, which mariadb writes as AUTO_INCREMENT, .*`,
		},
		{
			name: "SQL Server SERIAL beside an identity", dialect: "sqlserver",
			fields:  []schemamodel.Field{{Name: "id", Type: "SERIAL", Primary: true}, {Name: "n", Type: "INT", AutoInc: true}},
			wantErr: `table things declares 2 auto-increment columns \(id, n\); a serial type is written as one here, and sqlserver allows a table one: .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(serialTable(test.fields...), test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestGetOrderedCreateStatements_SerialKey_KeyControls holds the keys a serial
// column may start on MySQL: a unique column, and an index.
func TestGetOrderedCreateStatements_SerialKey_KeyControls(t *testing.T) {
	c := qt.New(t)
	database := serialTable(schemamodel.Field{Name: "id", Type: "INT", Primary: true}, schemamodel.Field{Name: "seq", Type: "SERIAL", Unique: true})
	indexed := serialTable(schemamodel.Field{Name: "id", Type: "INT", Primary: true}, schemamodel.Field{Name: "seq", Type: "SERIAL"})
	indexed.Indexes = []schemamodel.Index{{Name: "things_seq", StructName: "Thing", TableName: "things", Fields: []string{"seq"}}}

	_, uniqueErr := builtin.GetOrderedCreateStatements(database, "mysql")
	_, indexErr := builtin.GetOrderedCreateStatements(indexed, "mysql")

	c.Assert(uniqueErr, qt.IsNil)
	c.Assert(indexErr, qt.IsNil)
}
