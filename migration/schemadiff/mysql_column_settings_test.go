package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mysql"
	"ptah.run/migration/schemadiff"
)

// mysqlSettingsDesired declares a column whose character set and ON UPDATE
// clause the MySQL owner holds, with the declared type given.
func mysqlSettingsDesired(columnType string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "INT", Primary: true},
			{StructName: "Doc", Name: "stamp", Type: columnType, Nullable: true,
				Facets: must.Must(mysqlschema.WithColumnSettings(schemaext.Facets{},
					mysqlschema.ColumnSettings{Charset: "latin1", OnUpdate: "CURRENT_TIMESTAMP"}))},
		},
	}
}

// mysqlSettingsCurrent is the table a MySQL read returns, its column holding
// another character set and no ON UPDATE clause.
func mysqlSettingsCurrent() *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{Name: "docs", Columns: []catalog.Column{
		{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		{Name: "stamp", DataType: "varchar(40)", ColumnType: "varchar(40)", IsNullable: "YES", OrdinalPosition: 2,
			Facets: must.Must(mysqlschema.WithObservedColumnSettings(schemaext.Facets{}, mysqlschema.ColumnSettings{Charset: "utf8mb4"}))},
	}}}}
}

// The comparison does not compare the settings: a column whose only
// difference is its character set or ON UPDATE clause is not planned.
func TestCompare_MySQLColumnSettingsAloneAreNoChange(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(t.Context(), mysqlSettingsDesired("VARCHAR(40)"), mysqlSettingsCurrent(), dialect, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}

// A column that changes for another reason is rewritten with the declared
// settings, as the MODIFY COLUMN that carries its new type.
func TestCompare_MySQLColumnSettingsRideAModifiedColumn(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())

	diff, err := schemadiff.CompareWithDialect(t.Context(), mysqlSettingsDesired("VARCHAR(80)"), mysqlSettingsCurrent(), "mysql", runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsTrue)
	nodes, err := mysql.New().GenerateMigrationAST(t.Context(), runtime, diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "MODIFY COLUMN `stamp` VARCHAR(80) CHARACTER SET latin1 ON UPDATE CURRENT_TIMESTAMP")
}

// On another target the settings bound to the MySQL family take no part.
func TestCompare_MySQLColumnSettingsStayOutOfAnotherTarget(t *testing.T) {
	c := qt.New(t)
	current := mysqlSettingsCurrent()
	current.Tables[0].Columns[1].Facets = schemaext.Facets{}

	diff, err := schemadiff.CompareWithDialect(t.Context(), mysqlSettingsDesired("VARCHAR(40)"), current, "postgres", must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}
