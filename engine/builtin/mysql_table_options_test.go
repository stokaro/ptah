package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

// mysqlOptionsSource declares table orders with the common engine and the
// MySQL family's other options as platform properties.
const mysqlOptionsSource = `package entities

//ptah:schema:table name="orders" engine="InnoDB" platform.mysql.auto_increment="1000" platform.mysql.charset="utf8mb4" platform.mariadb.charset="latin1"
type Order struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}
`

// TestMySQLTableOptions_FollowTheTarget pins how the options are written: the
// MySQL owner takes over the common engine and reads its own properties on the
// mysql and mariadb targets, and another target leaves the properties out and
// keeps the common engine, which it reports as skipped.
func TestMySQLTableOptions_FollowTheTarget(t *testing.T) {
	database := must.Must(goschema.ParseSource(builtintest.Annotations(), "orders.go", mysqlOptionsSource))
	tests := []struct {
		dialect string
		caps    capability.Capabilities
		want    string
	}{
		{dialect: platform.MySQL, caps: capability.MySQL84(), want: ") ENGINE=InnoDB AUTO_INCREMENT=1000 CHARSET=utf8mb4;"},
		{dialect: platform.MariaDB, caps: capability.ForDialect(platform.MariaDB), want: ") ENGINE=InnoDB CHARSET=latin1;"},
		{dialect: platform.Postgres, caps: capability.Postgres17(), want: ");"},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(&database, test.dialect, test.caps)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// TestMySQLTableOptions_AreNotChangedOnATableThatExists pins the comparison:
// the options apply when a table is created, so a declaration whose character
// set differs from the one the table holds plans nothing, as it never has.
func TestMySQLTableOptions_AreNotChangedOnATableThatExists(t *testing.T) {
	c := qt.New(t)
	desired := must.Must(goschema.ParseSource(builtintest.Annotations(), "orders.go", mysqlOptionsSource))
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "orders", Columns: []catalog.Column{{Name: "id", DataType: "bigint", ColumnType: "bigint", IsNullable: "NO", IsPrimaryKey: true}},
			Facets: must.Must(must.Must(schemaext.NewFacets(&mysqlschema.ObservedTable{Charset: "latin1"})).
				WithTargetScope(mysqlschema.TableKind, platform.MySQL, platform.MariaDB))}},
		Constraints:     []catalog.Constraint{{Name: "PRIMARY", TableName: "orders", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}}},
		FeatureCoverage: must.Must(mysqlschema.TableCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), &desired, current, catalog.ServerInfo{Dialect: platform.MySQL}, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}
