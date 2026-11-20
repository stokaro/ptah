package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
)

// planOracleRollback plans diff in both directions against current on Oracle
// and renders the rollback.
func planOracleRollback(c *qt.C, dialect string, diff *difftypes.SchemaDiff, current *catalog.Database) string {
	c.Helper()
	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: must.Must(builtin.New()), Diff: diff,
			DesiredSchema: &schemamodel.Database{},
			CurrentSchema: current,
			Dialect:       dialect,
			Capabilities:  capability.ForDialect(dialect),
		})
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL(dialect, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// oracleKeyLossDiff is a diff that takes the key from column ID of table T,
// which Oracle's read carries on the column because the server named the key.
func oracleKeyLossDiff() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "T",
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "ID",
			Changes:    map[string]string{"primary_key": "true -> false"},
			Desired:    schemamodel.Field{StructName: "T", Name: "ID", Type: "NUMBER(10)"},
		}},
	}}}
}

// oracleKeyedCatalog is table T as Oracle reports it before the change: the key
// over A and B is a flag on the columns and no constraint row.
func oracleKeyedCatalog() *catalog.Database {
	return &catalog.Database{StoredNames: true, Tables: []catalog.Table{{Name: "T", Columns: []catalog.Column{
		{Name: "A", DataType: "NUMBER(10)", IsNullable: "NO", IsPrimaryKey: true},
		{Name: "ID", DataType: "NUMBER(10)", IsNullable: "NO", IsPrimaryKey: true},
		{Name: "C", DataType: "NUMBER(10)", IsNullable: "YES"},
	}}}}
}

// A key the forward change dropped without naming it is added back by the
// rollback over the columns the database held it on. The comparison lists no
// constraint removal for it, so the down file restored nothing but the
// columns.
func TestPlanBidirectionalSchemaDiff_OracleUnnamedKeyComesBack_HappyPath(t *testing.T) {
	c := qt.New(t)

	sql := planOracleRollback(c, platform.Oracle, oracleKeyLossDiff(), oracleKeyedCatalog())

	c.Assert(sql, qt.Contains, "ALTER TABLE T ADD CONSTRAINT T_pkey PRIMARY KEY (A, ID);", qt.Commentf("rollback:\n%s", sql))
}

// Nothing more is added back where the diff drops the key by name, which the
// ordinary reversal restores with its own name.
func TestPlanBidirectionalSchemaDiff_OracleUnnamedKeyComesBack_FailurePath(t *testing.T) {
	c := qt.New(t)
	named := oracleKeyLossDiff()
	named.ConstraintsRemoved = difftypes.ConstraintRemovals{{Name: "PK_T", TableName: "T", Type: "PRIMARY KEY"}}
	constrained := oracleKeyedCatalog()
	constrained.Constraints = []catalog.Constraint{{
		Name: "PK_T", TableName: "T", Type: "PRIMARY KEY", ColumnNames: []string{"A", "ID"}, ColumnName: "A",
	}}

	byName := planOracleRollback(c, platform.Oracle, named, constrained)

	c.Assert(byName, qt.Contains, "ADD CONSTRAINT PK_T PRIMARY KEY (A, ID)")
	c.Assert(byName, qt.Not(qt.Contains), "T_pkey")
}
