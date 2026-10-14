//go:build integration

package clickhouse_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// Each case changes one storage clause of a live table through the native
// generated plan, reads the table again and finds nothing left to plan, then
// runs the generated reverse and finds the original storage, columns and row.
// Both plans are asserted before they run: a wrong plan here is a statement
// against a shared server. {t} stands for the table's name.
func TestStoragePlanAppliesAndReversesInPlaceLive(t *testing.T) {
	for _, test := range []struct {
		name    string
		columns string
		storage string
		change  func(*chschema.DesiredTable)
		fields  []schemamodel.Field
		forward []string
		reverse []string
	}{
		{
			name: "sorting key gains a column", columns: "a Int32, u UInt64", storage: "ORDER BY (a, u) PRIMARY KEY a",
			change: func(v *chschema.DesiredTable) { v.OrderBy.Value = "a, u, k" },
			fields: []schemamodel.Field{
				{Name: "a", Type: "Int32"}, {Name: "u", Type: "UInt64"}, {Name: "k", Type: "Int32"},
				{Name: "note", Type: "String", Nullable: true, Default: "n"},
			},
			forward: []string{"ALTER TABLE {t} ADD COLUMN k Int32, ADD COLUMN note Nullable(String) DEFAULT 'n', MODIFY ORDER BY (a, u, k)"},
			reverse: []string{"ALTER TABLE {t} MODIFY ORDER BY (a, u)", "ALTER TABLE {t} DROP COLUMN k", "ALTER TABLE {t} DROP COLUMN note"},
		},
		{
			name: "sorting key loses its last column", columns: "a Int32, u UInt64", storage: "ORDER BY (a, u) PRIMARY KEY a",
			change:  func(v *chschema.DesiredTable) { v.OrderBy.Value = "a" },
			fields:  []schemamodel.Field{{Name: "a", Type: "Int32"}},
			forward: []string{"ALTER TABLE {t} MODIFY ORDER BY a", "ALTER TABLE {t} DROP COLUMN u"},
			reverse: []string{"ALTER TABLE {t} ADD COLUMN u UInt64, MODIFY ORDER BY (a, u)"},
		},
		{
			name: "sampling key", columns: "a Int32, u UInt64", storage: "ORDER BY (a, u)",
			change:  func(v *chschema.DesiredTable) { v.SampleBy.Value = "u" },
			fields:  []schemamodel.Field{{Name: "a", Type: "Int32"}, {Name: "u", Type: "UInt64"}},
			forward: []string{"ALTER TABLE {t} MODIFY SAMPLE BY u"},
			reverse: []string{"ALTER TABLE {t} REMOVE SAMPLE BY"},
		},
		{
			name: "settings", columns: "a Int32", storage: "ORDER BY a SETTINGS merge_with_ttl_timeout = 100, min_bytes_for_wide_part = 0",
			change: func(v *chschema.DesiredTable) {
				v.Settings.Value = "merge_with_ttl_timeout = 3600, ttl_only_drop_parts = 1"
			},
			fields:  []schemamodel.Field{{Name: "a", Type: "Int32"}},
			forward: []string{"ALTER TABLE {t} MODIFY SETTING merge_with_ttl_timeout = 3600, ttl_only_drop_parts = 1", "ALTER TABLE {t} RESET SETTING min_bytes_for_wide_part"},
			reverse: []string{"ALTER TABLE {t} MODIFY SETTING merge_with_ttl_timeout = 100, min_bytes_for_wide_part = 0", "ALTER TABLE {t} RESET SETTING ttl_only_drop_parts"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openLiveClickHouseRBACTarget(c)
			name := uniqueClickHouseRBACName("storage_plan")
			c.Cleanup(func() { cleanupClickHouseRBACFixture(c, conn, "DROP TABLE IF EXISTS "+name+" SYNC") })
			applyStatements(c, conn, []string{
				"CREATE TABLE " + name + " (" + test.columns + ") ENGINE = MergeTree " + test.storage,
				"INSERT INTO " + name + " (a) VALUES (1)",
			})
			current := readTTLTable(c, conn, name)
			before, found, err := schemaext.FacetAs[*chschema.ObservedTable](current.Tables[0].Facets, chschema.TableKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			after := before.Desired()
			test.change(after)
			desired := storageDeclaration(current.Tables[0], after, test.fields)
			runtime := must.Must(builtin.New())
			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, conn.Info(), nil, runtime)
			c.Assert(err, qt.IsNil)
			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "clickhouse", Capabilities: conn.Info().Capabilities,
			})
			c.Assert(err, qt.IsNil)
			forward := renderedStatements(c, plan.Forward.Nodes)
			c.Assert(forward, qt.DeepEquals, named(test.forward, name))
			reverse := renderedStatements(c, plan.Reverse.Nodes)
			c.Assert(reverse, qt.DeepEquals, named(test.reverse, name))

			applyStatements(c, conn, forward)
			noChange, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, readTTLTable(c, conn, name), conn.Info(), nil, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(noChange.HasChanges(), qt.IsFalse, qt.Commentf("%+v", noChange))

			applyStatements(c, conn, reverse)
			restored := readTTLTable(c, conn, name)
			original := storageDeclaration(current.Tables[0], before.Desired(), nil)
			original.Fields = nil
			for _, column := range current.Tables[0].Columns {
				original.Fields = append(original.Fields, schemamodel.Field{Name: column.Name, Type: column.DataType, StructName: "Event"})
			}
			unchanged, err := schemadiff.CompareWithDatabaseInfo(c.Context(), original, restored, conn.Info(), nil, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(unchanged.HasChanges(), qt.IsFalse, qt.Commentf("%+v", unchanged))
			c.Assert(restored.Tables[0].Columns, qt.DeepEquals, current.Tables[0].Columns)
			c.Assert(liveRowCount(c, conn, name), qt.Equals, uint64(1))
		})
	}
}

// A sorting key that would gain a column the table already has is refused by
// name before anything runs: ClickHouse sorts parts already on disk by the old
// key, so it adds to the key only columns the same ALTER TABLE adds.
func TestStoragePlanRefusesAnExistingColumnInTheSortingKeyLive(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	name := uniqueClickHouseRBACName("storage_refusal")
	c.Cleanup(func() { cleanupClickHouseRBACFixture(c, conn, "DROP TABLE IF EXISTS "+name+" SYNC") })
	applyStatements(c, conn, []string{"CREATE TABLE " + name + " (a Int32, b Int32) ENGINE = MergeTree ORDER BY a"})
	current := readTTLTable(c, conn, name)
	before, _, err := schemaext.FacetAs[*chschema.ObservedTable](current.Tables[0].Facets, chschema.TableKind)
	c.Assert(err, qt.IsNil)
	after := before.Desired()
	after.OrderBy.Value = "a, b"
	desired := storageDeclaration(current.Tables[0], after, []schemamodel.Field{{Name: "a", Type: "Int32"}, {Name: "b", Type: "Int32"}})
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, conn.Info(), nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), runtime, diff, "clickhouse")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*column "b" is already in the table.*`)
	c.Assert(statements, qt.IsNil)
}

// storageDeclaration declares table with the given storage and fields.
func storageDeclaration(table catalog.Table, storage *chschema.DesiredTable, fields []schemamodel.Field) *schemamodel.Database {
	declared := &schemamodel.Database{Tables: []schemamodel.Table{{Name: table.Name, Schema: table.Schema, StructName: "Event", Facets: must.Must(schemaext.NewFacets(storage))}}}
	for _, field := range fields {
		field.StructName = "Event"
		declared.Fields = append(declared.Fields, field)
	}
	return declared
}

// renderedStatements renders nodes for ClickHouse and returns the statements
// without the comments a plan carries before them.
func renderedStatements(c *qt.C, nodes []ast.Node) []string {
	c.Helper()
	sql, err := builtin.RenderSQL("clickhouse", nodes...)
	c.Assert(err, qt.IsNil)
	return sqlutil.SplitStatementsForDialect("clickhouse", sql)
}

func named(statements []string, table string) []string {
	result := make([]string, len(statements))
	for i, statement := range statements {
		result[i] = strings.ReplaceAll(statement, "{t}", table)
	}
	return result
}

func liveRowCount(c *qt.C, conn *dbschema.DatabaseConnection, table string) uint64 {
	c.Helper()
	var count uint64
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count() FROM "+table).Scan(&count), qt.IsNil)
	return count
}
