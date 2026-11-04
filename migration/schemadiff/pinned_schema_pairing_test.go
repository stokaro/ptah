package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// TestCompareReportingUndecidedAdditionsPairsNamesInThePinnedSchema compares
// a database read through a URL that pins database f3a, which reports its
// table as `t1`, as the desired side, with a document that names the same
// table `f3a.t1` as the current side, the shape `schema diff --from <file>
// --to <database>` compares. Under the rules of the server that pinned f3a
// the two are one table. Under the dialect's offline rules, which pin no
// schema, they are two, and a plan of that comparison creates `t1` and drops
// `f3a`.`t1` -- the table both sides hold.
func TestCompareReportingUndecidedAdditionsPairsNamesInThePinnedSchema(t *testing.T) {
	pinned := identifier.ForDialect("mysql")
	pinned.DefaultSchema = "f3a"
	tests := []struct {
		name      string
		semantics *identifier.Semantics
		added     []string
		removed   []string
	}{
		{name: "the rules of the server that pinned the database", semantics: &pinned},
		{name: "the dialect's offline rules", added: []string{"t1"}, removed: []string{"f3a.t1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T1", Name: "t1"}},
				Fields: []schemamodel.Field{{StructName: "T1", Name: "id", Type: "int", Primary: true}},
			}
			current := &catalog.Database{Tables: []catalog.Table{{Schema: "f3a", Name: "t1", Columns: []catalog.Column{
				{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true},
			}}}}
			opts := config.DefaultCompareOptions()
			opts.Dialect = "mysql"
			opts.IdentifierSemantics = test.semantics

			diff, _, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), desired, current, opts, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, test.added)
			c.Assert(diff.TablesRemoved.Names(), qt.DeepEquals, test.removed)
		})
	}
}
