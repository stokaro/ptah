package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
)

// TestComparisonRules_YDBRegistersItsRules pins the comparison rules the
// built-in runtime registers for YDB: a declared BIGINT is the Int64 the
// catalog reports, an auto-increment column is created NOT NULL, a reported
// default reads back as the value its literal was written from, a declared
// BTREE is the synchronous global index the catalog reports, an index's
// partitioning survives a rename, and a view's query is compared with the
// case of its names.
func TestComparisonRules_YDBRegistersItsRules(t *testing.T) {
	c := qt.New(t)

	rules, err := must.Must(builtin.New()).ComparisonRules("ydb")

	c.Assert(err, qt.IsNil)
	c.Assert(rules.Columns, qt.IsNotNil)
	c.Assert(rules.Indexes, qt.IsNotNil)
	c.Assert(rules.Views, qt.IsNotNil)
	c.Assert(rules.Columns.TypeChange(schemamodel.Field{Type: "BIGINT"}, catalog.Column{}, "Int64"), qt.Equals, "")
	c.Assert(rules.Columns.AlwaysNotNull(schemamodel.Field{Type: "BIGINT", AutoInc: true}), qt.IsTrue)
	value, isLiteral := rules.Columns.DefaultValue("5s")
	c.Assert([]any{value, isLiteral}, qt.DeepEquals, []any{"5", true})
	c.Assert(rules.Indexes.OptionsEqual(schemamodel.Index{Type: "btree"}, catalog.Index{Method: "GLOBAL SYNC"}), qt.IsTrue)
	c.Assert(rules.Indexes.RenameCarries(ydbschema.IndexPartitioningKind), qt.IsTrue)
	c.Assert(rules.Views.QueryEqual("select id from Items", "select id from items"), qt.IsFalse)
}
