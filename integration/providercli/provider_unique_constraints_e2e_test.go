//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// uniqueDeclarations are the column declarations of records with a column's
// UNIQUE on value, a named table-level UNIQUE over value and id, and a UNIQUE
// over the key, which folds into the key.
func uniqueDeclarations() *schemamodel.Database {
	desired := columnDeclarations()
	desired.Fields[0].Unique, desired.Fields[1].Unique = true, true
	desired.Constraints = []schemamodel.Constraint{{StructName: "Record", Name: "uq_records_pair", Type: "UNIQUE", Columns: []string{"value", "id"}}}
	return desired
}

// uniqueObservation is records holding the unique indexes an engine without
// UNIQUE constraints holds for uniqueDeclarations, under the names the host
// gives them.
func uniqueObservation() *catalog.Database {
	current := columnObservation()
	current.Indexes = []catalog.Index{
		{Name: "records_value_key", TableName: "records", Columns: []string{"value"}, IsUnique: true, Method: "global"},
		{Name: "uq_records_pair", TableName: "records", Columns: []string{"value", "id"}, IsUnique: true, Method: "global"},
	}
	return current
}

// A session that states its engine has no UNIQUE constraint has the host read
// each declared UNIQUE as the unique index that engine holds for it, so a
// table holding those indexes is the declared one, and the provider prepares
// a declaration that carries no constraint.
func TestProviderReadsUniqueAsAnIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "column-semantics")
	info := catalog.ServerInfo{Dialect: "fixture", Capabilities: capability.Of(map[capability.Capability]bool{capability.UniqueConstraints: false})}
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), uniqueDeclarations(), uniqueObservation(), info, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

// A session that does not answer leaves a declared UNIQUE a constraint: no
// answer is not an engine without UNIQUE constraints. The constraint is then
// missing from the table, and the indexes are not declared.
func TestProviderReadsUniqueAsAnIndexWithoutTheAnswer_HappyPath(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "column-semantics")
	info := catalog.ServerInfo{Dialect: "fixture", Capabilities: capability.Of(map[capability.Capability]bool{capability.SerialColumns: true})}
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), uniqueDeclarations(), uniqueObservation(), info, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsTrue)
	c.Assert(diff.IndexesRemoved, qt.HasLen, 2)
}
