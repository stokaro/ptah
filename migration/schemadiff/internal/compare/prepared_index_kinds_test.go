package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/internal/tableidentity"
	"ptah.run/migration/schemadiff/internal/compare"
)

// exactKinds compares a declared index's type with the held index's method
// as strings, as the rules of a target that prepares index kinds do.
type exactKinds struct{}

func (exactKinds) OptionsEqual(desired schemamodel.Index, current catalog.Index) bool {
	return desired.Type == current.Method
}

func (exactKinds) RenameCarries(schemaext.Kind) bool { return false }

// statedKindTables is table t prepared with the kind its preparation states
// for index name.
func statedKindTables(name, kind string) map[objectidentity.Key]schemapreparation.Table {
	subject := tableidentity.Subject("", "t", platform.YDB, identifier.ForDialect(platform.YDB))
	return map[objectidentity.Key]schemapreparation.Table{subject.Key(): {Subject: subject, IndexMethods: map[string]string{name: kind}}}
}

// kindIndexes is table t declaring one index, named declared and written
// with type, and holding one, named held and reported with method.
func kindIndexes(declared, typ, held, method string) (*schemamodel.Database, *catalog.Database) {
	return &schemamodel.Database{
		Tables:  []schemamodel.Table{{Name: "t"}},
		Indexes: []schemamodel.Index{{Name: declared, TableName: "t", Type: typ, Fields: []string{"a"}}},
	}, &catalog.Database{
		Indexes: []catalog.Index{{Name: held, TableName: "t", Method: method, Columns: []string{"a"}}},
	}
}

// The rules compare the kind a table's preparation states for an index in
// place of the type the source wrote, and the diff keeps the written type.
func TestIndexes_PreparedKindsReachTheRules(t *testing.T) {
	ref := []difftypes.IndexRef{{Name: "i", TableName: "t"}}
	for _, row := range []struct {
		name     string
		declared string
		prepared map[objectidentity.Key]schemapreparation.Table
		replaced []difftypes.IndexRef
		types    []string
	}{
		{"the held kind stated for another spelling", "GLOBAL ASYNC", statedKindTables("i", "async"), nil, nil},
		{"another kind stated", "GLOBAL ASYNC", statedKindTables("i", ""), ref, []string{"GLOBAL ASYNC"}},
		{"another index's kind stated", "GLOBAL ASYNC", statedKindTables("j", "async"), ref, []string{"GLOBAL ASYNC"}},
		{"no kind stated", "GLOBAL ASYNC", nil, ref, []string{"GLOBAL ASYNC"}},
		// The type the source wrote is compared where no kind is stated.
		{"another index's kind stated for the held spelling", "async", statedKindTables("j", ""), nil, nil},
		{"no kind stated for the held spelling", "async", nil, nil, nil},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := kindIndexes("i", row.declared, "i", "async")
			diff := &difftypes.SchemaDiff{}
			compare.IndexesWithSemantics(desired, current, diff, platform.YDB, identifier.ForDialect(platform.YDB), nil,
				capability.YDB262(), schemacomparison.Rules{Indexes: exactKinds{}}, row.prepared)
			c.Assert(diff.IndexesRemoved, qt.DeepEquals, row.replaced)
			c.Assert(diff.IndexesAdded.Refs(), qt.DeepEquals, row.replaced)
			c.Assert(addedTypes(diff), qt.DeepEquals, row.types)
			c.Assert(desired.Indexes[0].Type, qt.Equals, row.declared)
		})
	}
}

// A dropped and an added index pair as a rename by the kind their table's
// preparation states, and IndexRenames, which the feature comparison reads
// first, pairs them the same way.
func TestIndexes_PreparedKindsPairARename(t *testing.T) {
	renamed := []difftypes.IndexRename{{TableName: "t", From: "old_i", To: "i"}}
	for _, row := range []struct {
		name     string
		prepared map[objectidentity.Key]schemapreparation.Table
		renames  []difftypes.IndexRename
	}{
		{"the held kind stated", statedKindTables("i", "async"), renamed},
		{"no kind stated", nil, nil},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := kindIndexes("i", "GLOBAL ASYNC", "old_i", "async")
			rules := schemacomparison.Rules{Indexes: exactKinds{}}
			diff := &difftypes.SchemaDiff{}
			compare.IndexesWithSemantics(desired, current, diff, platform.YDB, identifier.ForDialect(platform.YDB), nil, capability.YDB262(), rules, row.prepared)
			c.Assert(diff.IndexesRenamed, qt.DeepEquals, row.renames)
			c.Assert(compare.IndexRenames(desired, current, platform.YDB, identifier.ForDialect(platform.YDB), nil, capability.YDB262(), rules, row.prepared),
				qt.DeepEquals, row.renames)
		})
	}
}

func addedTypes(diff *difftypes.SchemaDiff) []string {
	var types []string
	for _, added := range diff.IndexesAdded {
		types = append(types, added.Index.Type)
	}
	return types
}
