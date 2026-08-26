package concurrentindex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/concurrentindex"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// desiredWithConcurrentIndex is one table and one index on it, with the
// declaration's answer under the caller's control.
func desiredWithConcurrentIndex(concurrently bool) *goschema.Database {
	return &goschema.Database{
		Tables:  []goschema.Table{{StructName: "Widget", Name: "widget"}},
		Indexes: []goschema.Index{{StructName: "Widget", Name: "idx_widget_a", Fields: []string{"a"}, Concurrently: concurrently}},
	}
}

// diffAddingIndex is the comparison that adds the index above.
func diffAddingIndex() *types.SchemaDiff {
	return &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{{Name: "idx_widget_a", TableName: "widget"}},
	}
}

func postgresInfo() dbschematypes.DBInfo {
	return dbschematypes.DBInfo{Dialect: "postgres", Capabilities: capability.ForDialect("postgres")}
}

// TestDeclaredRefs_HonorsTheDeclaration is the reproduction from
// stokaro/ptah#2019, at the layer that decides it.
func TestDeclaredRefs_HonorsTheDeclaration(t *testing.T) {
	c := qt.New(t)

	refs := concurrentindex.DeclaredRefs(diffAddingIndex(), desiredWithConcurrentIndex(true), nil, postgresInfo())

	c.Assert(refs, qt.DeepEquals, []types.IndexRef{{Name: "idx_widget_a", TableName: "widget"}})
}

// TestDeclaredRefs_LeavesAnUndeclaredIndexAlone is the control for the test
// above: without it, a function returning every addition would pass it.
func TestDeclaredRefs_LeavesAnUndeclaredIndexAlone(t *testing.T) {
	c := qt.New(t)

	refs := concurrentindex.DeclaredRefs(diffAddingIndex(), desiredWithConcurrentIndex(false), nil, postgresInfo())

	c.Assert(refs, qt.HasLen, 0)
}

// TestDeclaredRefs_GatesOnTargetAndCapability pins the two gates the
// generator's tests already proved it needs. A target that cannot run the
// statement must get the plain build rather than one its server refuses.
func TestDeclaredRefs_GatesOnTargetAndCapability(t *testing.T) {
	tests := []struct {
		name string
		info dbschematypes.DBInfo
		want int
	}{
		{
			name: "postgres with the capability",
			info: postgresInfo(),
			want: 1,
		},
		{
			name: "outside the PostgreSQL family",
			info: dbschematypes.DBInfo{Dialect: "mysql", Capabilities: capability.ForDialect("mysql")},
			want: 0,
		},
		{
			// The nil set answers false for every key. A caller that resolves
			// no capabilities must not be handed a statement on that silence.
			name: "no capabilities established",
			info: dbschematypes.DBInfo{Dialect: "postgres"},
			want: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			refs := concurrentindex.DeclaredRefs(diffAddingIndex(), desiredWithConcurrentIndex(true), nil, test.info)

			c.Assert(refs, qt.HasLen, test.want)
		})
	}
}

// TestDeclaredRefs_ExcludesAPartitionedParent records why the decision cannot
// live in the planner: whether a table is a partitioned parent is a CATALOG
// fact, and PostgreSQL supports no concurrent index form for relkind 'p'.
//
// It is excluded rather than refused, because refusing would leave a project
// with a partitioned table unable to plan an index change at all.
func TestDeclaredRefs_ExcludesAPartitionedParent(t *testing.T) {
	tests := []struct {
		name        string
		partitioned bool
		want        int
	}{
		{name: "an ordinary table takes the declaration", partitioned: false, want: 1},
		{name: "a partitioned parent does not", partitioned: true, want: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			catalog := &dbschematypes.DBSchema{
				Tables: []dbschematypes.DBTable{{Name: "widget", Partitioned: test.partitioned}},
			}

			refs := concurrentindex.DeclaredRefs(diffAddingIndex(), desiredWithConcurrentIndex(true), catalog, postgresInfo())

			c.Assert(refs, qt.HasLen, test.want)
		})
	}
}

// TestDeclaredRefs_DoesNotApplyThePopulatedTableFilter separates a declaration
// from the generator's heuristic.
//
// The heuristic builds concurrently only for a table that already holds rows,
// because it is GUESSING what the operator would have wanted. A declaration is
// not guessing, so an index declared concurrent on an empty table is still
// built concurrently.
func TestDeclaredRefs_DoesNotApplyThePopulatedTableFilter(t *testing.T) {
	c := qt.New(t)
	empty := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{{Name: "widget", EstimatedRows: 0}},
	}

	refs := concurrentindex.DeclaredRefs(diffAddingIndex(), desiredWithConcurrentIndex(true), empty, postgresInfo())

	c.Assert(refs, qt.HasLen, 1)
}

func TestMergeRefs_IsAUnionKeepingFirstOrder(t *testing.T) {
	c := qt.New(t)
	a := types.IndexRef{Name: "a", TableName: "t"}
	b := types.IndexRef{Name: "b", TableName: "t"}

	c.Assert(concurrentindex.MergeRefs([]types.IndexRef{a}, []types.IndexRef{b, a}), qt.DeepEquals,
		[]types.IndexRef{a, b})
}
