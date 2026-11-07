package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
)

// A forward plan drops tables its declaration no longer names, so the
// foreign keys the database holds between them are the only edges that order
// their drops: a referenced table goes after the tables that reference it.
func TestTableDropOrderWithObservedDependencies_HappyPath(t *testing.T) {
	declared := []schemamodel.Table{{StructName: "Account", Name: "accounts"}, {StructName: "Project", Name: "projects"}}
	tests := []struct {
		name         string
		tables       []string
		dependencies map[string][]string
		observed     map[string][]string
		want         []string
	}{
		{name: "a referenced table after the table that references it", tables: []string{"accounts", "orders"},
			observed: map[string][]string{"orders": {"accounts"}}, want: []string{"orders", "accounts"}},
		{name: "a chain of references", tables: []string{"a", "b", "c"},
			observed: map[string][]string{"c": {"b"}, "b": {"a"}}, want: []string{"c", "b", "a"}},
		{name: "declared and observed edges in one sort", tables: []string{"accounts", "projects", "tasks"},
			dependencies: map[string][]string{"projects": {"accounts"}}, observed: map[string][]string{"tasks": {"projects"}},
			want: []string{"tasks", "projects", "accounts"}},
		{name: "a reference to a table the plan keeps", tables: []string{"accounts", "orders"},
			observed: map[string][]string{"accounts": {"kept"}}, want: []string{"accounts", "orders"}},
		{name: "a table that references itself", tables: []string{"accounts", "nodes"},
			observed: map[string][]string{"nodes": {"nodes"}}, want: []string{"accounts", "nodes"}},
		{name: "no observed edge", tables: []string{"accounts", "orders"}, want: []string{"accounts", "orders"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			ordered := deporder.TableDropOrderWithObservedDependencies(test.tables, declared, test.dependencies, test.observed)

			c.Assert(ordered, qt.DeepEquals, test.want)
		})
	}
}
