package builtin_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemacomparison"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtindialects"
)

// registeredRules names the members of rules a target registered, in the
// order Rules declares them.
func registeredRules(rules schemacomparison.Rules) []string {
	value := reflect.ValueOf(rules)
	var names []string
	for position := range value.NumField() {
		if !value.Field(position).IsNil() {
			names = append(names, value.Type().Field(position).Name)
		}
	}
	return names
}

// TestComparisonRules_EachTargetRegistersItsOwnersRules pins which comparison
// rules the built-in runtime registers for every built-in target. The shared
// comparison reads an index definition, a partial index's predicate, a
// generated column's expression and the index a server builds for a foreign
// key through these rules alone, so a target missing one is compared as if
// its engine had none of that behavior.
func TestComparisonRules_EachTargetRegistersItsOwnersRules(t *testing.T) {
	want := map[string][]string{
		"postgres":    {"Indexes", "GeneratedExpressions", "Routines"},
		"mysql":       {"Indexes", "ForeignKeyIndexes", "GeneratedExpressions", "Routines"},
		"mariadb":     {"Indexes", "ForeignKeyIndexes", "GeneratedExpressions", "Routines"},
		"clickhouse":  {"IndexDefinitions"},
		"sqlite":      {"Indexes"},
		"sqlserver":   {"IndexDefinitions", "IndexPredicates", "GeneratedExpressions", "Routines"},
		"cockroachdb": {"Indexes", "Routines"},
		"yugabytedb":  {"Indexes", "Routines"},
		"spanner":     {"Indexes", "ForeignKeyIndexes", "Routines"},
		"oracle":      {"Routines"},
		"ydb":         {"Columns", "Indexes", "Views"},
	}
	runtime := must.Must(builtin.New())
	described := builtindialects.Descriptors()
	c := qt.New(t)
	c.Assert(described, qt.HasLen, len(want))
	for _, dialect := range described {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			rules, err := runtime.ComparisonRules(dialect.Name)
			c.Assert(err, qt.IsNil)
			c.Assert(registeredRules(rules), qt.DeepEquals, want[dialect.Name])
		})
	}
}
