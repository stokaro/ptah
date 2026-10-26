package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
)

// equalIndexes calls every index the same, and carries every facet through a
// rename.
type equalIndexes struct{}

func (equalIndexes) OptionsEqual(schemamodel.Index, catalog.Index) bool { return true }

func (equalIndexes) RenameCarries(schemaext.Kind) bool { return true }

// keyedDefinitions calls an index changed when its uniqueness is.
type keyedDefinitions struct{}

func (keyedDefinitions) DefinitionChanged(desired schemamodel.Index, current catalog.Index, _ identifier.Semantics) bool {
	return desired.Unique != current.IsUnique
}

// plainPredicates reads every predicate as written.
type plainPredicates struct{}

func (plainPredicates) Spelling(predicate string) string { return predicate }

// noForeignKeyIndexes finds no index any foreign key owns.
type noForeignKeyIndexes struct{}

func (noForeignKeyIndexes) BackingIndexes(*catalog.Database, catalog.Constraint, identifier.Semantics) []catalog.Index {
	return nil
}

// plainExpressions reads every generated expression as written.
type plainExpressions struct{}

func (plainExpressions) Spelling(expression string) string { return expression }

// equalViews calls every pair of queries one query.
type equalViews struct{}

func (equalViews) QueryEqual(_, _ string) bool { return true }

// sqlRoutines gives a routine that states no language SQL.
type sqlRoutines struct{}

func (sqlRoutines) Defaults() schemacomparison.RoutineDefaults {
	return schemacomparison.RoutineDefaults{Language: "sql"}
}

func rulesProvider(rules schemacomparison.Rules) engine.Provider {
	return engine.Provider{ID: "example.org/comparison", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}, Comparison: rules}}}
}

func TestComparisonRules_HappyPath(t *testing.T) {
	t.Run("the registered rules under an alias", func(t *testing.T) {
		c := qt.New(t)
		rules := schemacomparison.Rules{
			Indexes: equalIndexes{}, IndexDefinitions: keyedDefinitions{}, IndexPredicates: plainPredicates{},
			ForeignKeyIndexes: noForeignKeyIndexes{}, GeneratedExpressions: plainExpressions{},
			Views: equalViews{}, Routines: sqlRoutines{},
		}
		runtime := mustRuntime(c, rulesProvider(rules))

		got, err := runtime.ComparisonRules("alternate")

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, rules)
	})

	t.Run("a target with no rules", func(t *testing.T) {
		c := qt.New(t)
		runtime := mustRuntime(c, rulesProvider(schemacomparison.Rules{}))

		got, err := runtime.ComparisonRules("custom")

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, schemacomparison.Rules{})
	})
}

func TestComparisonRules_FailurePath(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, rulesProvider(schemacomparison.Rules{Views: equalViews{}}))

	got, err := runtime.ComparisonRules("unregistered")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(got, qt.Equals, schemacomparison.Rules{})
}

func TestNew_RefusesTypedNilComparisonRules(t *testing.T) {
	var columns *sequenceColumns
	var batch *batchColumns
	var indexes *equalIndexes
	var views *equalViews
	var routines *sqlRoutines
	var definitions *keyedDefinitions
	var predicates *plainPredicates
	var foreignKeys *noForeignKeyIndexes
	var expressions *plainExpressions
	tests := []struct {
		name    string
		rules   schemacomparison.Rules
		message string
	}{
		{name: "column batch", rules: schemacomparison.Rules{ColumnService: batch}, message: `.*typed-nil column batch service`},
		{name: "column rules", rules: schemacomparison.Rules{Columns: columns}, message: `.*typed-nil column comparison service`},
		{name: "index rules", rules: schemacomparison.Rules{Indexes: indexes}, message: `.*typed-nil index comparison service`},
		{name: "view rules", rules: schemacomparison.Rules{Views: views}, message: `.*typed-nil view comparison service`},
		{name: "routine rules", rules: schemacomparison.Rules{Routines: routines}, message: `.*typed-nil routine comparison service`},
		{name: "index definition rules", rules: schemacomparison.Rules{IndexDefinitions: definitions}, message: `.*typed-nil index definition comparison service`},
		{name: "index predicate rules", rules: schemacomparison.Rules{IndexPredicates: predicates}, message: `.*typed-nil index predicate comparison service`},
		{name: "foreign key index rules", rules: schemacomparison.Rules{ForeignKeyIndexes: foreignKeys}, message: `.*typed-nil foreign key index comparison service`},
		{name: "generated expression rules", rules: schemacomparison.Rules{GeneratedExpressions: expressions}, message: `.*typed-nil generated expression comparison service`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			runtime, err := engine.New(rulesProvider(test.rules))

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

// Embedding supplies the batch methods; registration must reject conflicting
// owners before it invokes either service.
type batchColumns struct{ schemacomparison.ColumnService }

func TestNewRefusesCompetingColumnOwners(t *testing.T) {
	c := qt.New(t)
	runtime, err := engine.New(rulesProvider(schemacomparison.Rules{Columns: sequenceColumns{}, ColumnService: batchColumns{}}))
	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(runtime, qt.IsNil)
}
