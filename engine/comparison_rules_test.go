package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
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

// equalViews calls every pair of queries one query.
type equalViews struct{}

func (equalViews) QueryEqual(_, _ string) bool { return true }

func rulesProvider(rules schemacomparison.Rules) engine.Provider {
	return engine.Provider{ID: "example.org/comparison", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}, Comparison: rules}}}
}

func TestComparisonRules_HappyPath(t *testing.T) {
	t.Run("the registered rules under an alias", func(t *testing.T) {
		c := qt.New(t)
		rules := schemacomparison.Rules{Indexes: equalIndexes{}, Views: equalViews{}}
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
	var indexes *equalIndexes
	var views *equalViews
	tests := []struct {
		name    string
		rules   schemacomparison.Rules
		message string
	}{
		{name: "index rules", rules: schemacomparison.Rules{Indexes: indexes}, message: `.*typed-nil index comparison service`},
		{name: "view rules", rules: schemacomparison.Rules{Views: views}, message: `.*typed-nil view comparison service`},
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
