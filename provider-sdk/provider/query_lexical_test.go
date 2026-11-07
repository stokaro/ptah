package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// postgresLexical is the lexical section of a PostgreSQL-family engine.
func postgresLexical() *protocol.QueryLexical {
	return &protocol.QueryLexical{EscapeStrings: true, ContinuedStrings: true, UnicodeStrings: true, NestedComments: true}
}

// lexicalDescription is a description whose engine states lexical in its
// query dialect, with the feature listed by the provider and the engine.
func lexicalDescription(lexical *protocol.QueryLexical) *protocol.Description {
	description := dataDescription()
	description.Features = append(description.Features, provider.QueryLexicalFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.QueryLexicalFeature)
	description.Engines[0].QueryDialect = &protocol.QueryDialect{Placeholder: "dollar-numbered", Lexical: lexical}
	return description
}

func TestQueryLexical_HappyPath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		lexical *protocol.QueryLexical
	}{
		{name: "the PostgreSQL family", lexical: postgresLexical()},
		{name: "MySQL's executable comments and backslashes",
			lexical: &protocol.QueryLexical{BackslashEscapes: true, DashCommentNeedsSpace: true, ExecutableComments: "bang"}},
		{name: "the SQL standard alone", lexical: &protocol.QueryLexical{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateDescription(lexicalDescription(test.lexical), nil), qt.IsNil)
		})
	}
	t.Run("a host that lists the feature receives the rules", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(lexicalDescription(postgresLexical()), provider.TwoWayFeatures())
		c.Assert(negotiated.Engines[0].GetQueryDialect().GetLexical().GetNestedComments(), qt.IsTrue)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.QueryLexicalFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
	// A host of contract 25.33 reads the engine's text by the standard's rules
	// and receives the query dialect without them.
	t.Run("a host that does not list the feature receives the dialect without the rules", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(lexicalDescription(postgresLexical()),
			withoutFeature(provider.TwoWayFeatures(), provider.QueryLexicalFeature))
		c.Assert(negotiated.Engines[0].GetQueryDialect().GetPlaceholder(), qt.Equals, "dollar-numbered")
		c.Assert(negotiated.Engines[0].GetQueryDialect().GetLexical(), qt.IsNil)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.QueryLexicalFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.QueryLexicalFeature)
	})
	// The rules travel inside the query dialect, so a host that asks for the
	// rules and not the dialect receives neither, nor the feature.
	t.Run("a host that does not list the query dialect receives no rules", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(lexicalDescription(postgresLexical()),
			withoutFeature(provider.TwoWayFeatures(), provider.QueryDialectFeature))
		c.Assert(negotiated.Engines[0].GetQueryDialect(), qt.IsNil)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.QueryLexicalFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
}

func TestQueryLexical_FailurePath(t *testing.T) {
	t.Parallel()
	t.Run("the engine's feature without the provider's", func(t *testing.T) {
		c := qt.New(t)
		description := lexicalDescription(postgresLexical())
		description.Features = withoutFeature(description.Features, provider.QueryLexicalFeature)
		c.Assert(provider.ValidateDescription(description, nil), qt.ErrorMatches, `.*engine query\.lexical requires the provider's`)
	})
	t.Run("the feature without rules", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(provider.ValidateDescription(lexicalDescription(nil), nil), qt.ErrorMatches,
			`.*engine rows states lexical rules exactly when it lists query\.lexical`)
	})
	t.Run("rules without the feature", func(t *testing.T) {
		c := qt.New(t)
		description := lexicalDescription(postgresLexical())
		description.Engines[0].Features = withoutFeature(description.Engines[0].Features, provider.QueryLexicalFeature)
		c.Assert(provider.ValidateDescription(description, nil), qt.ErrorMatches,
			`.*engine rows states lexical rules exactly when it lists query\.lexical`)
	})
	t.Run("an engine that scans its own SQL", func(t *testing.T) {
		c := qt.New(t)
		description := lexicalDescription(postgresLexical())
		description.Features = append(description.Features, provider.ScanSQLFeature)
		description.Engines[0].Features = append(description.Engines[0].Features, provider.ScanSQLFeature)
		c.Assert(provider.ValidateDescription(description, nil), qt.ErrorMatches, `.*engine rows scans its own SQL and states no lexical rules`)
	})
	t.Run("an executable comment style with a space", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(provider.ValidateDescription(lexicalDescription(&protocol.QueryLexical{ExecutableComments: "m bang"}), nil), qt.ErrorMatches,
			`.*the executable comment style of engine rows is not a word`)
	})
}
