package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// sqlStatementFactsDescription declares an engine that reads SQL with the shared
// grammar and states that its server skips DROP VIEW IF EXISTS of a name
// another kind of object holds.
func sqlStatementFactsDescription() *protocol.Description {
	d, _, _ := sqlSchemaExchange()
	d.Features = []string{provider.NormalizeValuesFeature, provider.SQLStatementFactsFeature}
	d.Engines[0].Features = []string{"retention", provider.SQLStatementFactsFeature}
	d.Engines[0].SqlSchema = &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_FRAGMENTS, DropViewIfExistsSkipsOtherKinds: true}
	return d
}

func TestSQLStatementFacts_HappyPath(t *testing.T) {
	t.Run("a FRAGMENTS engine that states a fact", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(provider.ValidateDescription(sqlStatementFactsDescription(), nil), qt.IsNil)
	})
	t.Run("a FRAGMENTS engine that lists the feature and states every fact false", func(t *testing.T) {
		c := qt.New(t)
		d := sqlStatementFactsDescription()
		d.Engines[0].SqlSchema.DropViewIfExistsSkipsOtherKinds = false

		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	})
	t.Run("a host that lists the feature keeps the facts", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(sqlStatementFactsDescription(), provider.TwoWayFeatures())

		c.Assert(negotiated.GetEngines()[0].GetSqlSchema().GetDropViewIfExistsSkipsOtherKinds(), qt.IsTrue)
		c.Assert(negotiated.GetEngines()[0].GetFeatures(), qt.Contains, provider.SQLStatementFactsFeature)
	})
	t.Run("a host that does not list the feature reads every fact as false", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(sqlStatementFactsDescription(), slices.DeleteFunc(provider.TwoWayFeatures(), func(feature string) bool {
			return feature == provider.SQLStatementFactsFeature
		}))

		c.Assert(negotiated.GetEngines()[0].GetSqlSchema().GetDropViewIfExistsSkipsOtherKinds(), qt.IsFalse)
		c.Assert(negotiated.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.SQLStatementFactsFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
}

func TestSQLStatementFacts_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
		want   string
	}{
		{"a fact without the feature", func(d *protocol.Description) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(feature string) bool { return feature == provider.SQLStatementFactsFeature })
		}, `.*an engine states statement facts only when it lists source.sql-statement-facts`},
		{"an engine feature without the provider's", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(feature string) bool { return feature == provider.SQLStatementFactsFeature })
		}, `.*statement facts require the provider's source.sql-statement-facts`},
		{"a DOCUMENT engine", func(d *protocol.Description) {
			d.Features = append(d.Features, provider.ReadSQLSchemaFeature)
			d.Engines[0].Features = append(d.Engines[0].Features, provider.ReadSQLSchemaFeature)
			d.Engines[0].SqlSchema.Mode = protocol.SQLSchemaDeclaration_DOCUMENT
		}, `.*statement facts describe the documents of a FRAGMENTS engine`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d := sqlStatementFactsDescription()
			test.mutate(d)

			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, test.want)
		})
	}
}
