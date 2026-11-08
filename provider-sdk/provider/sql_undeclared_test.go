package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// undeclaredExchange is sqlSchemaExchange for an engine that reports
// undeclared statements, under a target that lists the feature when listed is
// true, with an incomplete reply that carries diagnostic alone.
func undeclaredExchange(listed bool, diagnostic *protocol.SQLDiagnostic) (*protocol.Description, *protocol.ReadSQLSchemaRequest, *protocol.SQLSchema) {
	d, request, _ := sqlSchemaExchange()
	d.Features = append(d.Features, provider.SQLUndeclaredFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.SQLUndeclaredFeature)
	request.TargetFeatures = map[bool][]string{true: {provider.SQLUndeclaredFeature}, false: nil}[listed]
	return d, request, &protocol.SQLSchema{Diagnostics: []*protocol.SQLDiagnostic{diagnostic}}
}

// undeclaredError is the UNDECLARED error a reading reports for the first
// statement of the second document.
func undeclaredError() *protocol.SQLDiagnostic {
	return &protocol.SQLDiagnostic{Code: protocol.SQLDiagnostic_UNDECLARED, Severity: protocol.SQLDiagnostic_ERROR, DocumentId: "two",
		Span: sqlSpan(0, 5), Message: "ALTER TABLE users names a table this load does not declare"}
}

// A reading reports a statement that changes what no document of its load
// declares as an UNDECLARED error, where both sides list the feature.
func TestSQLUndeclared_HappyPath(t *testing.T) {
	c := qt.New(t)
	description, request, result := undeclaredExchange(true, undeclaredError())

	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	c.Assert(provider.ValidateSQLSchema(description, request, result), qt.IsNil)
}

// UNDECLARED is refused without the feature on both sides and as a warning;
// an engine that lists the feature needs the provider's and its own SQL
// reading.
func TestSQLUndeclared_FailurePath(t *testing.T) {
	warning := undeclaredError()
	warning.Severity = protocol.SQLDiagnostic_WARNING
	for _, test := range []struct {
		name       string
		listed     bool
		diagnostic *protocol.SQLDiagnostic
		want       string
	}{
		{name: "a target that does not list the feature", diagnostic: undeclaredError(),
			want: `.*an UNDECLARED diagnostic, and schema.read-sql-undeclared was not negotiated`},
		{name: "a warning", listed: true, diagnostic: warning, want: `.*an UNDECLARED diagnostic is an error`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, result := undeclaredExchange(test.listed, test.diagnostic)

			c.Assert(provider.ValidateSQLSchema(description, request, result), qt.ErrorMatches, test.want)
		})
	}
	t.Run("an engine without the provider's feature", func(t *testing.T) {
		c := qt.New(t)
		description, _, _ := sqlSchemaExchange()
		description.Engines[0].Features = append(description.Engines[0].Features, provider.SQLUndeclaredFeature)

		c.Assert(provider.ValidateDescription(description, nil), qt.ErrorMatches, `.*undeclared statements require the provider's schema.read-sql-undeclared`)
	})
	t.Run("an engine that reads no SQL document", func(t *testing.T) {
		c := qt.New(t)
		description, _ := valueInputs()
		description.Features = append(description.Features, provider.SQLUndeclaredFeature)
		description.Engines[0].Features = append(description.Engines[0].Features, provider.SQLUndeclaredFeature)

		c.Assert(provider.ValidateDescription(description, nil), qt.ErrorMatches, `.*undeclared statements require the engine's SQL reading`)
	})
}
