package dbtest_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/migration/dbtest"
)

// silentSession is a native session whose provider runs no statement the
// operator wrote.
type silentSession struct{}

func (silentSession) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return silentSession{}, nil
}

func (silentSession) Info() catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: "example", Schema: "main", IdentifierSemantics: identifier.ForDialect("sqlite"),
		Provider: catalog.ProviderIdentity{ID: "example.provider", Version: "test", SemanticIdentity: "test-1", ProtocolRevision: 2,
			ArtifactSHA256: strings.Repeat("a", 64), ContractSHA256: strings.Repeat("b", 64)}}
}

// unreached carries the schema services the session must hand out; a case
// that only runs SQL calls none of them, so any call panics on the nil
// interface it embeds.
type unreached struct {
	catalog.SchemaReader
	catalog.SchemaWriter
}

func (silentSession) Reader([]string) catalog.SchemaReader { return unreached{} }
func (silentSession) Writer() catalog.SchemaWriter         { return unreached{} }
func (silentSession) OpenSession(context.Context) (dbschema.NativeSession, error) {
	return silentSession{}, nil
}
func (silentSession) RestrictUntrustedSQL(context.Context) error {
	return ptaherr.ErrUnsupportedFeature
}
func (silentSession) Close() error { return nil }

// A step that expects an error fails on an engine that runs no statement: the
// refusal to run the step's SQL is not the error the author expected.
func TestRunMigrationTest_StatementsUnavailable(t *testing.T) {
	c := qt.New(t)
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{"example": {Target: "example", Connector: silentSession{}}})
	c.Assert(err, qt.IsNil)
	cases := []dbtest.Case{{Name: "caught", Steps: []dbtest.Step{{Assert: &dbtest.Assertion{Query: "SELECT * FROM missing", ExpectAnyError: true}}}}}

	report, err := dbtest.RunMigrationTest(ctx, dbtest.Options{Runtime: selectedRuntime(c), Cases: cases, DBURL: "example://database"})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Failed(), qt.IsTrue)
	c.Assert(report.Cases[0].Steps, qt.HasLen, 1)
	c.Assert(report.Cases[0].Steps[0].Passed, qt.IsFalse)
	c.Assert(report.Cases[0].Steps[0].Kind, qt.Equals, dbtest.StepKind(""))
	c.Assert(report.Cases[0].Steps[0].Detail, qt.Contains, "run the step's SQL")
}
