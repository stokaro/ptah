package agentapi_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/agentapi"
	"ptah.run/internal/agentpolicy"
)

// schemaCall is one of the operations that takes a schema source.
type schemaCall struct {
	name string
	call func(*agentapi.Session, agentapi.SchemaSource) error
}

// schemaCalls is every operation a source reaches, so a bypass found in one is
// checked in all of them.
func schemaCalls() []schemaCall {
	return []schemaCall{
		{name: "validate_schema", call: func(s *agentapi.Session, src agentapi.SchemaSource) error {
			_, err := s.ValidateSchema(context.Background(),
				agentapi.ValidateSchemaRequest{Source: src, Dialect: "postgres"})
			return err
		}},
		{name: "render_schema", call: func(s *agentapi.Session, src agentapi.SchemaSource) error {
			_, err := s.RenderSchema(context.Background(),
				agentapi.RenderSchemaRequest{Source: src, Dialect: "postgres"})
			return err
		}},
		{name: "schema_lineage", call: func(s *agentapi.Session, src agentapi.SchemaSource) error {
			_, err := s.SchemaLineage(context.Background(),
				agentapi.SchemaLineageRequest{Source: src, Dialect: "postgres"})
			return err
		}},
	}
}

func TestSchemaSource_ANetworkReferenceCannotBypassNetworkArbitrary(t *testing.T) {
	// network.arbitrary is hard denied: no layer may grant it. A schema loader
	// that fetched an oci:// reference would be performing it under the name of
	// a schema operation, which is how a hard deny becomes a formality.
	tests := []struct {
		name      string
		reference string
	}{
		{name: "an oci artifact", reference: "oci://registry.invalid/schema:v1"},
		{name: "http", reference: "http://schema.invalid/schema.hcl"},
		{name: "https", reference: "https://schema.invalid/schema.hcl"},
		{name: "a scheme nobody has heard of", reference: "gopher://schema.invalid/x"},
	}

	for _, operation := range schemaCalls() {
		for _, test := range tests {
			t.Run(operation.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				session := openSession(c, c.TempDir())

				err := operation.call(session,
					agentapi.SchemaSource{SchemaFiles: []string{test.reference}})

				c.Assert(err, qt.ErrorIs, agentapi.ErrSourceNotLocal)
			})
		}
	}
}

func TestSchemaSource_APathOutsideTheScopeIsRefused(t *testing.T) {
	// filesystem.arbitrary_read is hard denied. A source path is chosen by the
	// model, so without a scope these operations are an arbitrary local read
	// wearing the name of a schema operation.
	for _, operation := range schemaCalls() {
		t.Run(operation.name, func(t *testing.T) {
			c := qt.New(t)
			permitted := c.TempDir()
			elsewhere := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(elsewhere, "models.go"),
				[]byte(bookshop), 0o600), qt.IsNil)
			session := openSession(c, permitted)

			err := operation.call(session,
				agentapi.SchemaSource{RootDirs: []string{elsewhere}})

			c.Assert(err, qt.ErrorIs, agentapi.ErrSourceOutsideScope)
		})
	}
}

func TestSchemaSource_ASymlinkOutOfTheScopeIsRefused(t *testing.T) {
	// The escape a lexical check would miss: a link planted inside a configured
	// directory, which a repository the model is reading can contain.
	c := qt.New(t)
	permitted := c.TempDir()
	elsewhere := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(elsewhere, "models.go"),
		[]byte(bookshop), 0o600), qt.IsNil)
	c.Assert(os.Symlink(elsewhere, filepath.Join(permitted, "link")), qt.IsNil)
	session := openSession(c, permitted)

	_, err := session.ValidateSchema(context.Background(), agentapi.ValidateSchemaRequest{
		Source:  agentapi.SchemaSource{RootDirs: []string{filepath.Join(permitted, "link")}},
		Dialect: "postgres",
	})

	c.Assert(err, qt.ErrorIs, agentapi.ErrSourceOutsideScope)
}

func TestSchemaSource_NoConfiguredScopePermitsNothing(t *testing.T) {
	// A process told no directory has not been told what an agent may read, and
	// the answer to an unasked question is not "everything this process can
	// open".
	for _, operation := range schemaCalls() {
		t.Run(operation.name, func(t *testing.T) {
			c := qt.New(t)
			session := openSession(c)

			err := operation.call(session,
				agentapi.SchemaSource{RootDirs: []string{c.TempDir()}})

			c.Assert(err, qt.ErrorIs, agentapi.ErrNoSourceScope)
		})
	}
}

func TestSchemaSource_APermittedPathStillWorks(t *testing.T) {
	// The control. A scope that refused everything would pass every test above
	// and make the operations useless.
	c := qt.New(t)
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "models.go"), []byte(bookshop), 0o600), qt.IsNil)
	session := openSession(c, root)

	response, err := session.ValidateSchema(context.Background(), agentapi.ValidateSchemaRequest{
		Source:  agentapi.SchemaSource{RootDirs: []string{root}},
		Dialect: "postgres",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(response.Valid, qt.IsTrue)
}

func TestSchemaSource_ThePolicyRefusalWinsOverTheSourceRefusal(t *testing.T) {
	// Authorization runs first, and this is how that is observable: a denied
	// call whose source would also be refused reports the policy, because the
	// policy decided before anything looked at the path.
	c := qt.New(t)
	session := sessionOptions{
		rules: denyEverySchemaCapability(),
		roots: []string{c.TempDir()},
	}.build(c)

	err := schemaCalls()[0].call(session,
		agentapi.SchemaSource{SchemaFiles: []string{"oci://registry.invalid/schema:v1"}})

	c.Assert(err, qt.Not(qt.ErrorIs), agentapi.ErrSourceNotLocal)
	c.Assert(err, qt.ErrorAs, new(*agentpolicy.DeniedError))
}

// denyEverySchemaCapability refuses all three schema operations.
func denyEverySchemaCapability() []agentpolicy.Rule {
	capabilities := []agentpolicy.Capability{
		agentpolicy.SchemaValidate,
		agentpolicy.SchemaRender,
		agentpolicy.SchemaLineage,
	}
	rules := make([]agentpolicy.Rule, 0, len(capabilities))
	for _, capability := range capabilities {
		rules = append(rules, agentpolicy.Rule{
			Capability: capability, Verdict: agentpolicy.VerdictDeny,
		})
	}
	return rules
}
