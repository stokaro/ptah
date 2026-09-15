//go:build integration

package atlasschema_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
)

// Planning reads the database at the scope the connection URL and the desired
// state name together, and `schema apply --plan` reads it again to check the
// plan's source fingerprint. The two reads have to agree in both directions. A
// verification read wider than the planning read makes a plan stale the moment
// it is saved; one narrower accepts a plan after a schema it writes has moved
// (stokaro/ptah#3285). Live because the property is about two reads of one
// server agreeing, which no fake can be wrong about in the same way.

// planScopeTwoSchemaDocument declares a schema the connection is not on, which
// is what makes the planning read wider than the verification read.
const planScopeTwoSchemaDocument = `schema "extra" {
}

schema "public" {
  comment = "standard public schema"
}

table "a" {
  schema = schema.public
  column "id" {
    null = true
    type = integer
  }
}

table "b" {
  schema = schema.extra
  column "id" {
    null = true
    type = integer
  }
}
`

func TestPlanLive_SavedPlanIsNotStaleAgainstItsOwnSource(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		// query is appended to the connection URL and selects its scope.
		query string
		// setup leaves the database in a state the document does not match, so
		// the plan carries statements rather than being empty.
		setup []string
		// file names the desired-state file, whose extension selects its loader.
		file string
		// document is the desired state, written to a file the plan reads.
		document string
		// wantBeyondURL is what the plan records for verification to read on top
		// of the URL's own scope.
		wantBeyondURL []string
	}{
		{
			// The regression row: `extra` exists and is empty, so the plan
			// creates only `extra.b` while the read that fingerprints the
			// source covers a schema the connected one is not.
			name:     "a document naming a schema beyond the connected one",
			setup:    []string{"CREATE SCHEMA extra", "CREATE TABLE public.a (id integer)"},
			file:     "desired.hcl",
			document: planScopeTwoSchemaDocument,
		},
		{
			// The planning read names `extra`, which does not exist, and the
			// realm read verification makes does not. The two still agree.
			name:     "a document creating a schema the realm does not have yet",
			setup:    []string{"CREATE TABLE public.a (id integer)"},
			file:     "desired.hcl",
			document: planScopeTwoSchemaDocument,
		},
		{
			// A URL limited to `public` does not cover `extra` on its own, so the
			// plan records it and verification reads it too.
			name:          "a URL pinned to the connected schema and a document naming a second one",
			query:         "search_path=public",
			setup:         []string{"CREATE SCHEMA extra", "CREATE TABLE public.a (id integer)"},
			file:          "desired.sql",
			document:      planScopeTwoSchemaSQL,
			wantBeyondURL: []string{"extra"},
		},
		{
			// The control: a document that names only the connected schema
			// reads at the same scope on both sides.
			name:  "a document naming only the connected schema",
			setup: []string{"CREATE TABLE public.a (id integer)"},
			file:  "desired.hcl",
			document: `schema "public" {
  comment = "standard public schema"
}

table "a" {
  schema = schema.public
  column "id" {
    null = true
    type = integer
  }
}

table "c" {
  schema = schema.public
  column "id" {
    null = true
    type = integer
  }
}
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := newInspectLiveConnection(c, ctx, test.query, test.setup)

			path := filepath.Join(c.TempDir(), test.file)
			c.Assert(os.WriteFile(path, []byte(test.document), 0o600), qt.IsNil)

			plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{
				ToURLs: []string{"file://" + path},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(plan.Statements, qt.Not(qt.HasLen), 0,
				qt.Commentf("an empty plan cannot show the fingerprints disagreeing"))
			c.Assert(plan.SchemasBeyondURL, qt.DeepEquals, test.wantBeyondURL)

			c.Assert(atlasschema.VerifyPlanTarget(ctx, conn, plan), qt.IsNil)
		})
	}
}

// planScopeTwoSchemaSQL is the same desired state as a SQL file. A URL limited
// to one schema refuses an HCL document declaring two, the way the pinned Atlas
// community binary does, so a SQL source is how a pinned URL meets a second
// schema.
const planScopeTwoSchemaSQL = `CREATE TABLE public.a (id integer);
CREATE SCHEMA extra;
CREATE TABLE extra.b (id integer);
`

// The other direction: a plan whose target moved has to be refused, wherever in
// the planning read the move happened. The statements a plan records write every
// schema its desired state names, so a verification read narrower than that
// accepts a plan that fails on the first statement it runs.
func TestPlanLive_SavedPlanIsStaleAfterTheTargetMoves(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		// query is appended to the connection URL and selects its scope.
		query string
		setup []string
		// file names the desired-state file, whose extension selects its loader.
		file     string
		document string
		// drift runs after the plan is saved, behind its back.
		drift string
	}{
		{
			name:     "a table added to the connected schema",
			setup:    []string{"CREATE SCHEMA extra", "CREATE TABLE public.a (id integer)"},
			file:     "desired.hcl",
			document: planScopeTwoSchemaDocument,
			drift:    "CREATE TABLE public.drifted (id integer)",
		},
		{
			name:     "the table the plan creates in the second schema already exists",
			setup:    []string{"CREATE SCHEMA extra", "CREATE TABLE public.a (id integer)"},
			file:     "desired.hcl",
			document: planScopeTwoSchemaDocument,
			drift:    "CREATE TABLE extra.b (id integer)",
		},
		{
			name:     "the schema the plan creates already exists",
			setup:    []string{"CREATE TABLE public.a (id integer)"},
			file:     "desired.hcl",
			document: planScopeTwoSchemaDocument,
			drift:    "CREATE SCHEMA extra",
		},
		{
			name:     "a URL pinned to the connected schema and a table created in the second one",
			query:    "search_path=public",
			setup:    []string{"CREATE SCHEMA extra", "CREATE TABLE public.a (id integer)"},
			file:     "desired.sql",
			document: planScopeTwoSchemaSQL,
			drift:    "CREATE TABLE extra.b (id integer)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := newInspectLiveConnection(c, ctx, test.query, test.setup)

			path := filepath.Join(c.TempDir(), test.file)
			c.Assert(os.WriteFile(path, []byte(test.document), 0o600), qt.IsNil)

			plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{
				ToURLs: []string{"file://" + path},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(atlasschema.VerifyPlanTarget(ctx, conn, plan), qt.IsNil,
				qt.Commentf("a plan stale before the drift cannot show the drift being seen"))

			_, err = conn.ExecContext(ctx, test.drift)
			c.Assert(err, qt.IsNil)

			var stale *atlasschema.StalePlanError
			c.Assert(atlasschema.VerifyPlanTarget(ctx, conn, plan), qt.ErrorAs, &stale)
		})
	}
}
