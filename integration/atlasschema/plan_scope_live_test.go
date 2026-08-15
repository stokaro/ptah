//go:build integration

package atlasschema_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasschema"
)

// A saved plan records an exclude list and no schema scope, so `schema apply
// --plan` re-reads the target at the connection's own default. Planning reads
// the database at the scope the desired state names, which for a document
// declaring a schema beyond the connected one is wider. The two reads have to
// produce the same from-fingerprint or a plan is stale the moment it is saved.
//
// Measured on PostgreSQL 17.10 with the planning read fingerprinted directly:
//
//	Error: pre-planned migration is stale: the target database schema does not
//	match the plan's source fingerprint
//
// against the database the plan had just been computed from, with nothing
// changed in between. Live because the property is about two reads of one
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
	c := qt.New(t)
	ctx := context.Background()

	tests := []struct {
		name string
		// setup leaves the database in a state the document does not match, so
		// the plan carries statements rather than being empty.
		setup []string
		// document is the desired state, written to a file the plan reads.
		document string
	}{
		{
			// The regression row: `extra` exists and is empty, so the plan
			// creates only `extra.b` while the read that fingerprints the
			// source covers a schema the verification read does not.
			name:     "a document naming a schema beyond the connected one",
			setup:    []string{"CREATE SCHEMA extra", "CREATE TABLE public.a (id integer)"},
			document: planScopeTwoSchemaDocument,
		},
		{
			// The control: a document that names only the connected schema
			// reads at the same scope on both sides, and did before too.
			name:  "a document naming only the connected schema",
			setup: []string{"CREATE TABLE public.a (id integer)"},
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
		c.Run(test.name, func(c *qt.C) {
			conn := newInspectLiveConnection(c.TB, ctx, "", test.setup)

			path := filepath.Join(c.TempDir(), "desired.hcl")
			c.Assert(os.WriteFile(path, []byte(test.document), 0o600), qt.IsNil)

			plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{
				ToURLs: []string{"file://" + path},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(plan.Statements, qt.Not(qt.HasLen), 0,
				qt.Commentf("an empty plan cannot show the fingerprints disagreeing"))

			c.Assert(atlasschema.VerifyPlanTarget(conn, plan), qt.IsNil)
		})
	}
}
