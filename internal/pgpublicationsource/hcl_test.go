package pgpublicationsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpublication"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/pgpublicationsource"
)

func parseHCL(c *qt.C, text string) (*schemamodel.Database, error) {
	c.Helper()
	return atlashcl.ParseWithOptions(c.Context(), []byte(text), "publications.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
}

const hclTables = `schema "public" {}
schema "audit" {}
table "orders" {
  schema = schema.public
  column "id" {
    type = integer
  }
}
table "events" {
  schema = schema.audit
  column "id" {
    type = integer
  }
}
`

// TestHCL_HappyPath pins what a publication block declares, in each
// membership form, with the options and owner it writes and nothing else.
func TestHCL_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		block string
		want  pgpublication.DesiredPublication
	}{
		{name: "empty", block: `publication "feed" {}`},
		{name: "all tables", block: `publication "feed" {
  all_tables = true
}`, want: pgpublication.DesiredPublication{AllTables: true}},
		{name: "tables and schemas", block: `publication "feed" {
  tables  = [table.public.orders, table.audit.events]
  schemas = ["audit"]
}`, want: pgpublication.DesiredPublication{Tables: []pgpublication.Member{{Schema: "public", Table: "orders"}, {Schema: "audit", Table: "events"}},
			Schemas: []string{"audit"}}},
		{name: "options and owner", block: `role "replicator" {}
publication "feed" {
  publish                    = ["insert", "update"]
  publish_via_partition_root = true
  publish_generated_columns  = "stored"
  owner                      = role.replicator
}`, want: pgpublication.DesiredPublication{Publish: []string{"insert", "update"}, ViaPartitionRoot: true,
			GeneratedColumns: pgpublication.GeneratedColumnsStored, Owner: "replicator"}},
		{name: "an empty publish set", block: `publication "feed" {
  publish = []
}`, want: pgpublication.DesiredPublication{Publish: make([]string, 0)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := parseHCL(c, hclTables+test.block)
			c.Assert(err, qt.IsNil)
			c.Assert(publications(c, *db), qt.DeepEquals, map[string]pgpublication.DesiredPublication{"feed": test.want})
			c.Assert(db.FeatureCoverage.Lookup(pgpublication.PublicationKind, pgpublication.Ref("other")).State, qt.Equals, schemaext.Complete)
		})
	}
}

// TestHCL_FailurePath pins the blocks the owner refuses.
func TestHCL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		block   string
		wantErr string
	}{
		{name: "all tables with tables", block: `publication "feed" {
  all_tables = true
  tables     = [table.public.orders]
}`, wantErr: `(?s).*FOR ALL TABLES.*`},
		{name: "unknown operation", block: `publication "feed" {
  publish = ["merge"]
}`, wantErr: `(?s).*merge.*`},
		{name: "unknown generated columns", block: `publication "feed" {
  publish_generated_columns = "virtual"
}`, wantErr: `(?s).*virtual.*`},
		{name: "unknown attribute", block: `publication "feed" {
  row_filter = "id > 0"
}`, wantErr: `(?s).*row_filter.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := parseHCL(c, hclTables+test.block)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestHCLExport_RoundTrips pins that the writer writes each membership form
// back as a block the reader reads to the same publication, and writes it
// the same way twice.
func TestHCLExport_RoundTrips(t *testing.T) {
	tests := []struct {
		name        string
		publication pgpublication.DesiredPublication
	}{
		{name: "empty"},
		{name: "all tables", publication: pgpublication.DesiredPublication{AllTables: true, Publish: make([]string, 0)}},
		{name: "tables and schemas", publication: pgpublication.DesiredPublication{
			Tables: []pgpublication.Member{{Schema: "public", Table: "orders"}, {Schema: "audit", Table: "Events Log"}}, Schemas: []string{"audit"},
			Publish: []string{"insert"}, ViaPartitionRoot: true, GeneratedColumns: pgpublication.GeneratedColumnsNone, Owner: "replicator"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			object := must.Must(pgpublicationsource.Declare("Orders Feed", test.publication))
			db := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "replicator"}}, FeatureObjects: must.Must(schemaext.NewObjects(object))}
			rendered, err := atlashclrender.RenderInspected(db, "public", atlashclrender.Options{HCL: builtintest.Runtime().HCL()})
			c.Assert(err, qt.IsNil)
			c.Assert(rendered.Diagnostics, qt.HasLen, 0)
			c.Assert(string(rendered.Data), qt.Contains, `publication "Orders Feed" {`)
			c.Assert(string(rendered.Data), qt.Not(qt.Contains), "schema = schema.public\n}")
			parsed := must.Must(parseHCL(c, string(rendered.Data)))
			c.Assert(parsed.FeatureObjects.Equal(db.FeatureObjects), qt.IsTrue, qt.Commentf("%s", rendered.Data))
			second := must.Must(atlashclrender.RenderInspected(parsed, "public", atlashclrender.Options{HCL: builtintest.Runtime().HCL()}))
			c.Assert(second.Data, qt.DeepEquals, rendered.Data)
		})
	}
}

// TestHCLExport_RefusesAFilter pins that the writer refuses a publication
// whose row filter or column list the block cannot say, rather than write a
// publication that publishes more.
func TestHCLExport_RefusesAFilter(t *testing.T) {
	c := qt.New(t)
	object := must.Must(pgpublication.DesiredObject(pgpublication.Ref("feed"), pgpublication.DesiredPublication{
		Tables: []pgpublication.Member{{Schema: "public", Table: "orders", RowFilter: "(id > 0)"}}}))
	db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(object))}

	rendered, err := atlashclrender.RenderInspected(db, "public", atlashclrender.Options{HCL: builtintest.Runtime().HCL()})

	c.Assert(err, qt.ErrorMatches, `(?s).*row filter.*`)
	c.Assert(rendered.Data, qt.HasLen, 0)
}
