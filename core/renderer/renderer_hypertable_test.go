package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
)

// TestRender_ADeclaredExtensionIsTheOfflineEvidence pins that a render produces
// a coherent script.
//
// A render has no connection to ask which extensions the target has, and the
// document in front of it declares them. Without that rule, a schema declaring
// the timescaledb extension and a hypertable rendered the CREATE EXTENSION and
// then skipped the call that needs it — a script that installs an extension and
// refuses to use it (stokaro/ptah#1026).
func TestRender_ADeclaredExtensionIsTheOfflineEvidence(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(hypertableDocumentDeclaringTheExtension(), platform.Postgres)

	c.Assert(err, qt.IsNil)
	script := strings.Join(statements, "\n")
	c.Assert(script, qt.Contains, `CREATE EXTENSION "timescaledb"`)
	c.Assert(script, qt.Contains, "SELECT create_hypertable('public.readings'")
	c.Assert(script, qt.Not(qt.Contains), "hypertable public.readings is not supported")
}

// TestRender_WithoutTheExtensionTheCallIsSkipped is the control the rule needs.
//
// A schema that declares a hypertable and NOT the extension is asking for a
// call the target may not have, and an offline render has nothing that says it
// does. Skipping with a comment is the answer; emitting it would produce a
// script that fails on `function create_hypertable(unknown, unknown) does not
// exist`.
func TestRender_WithoutTheExtensionTheCallIsSkipped(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(hypertableDocument(), platform.Postgres)

	c.Assert(err, qt.IsNil)
	script := strings.Join(statements, "\n")
	c.Assert(script, qt.Contains, "hypertable public.readings is not supported by this target; skipped.")
	c.Assert(script, qt.Not(qt.Contains), "create_hypertable")
}

// hypertableDocumentDeclaringTheExtension is the same document plus the
// extension that makes the call available.
func hypertableDocumentDeclaringTheExtension() *schemamodel.Database {
	database := hypertableDocument()
	database.Extensions = []schemamodel.Extension{{Name: "timescaledb"}}
	return database
}

// hypertableDocument declares one partitioned table and nothing else.
func hypertableDocument() *schemamodel.Database {
	return &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}},
		Tables:  []schemamodel.Table{{StructName: "T", Name: "readings", Schema: "public"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "time", Type: "TIMESTAMPTZ", Primary: true},
		},
		Hypertables: []schemamodel.Hypertable{{
			StructName: "T", Table: "public.readings", Column: "time",
		}},
	}
}
