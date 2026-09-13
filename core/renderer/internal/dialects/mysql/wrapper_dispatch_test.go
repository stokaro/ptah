package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/core/renderer/internal/dialects/mysql"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/renderdiag"
)

// wrapperOwnedRow is one of the four node kinds the MySQL wrapper answers
// itself instead of forwarding, with the text every public entry point
// produces.
type wrapperOwnedRow struct {
	kind string
	node ast.Node
	// want is the refusal line, and both entry points below write it. Render
	// accepts the node onto the wrapper and RenderSQL accepts it onto the
	// validating renderer, which hands it to the wrapper: each route reaches
	// the wrapper's dispatch, and the dispatch reaches the handler that spells
	// the dialect name in capitals.
	want string
}

// mysqlWrapperOwnedKinds is the four node kinds mysql.Renderer answers with a
// body of its own. Everything else it forwards.
func mysqlWrapperOwnedKinds() []wrapperOwnedRow {
	return []wrapperOwnedRow{
		{
			kind: "ExtensionNode",
			node: ast.NewExtension("pg_trgm"),
			want: "-- Extension pg_trgm not supported in MySQL\n",
		},
		{
			kind: "DropExtensionNode",
			node: ast.NewDropExtension("pg_trgm"),
			want: "-- DROP EXTENSION pg_trgm not supported in MySQL\n",
		},
		{
			kind: "CreatePolicyNode",
			node: ast.NewCreatePolicy("p1", "users"),
			want: "-- CREATE POLICY p1 not supported in MySQL\n",
		},
		{
			kind: "AlterTableEnableRLSNode",
			node: ast.NewAlterTableEnableRLS("users"),
			want: "-- ALTER TABLE users ENABLE ROW LEVEL SECURITY not supported in MySQL\n",
		},
	}
}

// TestMySQLWrapper_OwnedKindsHaveOneSpelling pins the refusal text and measures
// that both entry points produce it.
//
// One dispatch method answers a node kind, so one spelling of the refusal
// exists: the wrapper's, which capitalizes the dialect name. The shared
// mysqllike renderer writes the same refusal with its own lowercase dialect
// name, and this test is what keeps that text out of the MySQL wrapper's
// output -- a Render that hands the node to the shared renderer instead of
// accepting it here reddens the Render half of every row.
// TestMySQLWrapper_TheShippedSpellingIsTheWrappers measures that the spelling
// kept is the one `ptah schema render` emits.
func TestMySQLWrapper_OwnedKindsHaveOneSpelling(t *testing.T) {
	for _, row := range mysqlWrapperOwnedKinds() {
		t.Run(row.kind, func(t *testing.T) {
			c := qt.New(t)

			r, err := renderer.NewRenderer(platform.MySQL)
			c.Assert(err, qt.IsNil)

			rendered, err := r.Render(row.node)
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, row.want)

			renderedSQL, err := renderer.RenderSQL(platform.MySQL, row.node)
			c.Assert(err, qt.IsNil)
			c.Assert(renderedSQL, qt.Equals, row.want)
		})
	}
}

// TestMySQLWrapper_TheShippedSpellingIsTheWrappers measures which spelling a
// user reads.
//
// `ptah schema render` builds its statements through
// GetOrderedCreateStatements, which accepts each node onto the validating
// wrapper. That route reaches this wrapper's dispatch, so the capitalized
// spelling is the one a user reads, and it is the spelling the dispatch keeps.
func TestMySQLWrapper_TheShippedSpellingIsTheWrappers(t *testing.T) {
	c := qt.New(t)

	database := &schemamodel.Database{
		Extensions: []schemamodel.Extension{{Name: "pg_trgm"}},
	}

	statements, err := renderer.GetOrderedCreateStatements(database, platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"-- Extension pg_trgm not supported in MySQL\n"})
}

// policyHalfRow is one half of an RLS declaration, with the line it writes and
// the omission records it leaves behind.
type policyHalfRow struct {
	kind          string
	node          ast.Node
	wantOutput    string
	wantOmissions []renderdiag.Omission
}

// mysqlPolicyHalves is the two RLS pairs, each split between a wrapper-owned
// half and a forwarded half.
//
// CREATE POLICY and ENABLE ROW LEVEL SECURITY are answered by the wrapper,
// which writes a line and records nothing. DROP POLICY and DISABLE ROW LEVEL
// SECURITY are forwarded to the shared renderer, which writes a differently
// worded line and also records an omission. So the UP and DOWN halves of one
// declaration are reported in two shapes, and `ptah schema validate
// --no-skipped` sees only the DOWN half.
func mysqlPolicyHalves() []policyHalfRow {
	return []policyHalfRow{
		{
			kind:          "CreatePolicyNode",
			node:          ast.NewCreatePolicy("p1", "users"),
			wantOutput:    "-- CREATE POLICY p1 not supported in MySQL\n",
			wantOmissions: nil,
		},
		{
			kind:       "DropPolicyNode",
			node:       ast.NewDropPolicy("p1", "users"),
			wantOutput: "-- MYSQL: DROP POLICY p1 is not generated for this target; skipped.\n",
			wantOmissions: []renderdiag.Omission{
				{Reason: renderdiag.ReasonUnsupported, Kind: "DROP POLICY", Name: "p1"},
			},
		},
		{
			kind:          "AlterTableEnableRLSNode",
			node:          ast.NewAlterTableEnableRLS("users"),
			wantOutput:    "-- ALTER TABLE users ENABLE ROW LEVEL SECURITY not supported in MySQL\n",
			wantOmissions: nil,
		},
		{
			kind:       "AlterTableDisableRLSNode",
			node:       ast.NewAlterTableDisableRLS("users"),
			wantOutput: "-- MYSQL: DISABLE ROW LEVEL SECURITY on users is not generated for this target; skipped.\n",
			wantOmissions: []renderdiag.Omission{
				// The kind carries the trailing "on": the shared renderer
				// builds it from the sentence rather than from an object kind.
				{Reason: renderdiag.ReasonUnsupported, Kind: "DISABLE ROW LEVEL SECURITY on", Name: "users"},
			},
		},
	}
}

// TestMySQLWrapper_PolicyHalvesReportDifferently pins the asymmetry inside each
// RLS pair, so flattening a pair either way shows up.
//
// Folding the wrapper's two halves onto the shared renderer adds an omission
// record where there was none; folding the shared renderer's two halves onto
// the wrapper takes one away. Either direction is a change to what `ptah schema
// validate --no-skipped` reports, and neither changes what compiles.
//
// The sink is attached to the dialect renderer directly because the public
// omission entry point walks a schema model, which produces the UP half of a
// declaration and never the DOWN half -- so the pair cannot be compared there.
func TestMySQLWrapper_PolicyHalvesReportDifferently(t *testing.T) {
	for _, row := range mysqlPolicyHalves() {
		t.Run(row.kind, func(t *testing.T) {
			c := qt.New(t)

			sink := &renderdiag.Sink{}
			r := mysql.New()
			r.ReportOmissionsTo(sink)

			err := row.node.Accept(r)

			c.Assert(err, qt.IsNil)
			c.Assert(r.Output(), qt.Equals, row.wantOutput)
			c.Assert(sink.Omissions(), qt.DeepEquals, row.wantOmissions)
		})
	}
}

// TestMySQLWrapper_OwnAndForwardedWritesShareOneBuffer pins that this wrapper
// writes into the buffer its Output reads.
//
// The wrapper holds the shared renderer's bufwriter by pointer on purpose.
// Holding it by value gives the wrapper a copy: its four own methods then write
// into a buffer nothing reads, Output returns only what the forwards produced,
// and the render exits 0 with the refusal lines missing. Interleaving the two
// kinds of write and asserting the whole buffer is what separates one buffer
// from two -- an assertion on the wrapper's line alone passes on a renderer
// that dropped every forwarded statement instead.
func TestMySQLWrapper_OwnAndForwardedWritesShareOneBuffer(t *testing.T) {
	c := qt.New(t)

	r := mysql.New()

	c.Assert(ast.NewCreateSchema("app").Accept(r), qt.IsNil)
	c.Assert(ast.NewExtension("pg_trgm").Accept(r), qt.IsNil)
	c.Assert(ast.NewDropTable("users").Accept(r), qt.IsNil)

	c.Assert(r.Output(), qt.Equals, "CREATE SCHEMA `app`;\n"+
		"-- Extension pg_trgm not supported in MySQL\n"+
		"DROP TABLE `users`;\n")
}

// TestMySQLWrapper_ResetClearsWhatTheWrapperWrote is the other half of the
// shared-buffer property: Reset is delegated to the shared renderer, so it
// clears the wrapper's own writes only while the two hold one buffer.
func TestMySQLWrapper_ResetClearsWhatTheWrapperWrote(t *testing.T) {
	c := qt.New(t)

	r := mysql.New()
	c.Assert(ast.NewExtension("pg_trgm").Accept(r), qt.IsNil)
	c.Assert(r.Output(), qt.Equals, "-- Extension pg_trgm not supported in MySQL\n")

	r.Reset()
	c.Assert(r.Output(), qt.Equals, "")

	c.Assert(ast.NewCreatePolicy("p1", "users").Accept(r), qt.IsNil)
	c.Assert(r.Output(), qt.Equals, "-- CREATE POLICY p1 not supported in MySQL\n")
}
