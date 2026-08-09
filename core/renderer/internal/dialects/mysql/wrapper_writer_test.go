package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mysql"
)

// TestMySQLRenderer_WrapperVisitorsReachTheSharedBuffer pins that the visitors
// defined on the MySQL wrapper are observable through Output().
//
// The wrapper holds an inner mysqllike renderer and delegates Output(), Reset()
// and Render() to it. It used to declare `var w bufwriter.Writer`, hand &w to
// the inner renderer and then store `w` -- a COPY -- on itself, so all five
// visitors below wrote into a buffer nothing ever read. `ptah schema render
// --root-dir ext --dialect mysql` printed "-- Statement 1/2" followed by a blank
// line where the extension comment belonged (stokaro/ptah#931 item 5).
//
// Each row asserts the rendered text, not merely that it is non-empty: a
// length check passes on garbage, and the orphaned-buffer defect is exactly the
// kind that a length check would have kept reporting green.
func TestMySQLRenderer_WrapperVisitorsReachTheSharedBuffer(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		render func(*mysql.Renderer) error
		want   string
	}{
		{
			name:   "VisitExtension",
			render: func(r *mysql.Renderer) error { return r.VisitExtension(&ast.ExtensionNode{Name: "pg_trgm"}) },
			want:   "-- Extension pg_trgm not supported in MySQL",
		},
		{
			name:   "VisitDropExtension",
			render: func(r *mysql.Renderer) error { return r.VisitDropExtension(&ast.DropExtensionNode{Name: "pg_trgm"}) },
			want:   "-- DROP EXTENSION pg_trgm not supported in MySQL",
		},
		{
			name:   "VisitCreateFunction",
			render: func(r *mysql.Renderer) error { return r.VisitCreateFunction(&ast.CreateFunctionNode{Name: "touch"}) },
			want:   "-- CREATE FUNCTION touch not supported in MySQL",
		},
		{
			name:   "VisitCreatePolicy",
			render: func(r *mysql.Renderer) error { return r.VisitCreatePolicy(&ast.CreatePolicyNode{Name: "p1"}) },
			want:   "-- CREATE POLICY p1 not supported in MySQL",
		},
		{
			name: "VisitAlterTableEnableRLS",
			render: func(r *mysql.Renderer) error {
				return r.VisitAlterTableEnableRLS(&ast.AlterTableEnableRLSNode{Table: "users"})
			},
			want: "-- ALTER TABLE users ENABLE ROW LEVEL SECURITY not supported in MySQL",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			renderer := mysql.New()
			renderer.Reset()

			err := test.render(renderer)

			c.Assert(err, qt.IsNil)
			c.Assert(renderer.Output(), qt.Not(qt.Equals), "")
			c.Assert(renderer.Output(), qt.Contains, test.want)
		})
	}
}
