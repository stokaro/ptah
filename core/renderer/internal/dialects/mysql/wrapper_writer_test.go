package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer/internal/dialects/mysql"
)

// TestMySQLRenderer_WrapperVisitorsReachTheSharedBuffer pins that the visitors
// defined on the MySQL wrapper are observable through Output().
//
// The wrapper holds an inner mysqllike renderer and delegates Output(), Reset()
// and Render() to it. Declaring `var w bufwriter.Writer`, handing &w to the
// inner renderer and then storing `w` -- a COPY -- on itself makes all five
// visitors below write into a buffer nothing ever reads: `ptah schema render
// --root-dir ext --dialect mysql` prints "-- Statement 1/2" followed by a blank
// line where the extension comment belongs (stokaro/ptah#931 item 5).
//
// Each row asserts the rendered text, not merely that it is non-empty: a
// length check passes on garbage, and the orphaned-buffer defect is exactly the
// kind a length check keeps reporting green.
func TestMySQLRenderer_WrapperVisitorsReachTheSharedBuffer(t *testing.T) {
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
			name: "VisitCreateFunction",
			render: func(r *mysql.Renderer) error {
				return r.VisitCreateFunction(&ast.CreateFunctionNode{
					Name: "touch", Returns: "int", Volatility: "IMMUTABLE", Body: "RETURN 1",
				})
			},
			want: "CREATE FUNCTION `touch`() RETURNS int DETERMINISTIC RETURN 1;",
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			renderer := mysql.New()
			renderer.Reset()

			err := test.render(renderer)

			c.Assert(err, qt.IsNil)
			c.Assert(renderer.Output(), qt.Not(qt.Equals), "")
			c.Assert(renderer.Output(), qt.Contains, test.want)
		})
	}
}
