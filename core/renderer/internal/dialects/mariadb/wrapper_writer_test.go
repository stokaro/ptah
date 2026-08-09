package mariadb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mariadb"
)

// TestMariaDBRenderer_WrapperVisitorsReachTheSharedBuffer is the MariaDB half of
// the MySQL test of the same name; see that file for why an orphaned buffer made
// all five visitors render nothing (stokaro/ptah#931 item 5).
func TestMariaDBRenderer_WrapperVisitorsReachTheSharedBuffer(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		render func(*mariadb.Renderer) error
		want   string
	}{
		{
			name:   "VisitExtension",
			render: func(r *mariadb.Renderer) error { return r.VisitExtension(&ast.ExtensionNode{Name: "pg_trgm"}) },
			want:   "-- Extension pg_trgm not supported in MariaDB",
		},
		{
			name:   "VisitDropExtension",
			render: func(r *mariadb.Renderer) error { return r.VisitDropExtension(&ast.DropExtensionNode{Name: "pg_trgm"}) },
			want:   "-- DROP EXTENSION pg_trgm not supported in MariaDB",
		},
		{
			name:   "VisitCreateFunction",
			render: func(r *mariadb.Renderer) error { return r.VisitCreateFunction(&ast.CreateFunctionNode{Name: "touch"}) },
			want:   "-- CREATE FUNCTION touch not supported in MariaDB",
		},
		{
			name:   "VisitCreatePolicy",
			render: func(r *mariadb.Renderer) error { return r.VisitCreatePolicy(&ast.CreatePolicyNode{Name: "p1"}) },
			want:   "-- CREATE POLICY p1 not supported in MariaDB",
		},
		{
			name: "VisitAlterTableEnableRLS",
			render: func(r *mariadb.Renderer) error {
				return r.VisitAlterTableEnableRLS(&ast.AlterTableEnableRLSNode{Table: "users"})
			},
			want: "-- ALTER TABLE users ENABLE ROW LEVEL SECURITY not supported in MariaDB",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			renderer := mariadb.New()
			renderer.Reset()

			err := test.render(renderer)

			c.Assert(err, qt.IsNil)
			c.Assert(renderer.Output(), qt.Not(qt.Equals), "")
			c.Assert(renderer.Output(), qt.Contains, test.want)
		})
	}
}
