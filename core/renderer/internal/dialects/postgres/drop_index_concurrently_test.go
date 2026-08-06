package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/postgres"
)

// TestPostgreSQLRenderer_DropIndexConcurrently pins the rendered spelling of a
// non-blocking index drop and its capability gate.
//
// If the renderer change is reverted, every row prints the same string as the
// "capability withheld" row — the subject rows print
// `DROP INDEX IF EXISTS "idx_users_email";` where they want
// `DROP INDEX CONCURRENTLY IF EXISTS "idx_users_email";` — so the gate rows can
// no longer be told apart from the enabled rows.
func TestPostgreSQLRenderer_DropIndexConcurrently(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		dialect string
		node    func() *ast.DropIndexNode
		want    string
	}{
		{
			name:    "concurrently with the guard",
			caps:    capability.Postgres16(),
			dialect: platform.Postgres,
			node: func() *ast.DropIndexNode {
				return ast.NewDropIndex("idx_users_email").SetTable("users").SetIfExists().SetConcurrently()
			},
			want: "DROP INDEX CONCURRENTLY IF EXISTS \"idx_users_email\";\n",
		},
		{
			name:    "concurrently without the guard",
			caps:    capability.Postgres16().With(capability.DropIndexIfExists, false),
			dialect: platform.Postgres,
			node: func() *ast.DropIndexNode {
				return ast.NewDropIndex("idx_users_email").SetTable("users").SetIfExists().SetConcurrently()
			},
			want: "DROP INDEX CONCURRENTLY \"idx_users_email\";\n",
		},
		{
			name:    "capability withheld drops the keyword",
			caps:    capability.Postgres16().With(capability.DropIndexConcurrently, false),
			dialect: platform.Postgres,
			node: func() *ast.DropIndexNode {
				return ast.NewDropIndex("idx_users_email").SetTable("users").SetIfExists().SetConcurrently()
			},
			want: "DROP INDEX IF EXISTS \"idx_users_email\";\n",
		},
		{
			name:    "cockroachdb preset withholds the keyword",
			caps:    capability.CockroachDB23(),
			dialect: platform.CockroachDB,
			node: func() *ast.DropIndexNode {
				return ast.NewDropIndex("idx_users_email").SetTable("users").SetIfExists().SetConcurrently()
			},
			want: "DROP INDEX IF EXISTS \"users\"@\"idx_users_email\";\n",
		},
		{
			name:    "nil capability set is conservative",
			caps:    nil,
			dialect: platform.Postgres,
			node: func() *ast.DropIndexNode {
				return ast.NewDropIndex("idx_users_email").SetTable("users").SetConcurrently()
			},
			want: "DROP INDEX \"idx_users_email\";\n",
		},
		{
			name:    "unmarked drop is unchanged",
			caps:    capability.Postgres16(),
			dialect: platform.Postgres,
			node: func() *ast.DropIndexNode {
				return ast.NewDropIndex("idx_users_email").SetTable("users").SetIfExists()
			},
			want: "DROP INDEX IF EXISTS \"idx_users_email\";\n",
		},
		{
			name:    "schema-qualified target keeps the namespace",
			caps:    capability.Postgres16(),
			dialect: platform.Postgres,
			node: func() *ast.DropIndexNode {
				return ast.NewDropIndex("idx_user_order").SetTable("audit.orders").SetIfExists().SetConcurrently()
			},
			want: "DROP INDEX CONCURRENTLY IF EXISTS \"audit\".\"idx_user_order\";\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := postgres.NewWithCapabilities(tt.caps, tt.dialect).Render(tt.node())

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.want)
		})
	}
}
