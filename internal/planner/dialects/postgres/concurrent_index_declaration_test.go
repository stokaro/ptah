package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// declaredConcurrentIndexFixture is indexAdditionFixture with the declaration
// the source made, which is the axis the fixture beside it holds at false.
//
// internal/modelast carries schemamodel.Index.Concurrently onto the AST node so
// the render path can emit the keyword (stokaro/ptah#3042). The planner builds
// its nodes with the same converter, so the declaration arrives here too, and
// the tests below are what keep it from deciding anything on this path.
func declaredConcurrentIndexFixture() (*difftypes.SchemaDiff, *schemamodel.Database) {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Indexes: []schemamodel.Index{
			{Name: "idx_users_email", StructName: "User", Fields: []string{"email"}, Concurrently: true},
		},
	}
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexAdditionsFor(desired, difftypes.IndexRef{Name: "idx_users_email", TableName: "users"}),
	}
	return diff, desired
}

// TestPlanner_ADeclarationDoesNotDecideTheConcurrentBuild is the guard for the
// second half of stokaro/ptah#3042.
//
// The planner owns this answer, and it owns it for reasons the declaration
// cannot see. Two gates live above it and neither is expressible in the desired
// schema: the target capability, which CockroachDB-family presets withhold
// (issue #171), and the partitioned-table exclusion in
// concurrentindex.DeclaredRefs, where PostgreSQL refuses a concurrent build
// outright. A planner that could only switch the flag ON would let a
// declaration past both.
//
// The rows below are the same schema every existing case in this package uses,
// with the declaration set. Each one must render exactly what it renders
// without it.
func TestPlanner_ADeclarationDoesNotDecideTheConcurrentBuild(t *testing.T) {
	diff, desired := declaredConcurrentIndexFixture()

	t.Run("no policy and no ref keeps plain CREATE INDEX", func(t *testing.T) {
		c := qt.New(t)

		nodes, err := postgres.New().GenerateMigrationAST(withDeclaredObjects(diff, desired))
		c.Assert(err, qt.IsNil)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)
		sql = legacyRenderedSQL(sql)

		c.Assert(sql, qt.Not(qt.Contains), "CONCURRENTLY",
			qt.Commentf("the caller asked for no concurrent build; got:\n%s", sql))
		c.Assert(sql, qt.Contains, "CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);")
	})

	t.Run("a target without the capability keeps plain CREATE INDEX", func(t *testing.T) {
		c := qt.New(t)

		caps := capability.Postgres16().With(capability.CreateIndexConcurrently, false)
		nodes, err := postgres.NewWithCapabilities(caps).
			WithConcurrentIndexes().
			GenerateMigrationAST(withDeclaredObjects(diff, desired))
		c.Assert(err, qt.IsNil)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)
		sql = legacyRenderedSQL(sql)

		c.Assert(sql, qt.Not(qt.Contains), "CONCURRENTLY",
			qt.Commentf("the capability gate must win over the declaration; got:\n%s", sql))
		c.Assert(sql, qt.Contains, "CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);")
	})

	t.Run("the ref the caller resolved is what turns it on", func(t *testing.T) {
		c := qt.New(t)

		ref := difftypes.IndexRef{Name: "idx_users_email", TableName: "users"}
		nodes, err := postgres.New().
			WithConcurrentIndexRefs(ref).
			GenerateMigrationAST(withDeclaredObjects(diff, desired))
		c.Assert(err, qt.IsNil)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)
		sql = legacyRenderedSQL(sql)

		c.Assert(sql, qt.Contains, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_email ON users (email);",
			qt.Commentf("a resolved ref is the planner's own answer and must still be honored; got:\n%s", sql))
	})
}
