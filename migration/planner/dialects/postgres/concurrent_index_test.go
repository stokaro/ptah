package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func indexAdditionFixture() (*types.SchemaDiff, *goschema.Database) {
	diff := &types.SchemaDiff{IndexesAdded: []string{"idx_users_email"}}
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Indexes: []goschema.Index{
			{Name: "idx_users_email", StructName: "User", Fields: []string{"email"}},
		},
	}
	return diff, generated
}

// TestPlanner_ConcurrentIndexes covers the postgres consumer of the
// capability set (issues #225/#226): CREATE INDEX CONCURRENTLY is emitted
// only when BOTH the policy is opted into (WithConcurrentIndexes — the
// statement cannot run inside a transaction block, so it must be a caller
// choice, issue #152) AND the target capability set includes
// capability.CreateIndexConcurrently (a postgres-compatible engine without
// it, e.g. CockroachDB in issue #171, keeps plain CREATE INDEX).
func TestPlanner_ConcurrentIndexes(t *testing.T) {
	diff, generated := indexAdditionFixture()

	t.Run("default policy stays non-concurrent", func(t *testing.T) {
		c := qt.New(t)

		nodes := postgres.New().GenerateMigrationAST(diff, generated)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		c.Assert(strings.Contains(sql, "CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);"), qt.IsTrue,
			qt.Commentf("default output must be byte-identical to the pre-capability planner; got:\n%s", sql))
		c.Assert(strings.Contains(sql, "CONCURRENTLY"), qt.IsFalse)
	})

	t.Run("policy plus capability emits CONCURRENTLY", func(t *testing.T) {
		c := qt.New(t)

		nodes := postgres.New().WithConcurrentIndexes().GenerateMigrationAST(diff, generated)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		c.Assert(strings.Contains(sql, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_email ON users (email);"), qt.IsTrue,
			qt.Commentf("CONCURRENTLY must precede IF NOT EXISTS per the PostgreSQL grammar; got:\n%s", sql))
	})

	t.Run("policy without capability keeps plain CREATE INDEX", func(t *testing.T) {
		c := qt.New(t)

		caps := capability.Postgres16().With(capability.CreateIndexConcurrently, false)
		nodes := postgres.NewWithCapabilities(caps).WithConcurrentIndexes().GenerateMigrationAST(diff, generated)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		c.Assert(strings.Contains(sql, "CONCURRENTLY"), qt.IsFalse,
			qt.Commentf("the capability gate must win over the policy; got:\n%s", sql))
		c.Assert(strings.Contains(sql, "CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);"), qt.IsTrue)
	})

	t.Run("unique concurrent index", func(t *testing.T) {
		c := qt.New(t)

		uniqueDiff := &types.SchemaDiff{IndexesAdded: []string{"uq_users_email"}}
		uniqueGenerated := &goschema.Database{
			Tables: []goschema.Table{{StructName: "User", Name: "users"}},
			Indexes: []goschema.Index{
				{Name: "uq_users_email", StructName: "User", Fields: []string{"email"}, Unique: true},
			},
		}
		nodes := postgres.New().WithConcurrentIndexes().GenerateMigrationAST(uniqueDiff, uniqueGenerated)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)

		c.Assert(strings.Contains(sql, "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_users_email ON users (email);"), qt.IsTrue,
			qt.Commentf("UNIQUE and CONCURRENTLY must compose; got:\n%s", sql))
	})

	t.Run("WithConcurrentIndexes does not mutate the receiver", func(t *testing.T) {
		c := qt.New(t)

		base := postgres.New()
		_ = base.WithConcurrentIndexes()

		nodes := base.GenerateMigrationAST(diff, generated)
		sql, err := renderer.RenderSQL("postgres", nodes...)
		c.Assert(err, qt.IsNil)
		c.Assert(strings.Contains(sql, "CONCURRENTLY"), qt.IsFalse,
			qt.Commentf("the original planner must keep the default policy; got:\n%s", sql))
	})
}
