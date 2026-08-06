package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func indexRemovalFixture() (*types.SchemaDiff, *goschema.Database) {
	diff := &types.SchemaDiff{IndexesRemoved: []types.IndexRef{
		{Name: "idx_users_email", TableName: "users"},
	}}
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
	}
	return diff, generated
}

// indexRedefinitionFixture drops and rebuilds the same index identity. The
// planner pairs that drop with the rebuild, so it is not a standalone removal
// and must never become concurrent: PostgreSQL would then need the pair split
// across a transactional and a non-transactional file.
func indexRedefinitionFixture() (*types.SchemaDiff, *goschema.Database) {
	diff := &types.SchemaDiff{
		IndexesAdded:   []types.IndexRef{{Name: "idx_users_email", TableName: "users"}},
		IndexesRemoved: []types.IndexRef{{Name: "idx_users_email", TableName: "users"}},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Indexes: []goschema.Index{
			{Name: "idx_users_email", StructName: "User", Fields: []string{"email", "tenant"}},
		},
	}
	return diff, generated
}

// TestPlanner_ConcurrentIndexDrops pins that DROP INDEX CONCURRENTLY needs BOTH
// an explicit per-plan request AND capability.DropIndexConcurrently, and that
// requesting concurrent BUILDS never rewrites a drop.
//
// If the planner change is reverted, every row prints
// "DROP INDEX IF EXISTS idx_users_email;" and the three rows that want
// CONCURRENTLY fail with `no substring match found`; the negative rows keep
// passing, which is what makes the pair discriminating.
func TestPlanner_ConcurrentIndexDrops(t *testing.T) {
	removalRef := types.IndexRef{Name: "idx_users_email", TableName: "users"}

	tests := []struct {
		name    string
		fixture func() (*types.SchemaDiff, *goschema.Database)
		planner func() *postgres.Planner
		want    string
	}{
		{
			name:    "default policy stays blocking",
			fixture: indexRemovalFixture,
			planner: postgres.New,
			want:    "DROP INDEX IF EXISTS idx_users_email;",
		},
		{
			name:    "blanket drop policy emits CONCURRENTLY",
			fixture: indexRemovalFixture,
			planner: func() *postgres.Planner { return postgres.New().WithConcurrentIndexDrops() },
			want:    "DROP INDEX CONCURRENTLY IF EXISTS idx_users_email;",
		},
		{
			name:    "listed ref emits CONCURRENTLY",
			fixture: indexRemovalFixture,
			planner: func() *postgres.Planner { return postgres.New().WithConcurrentIndexDropRefs(removalRef) },
			want:    "DROP INDEX CONCURRENTLY IF EXISTS idx_users_email;",
		},
		{
			name:    "unlisted ref stays blocking",
			fixture: indexRemovalFixture,
			planner: func() *postgres.Planner {
				return postgres.New().WithConcurrentIndexDropRefs(types.IndexRef{Name: "idx_users_email", TableName: "orders"})
			},
			want: "DROP INDEX IF EXISTS idx_users_email;",
		},
		{
			name:    "concurrent builds do not rewrite drops",
			fixture: indexRemovalFixture,
			planner: func() *postgres.Planner { return postgres.New().WithConcurrentIndexes() },
			want:    "DROP INDEX IF EXISTS idx_users_email;",
		},
		{
			name:    "capability withheld keeps the blocking drop",
			fixture: indexRemovalFixture,
			planner: func() *postgres.Planner {
				caps := capability.Postgres16().With(capability.DropIndexConcurrently, false)
				return postgres.NewWithCapabilities(caps).WithConcurrentIndexDrops()
			},
			want: "DROP INDEX IF EXISTS idx_users_email;",
		},
		{
			name:    "redefinition keeps its paired blocking drop",
			fixture: indexRedefinitionFixture,
			planner: func() *postgres.Planner { return postgres.New().WithConcurrentIndexDrops() },
			want:    "DROP INDEX IF EXISTS idx_users_email;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff, generated := tt.fixture()

			nodes, err := tt.planner().GenerateMigrationASTChecked(diff, generated)
			c.Assert(err, qt.IsNil)
			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)

			c.Assert(legacyRenderedSQL(sql), qt.Contains, tt.want, qt.Commentf("got:\n%s", sql))
		})
	}
}
