package planner_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// unloggedDiff changes the table app.cache to unlogged, or back.
func unloggedDiff(c *qt.C, unlogged bool) *difftypes.SchemaDiff {
	c.Helper()
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Cache", Schema: "app", Name: "cache", PrimaryKey: []string{"id"},
			Facets: must.Must(pgtable.WithPersistence(schemaext.Facets{}, pgtable.Persistence{Unlogged: unlogged}))}},
		Fields:          []schemamodel.Field{{StructName: "Cache", Name: "id", Type: "bigint", Primary: true}},
		FeatureCoverage: must.Must(pgtable.PersistenceCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	current := &catalog.Database{Tables: []catalog.Table{{Schema: "app", Name: "cache",
		Facets:  must.Must(pgtable.WithObservedPersistence(schemaext.Facets{}, pgtable.Persistence{Unlogged: !unlogged})),
		Columns: []catalog.Column{{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true}},
	}}, FeatureCoverage: must.Must(pgtable.PersistenceCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))}
	return must.Must(schemadiff.CompareWithDialect(c.Context(), desired, current, platform.Postgres, builtintest.Runtime()))
}

// TestUnloggedChange_HappyPath writes a table's change between logged and
// unlogged as one ALTER TABLE on PostgreSQL.
func TestUnloggedChange_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		unlogged bool
		want     string
	}{
		{name: "made unlogged", unlogged: true, want: "ALTER TABLE \"app\".\"cache\" SET UNLOGGED"},
		{name: "made logged", unlogged: false, want: "ALTER TABLE \"app\".\"cache\" SET LOGGED"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), builtintest.Runtime(),
				unloggedDiff(c, test.unlogged), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0], qt.Contains, test.want)
		})
	}
}

// TestUnloggedChange_FailurePath refuses the change on a family member that
// creates no unlogged table. The comparison records none there, so only a diff
// built by hand reaches it, and writing nothing would report the change made.
func TestUnloggedChange_FailurePath(t *testing.T) {
	for _, dialect := range []string{platform.YugabyteDB, platform.CockroachDB, platform.Spanner} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), builtintest.Runtime(),
				unloggedDiff(c, true), dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*SET UNLOGGED.*`+dialect+`.*this target creates no unlogged table.*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}
