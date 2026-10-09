package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
)

// refreshFacets binds a schedule to the clickhouse target, as source decoding
// does.
func refreshFacets(schedule chschema.Schedule) schemaext.Facets {
	return must.Must(must.Must(schemaext.NewFacets(&chschema.DesiredRefresh{Schedule: schedule})).WithTargetScope(chschema.RefreshKind, platform.ClickHouse))
}

// TestRenderSQL_ClickHouseMaterializedViewRefreshClause pins where the clause
// goes and what it says.
//
// The position is measured rather than chosen: ClickHouse prints the clause
// between the view name and the storage clause, and that is where its own
// parser expects it (stokaro/ptah#1802).
func TestRenderSQL_ClickHouseMaterializedViewRefreshClause(t *testing.T) {
	tests := []struct {
		name   string
		facets schemaext.Facets
		want   string
	}{
		{
			name: "no schedule renders as it always did",
			want: "CREATE MATERIALIZED VIEW `mv` ENGINE = MergeTree ORDER BY tuple() AS",
		},
		{
			name:   "every",
			facets: refreshFacets(chschema.Schedule{Mode: "EVERY", Interval: "1 HOUR"}),
			want:   "CREATE MATERIALIZED VIEW `mv` REFRESH EVERY 1 HOUR ENGINE = MergeTree ORDER BY tuple() AS",
		},
		{
			name:   "after",
			facets: refreshFacets(chschema.Schedule{Mode: "AFTER", Interval: "30 MINUTE"}),
			want:   "CREATE MATERIALIZED VIEW `mv` REFRESH AFTER 30 MINUTE ENGINE = MergeTree ORDER BY tuple() AS",
		},
		{
			name: "every clause at once",
			facets: refreshFacets(chschema.Schedule{
				Mode: "EVERY", Interval: "1 DAY", Offset: "2 HOUR", Randomize: "30 MINUTE",
				DependsOn: []string{"ptah_test.other"}, Append: true,
			}),
			want: "CREATE MATERIALIZED VIEW `mv` REFRESH EVERY 1 DAY OFFSET 2 HOUR " +
				"RANDOMIZE FOR 30 MINUTE DEPENDS ON ptah_test.other APPEND " +
				"ENGINE = MergeTree ORDER BY tuple() AS",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := ast.NewCreateMaterializedView("mv").SetBody("SELECT count() AS c FROM src")
			node.Facets = test.facets

			sql, err := builtin.RenderSQL(platform.ClickHouse, node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRenderSQL_ClickHouseModifyRefresh is the statement that changes a
// schedule without losing the view's rows. It travels in the ALTER envelope
// that names the view.
func TestRenderSQL_ClickHouseModifyRefresh(t *testing.T) {
	c := qt.New(t)
	alter := &ast.AlterTableNode{Name: "analytics.mv", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{
		Payload: &chast.ModifyRefresh{Schedule: chschema.Schedule{Mode: "EVERY", Interval: "2 HOUR"}},
	}}}

	sql, err := builtin.RenderSQL(platform.ClickHouse, alter)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE `analytics`.`mv` MODIFY REFRESH EVERY 2 HOUR;")
}

// TestRenderSQL_ModifyRefreshIsClickHouseOnly states the boundary the whole
// design rests on: a refresh SCHEDULE is one engine's feature, not a shared
// abstraction. PostgreSQL refreshes a materialized view with a statement
// someone runs, and carries no schedule to alter (stokaro/ptah#1625,
// stokaro/ptah#1802).
func TestRenderSQL_ModifyRefreshIsClickHouseOnly(t *testing.T) {
	for _, dialect := range []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer,
	} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			alter := &ast.AlterTableNode{Name: "mv", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{
				Payload: &chast.ModifyRefresh{Schedule: chschema.Schedule{Mode: "EVERY", Interval: "1 HOUR"}},
			}}}

			sql, err := builtin.RenderSQL(dialect, alter)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// A schedule bound to ClickHouse is not part of the view on another target,
// so the view renders there without it; an unbound one reaches that target's
// renderer, which has no owner for it and refuses it.
func TestRenderSQL_RefreshScheduleOnAnotherTarget(t *testing.T) {
	c := qt.New(t)
	schedule := chschema.Schedule{Mode: "EVERY", Interval: "1 HOUR"}
	bound := ast.NewCreateMaterializedView("mv").SetBody("SELECT 1")
	bound.Facets = refreshFacets(schedule)

	sql, err := builtin.RenderSQL(platform.Postgres, bound)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE MATERIALIZED VIEW")
	c.Assert(sql, qt.Not(qt.Contains), "REFRESH")

	unbound := ast.NewCreateMaterializedView("mv").SetBody("SELECT 1")
	unbound.Facets = must.Must(schemaext.NewFacets(&chschema.DesiredRefresh{Schedule: schedule}))
	sql, err = builtin.RenderSQL(platform.Postgres, unbound)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(sql, qt.Equals, "")
}
