package postgres_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsast"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestPlanner_PlacesTimescaleOperationsInTheCommonOrder pins where each
// TimescaleDB operation joins the plan.
//
// A changed aggregate has no CREATE OR REPLACE -- measured on TimescaleDB
// 2.29.2, `CREATE OR REPLACE MATERIALIZED VIEW` is `syntax error at or near
// "MATERIALIZED"` -- so a modification is a drop and a create, in that order.
// An aggregate over an ordinary table answers `invalid continuous aggregate
// view`, so it is created after the create_hypertable call that partitions
// the table it reads and after the tables the plan creates, and before the
// views the plan creates, which may read it. A removal runs after the views and
// materialized views the plan drops, which may read the aggregate, and before
// the tables it drops, which the aggregate reads.
func TestPlanner_PlacesTimescaleOperationsInTheCommonOrder(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []string
	}{
		{
			name: "a replaced aggregate",
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
				aggregateChange("public", "hourly", observedAggregate("SELECT 1"), declaredAggregate("SELECT 2")),
			}},
			want: []string{"drop:public.hourly", "create:public.hourly"},
		},
		{
			name: "an undeclared aggregate",
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
				aggregateChange("public", "hourly", observedAggregate("SELECT 1"), nil),
			}},
			want: []string{"drop:public.hourly"},
		},
		{
			name: "an aggregate over a table this plan partitions",
			diff: &difftypes.SchemaDiff{
				FeatureChanges: []schemaext.ChangeRecord{aggregateChange("", "hourly", nil, declaredAggregate("SELECT 1"))},
				TablesModified: []difftypes.TableDiff{hypertableChange("readings", nil, &tsschema.DesiredHypertable{Column: "time"})},
			},
			want: []string{"hypertable:readings", "create:hourly"},
		},
		{
			name: "an aggregate over a table this plan creates",
			diff: &difftypes.SchemaDiff{
				FeatureChanges: []schemaext.ChangeRecord{aggregateChange("", "hourly", nil, declaredAggregate("SELECT 1"))},
				TablesAdded: difftypes.TableCreationsFor(&schemamodel.Database{
					Tables: []schemamodel.Table{{StructName: "R", Name: "readings"}},
					Fields: []schemamodel.Field{{StructName: "R", Name: "time", Type: "TIMESTAMPTZ"}},
				}, identifier.ForDialect(platform.Postgres), "readings"),
			},
			want: []string{"table:readings", "create:hourly"},
		},
		{
			name: "an aggregate over a table this plan drops",
			diff: &difftypes.SchemaDiff{
				FeatureChanges: []schemaext.ChangeRecord{aggregateChange("", "hourly", observedAggregate("SELECT 1"), nil)},
				TablesRemoved:  difftypes.TableRemovals{{Name: "readings"}},
			},
			want: []string{"drop:hourly", "drop table:readings"},
		},
		{
			// A view can read an aggregate, so the aggregate is dropped after
			// the views the plan drops: dropping it first answers that other
			// objects depend on it.
			name: "an aggregate a dropped view may read",
			diff: &difftypes.SchemaDiff{
				FeatureChanges: []schemaext.ChangeRecord{aggregateChange("", "hourly", observedAggregate("SELECT 1"), nil)},
				ViewsRemoved:   difftypes.ViewChanges{{Name: "recent"}},
			},
			want: []string{"drop view:recent", "drop:hourly"},
		},
		{
			// The other direction: the aggregate exists before a view that
			// may read it is created.
			name: "an aggregate a created view may read",
			diff: &difftypes.SchemaDiff{
				FeatureChanges: []schemaext.ChangeRecord{aggregateChange("", "hourly", nil, declaredAggregate("SELECT 1"))},
				ViewsAdded:     difftypes.ViewChanges{{Name: "recent", Body: "SELECT * FROM hourly"}},
				DeclaredViewLikes: difftypes.ViewLikeVocabulary{
					Views: []schemamodel.View{{Name: "recent", Body: "SELECT * FROM hourly"}},
				},
			},
			want: []string{"create:hourly", "create view:recent"},
		},
		{
			// A materialized view can read an aggregate, so the aggregate goes
			// after the materialized views the plan drops.
			name: "an aggregate a dropped materialized view may read",
			diff: &difftypes.SchemaDiff{
				FeatureChanges:           []schemaext.ChangeRecord{aggregateChange("", "hourly", observedAggregate("SELECT 1"), nil)},
				MaterializedViewsRemoved: difftypes.MaterializedViewChanges{{Name: "daily"}},
			},
			want: []string{"drop matview:daily", "drop:hourly"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := postgres.New().GenerateMigrationAST(context.Background(), must.Must(builtin.New()), test.diff)

			c.Assert(err, qt.IsNil)
			c.Assert(timescaleOrder(nodes), qt.DeepEquals, test.want)
		})
	}
}

// TestPlanner_RefusesWhatTimescaleDBCannotUndo pins the divergences that have
// no statement, and pins that they are REFUSALS rather than silence.
//
// Measured on TimescaleDB 2.29.2: `drop_hypertable` answers `function
// drop_hypertable(unknown) does not exist`, and no call repartitions an
// existing hypertable. Planning nothing would leave the table partitioned, the
// description saying otherwise, and an operator reading "no changes" believing
// the two agree -- a divergence that is permanent and invisible at once
// (stokaro/ptah#1026). The table is named, because an operator with several
// hypertables needs to know which one to write the migration for.
func TestPlanner_RefusesWhatTimescaleDBCannotUndo(t *testing.T) {
	current := &tsschema.ObservedHypertable{Column: "time", ChunkInterval: "7 days", Dimensions: 1}
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "the declaration stops naming it",
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{hypertableChange("readings", current, nil)}},
			want: `(?s).*readings is a hypertable and the desired schema does not declare one; TimescaleDB has no statement that turns a hypertable back into an ordinary table.*`,
		},
		{
			name: "the declaration moves the dimension",
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{hypertableChange("readings", current, &tsschema.DesiredHypertable{Column: "recorded_at"})}},
			want: `(?s).*hypertable readings is partitioned on "time" and the desired schema declares "recorded_at"; TimescaleDB has no statement that repartitions an existing hypertable.*`,
		},
		{
			name: "the declaration changes the interval",
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{hypertableChange("readings", current, &tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day"})}},
			want: `(?s).*hypertable readings has chunk interval "7 days" and the desired schema declares "1 day".*`,
		},
		{
			// A continuous aggregate holds its name as a relation, so a plan that
			// also creates a view under that name cannot apply.
			name: "a created view takes the aggregate's name",
			diff: &difftypes.SchemaDiff{
				FeatureChanges: []schemaext.ChangeRecord{aggregateChange("", "hourly", nil, declaredAggregate("SELECT 1"))},
				ViewsAdded:     difftypes.ViewChanges{{Name: "hourly", Body: "SELECT 1"}},
				DeclaredViewLikes: difftypes.ViewLikeVocabulary{
					Views: []schemamodel.View{{Name: "hourly", Body: "SELECT 1"}},
				},
			},
			want: `(?s).*a declared relation takes the name hourly, which a TimescaleDB continuous aggregate holds as a relation.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := postgres.New().GenerateMigrationAST(context.Background(), must.Must(builtin.New()), test.diff)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestPlanner_PlansAnAddedHypertable is the control the refusals need: the
// direction that HAS a statement still produces one, with the table spelled the
// way the declaration spells it.
func TestPlanner_PlansAnAddedHypertable(t *testing.T) {
	c := qt.New(t)

	nodes, err := postgres.New().GenerateMigrationAST(context.Background(), must.Must(builtin.New()),
		&difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{hypertableChange("readings", nil, &tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day"})}})

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.DeepEquals, []ast.Node{&ast.ExtensionStatement{Payload: &tsast.CreateHypertable{
		Table: "readings", Hypertable: tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day"},
	}}})
}

func declaredAggregate(body string) *tsschema.DesiredContinuousAggregate {
	return &tsschema.DesiredContinuousAggregate{Body: body}
}

func observedAggregate(definition string) *tsschema.ObservedContinuousAggregate {
	return &tsschema.ObservedContinuousAggregate{Definition: definition, HypertableName: "readings"}
}

func aggregateChange(schema, name string, before *tsschema.ObservedContinuousAggregate, after *tsschema.DesiredContinuousAggregate) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{
		Subject: tsschema.ContinuousAggregateRefWith(identifier.ForDialect(platform.Postgres), schema, name),
		Value:   &tsdiff.ContinuousAggregate{Before: before, After: after},
	}
}

// hypertableChange is a table whose hypertable settings change, carried the
// way the comparison carries it: on the table, with both sides captured.
func hypertableChange(table string, before *tsschema.ObservedHypertable, after *tsschema.DesiredHypertable) difftypes.TableDiff {
	subject := objectidentity.NewBuilder(identifier.ForDialect(platform.Postgres)).TableParts("", table)
	return difftypes.TableDiff{
		TableName: table,
		Desired:   schemacapture.TableDeclaration{Table: schemamodel.Table{Name: table}},
		FeatureChanges: []schemaext.ChangeRecord{{
			Subject: subject, Value: &tsdiff.Hypertable{Before: before, After: after},
		}},
	}
}

// timescaleOrder names the TimescaleDB statements a plan carries, and the
// table statements around them, in the order the plan carries them.
func timescaleOrder(nodes []ast.Node) []string {
	var order []string
	for _, node := range nodes {
		order = append(order, timescaleLabel(node)...)
	}
	return order
}

func timescaleLabel(node ast.Node) []string {
	switch typed := node.(type) {
	case *ast.CreateTableNode:
		return []string{"table:" + typed.Name}
	case *ast.DropTableNode:
		return []string{"drop table:" + typed.Name}
	case *ast.DropMaterializedViewNode:
		return []string{"drop matview:" + typed.Name}
	case *ast.DropViewNode:
		return []string{"drop view:" + typed.Name}
	case *ast.CreateViewNode:
		return []string{"create view:" + typed.Name}
	case *ast.ExtensionStatement:
		return extensionLabel(typed.Payload)
	default:
		return nil
	}
}

func extensionLabel(payload ast.ExtensionPayload) []string {
	switch typed := payload.(type) {
	case *tsast.CreateHypertable:
		return []string{"hypertable:" + typed.Table}
	case *tsast.ContinuousAggregate:
		name := typed.Name
		if typed.Schema != "" {
			name = typed.Schema + "." + name
		}
		var labels []string
		if typed.Change.Before != nil {
			labels = append(labels, "drop:"+name)
		}
		if typed.Change.After != nil {
			labels = append(labels, "create:"+name)
		}
		return labels
	default:
		return nil
	}
}

// TestPlanner_DropsAHypertableWithItsTable pins that a removed table carrying
// a hypertable is dropped, on every target of the family: DROP TABLE removes
// the hypertable and its chunks with the table, so TimescaleDB accounts for
// the hypertable through the table's removal instead of refusing it as a
// hypertable turned back into an ordinary table.
func TestPlanner_DropsAHypertableWithItsTable(t *testing.T) {
	facets := must.Must(schemaext.NewFacets(&tsschema.ObservedHypertable{Column: "time", ChunkInterval: "7 days", Dimensions: 1}))
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
	}{
		{name: "PostgreSQL", dialect: platform.Postgres, caps: capability.Postgres17()},
		{name: "CockroachDB", dialect: platform.CockroachDB, caps: capability.CockroachDB26()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			removal := difftypes.TableRemoval{Name: "readings"}
			removal.Current.Table = catalog.Table{Name: "readings", Facets: facets}

			nodes, err := postgres.NewForDialect(test.dialect, test.caps).GenerateMigrationAST(context.Background(), must.Must(builtin.New()),
				&difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{removal}})

			c.Assert(err, qt.IsNil)
			c.Assert(timescaleOrder(nodes), qt.DeepEquals, []string{"drop table:readings"})
		})
	}
}
