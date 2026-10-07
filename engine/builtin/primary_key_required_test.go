package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

func keylessEvents() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Event", Name: "events"}},
		Fields: []schemamodel.Field{{StructName: "Event", Name: "payload", Type: "TEXT"}},
	}
}

func keylessEventsNode() *ast.CreateTableNode {
	table := ast.NewCreateTable("events")
	table.AddColumn(ast.NewColumn("payload", "TEXT"))
	return table
}

func keyedEventsNode() *ast.CreateTableNode {
	table := keylessEventsNode()
	table.AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary())
	return table
}

// requireKey is a capability set that requires a primary key. The key decides
// and not the dialect name, so the PostgreSQL renderer handed this set refuses
// what it otherwise renders.
func requireKey() capability.Capabilities {
	return capability.Postgres16().With(capability.PrimaryKeyRequired, true)
}

// TestRender_KeylessTable_FailurePath pins the refusal on a target whose
// capability set carries capability.PrimaryKeyRequired. YDB answers the same
// table with `Primary key is required for ydb tables.`, at apply time and
// after the statements before it ran.
func TestRender_KeylessTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	visitor, err := builtin.NewRendererWithCapabilities("postgres", requireKey())
	c.Assert(err, qt.IsNil)
	sql, err := visitor.Render(keylessEventsNode())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `postgres requires a primary key and table "events" declares none`)
	c.Assert(sql, qt.Equals, "")
}

// TestRender_KeylessTable_HappyPath is the control for the refusal: a set
// without the key renders the keyless table, and a set with it renders a table
// that declares one.
func TestRender_KeylessTable_HappyPath(t *testing.T) {
	t.Run("a set without the key keeps a keyless table", func(t *testing.T) {
		c := qt.New(t)
		statements, err := builtin.GetOrderedCreateStatements(keylessEvents(), "postgres")
		c.Assert(err, qt.IsNil)
		c.Assert(statements, qt.DeepEquals,
			[]string{"-- POSTGRES TABLE: events --\nCREATE TABLE \"events\" (\n  \"payload\" TEXT NOT NULL\n);\n\n"})
	})

	t.Run("a set with the key renders a keyed table", func(t *testing.T) {
		c := qt.New(t)
		visitor, err := builtin.NewRendererWithCapabilities("postgres", requireKey())
		c.Assert(err, qt.IsNil)
		sql, err := visitor.Render(keyedEventsNode())
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, `"id" BIGINT PRIMARY KEY`)
	})
}

// TestRender_IndexCoveringColumnsFollowsTheCapability_FailurePath pins that
// the INCLUDE refusal reads the caller's set rather than a list of dialect
// names: the PostgreSQL renderer refuses a payload when the key is off.
func TestRender_IndexCoveringColumnsFollowsTheCapability_FailurePath(t *testing.T) {
	c := qt.New(t)
	visitor, err := builtin.NewRendererWithCapabilities("postgres",
		capability.Postgres16().With(capability.IndexCoveringColumns, false))
	c.Assert(err, qt.IsNil)
	sql, err := visitor.Render(indexIncludeNode(""))
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches,
		`postgres does not support INCLUDE columns on index "idx_accounts_email"; `+
			`target cockroachdb, postgres, spanner, sqlserver, ydb, or yugabytedb`)
	c.Assert(sql, qt.Equals, "")
}

// keyedEventSchemas are the ways a model gives a table its key, one per row.
// Each reaches the table through a different part of the model, which is the
// reason validation and rendering have to read them through the same lowering.
func keyedEventSchemas() map[string]*schemamodel.Database {
	events := []schemamodel.Table{{StructName: "Event", Name: "events"}}
	return map[string]*schemamodel.Database{
		"a field": {
			Tables: events,
			Fields: []schemamodel.Field{
				{StructName: "Event", Name: "id", Type: "BIGINT", Primary: true},
				{StructName: "Event", Name: "payload", Type: "TEXT"},
			},
		},
		"a composite key on the table": {
			Tables: []schemamodel.Table{{StructName: "Event", Name: "events", PrimaryKey: []string{"a", "b"}}},
			Fields: []schemamodel.Field{
				{StructName: "Event", Name: "a", Type: "BIGINT"},
				{StructName: "Event", Name: "b", Type: "BIGINT"},
			},
		},
		"a PRIMARY KEY constraint": {
			Tables: events,
			Fields: []schemamodel.Field{{StructName: "Event", Name: "id", Type: "BIGINT"}},
			Constraints: []schemamodel.Constraint{
				{StructName: "Event", Table: "events", Name: "events_pk", Type: "PRIMARY KEY", Columns: []string{"id"}},
			},
		},
	}
}

// TestValidateSchema_AgreesWithTheRenderOnARequiredKey_HappyPath is the
// control for the agreement: every way of declaring a key passes both
// validation and the render on a set that requires one.
func TestValidateSchema_AgreesWithTheRenderOnARequiredKey_HappyPath(t *testing.T) {
	for name, schema := range keyedEventSchemas() {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(builtin.ValidateSchemaWithCapabilities(schema, "postgres", requireKey()), qt.IsNil)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(schema, "postgres", requireKey())
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.Not(qt.HasLen), 0)
		})
	}
}

// TestValidateSchema_AgreesWithTheRenderOnARequiredKey_FailurePath pins that
// validation refuses the keyless table the render refuses, in the same words.
// Without the check validation passed it and the render refused it after, so a
// planner that validated first went on to build a plan it could not render.
func TestValidateSchema_AgreesWithTheRenderOnARequiredKey_FailurePath(t *testing.T) {
	c := qt.New(t)
	validated := builtin.ValidateSchemaWithCapabilities(keylessEvents(), "postgres", requireKey())
	c.Assert(validated, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(validated, qt.ErrorMatches, `postgres requires a primary key and table "events" declares none`)

	statements, rendered := builtin.GetOrderedCreateStatementsWithCapabilities(keylessEvents(), "postgres", requireKey())
	c.Assert(rendered, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(rendered.Error(), qt.Equals, validated.Error())
	c.Assert(statements, qt.IsNil)
}
