package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
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
	visitor, err := renderer.NewRendererWithCapabilities("postgres", requireKey())
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
		statements, err := renderer.GetOrderedCreateStatements(keylessEvents(), "postgres")
		c.Assert(err, qt.IsNil)
		c.Assert(statements, qt.DeepEquals,
			[]string{"-- POSTGRES TABLE: events --\nCREATE TABLE \"events\" (\n  \"payload\" TEXT NOT NULL\n);\n\n"})
	})

	t.Run("a set with the key renders a keyed table", func(t *testing.T) {
		c := qt.New(t)
		visitor, err := renderer.NewRendererWithCapabilities("postgres", requireKey())
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
	visitor, err := renderer.NewRendererWithCapabilities("postgres",
		capability.Postgres16().With(capability.IndexCoveringColumns, false))
	c.Assert(err, qt.IsNil)
	sql, err := visitor.Render(indexIncludeNode(""))
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches,
		`postgres does not support INCLUDE columns on index "idx_accounts_email"; `+
			`target cockroachdb, postgres, spanner, or yugabytedb`)
	c.Assert(sql, qt.Equals, "")
}
