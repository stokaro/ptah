package clickhouse

// White-box testing required: the subject is tableEngineOptionKeys, the
// unexported list this renderer reports the complement of. Spelling the list
// again in a black-box test would give the two copies nothing to disagree
// about, which is the defect the list is inside: a key the engine spec stopped
// reading, or one it reads that the list does not name, is a loss no report
// mentions (stokaro/ptah#2976).

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/renderdiag"
)

// optionValues gives each engine-spec key a value the rendered statement can be
// searched for.
//
// The values are distinct so that a key rendered into the wrong clause is still
// found, and each is a legal ClickHouse fragment so the render reaches the end.
var optionValues = map[string]string{
	"ENGINE":       "ReplacingMergeTree",
	"ORDER_BY":     "id",
	"PARTITION_BY": "id",
	"PRIMARY_KEY":  "id",
	"SAMPLE_BY":    "id",
	"SETTINGS":     "index_granularity = 4096",
	"TTL":          "created_at + INTERVAL 1 DAY",
}

// TestVisitCreateTable_RendersEveryTableOptionItKeeps binds the list to the
// engine spec that reads it.
//
// A key the list names is claimed to be carried, so the render has to show it.
// Dropping an arm from resolveTableEngineSpec while leaving its key here would
// otherwise produce a target that loses an option and reports nothing.
func TestVisitCreateTable_RendersEveryTableOptionItKeeps(t *testing.T) {
	for _, key := range tableEngineOptionKeys {
		t.Run(key, func(t *testing.T) {
			c := qt.New(t)
			value, declared := optionValues[key]
			c.Assert(declared, qt.IsTrue,
				qt.Commentf("%s has no value to render, so this case would assert nothing", key))

			renderer := New()
			sink := &renderdiag.Sink{}
			renderer.ReportOmissionsTo(sink)

			node := ast.NewCreateTable("events").
				AddColumn(ast.NewColumn("id", "INTEGER").SetNotNull()).
				AddColumn(ast.NewColumn("created_at", "TIMESTAMP").SetNotNull())
			node.SetOption("ORDER_BY", "id")
			node.SetOption(key, value)

			c.Assert(node.Accept(renderer), qt.IsNil)
			c.Assert(renderer.Output(), qt.Contains, value)
			c.Assert(sink.Omissions(), qt.HasLen, 0)
		})
	}
}

// TestVisitCreateTable_ReportsAnOptionOutsideTheEngineSpec is the other half.
//
// Every assertion above is satisfied by a renderer that reports nothing at all,
// so one key the list does not name has to arrive as a record.
func TestVisitCreateTable_ReportsAnOptionOutsideTheEngineSpec(t *testing.T) {
	c := qt.New(t)

	renderer := New()
	sink := &renderdiag.Sink{}
	renderer.ReportOmissionsTo(sink)

	node := ast.NewCreateTable("events").AddColumn(ast.NewColumn("id", "INTEGER").SetNotNull())
	node.SetOption("ORDER_BY", "id")
	node.SetOption("AUTO_INCREMENT", "100")

	c.Assert(node.Accept(renderer), qt.IsNil)
	c.Assert(sink.Omissions(), qt.DeepEquals, []renderdiag.Omission{{
		Reason:   renderdiag.ReasonUnsupported,
		Kind:     renderdiag.TableKind,
		Name:     "events",
		Property: renderdiag.TableOptionProperty + " AUTO_INCREMENT",
		Detail:   "100",
	}})
}
