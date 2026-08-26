package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// TestRenderWritesTheContinuousAggregateBlock pins that a description names an
// object a document would otherwise describe as a view.
//
// To PostgreSQL a continuous aggregate IS a view -- pg_class reports relkind
// 'v' -- and both ways of settling for that are wrong. Omitting it plans a DROP
// the server refuses (`cannot drop continuous aggregate using DROP VIEW`), and
// describing it as a view replays the rewritten body TimescaleDB stores, which
// selects from a relation in a schema the extension owns (stokaro/ptah#1026).
func TestRenderWritesTheContinuousAggregateBlock(t *testing.T) {
	c := qt.New(t)

	db := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}},
		Tables:  []schemamodel.Table{{StructName: "T", Name: "readings", Schema: "public"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "time", Type: "TIMESTAMPTZ", Primary: true},
		},
		ContinuousAggregates: []schemamodel.ContinuousAggregate{{
			Name: "hourly", Schema: "public", Body: "SELECT 1", MaterializedOnly: new(true),
		}},
	}

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains,
		"continuous_aggregate \"hourly\" {\n  schema = schema.public\n"+
			"  as = \"SELECT 1\"\n  materialized_only = true\n}")
}

// TestRenderTheAggregateSurvivesItsOwnDocument is the round trip the block
// exists for: what is written is read back as the same declaration.
func TestRenderTheAggregateSurvivesItsOwnDocument(t *testing.T) {
	c := qt.New(t)

	db := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}},
		Tables:  []schemamodel.Table{{StructName: "T", Name: "readings", Schema: "public"}},
		Fields:  []schemamodel.Field{{StructName: "T", Name: "time", Type: "TIMESTAMPTZ", Primary: true}},
		ContinuousAggregates: []schemamodel.ContinuousAggregate{{
			Name: "hourly", Schema: "public",
			Body: "SELECT time_bucket('1 hour', time) AS bucket FROM readings GROUP BY bucket",
		}},
	}

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
	c.Assert(err, qt.IsNil)

	reread, err := atlashclParse(result.Data)

	c.Assert(err, qt.IsNil)
	c.Assert(reread, qt.DeepEquals, db.ContinuousAggregates)
}

// atlashclParse reads a rendered document back, so a round trip is one call.
func atlashclParse(document []byte) ([]schemamodel.ContinuousAggregate, error) {
	db, err := atlashcl.Parse(document, "schema.hcl")
	if err != nil {
		return nil, err
	}
	return db.ContinuousAggregates, nil
}
