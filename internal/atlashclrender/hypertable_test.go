package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// TestRenderWritesTheHypertableBlock pins that a description says which tables
// are partitioned.
//
// Nothing else in a document can. Measured on TimescaleDB 2.29.2 / PostgreSQL
// 17.11, a hypertable answers `relkind = 'r'` and carries no extension
// ownership in `pg_depend`, so a document that describes the table describes an
// ORDINARY table — complete on its face, and wrong. Replaying it creates a table
// that is not partitioned, and a diff between the two reports no difference
// (stokaro/ptah#1026).
func TestRenderWritesTheHypertableBlock(t *testing.T) {
	c := qt.New(t)

	db := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}},
		Tables:  []schemamodel.Table{{StructName: "T", Name: "readings", Schema: "public"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "time", Type: "TIMESTAMPTZ", Primary: true},
		},
		Hypertables: []schemamodel.Hypertable{{
			Table: "public.readings", Column: "time", ChunkInterval: "1 day",
		}},
	}

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains,
		"hypertable \"readings\" {\n  schema = schema.public\n"+
			"  column = \"time\"\n  chunk_interval = \"1 day\"\n}")
}

// TestRenderOmitsAnIntervalTheDeclarationDidNot is the row that keeps a round
// trip converging.
//
// An omitted interval takes TimescaleDB's own default, and writing the default
// back would turn "whatever the server chooses" into a fixed value the next
// comparison holds the server to.
func TestRenderOmitsAnIntervalTheDeclarationDidNot(t *testing.T) {
	c := qt.New(t)

	db := &schemamodel.Database{
		Schemas:     []schemamodel.Schema{{Name: "public"}},
		Tables:      []schemamodel.Table{{StructName: "T", Name: "readings", Schema: "public"}},
		Fields:      []schemamodel.Field{{StructName: "T", Name: "time", Type: "TIMESTAMPTZ", Primary: true}},
		Hypertables: []schemamodel.Hypertable{{Table: "public.readings", Column: "time"}},
	}

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, "hypertable \"readings\" {")
	c.Assert(string(result.Data), qt.Not(qt.Contains), "chunk_interval")
}
