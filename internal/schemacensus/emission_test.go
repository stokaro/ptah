package schemacensus_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/schemacensus"
)

// TestMeasureEmissions_EveryPhysicalObjectIsEmittedOnce is #2606's second
// invariant, measured over the corpus rather than asserted.
//
// A physical object with two DDL emission paths shows up here as one name
// created twice in one render. #2583 was that shape: a self-reference derived
// by internal/deporder while the constraint path already carried it.
func TestMeasureEmissions_EveryPhysicalObjectIsEmittedOnce(t *testing.T) {
	c := qt.New(t)

	measured := schemacensus.MeasureEmissions()

	c.Assert(measured.Duplicates, qt.HasLen, 0, qt.Commentf(
		"a physical object was created more than once in one render, so it has two emission paths"))
}

// TestMeasureEmissions_TheCorpusActuallyRenders is the floor the assertion
// above needs.
//
// Zero duplicates is what a corpus that rendered nothing reports, and what an
// extractor that recognized nothing reports. Both would leave the invariant
// unmeasured while the test above stayed green, and neither is distinguishable
// from the invariant holding without a count.
//
// The number is a floor rather than an equality: adding a fixture or a release
// line raises it, and a gate that had to be edited for every such change would
// be edited without being read. It was 3986 when this was written.
func TestMeasureEmissions_TheCorpusActuallyRenders(t *testing.T) {
	c := qt.New(t)

	measured := schemacensus.MeasureEmissions()

	c.Assert(measured.Objects > 3000, qt.IsTrue, qt.Commentf(
		"the corpus emitted %d objects, so the duplicate check above measured almost nothing",
		measured.Objects))
}

// TestMeasureEmissions_EveryCellContributes is the floor one level down, and
// the total cannot stand in for it.
//
// A cell whose render errors contributes nothing, deliberately: a refusal
// creates no object, so there is nothing for the invariant to be about. What
// that also does is make a target that stops rendering ENTIRELY
// indistinguishable from one that has nothing to say -- the duplicate check
// reports zero because nothing was measured, and the total reports success
// because the others carried it.
//
// Measured on the corpus this was written against: 3986 objects over 31 cells,
// of which the largest is 181. Five could go dark and the total would still be
// above any floor worth writing. A renderer refusal introduced for one release
// line is an ordinary change -- #2586 was exactly that for one dialect.
//
// Per cell rather than per dialect, because a dialect is dark exactly when all
// of its cells are: a dialect check would report nothing this does not, and
// could not be made to fail on its own.
func TestMeasureEmissions_EveryCellContributes(t *testing.T) {
	c := qt.New(t)

	measured := schemacensus.MeasureEmissions()

	c.Assert(measured.DarkCells, qt.HasLen, 0, qt.Commentf(
		"these declared cells rendered no object at all, so nothing about them was measured"))
}

// TestMeasureEmissions_EveryFixtureContributes is the same floor on the other
// axis, and neither can be derived from the other.
//
// A fixture that renders nowhere still leaves every cell contributing, because
// the other fixtures do -- so the cell floor above passes over it. Measured:
// 97 fixtures, the largest contributing 93 of 3986, so any ten could stop
// contributing with every other assertion here green.
//
// Dark here means neither rendered nor refused, which is weaker than the cell
// rule on purpose. A fixture every target refuses is deliberate --
// `column-unique-expr` is refused on all 31 cells, because uniqueness over an
// expression is not implemented -- and a refusal is a measurement in this
// package rather than an absence of one. Requiring an OBJECT from every
// fixture would have made that fixture a permanent failure, which is how a
// gate gets an exemption list and then a stale one.
func TestMeasureEmissions_EveryFixtureContributes(t *testing.T) {
	c := qt.New(t)

	measured := schemacensus.MeasureEmissions()

	c.Assert(measured.DarkFixtures, qt.HasLen, 0, qt.Commentf(
		"these fixtures rendered no object on any declared cell"))
}

// TestMeasureEmissions_TheGuardsBlindSpotsAreWrittenDown is the half that keeps
// the guard from reporting over statements it cannot read.
//
// A statement shape the extractor does not classify contributes no object, so
// a duplicate inside it is invisible. Every such shape is listed here, and each
// one is either a statement that creates nothing or a fragment of one that
// does. A shape leaving this list is fine; a NEW one appearing means the
// renderers grew a statement nobody has decided about, and that decision
// belongs in review rather than in silence.
func TestMeasureEmissions_TheGuardsBlindSpotsAreWrittenDown(t *testing.T) {
	c := qt.New(t)

	measured := schemacensus.MeasureEmissions()

	c.Assert(measured.Unclassified, qt.DeepEquals, []string{
		// A PL/pgSQL or SQL function body carries its own semicolons, so the
		// split leaves these two tails behind. The CREATE FUNCTION that owns
		// them is the first fragment and is classified.
		"$$ LANGUAGE PLPGSQL",
		"$$ LANGUAGE SQL",
		// Alterations of an object something else created.
		"ALTER SEQUENCE \"PUBLIC\".\"ORDER_SEQ\"",
		"ALTER TABLE \"B\"",
		"ALTER TABLE \"T\"",
		"ALTER TABLE `B`",
		"ALTER TABLE `T`",
		// Documentation and privileges, which create no object.
		"COMMENT ON COLUMN",
		"COMMENT ON CONSTRAINT",
		"COMMENT ON INDEX",
		"COMMENT ON ROLE",
		"COMMENT ON SCHEMA",
		"COMMENT ON TABLE",
		// The third tail of a split function body.
		"END",
		// SQL Server's spelling of a comment.
		"EXEC SP_ADDEXTENDEDPROPERTY @NAME",
		"GRANT SELECT ON",
		"GRANT USAGE ON",
		// TimescaleDB converts a table that CREATE TABLE already made.
		"SELECT CREATE_HYPERTABLE('T', BY_RANGE('AT'),",
		"SELECT CREATE_HYPERTABLE('T', BY_RANGE('AT',",
	})
}

// TestEmissionsOf_ReadsTheShapesTheRenderersWrite pins the classification, one
// shape per row.
//
// The rows that matter are the ones a leading-clause reader gets wrong: a
// constraint written inside a CREATE TABLE body is the same physical object an
// ALTER TABLE ... ADD CONSTRAINT creates, and SQL Server puts both an index and
// a schema behind a conditional that begins the statement.
func TestEmissionsOf_ReadsTheShapesTheRenderersWrite(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
		want       []schemacensus.Emission
	}{
		{
			name:       "a table",
			statements: []string{`CREATE TABLE "nodes" ("id" BIGINT);`},
			want:       []schemacensus.Emission{{Kind: "table", Name: "nodes"}},
		},
		{
			name:       "a table and the constraint inside it",
			statements: []string{"CREATE TABLE `t` (`s` VARCHAR(32), CONSTRAINT `t_s_uq` UNIQUE (`s`));"},
			want: []schemacensus.Emission{
				{Kind: "table", Name: "t"},
				{Kind: "constraint", Name: "t.t_s_uq"},
			},
		},
		{
			name:       "a constraint added afterwards",
			statements: []string{`ALTER TABLE "nodes" ADD CONSTRAINT "fk_nodes_parent" FOREIGN KEY ("parent_id") REFERENCES "nodes"("id");`},
			want:       []schemacensus.Emission{{Kind: "constraint", Name: "nodes.fk_nodes_parent"}},
		},
		{
			name:       "an index behind SQL Server's existence check",
			statements: []string{"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_t_s' AND object_id = OBJECT_ID('t'))\nCREATE UNIQUE INDEX [idx_t_s] ON [t] ([s]);"},
			want:       []schemacensus.Emission{{Kind: "index", Name: "idx_t_s"}},
		},
		{
			name:       "a schema inside SQL Server's EXEC",
			statements: []string{"IF SCHEMA_ID('app') IS NULL\n    EXEC('CREATE SCHEMA [app]');"},
			want:       []schemacensus.Emission{{Kind: "schema", Name: "app"}},
		},
		{
			name:       "a comment header in front of the statement",
			statements: []string{"-- POSTGRES TABLE: nodes --\nCREATE TABLE \"nodes\" (\n  \"id\" BIGINT\n);\n\n"},
			want:       []schemacensus.Emission{{Kind: "table", Name: "nodes"}},
		},
		{
			name:       "a qualified name keeps its schema",
			statements: []string{`CREATE SEQUENCE "public"."order_seq" START 1;`},
			want:       []schemacensus.Emission{{Kind: "sequence", Name: "public.order_seq"}},
		},
		{
			name:       "SQLite's virtual table",
			statements: []string{`CREATE VIRTUAL TABLE "t" USING fts5(body);`},
			want:       []schemacensus.Emission{{Kind: "table", Name: "t"}},
		},
		{
			name:       "SQL Server's create-or-alter",
			statements: []string{"CREATE OR ALTER PROCEDURE [do_it]\nAS\nSELECT 1;"},
			want:       []schemacensus.Emission{{Kind: "procedure", Name: "do_it"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			emitted := schemacensus.EmissionsOf(test.statements)

			c.Assert(emitted.Objects, qt.DeepEquals, test.want)
			c.Assert(emitted.Unclassified, qt.HasLen, 0)
		})
	}
}

// TestEmissions_DuplicatesNamesWhatWasEmittedTwice is the control the corpus
// assertion cannot supply.
//
// The corpus reports zero duplicates, which is the answer a checker that
// cannot see one gives too. Planting the shape the invariant forbids is what
// separates them, and the shape is the one #2583 produced: a constraint
// written into the CREATE TABLE body and added again afterwards.
func TestEmissions_DuplicatesNamesWhatWasEmittedTwice(t *testing.T) {
	c := qt.New(t)

	emitted := schemacensus.EmissionsOf([]string{
		`CREATE TABLE "nodes" ("id" BIGINT, "parent_id" BIGINT, CONSTRAINT "fk_nodes_parent" FOREIGN KEY ("parent_id") REFERENCES "nodes"("id"));`,
		`ALTER TABLE "nodes" ADD CONSTRAINT "fk_nodes_parent" FOREIGN KEY ("parent_id") REFERENCES "nodes"("id");`,
	})

	c.Assert(emitted.Duplicates(), qt.DeepEquals,
		[]string{"constraint nodes.fk_nodes_parent emitted 2 times"})
}

// TestEmissions_TwoTablesMayShareAConstraintName is the discrimination the
// control above does not make.
//
// A checker keying a constraint by its bare name would report this pair as a
// duplicate, and PostgreSQL, MySQL and SQL Server all accept it: a constraint
// belongs to its table. Without this row, scoping the name could be dropped and
// every test here would still pass.
func TestEmissions_TwoTablesMayShareAConstraintName(t *testing.T) {
	c := qt.New(t)

	emitted := schemacensus.EmissionsOf([]string{
		`ALTER TABLE "a" ADD CONSTRAINT "fk_parent" FOREIGN KEY ("p") REFERENCES "a"("id");`,
		`ALTER TABLE "b" ADD CONSTRAINT "fk_parent" FOREIGN KEY ("p") REFERENCES "b"("id");`,
	})

	c.Assert(emitted.Duplicates(), qt.HasLen, 0)
	c.Assert(emitted.Objects, qt.HasLen, 2)
}

// TestEmissions_OneObjectSpelledTwoWaysIsOneObject pins the normalization.
//
// The corpus renders one declaration on ten dialects, so the same object
// arrives quoted three ways. A comparison holding the quoting would report a
// PostgreSQL duplicate and miss the MySQL one.
func TestEmissions_OneObjectSpelledTwoWaysIsOneObject(t *testing.T) {
	c := qt.New(t)

	emitted := schemacensus.EmissionsOf([]string{
		`CREATE TABLE "Nodes" ("id" BIGINT);`,
		"CREATE TABLE `nodes` (`id` BIGINT);",
		`CREATE TABLE [NODES] ([id] BIGINT);`,
	})

	c.Assert(emitted.Duplicates(), qt.DeepEquals, []string{"table nodes emitted 3 times"})
}
