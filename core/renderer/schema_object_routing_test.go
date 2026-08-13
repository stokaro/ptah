package renderer_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

// routingDialects is every canonical engine `--dialect` accepts. The grid below
// is this list crossed with routedObjectRows, and every cell has to carry an
// answer.
var routingDialects = []string{
	platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	platform.ClickHouse, platform.MySQL, platform.MariaDB, platform.SQLServer, platform.SQLite,
}

// routedObjectRows is one row per declared object kind, with the name the
// fixture gives that object. Each name is unique across the fixture, so a
// mention of it in the rendered SQL can only be about that object.
var routedObjectRows = []struct {
	kind   string
	object string
}{
	{kind: "sequence", object: "seq_probe"},
	{kind: "domain", object: "domain_probe"},
	{kind: "role", object: "role_probe"},
	{kind: "table", object: "table_probe"},
	{kind: "view", object: "view_probe"},
	{kind: "function", object: "func_probe"},
	{kind: "trigger", object: "trigger_probe"},
	{kind: "grant", object: "grant_probe"},
}

// routedObjectSchema declares one object of every kind in routedObjectRows.
//
// Materialized views are deliberately absent. Four of these renderers refuse one
// with an ERROR rather than a comment, on purpose (stokaro/ptah#931 item 3), and
// an error aborts the whole render -- so including one would decide this grid
// before any other kind was reached. That refusal is already pinned by
// TestRender_MaterializedViewIsRefusedWhereApplyRefusesIt.
//
// The grant names its own role rather than role_probe so that the grant row and
// the role row cannot be satisfied by the same mention.
func routedObjectSchema() *goschema.Database {
	start := int64(1000)
	return &goschema.Database{
		Sequences: []goschema.Sequence{{Name: "seq_probe", AsType: "bigint", Start: &start}},
		Domains:   []goschema.Domain{{Name: "domain_probe", BaseType: "TEXT"}},
		Roles:     []goschema.Role{{Name: "role_probe", Login: true, Inherit: true}},
		Tables:    []goschema.Table{{StructName: "T", Name: "table_probe"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", Name: "touched", Type: "TIMESTAMP", Nullable: true},
		},
		Views:     []goschema.View{{StructName: "V", Name: "view_probe", Body: "SELECT id FROM table_probe"}},
		Functions: []goschema.Function{{Name: "func_probe", Returns: "integer", Language: "sql", Body: "SELECT 1;"}},
		Triggers: []goschema.Trigger{{
			StructName: "TR", Name: "trigger_probe", Table: "table_probe",
			Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: "SELECT 1",
		}},
		Grants: []goschema.Grant{{
			StructName: "G", Role: "grant_probe", Privileges: []string{"SELECT"}, OnTable: "table_probe",
		}},
	}
}

// routedObjectAnswer classifies what one rendered schema says about one object:
//
//	"ddl"    a statement a server would execute names it
//	"named"  only a comment names it -- the target declines the object and says so
//	"silent" nothing in the output mentions it at all
//
// The comment/statement split is the whole measurement. Every renderer here
// writes its refusal as a comment that repeats the object's DDL keywords, so a
// plain substring search over the output cannot tell the two apart, and
// "silent" -- the defect -- looks exactly like a target that refused.
func routedObjectAnswer(sql, object string) string {
	executable, commented := false, false
	for line := range strings.SplitSeq(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.Contains(trimmed, object) {
			continue
		}
		if strings.HasPrefix(trimmed, "--") {
			commented = true
			continue
		}
		executable = true
	}
	return map[[2]bool]string{
		{true, false}:  "ddl",
		{true, true}:   "ddl",
		{false, true}:  "named",
		{false, false}: "silent",
	}[[2]bool{executable, commented}]
}

// routedObjectCell is one (dialect, object kind) cell of the grid.
type routedObjectCell struct {
	dialect string
	kind    string
	object  string
	answer  string
}

// routedObjectGrid renders the fixture once per dialect and classifies every
// declared object in it.
func routedObjectGrid(c *qt.C) []routedObjectCell {
	c.Helper()

	cells := make([]routedObjectCell, 0, len(routingDialects)*len(routedObjectRows))
	for _, dialect := range routingDialects {
		statements, err := renderer.GetOrderedCreateStatements(routedObjectSchema(), dialect)
		c.Assert(err, qt.IsNil, qt.Commentf("render failed for %s", dialect))
		sql := strings.Join(statements, "\n")
		for _, row := range routedObjectRows {
			cells = append(cells, routedObjectCell{
				dialect: dialect,
				kind:    row.kind,
				object:  row.object,
				answer:  routedObjectAnswer(sql, row.object),
			})
		}
	}
	return cells
}

// cellsAnswering returns one printable line per cell whose answer is the one
// given, so a failure names the objects and the dialects rather than a count.
func cellsAnswering(cells []routedObjectCell, answer string) []string {
	matching := slices.DeleteFunc(slices.Clone(cells), func(cell routedObjectCell) bool {
		return cell.answer != answer
	})
	lines := make([]string, 0, len(matching))
	for _, cell := range matching {
		lines = append(lines, fmt.Sprintf("%-12s %-9s %s", cell.dialect, cell.kind, cell.object))
	}
	return lines
}

// TestRender_NoDialectLosesADeclaredObject is the completion criterion for
// stokaro/ptah#929 item 5: every object a schema declares reaches its target's
// renderer, and the renderer either emits a statement for it or names it.
// Nothing disappears.
//
// Measured on the state this test was written against, one fixture declaring a
// sequence, a domain, a role, a table, a view and a function rendered offline
// for every dialect spelling `--dialect` lists: fifteen objects were SILENTLY
// absent at exit 0 -- the domain on clickhouse; the domain, role and function on
// mysql and on mariadb; and the sequence, domain, role and function on both
// sqlserver and sqlite. The cause was not the renderers, which had an arm for
// each of those kinds all along. It was the converter deleting the node first,
// gated on a list of dialect names, so there was nothing left for a renderer to
// report.
//
// The assertion is on the list of losing cells rather than on a count, so a
// regression prints which object vanished on which engine.
func TestRender_NoDialectLosesADeclaredObject(t *testing.T) {
	c := qt.New(t)

	cells := routedObjectGrid(c)

	// Control: the grid really covers every dialect and every kind. A fixture or
	// a classifier that produced no cells would satisfy the assertion below while
	// measuring nothing.
	c.Assert(cells, qt.HasLen, len(routingDialects)*len(routedObjectRows))

	silent := cellsAnswering(cells, "silent")
	c.Assert(silent, qt.HasLen, 0,
		qt.Commentf("%d of %d declared objects are absent with no diagnostic:\n%s",
			len(silent), len(cells), strings.Join(silent, "\n")))
}

// TestRender_TheRoutingGridDistinguishesItsAnswers is the control for the test
// above.
//
// "No cell is silent" is satisfied by a classifier that never returns "silent",
// and by a renderer that answered every kind on every engine with a comment. The
// rows here pin both edges: some cells must be executable DDL and some must be a
// named refusal, and the PostgreSQL row must be all DDL, so the grid cannot pass
// by refusing everything.
func TestRender_TheRoutingGridDistinguishesItsAnswers(t *testing.T) {
	c := qt.New(t)

	cells := routedObjectGrid(c)

	tests := []struct {
		name  string
		check func(*qt.C)
	}{{
		name: "postgres emits every kind",
		check: func(c *qt.C) {
			c.Assert(cellsAnswering(dialectCells(cells, platform.Postgres), "ddl"),
				qt.HasLen, len(routedObjectRows))
		},
	}, {
		name: "some cells are named refusals",
		check: func(c *qt.C) {
			c.Assert(len(cellsAnswering(cells, "named")) > 0, qt.IsTrue,
				qt.Commentf("no cell is a named refusal; the classifier reads everything as DDL"))
		},
	}, {
		name: "the table and the view are executable everywhere",
		check: func(c *qt.C) {
			for _, kind := range []string{"table", "view"} {
				c.Assert(cellsAnswering(kindCells(cells, kind), "ddl"), qt.HasLen, len(routingDialects),
					qt.Commentf("%s is not executable on every dialect", kind))
			}
		},
	}, {
		name: "sqlite refuses the four kinds it has no object for",
		check: func(c *qt.C) {
			c.Assert(cellsAnswering(dialectCells(cells, platform.SQLite), "named"), qt.DeepEquals, []string{
				"sqlite       sequence  seq_probe",
				"sqlite       domain    domain_probe",
				"sqlite       role      role_probe",
				"sqlite       function  func_probe",
				"sqlite       grant     grant_probe",
			})
		},
	}}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) { test.check(c) })
	}
}

// dialectCells and kindCells slice the grid along one axis.
func dialectCells(cells []routedObjectCell, dialect string) []routedObjectCell {
	return slices.DeleteFunc(slices.Clone(cells), func(cell routedObjectCell) bool {
		return cell.dialect != dialect
	})
}

func kindCells(cells []routedObjectCell, kind string) []routedObjectCell {
	return slices.DeleteFunc(slices.Clone(cells), func(cell routedObjectCell) bool {
		return cell.kind != kind
	})
}

// TestRender_SQLServerNamesTheSequenceWithoutClaimingItHasNone pins the answer
// to the one cell where naming the skip and telling the truth pull apart.
//
// SQL Server has had CREATE SEQUENCE since 2012. The renderer's refusal used to
// read "CREATE SEQUENCE ... is not supported", so routing the node there would
// have replaced a silent omission with a false claim about the engine -- and
// that is exactly why the converter withheld it, which is how the omission
// survived. The sentence now names Ptah's generator, which is true whatever the
// engine can do: capability.SQLServer2022 leaves Sequences off because Ptah has
// no SQL Server sequence reader and no SQL Server sequence planner.
//
// Both halves are asserted. Naming the object must not become emitting a
// CREATE SEQUENCE that nothing reads back or plans again.
func TestRender_SQLServerNamesTheSequenceWithoutClaimingItHasNone(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(routedObjectSchema(), platform.SQLServer)
	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")

	c.Assert(sql, qt.Contains,
		`-- SQLSERVER: CREATE SEQUENCE "seq_probe" is not generated for this target; skipped.`)
	c.Assert(sql, qt.Not(qt.Contains), `-- SQLSERVER: CREATE SEQUENCE "seq_probe" is not supported`)
	c.Assert(routedObjectAnswer(sql, "seq_probe"), qt.Equals, "named")
}

// TestRender_MySQLFamilyNamesRolesInsteadOfAbortingTheRender pins that a role
// declared for a MySQL-family target is reported, not fatal.
//
// The MySQL renderer answered a role node with an ERROR, and an error aborts the
// render of the WHOLE schema. That was invisible only because the converter
// deleted every role before the renderer could see one; once every declared
// object is routed, the same code turns a schema that used to render into a
// command that produces no SQL at all. A refusal that removes the other
// statements is a worse answer than the omission it replaces.
func TestRender_MySQLFamilyNamesRolesInsteadOfAbortingTheRender(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		c.Run(dialect, func(c *qt.C) {
			statements, err := renderer.GetOrderedCreateStatements(routedObjectSchema(), dialect)

			c.Assert(err, qt.IsNil)
			sql := strings.Join(statements, "\n")
			c.Assert(sql, qt.Contains,
				fmt.Sprintf("-- %s: role role_probe is not generated for this target; skipped.",
					strings.ToUpper(dialect)))
			// The rest of the schema still renders, which is what an error here
			// used to destroy.
			c.Assert(legacyRenderedSQL(sql), qt.Contains, "CREATE TABLE table_probe")
		})
	}
}
