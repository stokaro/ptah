package oracle_test

import (
	"bytes"
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	oracleplanner "ptah.run/internal/planner/dialects/oracle"
	"ptah.run/internal/rundiag"
)

// TestPlanner_OracleCarriesTheIdentityAColumnHolds plans a change to the
// identity of column ID and reads the MODIFY it writes. The operation carries
// the generation the column holds, which the renderer reads to tell a changed
// generation from an identity given to a column that has none.
func TestPlanner_OracleCarriesTheIdentityAColumnHolds(t *testing.T) {
	tests := []struct {
		name, change, previous string
	}{
		{name: "a changed generation", change: "BY_DEFAULT -> ALWAYS", previous: "BY_DEFAULT"},
		{name: "a dropped identity", change: "ALWAYS -> ", previous: "ALWAYS"},
		{name: "an identity given to a plain column", change: " -> BY_DEFAULT", previous: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName: "T",
				ColumnsModified: []difftypes.ColumnDiff{{
					ColumnName: "ID",
					Changes:    map[string]string{"identity": test.change},
					Desired:    schemamodel.Field{Name: "ID", StructName: "T", Type: "NUMBER(10)", IdentityGeneration: "ALWAYS"},
				}},
			}}}

			nodes, err := oracleplanner.New().GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)

			c.Assert(err, qt.IsNil)
			modifies := modifyColumnOperations(nodes)
			c.Assert(modifies, qt.HasLen, 1)
			c.Assert(modifies[0].PreviousIdentity, qt.Equals, test.previous)
			c.Assert(modifies[0].Changed, qt.Equals, ast.ColumnProperties{Identity: true})
		})
	}
}

// columnChange is a diff that makes changes to column T.ID, declared as field.
func columnChange(field schemamodel.Field, changes map[string]string) *difftypes.SchemaDiff {
	field.Name, field.StructName, field.Type = "ID", "T", "NUMBER(10)"
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:       "T",
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "ID", Changes: changes, Desired: field}},
	}}}
}

// generationChange is a diff that changes the identity of column T.ID from
// BY DEFAULT to ALWAYS, which field declares.
func generationChange(field schemamodel.Field) *difftypes.SchemaDiff {
	return columnChange(field, map[string]string{"identity": "BY_DEFAULT -> ALWAYS"})
}

// plainComments is the text of each comment in nodes that a plan made in the
// exact statement style keeps.
func plainComments(nodes []ast.Node) []string {
	var texts []string
	for _, node := range nodes {
		if comment, isComment := node.(*ast.CommentNode); isComment && !comment.Descriptive {
			texts = append(texts, comment.Text)
		}
	}
	return texts
}

// planWarnings plans diff on Oracle in a run of its own, and returns the plan,
// what the run wrote to its error stream by the time the plan returned, and
// what it wrote by the end of the run.
func planWarnings(c *qt.C, diff *difftypes.SchemaDiff) (nodes []ast.Node, written, flushed string) {
	c.Helper()
	var stream bytes.Buffer
	report := rundiag.New(rundiag.Options{Stream: &stream, Listing: rundiag.ListedAtDebugLevel})
	nodes, err := oracleplanner.New().GenerateMigrationAST(rundiag.With(context.Background(), report), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	written = stream.String()
	report.Flush()
	return nodes, written, stream.String()
}

// TestPlanner_OracleLeavesOutTheStartOfAChangedGeneration plans a changed
// generation whose declaration gives a start. The MODIFY carries the other
// options, and the run warns once, by the time the plan returns, with the same
// text as the comment the plan writes above the statement. Oracle restarts the
// counter at a start given to a column that holds rows.
func TestPlanner_OracleLeavesOutTheStartOfAChangedGeneration(t *testing.T) {
	const warning = "column T.ID declares START WITH 50, and the plan leaves it out: " +
		"a start given to an identity column that holds rows restarts its counter there, " +
		"and the next insert fails with ORA-00001 where a row holds the value. " +
		"Declare START WITH LIMIT VALUE, or run ALTER TABLE T MODIFY (ID GENERATED ALWAYS AS IDENTITY (START WITH 50)) " +
		"by hand once 50 is known to be free"
	tests := []struct {
		name                string
		field               schemamodel.Field
		wantStart, wantOpts string
	}{
		{
			name:     "an option list",
			field:    schemamodel.Field{IdentityGeneration: "ALWAYS", IdentityOptions: "START WITH 50 INCREMENT BY 5 CACHE 10"},
			wantOpts: "INCREMENT BY 5 CACHE 10",
		},
		{
			name:  "a start of its own",
			field: schemamodel.Field{IdentityGeneration: "ALWAYS", IdentityStart: "50", IdentityIncrement: "5"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, written, flushed := planWarnings(c, generationChange(test.field))

			modifies := modifyColumnOperations(nodes)
			c.Assert(modifies, qt.HasLen, 1)
			c.Assert(modifies[0].Column.IdentityStart, qt.Equals, test.wantStart)
			c.Assert(modifies[0].Column.IdentityOptions, qt.Equals, test.wantOpts)
			c.Assert(plainComments(nodes), qt.DeepEquals, []string{warning})
			c.Assert(written, qt.Equals, "warning: "+warning+"\n")
			c.Assert(flushed, qt.Equals, written)
		})
	}
}

// TestPlanner_OracleKeepsAStartThatMovesNoCounterBack is the control: a
// changed generation with no start, or with START WITH LIMIT VALUE, which
// starts after the highest value held, and a change other than the identity
// of a column that declares a start, keep the options, write no comment and
// raise no warning.
func TestPlanner_OracleKeepsAStartThatMovesNoCounterBack(t *testing.T) {
	tests := []struct {
		name    string
		field   schemamodel.Field
		changes map[string]string
	}{
		{
			name:    "no start",
			field:   schemamodel.Field{IdentityGeneration: "ALWAYS", IdentityIncrement: "5"},
			changes: map[string]string{"identity": "BY_DEFAULT -> ALWAYS"},
		},
		{
			name:    "START WITH LIMIT VALUE",
			field:   schemamodel.Field{IdentityGeneration: "ALWAYS", IdentityOptions: "START WITH LIMIT VALUE"},
			changes: map[string]string{"identity": "BY_DEFAULT -> ALWAYS"},
		},
		{
			name:    "a changed type",
			field:   schemamodel.Field{IdentityGeneration: "ALWAYS", IdentityStart: "50"},
			changes: map[string]string{"type": "NUMBER(5) -> NUMBER(10)"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, _, flushed := planWarnings(c, columnChange(test.field, test.changes))

			modifies := modifyColumnOperations(nodes)
			c.Assert(modifies, qt.HasLen, 1)
			c.Assert(modifies[0].Column.IdentityStart, qt.Equals, test.field.IdentityStart)
			c.Assert(modifies[0].Column.IdentityIncrement, qt.Equals, test.field.IdentityIncrement)
			c.Assert(modifies[0].Column.IdentityOptions, qt.Equals, test.field.IdentityOptions)
			c.Assert(plainComments(nodes), qt.IsNil)
			c.Assert(flushed, qt.Equals, "")
		})
	}
}

// TestPlanner_OracleSummarizesTheStartsItLeavesOut plans changed generations
// of two columns that each declare a start. The run writes one warning that
// names each column and its start, and each MODIFY keeps its own comment.
func TestPlanner_OracleSummarizesTheStartsItLeavesOut(t *testing.T) {
	c := qt.New(t)
	diff := generationChange(schemamodel.Field{IdentityGeneration: "ALWAYS", IdentityStart: "50"})
	second := diff.TablesModified[0].ColumnsModified[0]
	second.ColumnName, second.Desired.Name, second.Desired.IdentityStart = "ID2", "ID2", "7"
	diff.TablesModified[0].ColumnsModified = append(diff.TablesModified[0].ColumnsModified, second)

	nodes, written, _ := planWarnings(c, diff)

	c.Assert(plainComments(nodes), qt.HasLen, 2)
	c.Assert(written, qt.Equals, "warning: 2 identity starts are left out of the plan: T.ID (START WITH 50), T.ID2 (START WITH 7). "+
		"A start given to an identity column that holds rows restarts its counter there, "+
		"and the next insert fails with ORA-00001 where a row holds the value. "+
		"Declare START WITH LIMIT VALUE, or set each start by hand with "+
		"ALTER TABLE ... MODIFY (... GENERATED ... AS IDENTITY (START WITH n)) once the value is known to be free; PTAH_LOG_LEVEL=debug lists them\n")
}
