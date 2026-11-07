package notnullname_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/notnullname"
)

// add is one ADD COLUMN statement on t, its NOT NULL named.
func add(column, name string) *ast.AlterTableNode {
	node := ast.NewColumn(column, "text").SetNotNull()
	node.NotNullConstraintName = name
	return alterT(&ast.AddColumnOperation{Column: node})
}

// drop is one DROP COLUMN statement on t.
func drop(column string) *ast.AlterTableNode {
	return alterT(&ast.DropColumnOperation{ColumnName: column})
}

// setNotNull is one statement on t that makes a column NOT NULL under the name.
func setNotNull(column, name string) *ast.AlterTableNode {
	return alterT(&ast.ModifyColumnOperation{Column: ast.NewColumn(column, "text").SetNotNull(), NotNullConstraintName: name})
}

// rename is one RENAME CONSTRAINT statement on t.
func rename(from, to string) *ast.AlterTableNode {
	return alterT(&ast.RenameConstraintOperation{From: from, To: to})
}

func alterT(operations ...ast.AlterOperation) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: "t", Operations: operations}
}

// holding is a column of t that holds the name.
func holding(column, name string) schemamodel.Field {
	return schemamodel.Field{Name: column, NotNullConstraintName: name}
}

// removed is a table diff in which the columns go away.
func removed(columns ...schemamodel.Field) difftypes.TableDiff {
	return difftypes.TableDiff{TableName: "t", ColumnsRemoved: columns}
}

// renaming adds to table a column whose constraint moves from current to desired.
func renaming(table difftypes.TableDiff, current, desired string) difftypes.TableDiff {
	table.ColumnsModified = append(table.ColumnsModified, difftypes.ColumnDiff{
		ColumnName:                  "kept_" + current,
		NotNullConstraintNameChange: &difftypes.NotNullConstraintNameChange{Current: current, Desired: desired},
	})
	return table
}

// statements lists what nodes do, one line per statement and operation.
func statements(nodes []ast.Node) []string {
	lines := make([]string, 0, len(nodes))
	for _, node := range nodes {
		alter, ok := node.(*ast.AlterTableNode)
		if !ok {
			lines = append(lines, "other")
			continue
		}
		line := alter.Name + ":"
		for _, operation := range alter.Operations {
			switch op := operation.(type) {
			case *ast.AddColumnOperation:
				line += " add " + op.Column.Name + " as " + op.Column.NotNullConstraintName
			case *ast.DropColumnOperation:
				line += " drop " + op.ColumnName
			case *ast.RenameConstraintOperation:
				line += " rename " + op.From + " to " + op.To
			case *ast.ModifyColumnOperation:
				line += " set " + op.Column.Name + " as " + op.NotNullConstraintName
			}
		}
		lines = append(lines, line)
	}
	return lines
}

// Each row is a plan whose statements take a name another column holds at that
// point, and the moves that make it run.
func TestFree_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		table difftypes.TableDiff
		nodes []ast.Node
		want  []string
	}{
		{
			name:  "an added column takes the name of a column the plan removes later",
			table: removed(holding("full_name", "n")),
			nodes: []ast.Node{add("display_name", "n"), drop("full_name")},
			want:  []string{"t: rename n to n__ptah_old", "t: add display_name as n", "t: drop full_name"},
		},
		{
			name:  "a statement of another kind between the two keeps its place",
			table: removed(holding("full_name", "n")),
			nodes: []ast.Node{add("display_name", "n"), ast.NewComment("kept"), drop("full_name")},
			want:  []string{"t: rename n to n__ptah_old", "t: add display_name as n", "other", "t: drop full_name"},
		},
		{
			name:  "a column made NOT NULL takes the name of a column the plan removes later",
			table: removed(holding("b", "n")),
			nodes: []ast.Node{setNotNull("a", "n"), drop("b")},
			want:  []string{"t: rename n to n__ptah_old", "t: set a as n", "t: drop b"},
		},
		{
			name:  "a column made NOT NULL takes the name a renamed constraint holds until later",
			table: renaming(difftypes.TableDiff{TableName: "t"}, "n", "m"),
			nodes: []ast.Node{setNotNull("a", "n"), rename("n", "m")},
			want:  []string{"t: rename n to n__ptah_old", "t: set a as n", "t: rename n__ptah_old to m"},
		},
		{
			name:  "a renamed constraint takes the name of a column the plan removes later",
			table: renaming(removed(holding("a_new", "m")), "n", "m"),
			nodes: []ast.Node{rename("n", "m"), drop("a_new")},
			want:  []string{"t: rename m to m__ptah_old", "t: rename n to m", "t: drop a_new"},
		},
		{
			name:  "an added column takes the name a renamed constraint holds until later",
			table: renaming(difftypes.TableDiff{TableName: "t"}, "n", "m"),
			nodes: []ast.Node{add("b", "n"), rename("n", "m")},
			want:  []string{"t: rename n to n__ptah_old", "t: add b as n", "t: rename n__ptah_old to m"},
		},
		{
			name:  "two constraints trade names",
			table: renaming(renaming(difftypes.TableDiff{TableName: "t"}, "n", "m"), "m", "n"),
			nodes: []ast.Node{rename("n", "m"), rename("m", "n")},
			want:  []string{"t: rename m to m__ptah_old", "t: rename n to m", "t: rename m__ptah_old to n"},
		},
		{
			name:  "two added columns each take the name of a removed one",
			table: removed(holding("a", "n"), holding("b", "m")),
			nodes: []ast.Node{add("c", "n"), add("d", "m"), drop("a"), drop("b")},
			want: []string{
				"t: rename n to n__ptah_old", "t: add c as n",
				"t: rename m to m__ptah_old", "t: add d as m",
				"t: drop a", "t: drop b",
			},
		},
		{
			name:  "one statement that adds the column ahead of the drop still frees the name",
			table: removed(holding("full_name", "n")),
			nodes: []ast.Node{alterT(add("display_name", "n").Operations[0], drop("full_name").Operations[0])},
			want:  []string{"t: rename n to n__ptah_old", "t: add display_name as n drop full_name"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := notnullname.Free(test.nodes, &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{test.table}})

			c.Assert(statements(got), qt.DeepEquals, test.want)
		})
	}
}

// Each row is a plan with no statement that takes a name still held, which comes
// back as it went in.
func TestFree_LeavesAPlanThatRuns_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		table difftypes.TableDiff
		nodes []ast.Node
		want  []string
	}{
		{
			name:  "the holder's drop runs first",
			table: removed(holding("full_name", "n")),
			nodes: []ast.Node{drop("full_name"), add("display_name", "n")},
			want:  []string{"t: drop full_name", "t: add display_name as n"},
		},
		{
			name:  "one statement drops the holder and then adds the column",
			table: removed(holding("full_name", "n")),
			nodes: []ast.Node{alterT(drop("full_name").Operations[0], add("display_name", "n").Operations[0])},
			want:  []string{"t: drop full_name add display_name as n"},
		},
		{
			name:  "one statement drops the holder and then makes a column NOT NULL under its name",
			table: removed(holding("b", "n")),
			nodes: []ast.Node{alterT(drop("b").Operations[0], setNotNull("a", "n").Operations[0])},
			want:  []string{"t: drop b set a as n"},
		},
		{
			name:  "a column made NOT NULL leaves its name to the server",
			table: removed(holding("b", "n")),
			nodes: []ast.Node{setNotNull("a", ""), drop("b")},
			want:  []string{"t: set a as ", "t: drop b"},
		},
		{
			name:  "the holder's rename runs first",
			table: renaming(difftypes.TableDiff{TableName: "t"}, "n", "m"),
			nodes: []ast.Node{rename("n", "m"), add("b", "n")},
			want:  []string{"t: rename n to m", "t: add b as n"},
		},
		{
			name:  "the added column takes a name of its own",
			table: removed(holding("full_name", "n")),
			nodes: []ast.Node{add("display_name", "m"), drop("full_name")},
			want:  []string{"t: add display_name as m", "t: drop full_name"},
		},
		{
			name:  "the added column leaves its name to the server",
			table: removed(holding("full_name", "n")),
			nodes: []ast.Node{add("display_name", ""), drop("full_name")},
			want:  []string{"t: add display_name as ", "t: drop full_name"},
		},
		{
			name:  "the removed column reports no name",
			table: removed(holding("full_name", "")),
			nodes: []ast.Node{add("display_name", "n"), drop("full_name")},
			want:  []string{"t: add display_name as n", "t: drop full_name"},
		},
		{
			name:  "another table holds the name, and a name is the table's own",
			table: difftypes.TableDiff{TableName: "other", ColumnsRemoved: []schemamodel.Field{{Name: "a", NotNullConstraintName: "n"}}},
			nodes: []ast.Node{add("display_name", "n")},
			want:  []string{"t: add display_name as n"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := notnullname.Free(test.nodes, &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{test.table}})

			c.Assert(statements(got), qt.DeepEquals, test.want)
		})
	}
}

// A diff that names nothing changes nothing, and a nil one is no diff.
func TestFree_NoDiff_FailurePath(t *testing.T) {
	c := qt.New(t)
	nodes := []ast.Node{add("display_name", "n")}

	c.Assert(statements(notnullname.Free(nodes, nil)), qt.DeepEquals, []string{"t: add display_name as n"})
	c.Assert(statements(notnullname.Unfree(nodes, nil)), qt.DeepEquals, []string{"t: add display_name as n"})
}

// Each row is a plan [notnullname.Free] changed, which Unfree hands back as the
// planner wrote it, and a rename the author asked for stays.
func TestUnfree_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		table difftypes.TableDiff
		nodes []ast.Node
		want  []string
	}{
		{
			name:  "a freed name",
			table: removed(holding("full_name", "n")),
			nodes: []ast.Node{add("display_name", "n"), drop("full_name")},
			want:  []string{"t: add display_name as n", "t: drop full_name"},
		},
		{
			name:  "a rename that started from an interim name",
			table: renaming(difftypes.TableDiff{TableName: "t"}, "n", "m"),
			nodes: []ast.Node{add("b", "n"), rename("n", "m")},
			want:  []string{"t: add b as n", "t: rename n to m"},
		},
		{
			name:  "two constraints that trade names",
			table: renaming(renaming(difftypes.TableDiff{TableName: "t"}, "n", "m"), "m", "n"),
			nodes: []ast.Node{rename("n", "m"), rename("m", "n")},
			want:  []string{"t: rename n to m", "t: rename m to n"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{test.table}}

			got := notnullname.Unfree(notnullname.Free(test.nodes, diff), diff)

			c.Assert(statements(got), qt.DeepEquals, test.want)
		})
	}
}
