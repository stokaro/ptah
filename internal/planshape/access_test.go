package planshape_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planshape"
)

// planOf says what each statement does, one line each, so a row can pin the order of the
// statements and of the operations inside each.
func planOf(nodes []ast.Node) []string {
	lines := make([]string, 0, len(nodes))
	for _, node := range nodes {
		switch n := node.(type) {
		case *ast.AlterTableNode:
			written := n.Operations
			if n.OneStatement != nil {
				written = n.OneStatement
			}
			operations := make([]string, 0, len(written))
			for _, operation := range written {
				operations = append(operations, operationOf(operation))
			}
			lines = append(lines, n.Name+": "+strings.Join(operations, ", "))
		case *ast.CreateTableNode:
			lines = append(lines, "create "+n.Name)
		case *ast.DropTableNode:
			lines = append(lines, "drop table "+strings.Join(n.TableNames(), " "))
		case *ast.IndexNode:
			lines = append(lines, "index "+n.Name+" on "+n.Table)
		case *ast.DropIndexNode:
			lines = append(lines, "drop index "+n.Name+" on "+n.Table)
		default:
			lines = append(lines, fmt.Sprintf("%T", node))
		}
	}
	return lines
}

func operationOf(operation ast.AlterOperation) string {
	switch op := operation.(type) {
	case *ast.AddColumnOperation:
		return "add " + op.Column.Name
	case *ast.DropColumnOperation:
		return "drop " + op.ColumnName
	case *ast.ModifyColumnOperation:
		return "modify " + op.Column.Name
	case *ast.AddConstraintOperation:
		return "add constraint " + op.Constraint.Name
	case *ast.DropConstraintOperation:
		return "drop constraint " + op.ConstraintName
	case *ast.ReplaceIndexOperation:
		return "replace index " + op.Index.Name
	default:
		return fmt.Sprintf("%T", operation)
	}
}

// changes is a diff of the tables a plan changes.
func changes(tables ...difftypes.TableDiff) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: tables}
}

// columnWith is a nullable column set adds to.
func columnWith(name string, set func(*ast.ColumnNode)) *ast.ColumnNode {
	column := nullable(name, "int")
	set(column)
	return column
}

// heldColumns is a changed table holding columns in that order, with what a comparison
// read off it.
func heldColumns(table string, columns []string, constraints ...catalog.Constraint) difftypes.TableDiff {
	held := make([]catalog.Column, 0, len(columns))
	for _, column := range columns {
		held = append(held, catalog.Column{Name: column})
	}
	observation := observedTable(table, constraints)
	observation.Table.Columns = held
	return difftypes.TableDiff{TableName: table, Current: observation}
}

// indexOver is a table holding an index over x, read in one of the ways a reader says it.
func indexOver(index catalog.Index) *difftypes.SchemaDiff {
	index.Name, index.TableName = "t_i", "t"
	return changes(changedTable("t", nil, index))
}

// addedColumn is the operation that adds column.
func addedColumn(column *ast.ColumnNode) ast.AlterOperation {
	return &ast.AddColumnOperation{Column: column}
}

// generatedFrom sets the expression a generated column reads.
func generatedFrom(expression string) func(*ast.ColumnNode) {
	return func(column *ast.ColumnNode) { column.GeneratedExpression = expression }
}

// primaryKey is a named primary key over column.
func primaryKey(name, column string) *ast.ConstraintNode {
	return &ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Name: name, Columns: []string{column}}
}

// TestApply_WritesAStatementInTheBinarysOrder_HappyPath pins where each operation goes
// inside one ALTER TABLE when nothing holds it back: the primary key the statement drops,
// the checks, the columns by position, the primary key it adds, the unique constraints,
// then the foreign keys.
func TestApply_WritesAStatementInTheBinarysOrder_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		ops  []ast.AlterOperation
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "every kind of operation",
			ops: []ast.AlterOperation{
				addKey(foreignKey("fk_new", "f", "p")), dropConstraint("fk_old"), addKey(uniqueKey("uq_new", "u")), dropConstraint("ck_old"),
				dropConstraint("pk_old"), addKey(primaryKey("pk_new", "j")), addKey(checkConstraint("ck_new", "h > 0")), modifyColumn("m", "bigint"),
			},
			diff: changes(changedTable("t", []catalog.Constraint{
				observedForeignKey("t", "fk_old", "g", "q", "id"), observedKey("t", "ck_old", "CHECK", "e"), observedKey("t", "pk_old", "PRIMARY KEY", "k"),
			})),
			want: "t: drop constraint pk_old, drop constraint ck_old, add constraint ck_new, modify m, " +
				"add constraint pk_new, add constraint uq_new, drop constraint fk_old, add constraint fk_new",
		},
		{
			name: "columns by where the table holds them",
			ops:  []ast.AlterOperation{modifyColumn("x", "bigint"), addColumn("c"), dropColumn("d"), modifyColumn("m", "bigint")},
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName: "t", Current: heldColumns("t", []string{"keep", "m"}).Current,
				ColumnsRemoved: difftypes.ColumnChanges{{Name: "d"}}, ColumnsAdded: difftypes.ColumnChanges{{Name: "c"}},
			}}},
			want: "t: modify m, drop d, add c, modify x",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(platform.Postgres, []ast.Node{alter("t", test.ops...)}, test.diff)

			c.Assert(planOf(got), qt.DeepEquals, []string{test.want})
		})
	}
}

// TestApply_KeepsThePlannersOrderOverWhatTwoStepsShare_HappyPath is a pair of steps the
// binary's order would swap and that share something a server reads. Each row names the
// thing in its title.
func TestApply_KeepsThePlannersOrderOverWhatTwoStepsShare_HappyPath(t *testing.T) {
	aKeyOn := func(table, name, column string) []catalog.Constraint {
		return []catalog.Constraint{observedForeignKey(table, name, column, "p", "id")}
	}
	tests := []struct {
		name    string
		dialect string
		nodes   []ast.Node
		diff    *difftypes.SchemaDiff
		want    []string
	}{
		{
			name: "a check dropped behind its column on PostgreSQL", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn("n")), alter("t", dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{observedKey("t", "ck", "CHECK", "n")})),
			want:  []string{"t: drop n, drop constraint ck"},
		},
		{
			name: "a check beside its column on MySQL, which reads a statement as a whole", dialect: platform.MySQL,
			nodes: []ast.Node{alter("t", dropColumn("n")), alter("t", dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{observedKey("t", "ck", "CHECK", "n")})),
			want:  []string{"t: drop constraint ck, drop n"},
		},
		{
			name: "a check over a column the statement changes", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", modifyColumn("x", "bigint")), alter("t", addKey(checkConstraint("ck", "x > 0")))},
			want:  []string{"t: modify x, add constraint ck"},
		},
		{
			name: "a check over a column the statement adds, which PostgreSQL adds first", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", addColumn("x")), alter("t", addKey(checkConstraint("ck", "x > 0")))},
			want:  []string{"t: add constraint ck, add x"},
		},
		{
			name: "a generated column reading a column of the same name elsewhere", dialect: platform.Postgres,
			nodes: []ast.Node{alter("a.t", addedColumn(columnWith("g", generatedFrom("x * 2")))), alter("b.t", addColumn("x"))},
			want:  []string{"a.t: add g", "b.t: add x"},
		},
		{
			name: "a generated column with another change between it and what it reads", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", addedColumn(columnWith("g", generatedFrom("x * 2"))), dropColumn("y"), addColumn("x"))},
			want:  []string{"t: add g, drop y, add x"},
		},
		{
			name: "a generated column reading a column under another case", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", addedColumn(columnWith("g", generatedFrom("x * 2"))), addColumn("X"))},
			want:  []string{"t: add X, add g"},
		},
		{
			name: "a foreign key dropped ahead of its own column stays in its table's statement", dialect: platform.Postgres,
			nodes: []ast.Node{alter("z", dropConstraint("z_fk"), dropColumn("p_id")), alter("b", addColumn("x"))},
			diff:  changes(changedTable("z", aKeyOn("z", "z_fk", "p_id"))),
			want:  []string{"b: add x", "z: drop constraint z_fk, drop p_id"},
		},
		{
			name: "a unique constraint another table's key needs gone stays in its statement", dialect: platform.Postgres,
			nodes: []ast.Node{
				alter("b", addColumn("x")),
				alter("z", dropConstraint("z_u"), dropColumn("y")),
				alter("a", addKey(foreignKeyTo("a_z", "z_code", "z", "code"))),
			},
			diff: changes(changedTable("z", []catalog.Constraint{observedKey("z", "z_u", "UNIQUE", "code")})),
			want: []string{"b: add x", "z: drop constraint z_u, drop y", "a: add constraint a_z"},
		},
		{
			name: "a table behind a table it depends on, with all its changes", dialect: platform.Postgres,
			nodes: []ast.Node{alter("z", modifyColumn("id", "bigint")), alter("a", addColumn("x")), alter("a", modifyColumn("z_id", "bigint"))},
			diff:  changes(changedTable("a", []catalog.Constraint{observedForeignKey("a", "a_z", "z_id", "z", "id")})),
			want:  []string{"z: modify id", "a: add x, modify z_id"},
		},
		{
			name: "a table with a key to itself takes its place by name", dialect: platform.Postgres,
			nodes: []ast.Node{alter("a", addKey(foreignKeyTo("a_p", "parent", "a", "id"))), alter("b", addColumn("x"))},
			want:  []string{"a: add constraint a_p", "b: add x"},
		},
		{
			name: "an index reading a column by its part", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewIndex("t_i", "t").SetParts([]ast.IndexPart{{Name: "x"}}), alter("t", modifyColumn("x", "bigint"))},
			want:  []string{"index t_i on t", "t: modify x"},
		},
		{
			name: "an index reading a column by an expression", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewIndex("t_i", "t").SetParts([]ast.IndexPart{{Expr: "lower(x)"}}), alter("t", modifyColumn("x", "bigint"))},
			want:  []string{"index t_i on t", "t: modify x"},
		},
		{
			name: "an index reading a column in its condition", dialect: platform.Postgres,
			nodes: []ast.Node{&ast.IndexNode{Name: "t_i", Table: "t", Columns: []string{"y"}, Condition: "x > 0"}, alter("t", modifyColumn("x", "bigint"))},
			want:  []string{"index t_i on t", "t: modify x"},
		},
		{
			name: "an index including a column", dialect: platform.Postgres,
			nodes: []ast.Node{&ast.IndexNode{Name: "t_i", Table: "t", Columns: []string{"y"}, IncludeColumns: []string{"x"}}, alter("t", modifyColumn("x", "bigint"))},
			want:  []string{"index t_i on t", "t: modify x"},
		},
		{
			name: "a unique index its table's own key reads", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewIndex("t_u", "t", "code").SetUnique(), alter("t", addKey(foreignKeyTo("t_fk", "parent_code", "t", "code")))},
			want:  []string{"index t_u on t", "t: add constraint t_fk"},
		},
		{
			name: "a replaced index its table's own key reads", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", &ast.ReplaceIndexOperation{Index: ast.NewIndex("t_i", "t", "code")}), alter("t", addKey(foreignKeyTo("t_fk", "parent_code", "t", "code")))},
			want:  []string{"t: replace index t_i, add constraint t_fk"},
		},
		{
			name: "a constraint over a column", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", addKey(&ast.ConstraintNode{Type: ast.UniqueConstraint, Name: "u", Columns: []string{"x"}}), modifyColumn("x", "bigint"))},
			want:  []string{"t: add constraint u, modify x"},
		},
		{
			name: "a constraint over a column by its part", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", addKey(&ast.ConstraintNode{Type: ast.UniqueConstraint, Name: "u", ColumnParts: []ast.ConstraintColumn{{Name: "x"}}}), modifyColumn("x", "bigint"))},
			want:  []string{"t: add constraint u, modify x"},
		},
		{
			name: "a constraint over an expression", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", addKey(&ast.ConstraintNode{Type: ast.UniqueConstraint, Name: "u", ColumnParts: []ast.ConstraintColumn{{Expr: "x + 1"}}}), modifyColumn("x", "bigint"))},
			want:  []string{"t: add constraint u, modify x"},
		},
		{
			name: "a constraint including a column", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", addKey(&ast.ConstraintNode{Type: ast.UniqueConstraint, Name: "u", Columns: []string{"y"}, IncludeColumns: []string{"x"}}), modifyColumn("x", "bigint"))},
			want:  []string{"t: add constraint u, modify x"},
		},
		{
			name: "a check dropped over a column a reader names once", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn("x"), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{{TableName: "t", Name: "ck", Type: "CHECK", ColumnName: "x"}})),
			want:  []string{"t: drop x, drop constraint ck"},
		},
		{
			name: "a check dropped over a column only its clause names", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn("x"), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{{TableName: "t", Name: "ck", Type: "CHECK", CheckClause: new("x > 0")}})),
			want:  []string{"t: drop x, drop constraint ck"},
		},
		{
			name: "a key whose reader names one referenced column", dialect: platform.Postgres,
			nodes: []ast.Node{alter("z", dropConstraint("z_fk")), alter("p", dropColumn("id"))},
			diff: changes(changedTable("z", []catalog.Constraint{
				{TableName: "z", Name: "z_fk", Type: "FOREIGN KEY", ColumnNames: []string{"p_id"}, ForeignTable: new("p"), ForeignColumn: new("id")},
			})),
			want: []string{"z: drop constraint z_fk", "p: drop id"},
		},
		{
			name: "an index a reader lists by part, under a key on MySQL", dialect: platform.MySQL,
			nodes: []ast.Node{alter("t", &ast.DropConstraintOperation{ConstraintName: "fk", ForeignKey: true}), ast.NewDropIndex("t_i").SetTable("t")},
			diff: changes(changedTable("t", []catalog.Constraint{observedForeignKey("t", "fk", "x", "p", "id")},
				catalog.Index{Name: "t_i", TableName: "t", Parts: []catalog.IndexPart{{Name: "x"}}})),
			want: []string{"t: drop constraint fk", "drop index t_i on t"},
		},
		{
			name: "a column going with a check over its name, which holds a space", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn("my col"), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{{TableName: "t", Name: "ck", Type: "CHECK", CheckClause: new(`"my col" > 0`)}})),
			want:  []string{"t: drop my col, drop constraint ck"},
		},
		{
			name: "a column going with a check over its name, which holds a hyphen", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn("my-col"), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{{TableName: "t", Name: "ck", Type: "CHECK", CheckClause: new(`"my-col" > 0`)}})),
			want:  []string{"t: drop my-col, drop constraint ck"},
		},
		{
			name: "a column going with a check over its name, which holds a dot", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn("my.col"), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{{TableName: "t", Name: "ck", Type: "CHECK", CheckClause: new(`"my.col" > 0`)}})),
			want:  []string{"t: drop my.col, drop constraint ck"},
		},
		{
			name: "a column going with a check over its name, which holds a quote", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn(`my"col`), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{{TableName: "t", Name: "ck", Type: "CHECK", CheckClause: new(`"my""col" > 0`)}})),
			want:  []string{`t: drop my"col, drop constraint ck`},
		},
		{
			name: "a column going with a check over its name, in capitals and with a space", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn("Upper Col"), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{{TableName: "t", Name: "ck", Type: "CHECK", CheckClause: new(`"Upper Col" > 0`)}})),
			want:  []string{"t: drop Upper Col, drop constraint ck"},
		},
		{
			name: "a column going with a check whose literal holds a quote ahead of its name", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn("my col"), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{{TableName: "t", Name: "ck", Type: "CHECK", CheckClause: new(`c <> 'it"s' AND "my col" > 0`)}})),
			want:  []string{"t: drop my col, drop constraint ck"},
		},
		{
			name: "a column going with a check whose escape string holds an apostrophe ahead of its name", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn("my col"), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{{TableName: "t", Name: "ck", Type: "CHECK", CheckClause: new(`E'it\'s' <> "my col"`)}})),
			want:  []string{"t: drop my col, drop constraint ck"},
		},
		{
			name: "a column going with a check whose dollar quote holds an apostrophe ahead of its name", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn("my col"), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{{TableName: "t", Name: "ck", Type: "CHECK", CheckClause: new(`$$it's$$ <> "my col"`)}})),
			want:  []string{"t: drop my col, drop constraint ck"},
		},
		{
			name: "a column going with a check over its name, which holds an apostrophe", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn("it's"), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{{TableName: "t", Name: "ck", Type: "CHECK", CheckClause: new(`"it's" > 0`)}})),
			want:  []string{"t: drop it's, drop constraint ck"},
		},
		{
			name: "a column in backticks on MySQL, where one statement is free", dialect: platform.MySQL,
			nodes: []ast.Node{alter("t", dropColumn("my col"), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{{TableName: "t", Name: "ck", Type: "CHECK", CheckClause: new("`my col` > 0")}})),
			want:  []string{"t: drop constraint ck, drop my col"},
		},
		{
			name: "an index dropped beside a dropped column goes first, since the column would take it", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn("x")), ast.NewDropIndex("t_i").SetTable("t")},
			diff:  indexOver(catalog.Index{Columns: []string{"x"}}),
			want:  []string{"drop index t_i on t", "t: drop x"},
		},
		{
			name: "an index dropped beside a column added to its name goes first", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", addColumn("x")), ast.NewDropIndex("t_i").SetTable("t")},
			diff:  indexOver(catalog.Index{Columns: []string{"x"}}),
			want:  []string{"drop index t_i on t", "t: add x"},
		},
		{
			name: "a primary key's index a key stands on", dialect: platform.Postgres,
			nodes: []ast.Node{alter("z", dropConstraint("z_t")), ast.NewDropIndex("t_pk").SetTable("t")},
			diff: changes(
				changedTable("z", []catalog.Constraint{observedForeignKey("z", "z_t", "t_id", "t", "id")}),
				changedTable("t", nil, catalog.Index{Name: "t_pk", TableName: "t", Columns: []string{"id"}, IsPrimary: true}),
			),
			want: []string{"z: drop constraint z_t", "drop index t_pk on t"},
		},
		{
			name: "a unique constraint's index a key stands on", dialect: platform.Postgres,
			nodes: []ast.Node{alter("z", dropConstraint("z_p")), ast.NewDropIndex("p_uq").SetTable("p"), alter("a", addColumn("x"))},
			diff: changes(
				changedTable("z", aKeyOn("z", "z_p", "p_id")),
				changedTable("p", []catalog.Constraint{observedKey("p", "p_uq", "UNIQUE", "id")}),
			),
			want: []string{"z: drop constraint z_p", "a: add x", "drop index p_uq on p"},
		},
		{
			name: "an index dropped beside a type change goes first, since it goes either way", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", modifyColumn("x", "bigint")), ast.NewDropIndex("t_i").SetTable("t")},
			diff:  indexOver(catalog.Index{Columns: []string{"x"}}),
			want:  []string{"drop index t_i on t", "t: modify x"},
		},
		{
			name: "the counter of a table, on an identity column", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", modifyColumn("b", "int")), alter("t", &ast.ModifyColumnOperation{Column: columnWith("a", func(column *ast.ColumnNode) { column.IdentityGeneration = "ALWAYS" })})},
			diff:  changes(heldColumns("t", []string{"a", "b"})),
			want:  []string{"t: modify b, modify a"},
		},
		{
			name: "the counter of a table, on an auto increment column", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", modifyColumn("b", "int")), alter("t", &ast.ModifyColumnOperation{Column: columnWith("a", func(column *ast.ColumnNode) { column.AutoInc = true })})},
			diff:  changes(heldColumns("t", []string{"a", "b"})),
			want:  []string{"t: modify b, modify a"},
		},
		{
			name: "an auto increment column under an index on MySQL", dialect: platform.MySQL,
			nodes: []ast.Node{alter("t", &ast.ModifyColumnOperation{Column: columnWith("x", func(column *ast.ColumnNode) { column.AutoInc = true })}), ast.NewDropIndex("t_i").SetTable("t")},
			diff:  indexOver(catalog.Index{Columns: []string{"x"}}),
			want:  []string{"t: modify x", "drop index t_i on t"},
		},
		{
			name: "a primary key column under an index on MySQL", dialect: platform.MySQL,
			nodes: []ast.Node{alter("t", &ast.ModifyColumnOperation{Column: columnWith("x", func(column *ast.ColumnNode) { column.Primary = true })}), ast.NewDropIndex("t_i").SetTable("t")},
			diff:  indexOver(catalog.Index{Columns: []string{"x"}}),
			want:  []string{"t: modify x", "drop index t_i on t"},
		},
		{
			name: "a unique column under an index on MySQL", dialect: platform.MySQL,
			nodes: []ast.Node{alter("t", &ast.ModifyColumnOperation{Column: columnWith("x", func(column *ast.ColumnNode) { column.Unique = true })}), ast.NewDropIndex("t_i").SetTable("t")},
			diff:  indexOver(catalog.Index{Columns: []string{"x"}}),
			want:  []string{"t: modify x", "drop index t_i on t"},
		},
		{
			name: "a primary key dropped over an index on MySQL", dialect: platform.MySQL,
			nodes: []ast.Node{alter("t", &ast.DropConstraintOperation{ConstraintName: "PRIMARY", PrimaryKey: true}), ast.NewDropIndex("t_k").SetTable("t")},
			diff: changes(changedTable("t", []catalog.Constraint{observedKey("t", "t_pkey", "PRIMARY KEY", "k")},
				catalog.Index{Name: "t_k", TableName: "t", Columns: []string{"k"}})),
			want: []string{"t: drop constraint PRIMARY", "drop index t_k on t"},
		},
		{
			name: "the one primary key of a table, on a column that takes it", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", &ast.ModifyColumnOperation{Column: columnWith("x", func(column *ast.ColumnNode) { column.Primary = true })}, dropConstraint("t_pkey"))},
			diff:  changes(changedTable("t", []catalog.Constraint{observedKey("t", "t_pkey", "PRIMARY KEY", "y")})),
			want:  []string{"t: modify x, drop constraint t_pkey"},
		},
		{
			name: "a column's check reading another column", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", modifyColumn("x", "int")), alter("t", &ast.ModifyColumnOperation{Column: columnWith("y", func(column *ast.ColumnNode) { column.Check = "x > 0" })})},
			diff:  changes(heldColumns("t", []string{"y", "x"})),
			want:  []string{"t: modify x, modify y"},
		},
		{
			name: "a generated column changed behind what it reads", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", modifyColumn("x", "int")), alter("t", &ast.ModifyColumnOperation{Column: columnWith("y", generatedFrom("x * 2"))})},
			diff:  changes(heldColumns("t", []string{"y", "x"})),
			want:  []string{"t: modify x, modify y"},
		},
		{
			name: "a check's name a column takes", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", addedColumn(columnWith("x", func(column *ast.ColumnNode) { column.CheckName = "ck" })), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{observedKey("t", "ck", "CHECK", "y")})),
			want:  []string{"t: add x, drop constraint ck"},
		},
		{
			name: "a column's inline key into a column another table gains", dialect: platform.Postgres,
			nodes: []ast.Node{
				alter("z", addColumn("code")),
				alter("a", addedColumn(columnWith("z_code", func(column *ast.ColumnNode) {
					column.ForeignKey = &ast.ForeignKeyRef{Table: "z", Column: "code", Name: "a_z"}
				}))),
			},
			want: []string{"z: add code", "a: add z_code"},
		},
		{
			name: "a change at the referenced end of a key, under another case", dialect: platform.Postgres,
			nodes: []ast.Node{alter("z", modifyColumn("ID", "bigint")), alter("a", modifyColumn("z_id", "bigint"))},
			diff:  changes(changedTable("a", []catalog.Constraint{observedForeignKey("a", "a_z", "z_id", "z", "id")})),
			want:  []string{"z: modify ID", "a: modify z_id"},
		},
		{
			name: "a foreign key's name moving to a table that sorts earlier", dialect: platform.Postgres,
			nodes: []ast.Node{alter("z", dropConstraint("n")), alter("a", addKey(foreignKey("n", "p_id", "p")))},
			diff:  changes(changedTable("z", aKeyOn("z", "n", "p_id"))),
			want:  []string{"z: drop constraint n", "a: add constraint n"},
		},
		{
			name: "a table's key dropped with the table, ahead of the column it references", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewDropTable("c"), alter("p", dropColumn("id"))},
			diff: &difftypes.SchemaDiff{
				TablesRemoved: difftypes.TableRemovals{{Name: "c", Current: observedTable("c", nil)}},
				ForeignKeysRemovedWithTables: []difftypes.ForeignKeyRemovalInfo{
					{TableName: "c", Name: "c_p", Columns: []string{"p_id"}, ForeignTable: "p", ForeignColumns: []string{"id"}},
				},
			},
			want: []string{"drop table c", "p: drop id"},
		},
		{
			name: "a table dropped ahead of a change to a column keyed to it", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewDropTable("p"), alter("z", modifyColumn("p_id", "bigint"))},
			diff: &difftypes.SchemaDiff{
				TablesRemoved:  difftypes.TableRemovals{{Name: "p", Current: observedTable("p", nil)}},
				TablesModified: []difftypes.TableDiff{changedTable("z", aKeyOn("z", "z_p", "p_id"))},
			},
			want: []string{"drop table p", "z: modify p_id"},
		},
		{
			name: "a name a dropped table frees", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewDropTable("x"), ast.NewIndex("x", "a", "c")},
			diff:  droppedTables("x"),
			want:  []string{"drop table x", "index x on a"},
		},
		{
			name: "an index's name a dropped table frees", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewDropTable("old"), ast.NewIndex("idx", "a", "c")},
			diff: &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{
				{Name: "old", Current: observedTable("old", nil, catalog.Index{Name: "idx", TableName: "old", Columns: []string{"c"}})},
			}},
			want: []string{"drop table old", "index idx on a"},
		},
		{
			name: "a constraint's name a dropped table frees", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewDropTable("old"), ast.NewIndex("idx", "a", "c")},
			diff: &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{
				{Name: "old", Current: observedTable("old", []catalog.Constraint{observedKey("old", "idx", "UNIQUE", "c")})},
			}},
			want: []string{"drop table old", "index idx on a"},
		},
		{
			name: "an index's name a new table takes", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewDropTable("old"), &ast.CreateTableNode{Name: "n", Indexes: []*ast.IndexNode{ast.NewIndex("idx", "n", "c")}}},
			diff: &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{
				{Name: "old", Current: observedTable("old", nil, catalog.Index{Name: "idx", TableName: "old", Columns: []string{"c"}})},
			}},
			want: []string{"drop table old", "create n"},
		},
		{
			name: "a new table typed by another", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewCreateTable("z").AddColumn(key("id")), ast.NewCreateTable("a").AddColumn(nullable("payload", "z"))},
			want:  []string{"create z", "create a"},
		},
		{
			name: "a column typed by a table the plan changes", dialect: platform.Postgres,
			nodes: []ast.Node{alter("z", addColumn("x")), alter("a", &ast.AddColumnOperation{Column: nullable("payload", "z")})},
			want:  []string{"z: add x", "a: add payload"},
		},
		{
			name: "a column typed by a table the plan drops", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewDropTable("n"), alter("a", &ast.AddColumnOperation{Column: nullable("payload", "n")})},
			diff:  droppedTables("n"),
			want:  []string{"drop table n", "a: add payload"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(test.dialect, test.nodes, test.diff)

			c.Assert(planOf(got), qt.DeepEquals, test.want)
		})
	}
}

// TestApply_TwoChangedTablesWithKeysToEachOther_HappyPath has no chain to measure a depth
// by. Either table can go first, so only the statements are pinned.
func TestApply_TwoChangedTablesWithKeysToEachOther_HappyPath(t *testing.T) {
	c := qt.New(t)
	nodes := []ast.Node{alter("a", addKey(foreignKeyTo("a_b", "b_id", "b", "id"))), alter("b", addKey(foreignKeyTo("b_a", "a_id", "a", "id")))}

	got := planshape.Apply(platform.Postgres, nodes, nil)

	c.Assert(got, qt.HasLen, 2)
}

// TestApply_MovesWhatItCanShowIsFree_HappyPath is a step the binary writes elsewhere and
// that shares nothing with what it moves over. A rule that held it back would be correct
// and would cost the file its order, so each row pins the move.
func TestApply_MovesWhatItCanShowIsFree_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		nodes   []ast.Node
		diff    *difftypes.SchemaDiff
		want    []string
	}{
		{
			name: "tables by name whatever their case", dialect: platform.Postgres,
			nodes: []ast.Node{alter("Zed", addColumn("x")), alter("apple", addColumn("y"))},
			want:  []string{"apple: add y", "Zed: add x"},
		},
		{
			name: "a table of one bare name beside a new table of another schema", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewCreateTable("a.t").AddColumn(key("id")), ast.NewCreateTable("a.u").AddColumn(key("id")), alter("b.t", addColumn("y"))},
			want:  []string{"create a.t", "b.t: add y", "create a.u"},
		},
		{
			name: "a change to a new table in a schema behind every new table", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewCreateTable("a.t").AddColumn(key("id")), ast.NewCreateTable("a.u").AddColumn(key("id")), alter("a.t", addColumn("x"))},
			want:  []string{"create a.t", "create a.u", "a.t: add x"},
		},
		{
			name: "a key into a new table named without its schema waits for every new table", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewCreateTable("public.n").AddColumn(key("id")), ast.NewCreateTable("public.o").AddColumn(key("id")), alter("z", addKey(foreignKey("z_n", "a", "n")))},
			want:  []string{"create public.n", "create public.o", "z: add constraint z_n"},
		},
		{
			name: "a change to a new table behind every new table", dialect: platform.Postgres,
			nodes: []ast.Node{ast.NewCreateTable("n").AddColumn(key("id")), ast.NewCreateTable("o").AddColumn(key("id")), alter("n", addColumn("x"))},
			want:  []string{"create n", "create o", "n: add x"},
		},
		{
			name: "a replaced index", dialect: platform.Postgres,
			nodes: []ast.Node{alter("z", &ast.ReplaceIndexOperation{Index: ast.NewIndex("z_i", "z", "x")}), alter("a", addColumn("b"))},
			want:  []string{"a: add b", "z: replace index z_i"},
		},
		{
			name: "a constraint only a host's reading describes", dialect: platform.Postgres,
			nodes: []ast.Node{alter("z", addColumn("a")), alter("m", dropConstraint("m_ck")), alter("a", addColumn("b"))},
			diff:  &difftypes.SchemaDiff{ObservedConstraintHosts: []schemacapture.TableObservation{observedTable("m", []catalog.Constraint{observedKey("m", "m_ck", "CHECK", "x")})}},
			want:  []string{"a: add b", "m: drop constraint m_ck", "z: add a"},
		},
		{
			name: "an index dropped from a table whose only new constraint is a check", dialect: platform.Postgres,
			nodes: []ast.Node{alter("z", addColumn("a")), alter("m", addKey(checkConstraint("m_ck", "x > 0"))), ast.NewDropIndex("m_i").SetTable("m"), alter("a", addColumn("b"))},
			want:  []string{"a: add b", "drop index m_i on m", "m: add constraint m_ck", "z: add a"},
		},
		{
			name: "a primary key MySQL drops under no name of its own", dialect: platform.MySQL,
			nodes: []ast.Node{alter("z", &ast.DropConstraintOperation{ConstraintName: "PRIMARY", PrimaryKey: true}), alter("a", addColumn("x"))},
			diff:  changes(changedTable("z", []catalog.Constraint{observedKey("z", "z_pkey", "PRIMARY KEY", "k")})),
			want:  []string{"a: add x", "z: drop constraint PRIMARY"},
		},
		{
			name: "a key over several referenced columns", dialect: platform.Postgres,
			nodes: []ast.Node{
				alter("z", addColumn("x")),
				alter("a", addKey(&ast.ConstraintNode{Type: ast.ForeignKeyConstraint, Name: "a_p", Columns: []string{"p1", "p2"}, Reference: &ast.ForeignKeyRef{Table: "p", Columns: []string{"id1", "id2"}}})),
			},
			want: []string{"a: add constraint a_p", "z: add x"},
		},
		{
			name: "a check whose literal spells a column's name", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", dropColumn("my col"), dropConstraint("ck"))},
			diff:  changes(changedTable("t", []catalog.Constraint{{TableName: "t", Name: "ck", Type: "CHECK", CheckClause: new(`c <> 'my col'`)}})),
			want:  []string{"t: drop constraint ck, drop my col"},
		},
		{
			name: "a column in backticks a generated column reads, added behind it on MySQL", dialect: platform.MySQL,
			nodes: []ast.Node{alter("t", addedColumn(columnWith("g", generatedFrom("`my col` * 2"))), addColumn("my col"))},
			want:  []string{"t: add my col, add g"},
		},
		{
			name: "a column typed after its own table", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", &ast.ModifyColumnOperation{Column: nullable("b", "t")}), alter("t", modifyColumn("a", "int"))},
			diff:  changes(heldColumns("t", []string{"a", "b"})),
			want:  []string{"t: modify a, modify b"},
		},
		{
			name: "a drop the planner marks as a foreign key, whatever a table of that name holds", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", &ast.DropConstraintOperation{ConstraintName: "c", ForeignKey: true}, modifyColumn("m", "bigint"))},
			diff:  changes(changedTable("t", []catalog.Constraint{observedKey("t", "c", "CHECK", "y")})),
			want:  []string{"t: modify m, drop constraint c"},
		},
		{
			name: "a drop the planner marks as a unique constraint", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", modifyColumn("m", "bigint"), &ast.DropConstraintOperation{ConstraintName: "c", Unique: true})},
			diff:  changes(changedTable("t", []catalog.Constraint{observedForeignKey("t", "c", "y", "p", "id")})),
			want:  []string{"t: drop constraint c, modify m"},
		},
		{
			name: "a drop the planner marks as a check", dialect: platform.Postgres,
			nodes: []ast.Node{alter("t", modifyColumn("m", "bigint"), &ast.DropConstraintOperation{ConstraintName: "c", Check: true})},
			diff:  changes(changedTable("t", []catalog.Constraint{observedForeignKey("t", "c", "y", "p", "id")})),
			want:  []string{"t: drop constraint c, modify m"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(test.dialect, test.nodes, test.diff)

			c.Assert(planOf(got), qt.DeepEquals, test.want)
		})
	}
}

// TestApply_AnIndexThePlanWritesSaysNothingAboutADrop_HappyPath is a drop of an index the
// same plan writes again. The database holds the old index, and only a read says what that
// one covers, so the new one's column does not hold the drop back.
func TestApply_AnIndexThePlanWritesSaysNothingAboutADrop_HappyPath(t *testing.T) {
	c := qt.New(t)
	counter := &ast.ModifyColumnOperation{Column: columnWith("x", func(column *ast.ColumnNode) { column.AutoInc = true })}
	nodes := []ast.Node{alter("m", counter), ast.NewDropIndex("m_i").SetTable("m"), ast.NewIndex("m_i", "m", "x").SetUnique()}

	got := planshape.Apply(platform.Postgres, nodes, nil)

	c.Assert(planOf(got), qt.DeepEquals, []string{"drop index m_i on m", "m: modify x", "index m_i on m"})
}

// TestApply_AChangeThatNeedsAnotherStatementKeepsAnOrderThatRuns_HappyPath is a table's changes
// that cannot be one statement, because a backfill stands among them. A check over a new column
// goes ahead of the column inside one statement and behind it in a sequence, and the node holds
// both, so the file takes the one that runs.
func TestApply_AChangeThatNeedsAnotherStatementKeepsAnOrderThatRuns_HappyPath(t *testing.T) {
	c := qt.New(t)
	backfilled := columnWith("a", func(column *ast.ColumnNode) {
		column.Nullable = false
		column.Default = &ast.DefaultValue{Value: "1"}
	})
	nodes := []ast.Node{
		alter("t", addColumn("z")),
		alter("t", &ast.ModifyColumnOperation{Column: backfilled, HasChanged: true, Changed: ast.ColumnProperties{Nullability: true}}),
		alter("t", addKey(checkConstraint("ck", "z > 0"))),
	}

	got := planshape.Apply(platform.Postgres, nodes, nil)

	c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Equals,
		"ALTER TABLE \"t\" ADD COLUMN \"z\" integer NULL;\n"+
			"ALTER TABLE \"t\" ADD CONSTRAINT \"ck\" CHECK (z > 0);\n"+
			"UPDATE \"t\" SET \"a\" = '1' WHERE \"a\" IS NULL;\n"+
			"ALTER TABLE \"t\" ALTER COLUMN \"a\" SET NOT NULL;\n\n")
}

// TestApply_AForeignKeyThatMustWaitKeepsTheOthersWaiting_HappyPath is a key of a new table
// on an engine or in a plan where it cannot join its CREATE TABLE.
func TestApply_AForeignKeyThatMustWaitKeepsTheOthersWaiting_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		nodes   []ast.Node
		want    []string
	}{
		{
			name: "tables that reference each other, and a key into a table the plan does not touch", dialect: platform.Postgres,
			nodes: []ast.Node{
				ast.NewCreateTable("x").AddColumn(key("id")),
				ast.NewCreateTable("y").AddColumn(key("id")),
				alter("x", addKey(foreignKey("x_y", "y_id", "y"))),
				alter("y", addKey(foreignKey("y_x", "x_id", "x"))),
				alter("x", addKey(foreignKey("x_a", "a_id", "a"))),
			},
			want: []string{"create x", "create y", "x: add constraint x_y, add constraint x_a", "y: add constraint y_x"},
		},
		{
			name: "an engine that takes one change at a time", dialect: platform.SQLite,
			nodes: []ast.Node{
				ast.NewCreateTable("n").AddColumn(key("id")),
				alter("a", addColumn("x")),
				alter("n", addKey(foreignKey("n_a", "a_id", "a"))),
			},
			want: []string{"a: add x", "create n", "n: add constraint n_a"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(test.dialect, test.nodes, nil)

			c.Assert(planOf(got), qt.DeepEquals, test.want)
		})
	}
}

// TestApply_EndsTheRunAtWhatItCannotShowIsFree_FailurePath is a statement the pass cannot
// say the access of, or that a server takes differently when it moves. Nothing moves over
// it, so each row holds the tables on each side where the planner put them.
func TestApply_EndsTheRunAtWhatItCannotShowIsFree_FailurePath(t *testing.T) {
	calling := func(expression string) *ast.ColumnNode {
		return columnWith("n", func(column *ast.ColumnNode) { column.Default = &ast.DefaultValue{Expression: expression} })
	}
	declared := func(name string) *difftypes.SchemaDiff {
		return &difftypes.SchemaDiff{DeclaredFunctions: difftypes.FunctionOrdering{Order: []string{name}}}
	}
	routine := func(name string) difftypes.FunctionChanges {
		return difftypes.FunctionChanges{{Function: schemamodel.Function{Name: name}}}
	}
	owned := must.Must(schemaext.NewObjects(schemaext.Object{
		Ref:   pgtable.ExclusionRef("m", "m_x", identifier.ForDialect("postgres")),
		Value: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "x WITH ="}},
	}))
	unknownKind := ast.ConstraintType(99)
	tests := []struct {
		name string
		kept []ast.Node
		diff *difftypes.SchemaDiff
		want []string
	}{
		{name: "a table written from a select", kept: []ast.Node{&ast.CreateTableNode{Name: "m", SelectBody: "SELECT 1"}}, want: []string{"z: add a", "create m", "a: add b"}},
		{name: "a table that owns objects", kept: []ast.Node{&ast.CreateTableNode{Name: "m", OwnedObjects: owned}}, want: []string{"z: add a", "create m", "a: add b"}},
		{name: "a table that owns constraints", kept: []ast.Node{&ast.CreateTableNode{Name: "m", OwnedConstraints: []*ast.OwnedIndexConstraint{{Name: "m_x"}}}}, want: []string{"z: add a", "create m", "a: add b"}},
		{name: "an index built concurrently", kept: []ast.Node{&ast.IndexNode{Name: "m_i", Table: "m", Columns: []string{"x"}, Concurrently: true}}, want: []string{"z: add a", "index m_i on m", "a: add b"}},
		{name: "an index dropped without its table", kept: []ast.Node{ast.NewDropIndex("m_i")}, want: []string{"z: add a", "drop index m_i on ", "a: add b"}},
		{name: "an index dropped concurrently", kept: []ast.Node{ast.NewDropIndex("m_i").SetTable("m").SetConcurrently()}, want: []string{"z: add a", "drop index m_i on m", "a: add b"}},
		{name: "an index dropped with what depends on it", kept: []ast.Node{ast.NewDropIndex("m_i").SetTable("m").SetCascade()}, want: []string{"z: add a", "drop index m_i on m", "a: add b"}},
		{
			name: "an index dropped from a table a read says does not hold it",
			kept: []ast.Node{ast.NewDropIndex("m_i").SetTable("m")}, diff: changes(changedTable("m", nil)),
			want: []string{"z: add a", "drop index m_i on m", "a: add b"},
		},
		{
			name: "an index over a declared routine",
			kept: []ast.Node{ast.NewIndex("m_i", "m").SetParts([]ast.IndexPart{{Expr: "next_code(x)"}})}, diff: declared("next_code"),
			want: []string{"z: add a", "index m_i on m", "a: add b"},
		},
		{
			name: "a new table's column calling a declared routine",
			kept: []ast.Node{ast.NewCreateTable("m").AddColumn(calling("next_code()"))}, diff: declared("next_code"),
			want: []string{"z: add a", "create m", "a: add b"},
		},
		{
			name: "a new table's check calling a declared routine",
			kept: []ast.Node{ast.NewCreateTable("m").AddColumn(key("id")).AddConstraint(checkConstraint("m_ck", "next_code(id) > 0"))}, diff: declared("next_code"),
			want: []string{"z: add a", "create m", "a: add b"},
		},
		{
			name: "a new table's constraint of a kind with no rule",
			kept: []ast.Node{ast.NewCreateTable("m").AddColumn(key("id")).AddConstraint(&ast.ConstraintNode{Type: unknownKind, Name: "m_x"})},
			want: []string{"z: add a", "create m", "a: add b"},
		},
		{
			name: "a check calling a declared routine",
			kept: []ast.Node{alter("m", addKey(checkConstraint("m_ck", "next_code(x) > 0")))}, diff: declared("next_code"),
			want: []string{"z: add a", "m: add constraint m_ck", "a: add b"},
		},
		{
			name: "a key built from an index",
			kept: []ast.Node{alter("m", addKey(&ast.ConstraintNode{Type: ast.UniqueConstraint, Name: "m_u", UsingIndex: "m_i"}))},
			want: []string{"z: add a", "m: add constraint m_u", "a: add b"},
		},
		{
			name: "a constraint of a kind with no rule",
			kept: []ast.Node{alter("m", addKey(&ast.ConstraintNode{Type: unknownKind, Name: "m_x"}))},
			want: []string{"z: add a", "m: add constraint m_x", "a: add b"},
		},
		{
			name: "a key naming no referenced column",
			kept: []ast.Node{alter("m", addKey(&ast.ConstraintNode{Type: ast.ForeignKeyConstraint, Name: "m_p", Columns: []string{"x"}, Reference: &ast.ForeignKeyRef{Table: "p"}}))},
			want: []string{"z: add a", "m: add constraint m_p", "a: add b"},
		},
		{
			name: "a column's inline key naming no referenced column",
			kept: []ast.Node{alter("m", addedColumn(columnWith("x", func(column *ast.ColumnNode) { column.ForeignKey = &ast.ForeignKeyRef{Table: "p", Name: "m_p"} })))},
			want: []string{"z: add a", "m: add x", "a: add b"},
		},
		{
			name: "the drop of a constraint of a kind with no rule",
			kept: []ast.Node{alter("m", dropConstraint("m_x"))}, diff: changes(changedTable("m", []catalog.Constraint{observedKey("m", "m_x", "EXCLUDE", "x")})),
			want: []string{"z: add a", "m: drop constraint m_x", "a: add b"},
		},
		{
			name: "a default calling a routine the plan keeps",
			kept: []ast.Node{alter("m", addedColumn(calling("next_code()")))}, diff: &difftypes.SchemaDiff{RoutinesKept: routine("next_code")},
			want: []string{"z: add a", "m: add n", "a: add b"},
		},
		{
			name: "a default calling a routine the plan adds",
			kept: []ast.Node{alter("m", addedColumn(calling("next_code()")))}, diff: &difftypes.SchemaDiff{FunctionsAdded: routine("next_code")},
			want: []string{"z: add a", "m: add n", "a: add b"},
		},
		{
			name: "a default calling a routine the plan drops",
			kept: []ast.Node{alter("m", addedColumn(calling("next_code()")))}, diff: &difftypes.SchemaDiff{FunctionsRemoved: routine("next_code")},
			want: []string{"z: add a", "m: add n", "a: add b"},
		},
		{
			name: "a default calling a routine in capitals",
			kept: []ast.Node{alter("m", addedColumn(calling("NEXT_CODE()")))}, diff: declared("next_code"),
			want: []string{"z: add a", "m: add n", "a: add b"},
		},
		{
			name: "a default calling a routine whose name holds a dollar",
			kept: []ast.Node{alter("m", addedColumn(calling("next$code()")))}, diff: declared("next$code"),
			want: []string{"z: add a", "m: add n", "a: add b"},
		},
		{
			name: "a default calling a routine whose name is not ASCII",
			kept: []ast.Node{alter("m", addedColumn(calling("naïve()")))}, diff: declared("naïve"),
			want: []string{"z: add a", "m: add n", "a: add b"},
		},
		{
			name: "a drop of what the same plan adds, which says nothing of what is there",
			kept: []ast.Node{alter("m", dropConstraint("m_c"), addKey(checkConstraint("m_c", "x > 0")))},
			want: []string{"z: add a", "m: drop constraint m_c, add constraint m_c", "a: add b"},
		},
		{
			name: "a table the plan creates and drops, which no read describes",
			kept: []ast.Node{ast.NewCreateTable("m").AddColumn(key("id")), ast.NewDropTable("m")},
			want: []string{"z: add a", "create m", "drop table m", "a: add b"},
		},
		{
			name: "an index no read describes on a table a new key ties",
			kept: []ast.Node{ast.NewDropIndex("m_i").SetTable("m"), alter("c", addKey(foreignKey("c_m", "m_id", "m")))},
			want: []string{"z: add a", "drop index m_i on m", "a: add b", "c: add constraint c_m"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes := append(append([]ast.Node{alter("z", addColumn("a"))}, test.kept...), alter("a", addColumn("b")))

			got := planshape.Apply(platform.Postgres, nodes, test.diff)

			c.Assert(planOf(got), qt.DeepEquals, test.want)
		})
	}
}

// TestAlignRollback_PlacesAStepByWhatItUndoes_HappyPath is a rollback whose steps undo
// forward's, or nothing in it. Forward reads backwards, and a step that undoes nothing goes
// ahead.
func TestAlignRollback_PlacesAStepByWhatItUndoes_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		forward  []ast.Node
		rollback []ast.Node
		diff     *difftypes.SchemaDiff
		want     []string
	}{
		{
			name:     "a step that undoes nothing",
			forward:  []ast.Node{alter("a", addColumn("x"))},
			rollback: []ast.Node{alter("a", dropColumn("x")), alter("z", addColumn("y"))},
			want:     []string{"z: add y", "a: drop x"},
		},
		{
			name:     "an index forward dropped",
			forward:  []ast.Node{ast.NewDropIndex("i").SetTable("z"), alter("a", addColumn("x"))},
			rollback: []ast.Node{ast.NewIndex("i", "z", "c"), alter("a", dropColumn("x"))},
			want:     []string{"a: drop x", "index i on z"},
		},
		{
			name:     "a table forward dropped",
			forward:  []ast.Node{ast.NewDropTable("n"), alter("a", addColumn("x"))},
			rollback: []ast.Node{ast.NewCreateTable("n").AddColumn(key("id")), alter("a", dropColumn("x"))},
			want:     []string{"a: drop x", "create n"},
		},
		{
			name:     "a column forward changed",
			forward:  []ast.Node{alter("a", modifyColumn("x", "bigint")), alter("z", modifyColumn("y", "bigint"))},
			rollback: []ast.Node{alter("a", modifyColumn("x", "int")), alter("z", modifyColumn("y", "int"))},
			want:     []string{"z: modify y", "a: modify x"},
		},
		{
			name:     "a name forward freed and then took",
			forward:  []ast.Node{ast.NewDropIndex("z_pkey").SetTable("a"), ast.NewCreateTable("z").AddColumn(key("id"))},
			rollback: []ast.Node{ast.NewIndex("z_pkey", "a", "x"), ast.NewDropTable("z")},
			want:     []string{"drop table z", "index z_pkey on a"},
		},
		{
			name:     "a name freed behind a step on another table",
			forward:  []ast.Node{ast.NewDropIndex("z_pkey").SetTable("a"), ast.NewCreateTable("z").AddColumn(key("id")), alter("z", addColumn("y"))},
			rollback: []ast.Node{ast.NewIndex("z_pkey", "a", "x"), alter("z", dropColumn("y")), ast.NewDropTable("z")},
			want:     []string{"z: drop y", "drop table z", "index z_pkey on a"},
		},
		{
			name:     "a name that cannot wait, since a step between reads what the index covers",
			forward:  []ast.Node{ast.NewDropIndex("i").SetTable("t"), alter("t", addColumn("x")), ast.NewIndex("i", "s", "c")},
			rollback: []ast.Node{ast.NewIndex("i", "t", "x"), alter("t", dropColumn("x")), ast.NewDropIndex("i").SetTable("s")},
			want:     []string{"index i on t", "drop index i on s", "t: drop x"},
		},
		{
			name:     "a statement forward wrote in another order than it runs in",
			forward:  []ast.Node{&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{dropColumn("n"), dropConstraint("ck")}, OneStatement: []ast.AlterOperation{dropConstraint("ck"), dropColumn("n")}}},
			rollback: []ast.Node{alter("t", addColumn("n")), alter("t", addKey(checkConstraint("ck", "n > 0")))},
			want:     []string{"t: add n, add constraint ck"},
		},
		{
			name:     "a primary key whose name says nothing of the other columns a table held one over",
			forward:  []ast.Node{alter("t", modifyColumn("id", "bigint")), alter("t", addKey(primaryKey("pk_new", "k")))},
			rollback: []ast.Node{alter("t", modifyColumn("id", "int")), alter("t", dropConstraint("pk_new"))},
			diff:     changes(changedTable("t", []catalog.Constraint{observedKey("t", "pk_old", "PRIMARY KEY", "id")})),
			want:     []string{"t: modify id, drop constraint pk_new"},
		},
		{
			name:     "a primary key forward dropped under a word that names nothing",
			forward:  []ast.Node{alter("a", &ast.DropConstraintOperation{ConstraintName: "PRIMARY", PrimaryKey: true}), alter("z", addColumn("y"))},
			rollback: []ast.Node{alter("a", addKey(primaryKey("a_pkey", "id"))), alter("z", dropColumn("y"))},
			want:     []string{"z: drop y", "a: add constraint a_pkey"},
		},
		{
			name:     "a name a new table takes from an index the rollback drops",
			forward:  []ast.Node{ast.NewIndex("n", "z", "c")},
			rollback: []ast.Node{ast.NewDropIndex("n").SetTable("z"), ast.NewCreateTable("n").AddColumn(key("id"))},
			want:     []string{"drop index n on z", "create n"},
		},
		{
			name:     "a constraint forward added",
			forward:  []ast.Node{alter("a", addKey(checkConstraint("c1", "x > 0"))), alter("z", addKey(checkConstraint("c2", "y > 0")))},
			rollback: []ast.Node{alter("a", dropConstraint("c1")), alter("z", dropConstraint("c2"))},
			want:     []string{"z: drop constraint c2", "a: drop constraint c1"},
		},
		{
			name:     "a constraint forward added under another case",
			forward:  []ast.Node{alter("a", addKey(checkConstraint("Ck", "x > 0"))), alter("z", addColumn("y"))},
			rollback: []ast.Node{alter("a", dropConstraint("cK")), alter("z", dropColumn("y"))},
			want:     []string{"z: drop y", "a: drop constraint cK"},
		},
		{
			name:     "a primary key forward added under no name",
			forward:  []ast.Node{alter("a", addKey(&ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Columns: []string{"id"}})), alter("z", addColumn("y"))},
			rollback: []ast.Node{alter("a", dropConstraint("a_pkey")), alter("z", dropColumn("y"))},
			want:     []string{"z: drop y", "a: drop constraint a_pkey"},
		},
		{
			name:     "a column a generated column reads stays behind it",
			forward:  []ast.Node{alter("t", addedColumn(columnWith("g", generatedFrom("base * 2"))), addColumn("base"))},
			rollback: []ast.Node{alter("t", dropColumn("g")), alter("t", dropColumn("base"))},
			want:     []string{"t: drop g, drop base"},
		},
		{
			name:     "a drop only the comparison describes",
			forward:  []ast.Node{alter("a", dropColumn("b")), alter("z", dropColumn("y"))},
			rollback: []ast.Node{alter("a", addColumn("b")), alter("m", dropConstraint("m_ck")), alter("z", addColumn("y"))},
			diff:     changes(changedTable("m", []catalog.Constraint{observedKey("m", "m_ck", "CHECK", "x")})),
			want:     []string{"m: drop constraint m_ck", "z: add y", "a: add b"},
		},
		{
			name:     "an index no read describes on a table the rollback's own key ties",
			forward:  []ast.Node{alter("a", dropColumn("b")), alter("z", dropColumn("y"))},
			rollback: []ast.Node{alter("a", addColumn("b")), ast.NewDropIndex("m_i").SetTable("m"), alter("c", addKey(foreignKey("c_m", "m_id", "m"))), alter("z", addColumn("y"))},
			want:     []string{"a: add b", "drop index m_i on m", "c: add constraint c_m", "z: add y"},
		},
		{
			name:     "a drop of what the rollback itself adds, which says nothing of what is there",
			forward:  []ast.Node{alter("a", dropColumn("b")), alter("z", dropColumn("y"))},
			rollback: []ast.Node{alter("a", addColumn("b")), alter("m", dropConstraint("m_c"), addKey(checkConstraint("m_c", "x > 0"))), alter("z", addColumn("y"))},
			want:     []string{"a: add b", "m: drop constraint m_c, add constraint m_c", "z: add y"},
		},
		{
			name:     "a constraint a rollback adds right after creating its table",
			forward:  []ast.Node{ast.NewDropTable("n")},
			rollback: []ast.Node{ast.NewCreateTable("n").AddColumn(key("id")), alter("n", addKey(checkConstraint("n_ck", "id > 0")))},
			want:     []string{"create n"},
		},
		{
			name:     "a forward drop nothing describes",
			forward:  []ast.Node{alter("t", dropConstraint("unknown"))},
			rollback: []ast.Node{alter("a", addColumn("x"))},
			want:     []string{"a: add x"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.AlignRollback(platform.Postgres, test.forward, test.rollback, test.diff)

			c.Assert(planOf(got), qt.DeepEquals, test.want)
		})
	}
}

// TestAlignRollback_ReadsWhatForwardWrote_HappyPath is a rollback that drops what forward
// wrote inside a column: only forward's statements say it exists, so the step is free to
// move only because the pass read them. A step that undoes nothing in forward goes first.
func TestAlignRollback_ReadsWhatForwardWrote_HappyPath(t *testing.T) {
	column := func(set func(*ast.ColumnNode)) *ast.ColumnNode { return columnWith("x", set) }
	unique := func(column *ast.ColumnNode) { column.Unique = true }
	primary := func(column *ast.ColumnNode) { column.Primary = true }
	inlineKey := func(column *ast.ColumnNode) {
		column.ForeignKey = &ast.ForeignKeyRef{Table: "p", Column: "id", Name: "m_p"}
	}
	tests := []struct {
		name    string
		dialect string
		wrote   ast.Node
		dropped string
	}{
		{name: "a unique column of a new table", dialect: platform.Postgres, wrote: ast.NewCreateTable("m").AddColumn(column(unique)), dropped: "m_x_key"},
		{name: "a unique column added", dialect: platform.Postgres, wrote: alter("m", addedColumn(column(unique))), dropped: "m_x_key"},
		{name: "a unique column changed", dialect: platform.Postgres, wrote: alter("m", &ast.ModifyColumnOperation{Column: column(unique)}), dropped: "m_x_key"},
		{name: "a unique column added on MySQL, which names it after the column", dialect: platform.MySQL, wrote: alter("m", addedColumn(column(unique))), dropped: "x"},
		{name: "a primary key column added", dialect: platform.Postgres, wrote: alter("m", addedColumn(column(primary))), dropped: "m_pkey"},
		{name: "a column added with an inline key", dialect: platform.Postgres, wrote: alter("m", addedColumn(column(inlineKey))), dropped: "m_p"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			forward := []ast.Node{test.wrote, alter("a", dropColumn("b")), alter("z", dropColumn("y"))}
			rollback := []ast.Node{alter("a", addColumn("b")), alter("m", dropConstraint(test.dropped)), alter("z", addColumn("y"))}

			got := planshape.AlignRollback(test.dialect, forward, rollback, nil)

			c.Assert(planOf(got), qt.DeepEquals, []string{"m: drop constraint " + test.dropped, "z: add y", "a: add b"})
		})
	}
}

// TestAlignRollback_TheDropOfAKeyWithNoTableDropBehindIt_HappyPath is the drop of a
// constraint that cannot go with a table: forward wrote it elsewhere, nothing behind it
// drops the table, or something between them reads its column. It stays.
func TestAlignRollback_TheDropOfAKeyWithNoTableDropBehindIt_HappyPath(t *testing.T) {
	forward := func() []ast.Node {
		return []ast.Node{
			ast.NewCreateTable("n").AddColumn(key("id")).AddColumn(nullable("a_code", "int")).
				AddConstraint(foreignKeyTo("n_a", "a_code", "a", "code")),
			ast.NewCreateTable("x").AddColumn(key("id")).AddColumn(nullable("n_code", "int")).
				AddConstraint(foreignKeyTo("x_n", "n_code", "n", "a_code")),
			alter("n", addKey(checkConstraint("n_ck", "id > 0"))),
		}
	}
	dropKey := func() ast.Node { return alter("n", dropConstraint("n_a")) }
	tests := []struct {
		name     string
		rollback []ast.Node
		want     []string
	}{
		{name: "no drop of the table", rollback: []ast.Node{dropKey()}, want: []string{"n: drop constraint n_a"}},
		{
			name:     "a constraint forward did not write inside the table",
			rollback: []ast.Node{alter("n", dropConstraint("n_ck")), ast.NewDropTable("n")},
			want:     []string{"n: drop constraint n_ck", "drop table n"},
		},
		{name: "the table's drop ahead of it", rollback: []ast.Node{ast.NewDropTable("n"), dropKey()}, want: []string{"drop table n", "n: drop constraint n_a"}},
		{
			name:     "a key between them that reads what it wrote",
			rollback: []ast.Node{dropKey(), alter("x", dropConstraint("x_n")), ast.NewDropTable("n")},
			want:     []string{"n: drop constraint n_a", "x: drop constraint x_n", "drop table n"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.AlignRollback(platform.Postgres, forward(), test.rollback, nil)

			c.Assert(planOf(got), qt.DeepEquals, test.want)
		})
	}
}
