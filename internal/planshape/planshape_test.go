package planshape_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planshape"
)

// key is an integer primary key column as the planner builds it.
func key(name string) *ast.ColumnNode {
	return ast.NewColumn(name, "int").SetPrimary()
}

// nullable is a nullable column of the type.
func nullable(name, typeName string) *ast.ColumnNode {
	column := ast.NewColumn(name, typeName)
	column.Nullable = true
	return column
}

// addConstraint is the ALTER TABLE the planner writes after creating a table.
func addConstraint(table string, constraint *ast.ConstraintNode) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: constraint}}}
}

func checkConstraint(name, expression string) *ast.ConstraintNode {
	return &ast.ConstraintNode{Type: ast.CheckConstraint, Name: name, Expression: expression}
}

func foreignKey(name, column, table string) *ast.ConstraintNode {
	return ast.NewForeignKeyConstraint(name, []string{column}, &ast.ForeignKeyRef{Table: table, Column: "id"})
}

// TestApply_WritesATableAsTheCatalogPrintsIt_HappyPath pins the shape of a new
// table on each engine the community binary has a driver for: the key and the
// checks and foreign keys the plan adds after it inside it, in the order a catalog
// read lists them, each type under the catalog's name, and NULL on a nullable
// column.
func TestApply_WritesATableAsTheCatalogPrintsIt_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{
			name:    "PostgreSQL",
			dialect: platform.Postgres,
			want: "CREATE TABLE \"owners\" (\n  \"id\" integer NOT NULL,\n  PRIMARY KEY (\"id\")\n);\n\n" +
				"CREATE TABLE \"items\" (\n  \"id\" integer NOT NULL,\n  \"note\" character varying(10) NULL,\n  \"owner\" integer NULL,\n  PRIMARY KEY (\"id\"),\n" +
				"  CONSTRAINT \"fk\" FOREIGN KEY (\"owner\") REFERENCES \"owners\" (\"id\") ON UPDATE NO ACTION ON DELETE NO ACTION,\n" +
				"  CONSTRAINT \"ck\" CHECK (owner > 0)\n);\n\n",
		},
		{
			name:    "MySQL",
			dialect: platform.MySQL,
			want: "CREATE TABLE `owners` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n);\n\n" +
				"CREATE TABLE `items` (\n  `id` int NOT NULL,\n  `note` varchar(10) NULL,\n  `owner` int NULL,\n  PRIMARY KEY (`id`),\n" +
				"  CONSTRAINT `fk` FOREIGN KEY (`owner`) REFERENCES `owners` (`id`) ON UPDATE NO ACTION ON DELETE NO ACTION,\n" +
				"  CONSTRAINT `ck` CHECK (owner > 0)\n) ENGINE=InnoDB;\n\n",
		},
		{
			name:    "MariaDB",
			dialect: platform.MariaDB,
			want: "CREATE TABLE `owners` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n);\n\n" +
				"CREATE TABLE `items` (\n  `id` int NOT NULL,\n  `note` varchar(10) NULL,\n  `owner` int NULL,\n  PRIMARY KEY (`id`),\n" +
				"  CONSTRAINT `fk` FOREIGN KEY (`owner`) REFERENCES `owners` (`id`) ON UPDATE RESTRICT ON DELETE RESTRICT,\n" +
				"  CONSTRAINT `ck` CHECK (owner > 0)\n) ENGINE=InnoDB;\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes := []ast.Node{
				ast.NewCreateTable("owners").AddColumn(key("id")),
				ast.NewCreateTable("items").AddColumn(key("id")).AddColumn(nullable("note", "varchar(10)")).AddColumn(nullable("owner", "int")),
				addConstraint("items", checkConstraint("ck", "owner > 0")),
				addConstraint("items", foreignKey("fk", "owner", "owners")),
			}

			got := planshape.Apply(test.dialect, nodes, nil)

			c.Assert(must.Must(builtin.RenderSQL(test.dialect, got...)), qt.Equals, test.want)
		})
	}
}

// TestApply_SpellsATypeUnderTheCatalogsName_HappyPath is the type of an added
// column.
func TestApply_SpellsATypeUnderTheCatalogsName_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		spelled string
		want    string
	}{
		{name: "int on PostgreSQL", dialect: platform.Postgres, spelled: "int", want: "integer"},
		{name: "int8 on PostgreSQL", dialect: platform.Postgres, spelled: "INT8", want: "bigint"},
		{name: "varchar on PostgreSQL", dialect: platform.Postgres, spelled: "varchar", want: "character varying"},
		{name: "a sized varchar on PostgreSQL", dialect: platform.Postgres, spelled: "VARCHAR (20)", want: "character varying(20)"},
		{name: "char on PostgreSQL", dialect: platform.Postgres, spelled: "char(3)", want: "character(3)"},
		{name: "varbit on PostgreSQL", dialect: platform.Postgres, spelled: "varbit(3)", want: "bit varying(3)"},
		{name: "an array on PostgreSQL", dialect: platform.Postgres, spelled: "int[]", want: "integer[]"},
		{name: "a precision without a zone", dialect: platform.Postgres, spelled: "timestamp(3) without time zone", want: "timestamp(3)"},
		{name: "a precision with a zone", dialect: platform.Postgres, spelled: "TIMESTAMP (3) WITH TIME ZONE", want: "timestamptz(3)"},
		{name: "a time's zone", dialect: platform.Postgres, spelled: "time(3) with time zone", want: "timetz(3)"},
		{name: "a precision in an array", dialect: platform.Postgres, spelled: "timestamp(3) without time zone[]", want: "timestamp(3) without time zone[]"},
		{name: "a precision in an array of the short name", dialect: platform.Postgres, spelled: "timestamp(3)[]", want: "timestamp(3) without time zone[]"},
		{name: "a zoned array", dialect: platform.Postgres, spelled: "timestamptz(3)[]", want: "timestamp(3) with time zone[]"},
		{name: "an array with no precision", dialect: platform.Postgres, spelled: "timestamp[]", want: "timestamp without time zone[]"},
		{name: "a time array", dialect: platform.Postgres, spelled: "timetz[]", want: "time with time zone[]"},
		{name: "the zone's name without a precision", dialect: platform.Postgres, spelled: "timestamp with time zone", want: "timestamptz"},
		{name: "no zone's name without a precision", dialect: platform.Postgres, spelled: "timestamp without time zone", want: "timestamp"},
		{name: "a time's zone without a precision", dialect: platform.Postgres, spelled: "time with time zone", want: "timetz"},
		{name: "a timestamp keeps its short name on PostgreSQL", dialect: platform.Postgres, spelled: "timestamptz", want: "timestamptz"},
		{name: "a type the catalog does not list on PostgreSQL", dialect: platform.Postgres, spelled: "public.Money2", want: "public.Money2"},
		{name: "integer on MySQL", dialect: platform.MySQL, spelled: "INTEGER", want: "int"},
		{name: "a one-digit tinyint on MySQL", dialect: platform.MySQL, spelled: "tinyint(1)", want: "bool"},
		{name: "an enum keeps its letters on MySQL", dialect: platform.MySQL, spelled: "enum('A','b')", want: "enum('A','b')"},
		{name: "upper case on SQLite", dialect: platform.SQLite, spelled: "VARCHAR(200)", want: "varchar(200)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			column := nullable("c", test.spelled)
			nodes := []ast.Node{&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: column}}}}

			planshape.Apply(test.dialect, nodes, nil)

			c.Assert(column.Type, qt.Equals, test.want)
			c.Assert(column.ExplicitNull, qt.IsTrue)
		})
	}
}

// TestApply_AnEnumColumnQuotesItsType_HappyPath is the catalog's print of a
// column typed by an enum, and a serial, which its type makes NOT NULL.
func TestApply_ColumnsTheTypeDecides_HappyPath(t *testing.T) {
	c := qt.New(t)
	enum := nullable("s", "app.status")
	enum.EnumType = true
	serial := nullable("n", "serial")

	planshape.Apply(platform.Postgres, []ast.Node{ast.NewCreateTable("t").AddColumn(enum).AddColumn(serial)}, nil)

	c.Assert(enum.Type, qt.Equals, `"app"."status"`)
	c.Assert(serial.Nullable, qt.IsFalse)
	c.Assert(serial.ExplicitNull, qt.IsFalse)
}

// TestApply_LeavesAConstraintItCannotMove_FailurePath is each foreign key or check
// a new table cannot hold when it is created: a key into a table the plan creates
// later, which sends every other key of the plan after the tables too, a key into a
// table the plan changes, and a constraint behind a statement it may need.
func TestApply_LeavesAConstraintItCannotMove_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		nodes func() []ast.Node
		want  int
	}{
		{
			name: "tables that reference each other",
			nodes: func() []ast.Node {
				return []ast.Node{
					ast.NewCreateTable("a").AddColumn(key("id")).AddColumn(nullable("b_id", "int")),
					ast.NewCreateTable("b").AddColumn(key("id")).AddColumn(nullable("a_id", "int")),
					ast.NewCreateTable("c").AddColumn(key("id")).AddColumn(nullable("a_id", "int")),
					addConstraint("a", foreignKey("a_b", "b_id", "b")),
					addConstraint("b", foreignKey("b_a", "a_id", "a")),
					addConstraint("c", foreignKey("c_a", "a_id", "a")),
				}
			},
			want: 3,
		},
		{
			name: "a key into a table the plan changes behind another statement",
			nodes: func() []ast.Node {
				return []ast.Node{
					ast.NewCreateTable("c").AddColumn(key("id")).AddColumn(nullable("p_id", "int")),
					ast.NewRawSQL("UPDATE p SET n = 1;"),
					&ast.AlterTableNode{Name: "p", Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: nullable("n", "int")}}},
					addConstraint("c", foreignKey("c_p", "p_id", "p")),
				}
			},
			want: 2,
		},
		{
			name: "a check behind a routine",
			nodes: func() []ast.Node {
				return []ast.Node{
					ast.NewCreateTable("c").AddColumn(key("id")).AddColumn(nullable("n", "int")),
					ast.NewRawSQL("CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;"),
					addConstraint("c", checkConstraint("ck", "n > f()")),
				}
			},
			want: 1,
		},
		{
			name: "a constraint of a table of that name in another schema",
			nodes: func() []ast.Node {
				return []ast.Node{
					ast.NewCreateTable("a.t").AddColumn(key("id")),
					addConstraint("b.t", checkConstraint("ck", "id > 0")),
				}
			},
			want: 1,
		},
		{
			name: "a constraint of a table whose name differs in case",
			nodes: func() []ast.Node {
				return []ast.Node{
					ast.NewCreateTable("Users").AddColumn(key("id")),
					addConstraint("users", checkConstraint("ck", "id > 0")),
				}
			},
			want: 1,
		},
		{
			name: "a key into a table of that name in another schema, which the plan changes",
			nodes: func() []ast.Node {
				return []ast.Node{
					ast.NewCreateTable("a.t").AddColumn(key("id")).AddColumn(nullable("p_id", "int")),
					&ast.AlterTableNode{Name: "b.t", Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: nullable("n", "int")}}},
					addConstraint("a.t", foreignKey("a_b", "p_id", "b.t")),
				}
			},
			want: 2,
		},
		{
			name: "a key into a table of that name the plan creates later",
			nodes: func() []ast.Node {
				return []ast.Node{
					ast.NewCreateTable("a.t").AddColumn(key("id")).AddColumn(nullable("p_id", "int")),
					ast.NewCreateTable("b.t").AddColumn(key("id")),
					addConstraint("a.t", foreignKey("a_b", "p_id", "b.t")),
				}
			},
			want: 1,
		},
		{
			name: "a key naming a later table without its schema",
			nodes: func() []ast.Node {
				return []ast.Node{
					ast.NewCreateTable("c").AddColumn(key("id")).AddColumn(nullable("p_id", "int")),
					ast.NewCreateTable("a.p").AddColumn(key("id")),
					addConstraint("c", foreignKey("c_p", "p_id", "p")),
				}
			},
			want: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(platform.Postgres, test.nodes(), nil)

			c.Assert(alterCount(got), qt.Equals, test.want)
		})
	}
}

// alterCount is how many ALTER TABLE statements a plan keeps.
func alterCount(nodes []ast.Node) int {
	count := 0
	for _, node := range nodes {
		if _, ok := node.(*ast.AlterTableNode); ok {
			count++
		}
	}
	return count
}

// TestApply_AKeyOfATableToItself_HappyPath stays inside the table: nothing else
// has to exist first.
func TestApply_AKeyOfATableToItself_HappyPath(t *testing.T) {
	c := qt.New(t)

	got := planshape.Apply(platform.Postgres, []ast.Node{
		ast.NewCreateTable("t").AddColumn(key("id")).AddColumn(nullable("parent", "int")),
		addConstraint("t", foreignKey("t_parent", "parent", "t")),
	}, nil)

	c.Assert(alterCount(got), qt.Equals, 0)
}

// TestApply_AnIndexFollowsItsTable_HappyPath moves a new table's index to right
// after the table, so a foreign key of the next table may read it.
func TestApply_AnIndexFollowsItsTable_HappyPath(t *testing.T) {
	c := qt.New(t)
	first, second := ast.NewCreateTable("a").AddColumn(key("id")), ast.NewCreateTable("b").AddColumn(key("id"))
	index := ast.NewIndex("a_id", "a", "id")

	got := planshape.Apply(platform.Postgres, []ast.Node{first, second, index}, nil)

	c.Assert(got, qt.DeepEquals, []ast.Node{first, index, second})
}

// TestApply_AnIndexStaysBehindItsOwnTable_FailurePath keeps an index of b.t behind
// the column b.t gains: a new a.t is another table.
func TestApply_AnIndexStaysBehindItsOwnTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	created := ast.NewCreateTable("a.t").AddColumn(key("id"))
	changed := &ast.AlterTableNode{Name: "b.t", Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: nullable("n", "int")}}}
	index := ast.NewIndex("t_n", "b.t", "n")

	got := planshape.Apply(platform.Postgres, []ast.Node{created, changed, index}, nil)

	c.Assert(tablesOf(got), qt.DeepEquals, []string{"a.t", "b.t", "b.t"})
	c.Assert(got[0], qt.Equals, ast.Node(created))
	c.Assert(got[2], qt.Equals, ast.Node(index))
}

// TestApply_ASameNamedTableKeepsItsConstraint_FailurePath leaves new a.t with what
// its own statements gave it: a constraint of b.t stops on b.t.
func TestApply_ASameNamedTableKeepsItsConstraint_FailurePath(t *testing.T) {
	c := qt.New(t)
	created := ast.NewCreateTable("a.t").AddColumn(key("id"))

	planshape.Apply(platform.Postgres, []ast.Node{created, addConstraint("b.t", checkConstraint("ck", "id > 0"))}, nil)

	c.Assert(created.Constraints, qt.HasLen, 1)
	c.Assert(created.Constraints[0].Type, qt.Equals, ast.PrimaryKeyConstraint)
}

// TestApply_ANameWithTheSameLettersKeepsItsConstraint_FailurePath leaves a
// constraint on its own table where the new table's name reads the same with its
// quotes taken off.
func TestApply_ANameWithTheSameLettersKeepsItsConstraint_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, created, altered string }{
		{"a dot inside quotes", `"a.b"`, "a.b"},
		{"a quoted schema", `"a.b"`, `"a"."b"`},
		{"a doubled quote", `"a""b"`, "ab"},
		{"a quote a reader kept", `a"b`, "ab"},
		{"a doubled backquote", "`a``b`", "ab"},
		{"a backquote a reader kept", "a`b", "ab"},
		{"a schema of the same letters", `a.b.c`, `"a.b".c`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			created := ast.NewCreateTable(test.created).AddColumn(key("id"))

			planshape.Apply(platform.Postgres, []ast.Node{created, addConstraint(test.altered, checkConstraint("ck", "id > 0"))}, nil)

			c.Assert(created.Constraints, qt.HasLen, 1)
			c.Assert(created.Constraints[0].Type, qt.Equals, ast.PrimaryKeyConstraint)
		})
	}
}

// TestApply_OneNameInTwoSpellingsTakesItsConstraint_HappyPath moves a constraint
// onto a new table however a statement quotes the table's name.
func TestApply_OneNameInTwoSpellingsTakesItsConstraint_HappyPath(t *testing.T) {
	for _, test := range []struct{ name, created, altered string }{
		{"quotes on every part", "a.b", `"a"."b"`},
		{"quotes on one part", `a."b"`, `"a".b`},
		{"a quoted dot", `"a.b"`, `"a.b"`},
		{"a doubled quote and a kept one", `"a""b"`, `a"b`},
		{"backquotes", "`a`.`b`", "a.b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			created := ast.NewCreateTable(test.created).AddColumn(key("id"))

			planshape.Apply(platform.Postgres, []ast.Node{created, addConstraint(test.altered, checkConstraint("ck", "id > 0"))}, nil)

			c.Assert(created.Constraints, qt.HasLen, 2)
			c.Assert(created.Constraints[1].Name, qt.Equals, "ck")
		})
	}
}

// TestApply_AUniqueColumnTakesTheTablesCase_HappyPath writes the name PostgreSQL
// gives the key, which keeps the case of a quoted table's name.
func TestApply_AUniqueColumnTakesTheTablesCase_HappyPath(t *testing.T) {
	c := qt.New(t)
	column := nullable("code", "int")
	column.Unique = true
	table := ast.NewCreateTable("app.Users").AddColumn(column)

	planshape.Apply(platform.Postgres, []ast.Node{table}, nil)

	c.Assert(table.Constraints, qt.HasLen, 1)
	c.Assert(table.Constraints[0].Name, qt.Equals, "Users_code_key")
}

// TestApply_LeavesAnEngineTheBinaryHasNoDriverFor_FailurePath returns the plan as
// the planner wrote it.
func TestApply_LeavesAnEngineTheBinaryHasNoDriverFor_FailurePath(t *testing.T) {
	tests := []string{platform.SQLServer, platform.ClickHouse, platform.Oracle, platform.CockroachDB, "unknown"}
	for _, dialect := range tests {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			column := nullable("c", "int")
			table := ast.NewCreateTable("t").AddColumn(column)

			got := planshape.Apply(dialect, []ast.Node{table, addConstraint("t", checkConstraint("ck", "c > 0"))}, nil)

			c.Assert(got, qt.HasLen, 2)
			c.Assert(column.ExplicitNull, qt.IsFalse)
			c.Assert(column.Type, qt.Equals, "int")
		})
	}
}

// alter is an ALTER TABLE of the operations.
func alter(table string, operations ...ast.AlterOperation) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: table, Operations: operations}
}

func addColumn(name string) ast.AlterOperation {
	return &ast.AddColumnOperation{Column: nullable(name, "int")}
}

func dropColumn(name string) ast.AlterOperation {
	return &ast.DropColumnOperation{ColumnName: name}
}

func dropConstraint(name string) ast.AlterOperation {
	return &ast.DropConstraintOperation{ConstraintName: name}
}

func addKey(constraint *ast.ConstraintNode) ast.AlterOperation {
	return &ast.AddConstraintOperation{Constraint: constraint}
}

// foreignKeyTo is a foreign key on column into a named column of table.
func foreignKeyTo(name, column, table, referenced string) *ast.ConstraintNode {
	constraint := foreignKey(name, column, table)
	constraint.Reference.Column = referenced
	return constraint
}

// observedForeignKey is a foreign key as a comparison reads it off a table.
func observedForeignKey(table, name, column, foreignTable, foreignColumn string) catalog.Constraint {
	return catalog.Constraint{
		TableName: table, Name: name, Type: "FOREIGN KEY", ColumnNames: []string{column},
		ForeignTable: new(foreignTable), ForeignColumns: []string{foreignColumn},
	}
}

// observedKey is a primary key, unique constraint or check as a comparison reads it.
func observedKey(table, name, kind string, columns ...string) catalog.Constraint {
	return catalog.Constraint{TableName: table, Name: name, Type: kind, ColumnNames: columns}
}

// changedTable is a table a plan changes, with what a comparison read off it.
func changedTable(name string, constraints []catalog.Constraint, indexes ...catalog.Index) difftypes.TableDiff {
	return difftypes.TableDiff{TableName: name, Current: observedTable(name, constraints, indexes...)}
}

func observedTable(name string, constraints []catalog.Constraint, indexes ...catalog.Index) schemacapture.TableObservation {
	return schemacapture.TableObservation{Table: catalog.Table{Name: name}, Constraints: constraints, Indexes: indexes}
}

// droppedTables is a diff naming the tables a plan drops, as it read them.
func droppedTables(names ...string) *difftypes.SchemaDiff {
	removals := make(difftypes.TableRemovals, 0, len(names))
	for _, name := range names {
		removals = append(removals, difftypes.TableRemoval{Name: name, Current: observedTable(name, nil)})
	}
	return &difftypes.SchemaDiff{TablesRemoved: removals}
}

// TestApply_GroupsAndOrdersATablesChanges_HappyPath pins the statements of a plan as
// the community binary writes them: the tables the plan changes by name, each as its
// index drops, one ALTER TABLE and its new indexes, then the tables it creates, then
// the foreign keys that wait for a new table, then the tables it drops.
func TestApply_GroupsAndOrdersATablesChanges_HappyPath(t *testing.T) {
	c := qt.New(t)
	nodes := []ast.Node{
		ast.NewCreateTable("n").AddColumn(key("id")),
		alter("z", addColumn("a")),
		alter("b", addColumn("c")),
		alter("b", dropColumn("d")),
		ast.NewIndex("b_c", "b", "c"),
		ast.NewDropIndex("b_old").SetTable("b"),
		alter("z", &ast.AddConstraintOperation{Constraint: foreignKey("z_n", "a", "n")}),
		ast.NewDropTable("gone"),
	}

	got := planshape.Apply(platform.Postgres, nodes, droppedTables("gone"))

	c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Equals,
		"DROP INDEX \"b_old\";\n"+
			"ALTER TABLE \"b\" ADD COLUMN \"c\" integer NULL, DROP COLUMN \"d\";\n\n"+
			"CREATE INDEX \"b_c\" ON \"b\" (\"c\");\n"+
			"ALTER TABLE \"z\" ADD COLUMN \"a\" integer NULL;\n\n"+
			"CREATE TABLE \"n\" (\n  \"id\" integer NOT NULL,\n  PRIMARY KEY (\"id\")\n);\n\n"+
			"ALTER TABLE \"z\" ADD\nCONSTRAINT \"z_n\" FOREIGN KEY (\"a\") REFERENCES \"n\" (\"id\") ON UPDATE NO ACTION ON DELETE NO ACTION;\n\n"+
			"DROP TABLE \"gone\";\n")
}

// TestApply_MovesNothingOverAnotherStatement_FailurePath is a statement of another
// kind in the plan: what depends on it stays on its side, so the tables are ordered
// on each side of it and no further.
func TestApply_MovesNothingOverAnotherStatement_FailurePath(t *testing.T) {
	c := qt.New(t)
	late := alter("a", addColumn("c"))
	raw := ast.NewRawSQL("UPDATE z SET a = 1;")
	early := alter("z", addColumn("a"))

	got := planshape.Apply(platform.Postgres, []ast.Node{early, raw, late}, nil)

	c.Assert(got, qt.HasLen, 3)
	c.Assert(got[1], qt.Equals, ast.Node(raw))
	c.Assert(got[0].(*ast.AlterTableNode).Name, qt.Equals, "z")
	c.Assert(got[2].(*ast.AlterTableNode).Name, qt.Equals, "a")
}

// TestApply_LeavesATableStatementItCannotMove_FailurePath is a statement another may
// rely on as it stands: a request for an online change, and an operation a planner
// writes on its own. It ends the run of statements the pass orders.
func TestApply_LeavesATableStatementItCannotMove_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		kept *ast.AlterTableNode
	}{
		{name: "an algorithm", kept: &ast.AlterTableNode{Name: "m", Algorithm: "INPLACE", Operations: []ast.AlterOperation{addColumn("x")}}},
		{name: "a lock", kept: &ast.AlterTableNode{Name: "m", Lock: "NONE", Operations: []ast.AlterOperation{addColumn("x")}}},
		{name: "a rename", kept: alter("m", &ast.RenameColumnOperation{OldName: "x", NewName: "y"})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(platform.Postgres, []ast.Node{alter("z", addColumn("a")), test.kept, alter("a", addColumn("b"))}, nil)

			c.Assert(got, qt.HasLen, 3)
			c.Assert(got[1], qt.Equals, ast.Node(test.kept))
			c.Assert(got[0].(*ast.AlterTableNode).Name, qt.Equals, "z")
		})
	}
}

// TestApply_AGeneratedColumn_HappyPath writes an added generated column behind the
// column it reads, since PostgreSQL refuses the other order, whatever a chain of them
// says. A removed one stays on the side of the column it reads where the planner put it.
func TestApply_AGeneratedColumn_HappyPath(t *testing.T) {
	generated := func(name, expression string) ast.AlterOperation {
		column := nullable(name, "int")
		column.GeneratedExpression = expression
		return &ast.AddColumnOperation{Column: column}
	}
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		ops  func() []ast.AlterOperation
		want string
	}{
		{
			name: "a column added behind the one reading it",
			ops: func() []ast.AlterOperation {
				return []ast.AlterOperation{generated("doubled", "fresh * 2"), addColumn("fresh")}
			},
			want: `ALTER TABLE "t" ADD COLUMN "fresh" integer NULL, ADD COLUMN "doubled" integer NULL`,
		},
		{
			name: "a chain added last first",
			ops: func() []ast.AlterOperation {
				return []ast.AlterOperation{generated("g2", "g1 * 2"), generated("g1", "x * 2"), addColumn("x")}
			},
			want: `ALTER TABLE "t" ADD COLUMN "x" integer NULL, ADD COLUMN "g1" integer NULL`,
		},
		{
			name: "two reading each other, which the server refuses",
			ops: func() []ast.AlterOperation {
				return []ast.AlterOperation{generated("g1", "g2 * 2"), generated("g2", "g1 * 2")}
			},
			want: `ALTER TABLE "t" ADD COLUMN`,
		},
		{
			name: "a generated column and what it reads, dropped",
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:      "t",
				ColumnsRemoved: difftypes.ColumnChanges{{Name: "base"}, {Name: "derived", GeneratedExpression: "base * 2"}},
				Current:        observedTable("t", nil),
			}}},
			ops: func() []ast.AlterOperation {
				return []ast.AlterOperation{dropColumn("derived"), dropColumn("base")}
			},
			want: `ALTER TABLE "t" DROP COLUMN "derived", DROP COLUMN "base"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(platform.Postgres, []ast.Node{alter("t", test.ops()...)}, test.diff)

			c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Contains, test.want)
		})
	}
}

// TestApply_GroupsOnTheMySQLFamily_HappyPath writes a table's changes in one
// statement on the engines whose grammar takes it.
func TestApply_GroupsOnTheMySQLFamily_HappyPath(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(dialect, []ast.Node{alter("t", addColumn("a")), alter("t", dropColumn("b"))}, nil)

			c.Assert(must.Must(builtin.RenderSQL(dialect, got...)), qt.Contains, "ALTER TABLE `t` ADD COLUMN `a` int NULL, DROP COLUMN `b`;")
		})
	}
}

// TestApply_ForeignKeysThatWaitReadByTable_HappyPath orders the foreign keys that wait
// for a new table by the table they are on.
func TestApply_ForeignKeysThatWaitReadByTable_HappyPath(t *testing.T) {
	c := qt.New(t)
	nodes := []ast.Node{
		ast.NewCreateTable("n").AddColumn(key("id")),
		ast.NewCreateTable("o").AddColumn(key("id")),
		alter("z", &ast.AddConstraintOperation{Constraint: foreignKey("z_n", "a", "n")}),
		alter("b", &ast.AddConstraintOperation{Constraint: foreignKey("b_n", "a", "n")}),
	}

	got := planshape.Apply(platform.Postgres, nodes, nil)

	c.Assert(tablesOf(got), qt.DeepEquals, []string{"n", "o", "b", "z"})
}

// TestApply_IndexesOfATableReadByName_HappyPath orders the index drops and the new
// indexes of a table by name.
func TestApply_IndexesOfATableReadByName_HappyPath(t *testing.T) {
	c := qt.New(t)
	nodes := []ast.Node{
		ast.NewIndex("t_z", "t", "a"),
		ast.NewIndex("t_b", "t", "a"),
		ast.NewDropIndex("t_y").SetTable("t"),
		ast.NewDropIndex("t_c").SetTable("t"),
	}

	got := planshape.Apply(platform.Postgres, nodes, nil)

	c.Assert(got, qt.HasLen, 4)
	c.Assert(got[0].(*ast.DropIndexNode).Name, qt.Equals, "t_c")
	c.Assert(got[1].(*ast.DropIndexNode).Name, qt.Equals, "t_y")
	c.Assert(got[2].(*ast.IndexNode).Name, qt.Equals, "t_b")
	c.Assert(got[3].(*ast.IndexNode).Name, qt.Equals, "t_z")
}

// TestApply_AForeignKeyDropLeadsOtherTables_HappyPath keeps a foreign key's drop in
// front where another table's change needs the key gone. Otherwise it joins its table's
// statement, as the community binary writes it.
func TestApply_AForeignKeyDropLeadsOtherTables_HappyPath(t *testing.T) {
	keys := []catalog.Constraint{
		observedForeignKey("z", "z_fk", "p_id", "p", "id"),
		observedKey("z", "z_ck", "CHECK", "n"),
	}
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{
		changedTable("z", keys),
		changedTable("p", nil, catalog.Index{Name: "p_id", TableName: "p", Columns: []string{"id"}, IsUnique: true}),
	}}
	tests := []struct {
		name string
		drop ast.AlterOperation
		rest []ast.Node
		want []string
	}{
		{name: "alone", drop: dropConstraint("z_fk"), want: []string{"z"}},
		{name: "beside a table that does not need it gone", drop: dropConstraint("z_fk"), rest: []ast.Node{alter("a", dropColumn("x"))}, want: []string{"a", "z"}},
		{name: "ahead of a column it references", drop: dropConstraint("z_fk"), rest: []ast.Node{alter("p", dropColumn("id"))}, want: []string{"z", "p", "z"}},
		{name: "ahead of an index it stands on", drop: dropConstraint("z_fk"), rest: []ast.Node{ast.NewDropIndex("p_id").SetTable("p")}, want: []string{"z", "p", "z"}},
		{name: "another constraint", drop: dropConstraint("z_ck"), rest: []ast.Node{alter("p", dropColumn("id"))}, want: []string{"p", "z"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes := append([]ast.Node{alter("z", test.drop, dropColumn("y"))}, test.rest...)

			got := planshape.Apply(platform.Postgres, nodes, diff)

			c.Assert(tablesOf(got), qt.DeepEquals, test.want)
		})
	}
}

// tablesOf names the table each statement is on.
func tablesOf(nodes []ast.Node) []string {
	names := make([]string, 0, len(nodes))
	for _, node := range nodes {
		switch n := node.(type) {
		case *ast.AlterTableNode:
			names = append(names, n.Name)
		case *ast.IndexNode:
			names = append(names, n.Table)
		case *ast.DropIndexNode:
			names = append(names, n.Table)
		case *ast.CreateTableNode:
			names = append(names, n.Name)
		case *ast.DropTableNode:
			names = append(names, n.TableNames()...)
		}
	}
	return names
}

// TestApply_AnEngineThatTakesOneChangeAtATime_HappyPath keeps SQLite's ALTER TABLE
// one operation each, in the same order.
func TestApply_AnEngineThatTakesOneChangeAtATime_HappyPath(t *testing.T) {
	c := qt.New(t)

	got := planshape.Apply(platform.SQLite, []ast.Node{alter("b", addColumn("x")), alter("a", addColumn("y")), alter("a", addColumn("z"))}, nil)

	c.Assert(alterCount(got), qt.Equals, 3)
	c.Assert(got[0].(*ast.AlterTableNode).Name, qt.Equals, "a")
	c.Assert(got[0].(*ast.AlterTableNode).OneStatement, qt.IsNil)
}

// TestAlignRollback_ReadsAsTheForwardPlanBackwards_HappyPath is a rollback planned in
// another order. Each statement moves to where the one it undoes puts it, the
// operations of an ALTER TABLE reverse, and the constraint forward wrote inside a
// table goes with the table.
func TestAlignRollback_ReadsAsTheForwardPlanBackwards_HappyPath(t *testing.T) {
	c := qt.New(t)
	created := ast.NewCreateTable("n").AddColumn(key("id")).AddConstraint(checkConstraint("n_ck", "id > 0"))
	forward := []ast.Node{
		alter("a", addColumn("x"), dropColumn("y")),
		ast.NewIndex("a_x", "a", "x"),
		created,
		ast.NewDropTable("gone"),
	}
	rollback := []ast.Node{
		ast.NewCreateTable("gone").AddColumn(key("id")),
		alter("n", &ast.DropConstraintOperation{ConstraintName: "n_ck"}),
		ast.NewDropIndex("a_x").SetTable("a"),
		alter("a", addColumn("y"), dropColumn("x")),
		ast.NewDropTable("n"),
	}

	got := planshape.AlignRollback(platform.Postgres, forward, rollback, nil)

	c.Assert(got, qt.HasLen, 4)
	c.Assert(got[0].(*ast.CreateTableNode).Name, qt.Equals, "gone")
	c.Assert(got[1].(*ast.DropTableNode).TableNames(), qt.DeepEquals, []string{"n"})
	c.Assert(got[2].(*ast.DropIndexNode).Name, qt.Equals, "a_x")
	operations := got[3].(*ast.AlterTableNode).Operations
	c.Assert(operations[0].(*ast.AddColumnOperation).Column.Name, qt.Equals, "y")
	c.Assert(operations[1].(*ast.DropColumnOperation).ColumnName, qt.Equals, "x")
}

// TestAlignRollback_LeavesAnotherEngineAndAnotherStatement_FailurePath is an engine
// the community binary has no driver for, and a statement of another kind, which
// holds its place.
func TestAlignRollback_LeavesAnotherEngineAndAnotherStatement_FailurePath(t *testing.T) {
	forward := []ast.Node{ast.NewCreateTable("a").AddColumn(key("id")), ast.NewCreateTable("b").AddColumn(key("id"))}
	raw := ast.NewRawSQL("DELETE FROM a;")
	dropA, dropB := ast.NewDropTable("a"), ast.NewDropTable("b")
	tests := []struct {
		name    string
		dialect string
		want    []ast.Node
	}{
		{name: "an engine with no reference", dialect: platform.SQLServer, want: []ast.Node{dropA, raw, dropB}},
		{name: "a statement of another kind", dialect: platform.Postgres, want: []ast.Node{dropA, raw, dropB}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.AlignRollback(test.dialect, forward, []ast.Node{dropA, raw, dropB}, nil)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestAlignRollback_AnEngineWithNoReference_FailurePath leaves a rollback in the
// order it was planned on an engine the community binary has no driver for.
func TestAlignRollback_AnEngineWithNoReference_FailurePath(t *testing.T) {
	c := qt.New(t)
	forward := []ast.Node{ast.NewCreateTable("a").AddColumn(key("id")), ast.NewCreateTable("b").AddColumn(key("id"))}
	dropA, dropB := ast.NewDropTable("a"), ast.NewDropTable("b")

	got := planshape.AlignRollback(platform.SQLServer, forward, []ast.Node{dropA, dropB}, nil)

	c.Assert(got, qt.DeepEquals, []ast.Node{dropA, dropB})
}

// A key written apart from its columns makes them NOT NULL in the catalog, so they
// read that way here. Writing NULL beside the key, PostgreSQL refuses the table with
// `conflicting NULL/NOT NULL declarations`. A key does not make a column NOT NULL on
// SQLite, so SQLite keeps what was declared.
func TestApply_TheColumnsOfAKey_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{
			name:    "PostgreSQL",
			dialect: platform.Postgres,
			want:    "CREATE TABLE \"t\" (\n  \"a\" integer NOT NULL,\n  \"b\" integer NOT NULL,\n  \"c\" integer NULL,\n  PRIMARY KEY (\"a\", \"b\")\n);\n\n",
		},
		{
			name:    "MySQL",
			dialect: platform.MySQL,
			want:    "CREATE TABLE `t` (\n  `a` int NOT NULL,\n  `b` int NOT NULL,\n  `c` int NULL,\n  PRIMARY KEY (`a`, `b`)\n);\n\n",
		},
		{
			name:    "SQLite",
			dialect: platform.SQLite,
			want:    "CREATE TABLE \"t\" (\n  \"a\" int NULL,\n  \"b\" int NULL,\n  \"c\" int NULL,\n  PRIMARY KEY (\"a\", \"b\")\n);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := ast.NewCreateTable("t").AddColumn(nullable("a", "int")).AddColumn(nullable("b", "int")).AddColumn(nullable("c", "int"))
			table.AddConstraint(ast.NewPrimaryKeyConstraint("a", "b"))

			got := planshape.Apply(test.dialect, []ast.Node{table}, nil)

			c.Assert(must.Must(builtin.RenderSQL(test.dialect, got...)), qt.Equals, test.want)
		})
	}
}

// The name PostgreSQL gives a NOT NULL nobody named stays out of the statement, and a
// name the author chose stays in it.
func TestApply_TheNameOfANotNull_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		table  string
		column string
		named  string
		want   string
	}{
		{name: "the server's own name", table: "t", column: "c", named: "t_c_not_null", want: ""},
		{name: "the server's own name, of a quoted table", table: "Tbl", column: "c", named: "Tbl_c_not_null", want: ""},
		{name: "the server's own name, in a schema", table: "app.t", column: "c", named: "t_c_not_null", want: ""},
		{
			name:   "the server's cut name for a long column",
			table:  "t",
			column: "a_column_name_that_is_much_longer_than_fifty_four_bytes_x",
			named:  "t_a_column_name_that_is_much_longer_than_fifty_four_by_not_null",
			want:   "",
		},
		{name: "the author's name", table: "t", column: "c", named: "c_required", want: "c_required"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			column := ast.NewColumn(test.column, "int")
			column.NotNullConstraintName = test.named

			planshape.Apply(platform.Postgres, []ast.Node{ast.NewCreateTable(test.table).AddColumn(column)}, nil)

			c.Assert(column.NotNullConstraintName, qt.Equals, test.want)
		})
	}
}

func TestApply_AddColumnOmitsTheServerCutNotNullName(t *testing.T) {
	c := qt.New(t)
	const columnName = "a_column_name_that_is_much_longer_than_fifty_four_bytes_x"
	column := ast.NewColumn(columnName, "text").SetNotNull()
	column.NotNullConstraintName = "t_a_column_name_that_is_much_longer_than_fifty_four_by_not_null"
	alter := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: column}}}

	nodes := planshape.Apply(platform.Postgres, []ast.Node{alter}, nil)
	got := must.Must(builtin.RenderSQL(platform.Postgres, nodes...))

	wantComment := `-- POSTGRES: ADD COLUMN "` + columnName + `" fails if "t" holds any row; the column is NOT NULL and declares no default to fill it with.` + "\n"
	c.Assert(got, qt.Equals, wantComment+`ALTER TABLE "t" ADD COLUMN "`+columnName+`" text NOT NULL;`+"\n\n")
}

// Constraints of one kind read by name, as a catalog lists them.
func TestApply_ConstraintsOfOneKindByName_HappyPath(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("t").AddColumn(key("id")).AddColumn(nullable("a", "int"))
	table.AddConstraint(checkConstraint("z_ck", "a > 0"))
	table.AddConstraint(checkConstraint("a_ck", "a < 9"))

	planshape.Apply(platform.Postgres, []ast.Node{table}, nil)

	names := make([]string, 0, len(table.Constraints))
	for _, constraint := range table.Constraints {
		names = append(names, constraint.Name)
	}
	c.Assert(names, qt.DeepEquals, []string{"", "a_ck", "z_ck"})
}

// New tables read by how long a chain of new tables each needs ahead of it, then by
// name, each with its indexes. SQLite keeps the planner's order.
func TestApply_OrdersNewTablesByTheChainTheyNeed_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{name: "PostgreSQL", dialect: platform.Postgres, want: []string{"owners", "pairs", "self", "items", "items_i", "tags"}},
		{name: "MySQL", dialect: platform.MySQL, want: []string{"owners", "pairs", "self", "items", "items_i", "tags"}},
		{name: "SQLite", dialect: platform.SQLite, want: []string{"owners", "items", "items_i", "tags", "pairs", "self"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes := []ast.Node{
				ast.NewCreateTable("owners").AddColumn(key("id")),
				ast.NewCreateTable("items").AddColumn(key("id")).AddColumn(nullable("owner", "int")),
				addConstraint("items", foreignKey("items_owner", "owner", "owners")),
				ast.NewIndex("items_i", "items", "owner"),
				ast.NewCreateTable("tags").AddColumn(key("id")).AddColumn(nullable("item", "int")),
				addConstraint("tags", foreignKey("tags_item", "item", "items")),
				ast.NewCreateTable("pairs").AddColumn(key("id")),
				ast.NewCreateTable("self").AddColumn(key("id")).AddColumn(nullable("parent", "int")),
				addConstraint("self", foreignKey("self_parent", "parent", "self")),
			}

			got := planshape.Apply(test.dialect, nodes, nil)

			names := make([]string, 0, len(got))
			for _, node := range got {
				names = append(names, createdName(node))
			}
			c.Assert(names, qt.DeepEquals, test.want)
		})
	}
}

// createdName names a statement a plan writes for a new table.
func createdName(node ast.Node) string {
	if table, ok := node.(*ast.CreateTableNode); ok {
		return table.Name
	}
	return node.(*ast.IndexNode).Name
}

// A plan may change a column twice, stepping it through a state it needs between two
// statements. Written as one statement on the MySQL family, the last change says it all:
// the server answers `Unknown column` to a column named twice. PostgreSQL writes what
// changed, so each stays.
func TestApply_AColumnChangedTwice_HappyPath(t *testing.T) {
	modify := func(columnType string) ast.AlterOperation {
		return &ast.ModifyColumnOperation{Column: nullable("c", columnType)}
	}
	tests := []struct {
		name    string
		dialect string
		want    int
	}{
		{name: "MySQL", dialect: platform.MySQL, want: 1},
		{name: "MariaDB", dialect: platform.MariaDB, want: 1},
		{name: "PostgreSQL", dialect: platform.Postgres, want: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(test.dialect, []ast.Node{alter("t", modify("int")), alter("t", modify("bigint"))}, nil)

			c.Assert(got, qt.HasLen, 1)
			operations := got[0].(*ast.AlterTableNode).OneStatement
			c.Assert(operations, qt.HasLen, test.want)
			c.Assert(operations[len(operations)-1].(*ast.ModifyColumnOperation).Column.Type, qt.Equals, "bigint")
		})
	}
}

// addCComment is what a plan says above the statement that adds column c to t.
const addCComment = `-- POSTGRES: ADD COLUMN "c" fails if "t" holds any row; the column is NOT NULL and declares no default to fill it with.` + "\n"

// heldNotNullName is a diff that adds column c under the name a removed column holds,
// and the two statements as the planner writes them.
func heldNotNullName(table, name string) (diff *difftypes.SchemaDiff, add, drop *ast.AlterTableNode) {
	column := ast.NewColumn("c", "text").SetNotNull()
	column.NotNullConstraintName = name
	diff = &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:      table,
		ColumnsAdded:   []schemamodel.Field{{Name: "c", Type: "text", NotNullConstraintName: name}},
		ColumnsRemoved: []schemamodel.Field{{Name: "old", Type: "text", NotNullConstraintName: name}},
	}}}
	return diff, alter(table, &ast.AddColumnOperation{Column: column}), alter(table, dropColumn("old"))
}

// freesHeldName is the rename the planner writes ahead of the statement that takes name.
func freesHeldName(table, name string) *ast.AlterTableNode {
	return alter(table, &ast.RenameConstraintOperation{From: name, To: name + "__ptah_old"})
}

// A rollback's planner freed a name for its own order, one statement per change. Merged
// with the drop of the column holding it, the addition needs no such thing: PostgreSQL
// runs the drops of a statement first. A statement of another kind between them keeps
// them apart, and there the rename stays. It also stays behind the name the server gives
// anyway, which the statement leaves out: the server gives that name only once it is free.
func TestAlignRollback_ANotNullNameStillHeld_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		between []ast.Node
		column  string
		want    string
	}{
		{
			name:   "merged with the drop",
			column: "x",
			want:   addCComment + `ALTER TABLE "t" DROP COLUMN "old", ADD COLUMN "c" text CONSTRAINT "x" NOT NULL;` + "\n\n",
		},
		{
			name:    "kept apart from the drop",
			between: []ast.Node{ast.NewRawSQL("SELECT 1;")},
			column:  "x",
			want: `ALTER TABLE "t" RENAME CONSTRAINT "x" TO "x__ptah_old";` + "\n\n" +
				addCComment + `ALTER TABLE "t" ADD COLUMN "c" text CONSTRAINT "x" NOT NULL;` + "\n\n" +
				"SELECT 1;\n" +
				`ALTER TABLE "t" DROP COLUMN "old";` + "\n\n",
		},
		{
			name:    "kept apart from the drop, under the name the server gives",
			between: []ast.Node{ast.NewRawSQL("SELECT 1;")},
			column:  "t_c_not_null",
			want: `ALTER TABLE "t" RENAME CONSTRAINT "t_c_not_null" TO "t_c_not_null__ptah_old";` + "\n\n" +
				addCComment + `ALTER TABLE "t" ADD COLUMN "c" text NOT NULL;` + "\n\n" +
				"SELECT 1;\n" +
				`ALTER TABLE "t" DROP COLUMN "old";` + "\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff, add, drop := heldNotNullName("t", test.column)
			planned := slices.Concat([]ast.Node{freesHeldName("t", test.column), add}, test.between, []ast.Node{drop})

			forward := []ast.Node{alter("t", dropColumn("c"), addColumn("old"))}

			got := planshape.AlignRollback(platform.Postgres, forward, planned, diff)

			c.Assert(must.Must(builtin.RenderSQLWithCapabilities(platform.Postgres, capability.Postgres18(), got...)), qt.Equals, test.want)
		})
	}
}

// The forward plan reads the same: the merged statement needs no rename, and where a
// statement of another kind keeps the two apart the rename goes ahead of the addition.
func TestApply_ANotNullNameStillHeld_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		between []ast.Node
		want    string
	}{
		{
			name: "merged with the drop",
			want: addCComment + `ALTER TABLE "t" DROP COLUMN "old", ADD COLUMN "c" text CONSTRAINT "x" NOT NULL;` + "\n\n",
		},
		{
			name:    "kept apart from the drop",
			between: []ast.Node{ast.NewRawSQL("SELECT 1;")},
			want: `ALTER TABLE "t" RENAME CONSTRAINT "x" TO "x__ptah_old";` + "\n\n" +
				addCComment + `ALTER TABLE "t" ADD COLUMN "c" text CONSTRAINT "x" NOT NULL;` + "\n\n" +
				"SELECT 1;\n" +
				`ALTER TABLE "t" DROP COLUMN "old";` + "\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff, add, drop := heldNotNullName("t", "x")

			got := planshape.Apply(platform.Postgres, slices.Concat([]ast.Node{add}, test.between, []ast.Node{drop}), diff)

			c.Assert(must.Must(builtin.RenderSQLWithCapabilities(platform.Postgres, capability.Postgres18(), got...)), qt.Equals, test.want)
		})
	}
}
