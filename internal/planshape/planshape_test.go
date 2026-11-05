package planshape_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
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

			got := planshape.Apply(test.dialect, nodes)

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

			planshape.Apply(test.dialect, nodes)

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

	planshape.Apply(platform.Postgres, []ast.Node{ast.NewCreateTable("t").AddColumn(enum).AddColumn(serial)})

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
			name: "a key into a table the plan changes",
			nodes: func() []ast.Node {
				return []ast.Node{
					ast.NewCreateTable("c").AddColumn(key("id")).AddColumn(nullable("p_id", "int")),
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(platform.Postgres, test.nodes())

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
	})

	c.Assert(alterCount(got), qt.Equals, 0)
}

// TestApply_AnIndexFollowsItsTable_HappyPath moves a new table's index to right
// after the table, so a foreign key of the next table may read it.
func TestApply_AnIndexFollowsItsTable_HappyPath(t *testing.T) {
	c := qt.New(t)
	first, second := ast.NewCreateTable("a").AddColumn(key("id")), ast.NewCreateTable("b").AddColumn(key("id"))
	index := ast.NewIndex("a_id", "a", "id")

	got := planshape.Apply(platform.Postgres, []ast.Node{first, second, index})

	c.Assert(got, qt.DeepEquals, []ast.Node{first, index, second})
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

			got := planshape.Apply(dialect, []ast.Node{table, addConstraint("t", checkConstraint("ck", "c > 0"))})

			c.Assert(got, qt.HasLen, 2)
			c.Assert(column.ExplicitNull, qt.IsFalse)
			c.Assert(column.Type, qt.Equals, "int")
		})
	}
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

			got := planshape.Apply(test.dialect, []ast.Node{table})

			c.Assert(must.Must(builtin.RenderSQL(test.dialect, got...)), qt.Equals, test.want)
		})
	}
}

// The name PostgreSQL gives a NOT NULL nobody named stays out of the statement, and a
// name the author chose stays in it.
func TestApply_TheNameOfANotNull_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		named string
		want  string
	}{
		{name: "the server's own name", named: "t_c_not_null", want: ""},
		{name: "the author's name", named: "c_required", want: "c_required"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			column := ast.NewColumn("c", "int")
			column.NotNullConstraintName = test.named

			planshape.Apply(platform.Postgres, []ast.Node{ast.NewCreateTable("t").AddColumn(column)})

			c.Assert(column.NotNullConstraintName, qt.Equals, test.want)
		})
	}
}

// Constraints of one kind read by name, as a catalog lists them.
func TestApply_ConstraintsOfOneKindByName_HappyPath(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("t").AddColumn(key("id")).AddColumn(nullable("a", "int"))
	table.AddConstraint(checkConstraint("z_ck", "a > 0"))
	table.AddConstraint(checkConstraint("a_ck", "a < 9"))

	planshape.Apply(platform.Postgres, []ast.Node{table})

	names := make([]string, 0, len(table.Constraints))
	for _, constraint := range table.Constraints {
		names = append(names, constraint.Name)
	}
	c.Assert(names, qt.DeepEquals, []string{"", "a_ck", "z_ck"})
}
