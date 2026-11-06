package planshape_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planshape"
)

func uniqueKey(name, column string) *ast.ConstraintNode {
	return ast.NewUniqueConstraint(name, column)
}

// modifyColumn is a column restated as MySQL and MariaDB write a change to it.
func modifyColumn(name, typeName string) ast.AlterOperation {
	return &ast.ModifyColumnOperation{Column: nullable(name, typeName)}
}

// selfKeyDiff is a table holding a foreign key to itself.
func selfKeyDiff() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{
		changedTable("t", []catalog.Constraint{observedForeignKey("t", "fk", "parent_id", "t", "id")}),
	}}
}

// keyDiff is a child holding a foreign key on p_id, and an index on the same column.
func keyDiff() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{
		changedTable("c", []catalog.Constraint{observedForeignKey("c", "fk", "p_id", "p", "id")},
			catalog.Index{Name: "c_p", TableName: "c", Columns: []string{"p_id"}}),
	}}
}

// TestApply_WhatDependsKeepsThePlannersOrder_HappyPath is a plan where the binary's order
// by table name would put a statement ahead of what it needs. The order inside each row
// is the planner's, as a server takes it; the pass moves only what it can show is free.
func TestApply_WhatDependsKeepsThePlannersOrder_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		nodes   func() []ast.Node
		diff    func() *difftypes.SchemaDiff
		want    string
	}{
		{
			name: "a key into a column a later table gains", dialect: platform.Postgres,
			nodes: func() []ast.Node {
				return []ast.Node{
					alter("a", addColumn("z_code")),
					alter("z", addColumn("code")),
					alter("z", addKey(uniqueKey("z_code_key", "code"))),
					alter("a", addKey(foreignKeyTo("a_z", "z_code", "z", "code"))),
				}
			},
			want: "ALTER TABLE \"z\" ADD COLUMN \"code\" integer NULL, ADD CONSTRAINT \"z_code_key\" UNIQUE (\"code\");\n\n" +
				"ALTER TABLE \"a\" ADD COLUMN \"z_code\" integer NULL, ADD\n" +
				"CONSTRAINT \"a_z\" FOREIGN KEY (\"z_code\") REFERENCES \"z\" (\"code\") ON UPDATE NO ACTION ON DELETE NO ACTION;\n\n",
		},
		{
			name: "a key into a column another table already has", dialect: platform.Postgres,
			nodes: func() []ast.Node {
				return []ast.Node{
					alter("z", addColumn("other")),
					alter("a", addColumn("z_id")),
					alter("a", addKey(foreignKeyTo("a_z", "z_id", "z", "id"))),
				}
			},
			want: "ALTER TABLE \"z\" ADD COLUMN \"other\" integer NULL;\n\n" +
				"ALTER TABLE \"a\" ADD COLUMN \"z_id\" integer NULL, ADD\n" +
				"CONSTRAINT \"a_z\" FOREIGN KEY (\"z_id\") REFERENCES \"z\" (\"id\") ON UPDATE NO ACTION ON DELETE NO ACTION;\n\n",
		},
		{
			name: "a key dropped with its column", dialect: platform.Postgres,
			nodes: func() []ast.Node {
				return []ast.Node{alter("c", dropConstraint("fk")), alter("c", dropColumn("p_id"))}
			},
			diff: keyDiff,
			want: "ALTER TABLE \"c\" DROP CONSTRAINT \"fk\", DROP COLUMN \"p_id\";\n\n",
		},
		{
			name: "a key dropped ahead of the index it needs", dialect: platform.MySQL,
			nodes: func() []ast.Node {
				return []ast.Node{alter("c", &ast.DropConstraintOperation{ConstraintName: "fk", ForeignKey: true}), ast.NewDropIndex("c_p").SetTable("c")}
			},
			diff: keyDiff,
			want: "ALTER TABLE `c` DROP FOREIGN KEY `fk`;\n\nDROP INDEX `c_p` ON `c`;\n",
		},
		{
			name: "an index dropped ahead of a key on PostgreSQL, where it backs none", dialect: platform.Postgres,
			nodes: func() []ast.Node {
				return []ast.Node{alter("c", dropConstraint("fk")), ast.NewDropIndex("c_p").SetTable("c")}
			},
			diff: keyDiff,
			want: "DROP INDEX \"c_p\";\nALTER TABLE \"c\" DROP CONSTRAINT \"fk\";\n\n",
		},
		{
			name: "a change at each end of a key that stays", dialect: platform.Postgres,
			nodes: func() []ast.Node {
				return []ast.Node{alter("z", modifyColumn("id", "bigint")), alter("a", modifyColumn("z_id", "bigint"))}
			},
			diff: func() *difftypes.SchemaDiff {
				return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{
					changedTable("a", []catalog.Constraint{observedForeignKey("a", "a_z", "z_id", "z", "id")}),
				}}
			},
			want: "ALTER TABLE \"z\" ALTER COLUMN \"id\" TYPE bigint, ALTER COLUMN \"id\" DROP NOT NULL, ALTER COLUMN \"id\" DROP DEFAULT;\n\n" +
				"ALTER TABLE \"a\" ALTER COLUMN \"z_id\" TYPE bigint, ALTER COLUMN \"z_id\" DROP NOT NULL, ALTER COLUMN \"z_id\" DROP DEFAULT;\n\n",
		},
		{
			name: "an index name moving to a table that sorts earlier", dialect: platform.Postgres,
			nodes: func() []ast.Node {
				return []ast.Node{ast.NewDropIndex("idx_x").SetTable("z"), ast.NewIndex("idx_x", "a", "x")}
			},
			want: "DROP INDEX \"idx_x\";\nCREATE INDEX \"idx_x\" ON \"a\" (\"x\");\n",
		},
		{
			name: "a column typed by a new table", dialect: platform.Postgres,
			nodes: func() []ast.Node {
				return []ast.Node{
					ast.NewCreateTable("n").AddColumn(key("id")),
					alter("a", &ast.AddColumnOperation{Column: nullable("payload", "n")}),
				}
			},
			want: "CREATE TABLE \"n\" (\n  \"id\" integer NOT NULL,\n  PRIMARY KEY (\"id\")\n);\n\n" +
				"ALTER TABLE \"a\" ADD COLUMN \"payload\" n NULL;\n\n",
		},
		{
			name: "two tables of one name in two schemas", dialect: platform.Postgres,
			nodes: func() []ast.Node {
				return []ast.Node{alter("b.t", dropColumn("secret")), alter("a.t", addColumn("added"))}
			},
			want: "ALTER TABLE \"a\".\"t\" ADD COLUMN \"added\" integer NULL;\n\nALTER TABLE \"b\".\"t\" DROP COLUMN \"secret\";\n\n",
		},
		{
			name: "two tables whose names differ in case", dialect: platform.Postgres,
			nodes: func() []ast.Node {
				return []ast.Node{alter("users", addColumn("lower_col")), alter("Users", addColumn("upper_col"))}
			},
			want: "ALTER TABLE \"users\" ADD COLUMN \"lower_col\" integer NULL;\n\nALTER TABLE \"Users\" ADD COLUMN \"upper_col\" integer NULL;\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(test.dialect, test.nodes(), callDiff(test.diff))

			c.Assert(must.Must(builtin.RenderSQL(test.dialect, got...)), qt.Equals, test.want)
		})
	}
}

// callDiff is what a row's diff builder returns, and no diff for a row without one.
func callDiff(build func() *difftypes.SchemaDiff) *difftypes.SchemaDiff {
	if build == nil {
		return nil
	}
	return build()
}

// TestApply_AKeyOfANewTableJoinsItWhenItsNeedsStandAhead_HappyPath moves a foreign key of a
// new table into its CREATE TABLE only where what the key needs comes ahead of that
// statement.
func TestApply_AKeyOfANewTableJoinsItWhenItsNeedsStandAhead_HappyPath(t *testing.T) {
	newTable := func() *ast.CreateTableNode {
		return ast.NewCreateTable("n").AddColumn(key("id")).AddColumn(nullable("a_code", "int"))
	}
	tests := []struct {
		name  string
		nodes func() []ast.Node
		want  []string
	}{
		{
			name: "a key into a column another table gains",
			nodes: func() []ast.Node {
				return []ast.Node{
					newTable(),
					alter("a", addColumn("code")),
					alter("a", addKey(uniqueKey("a_code_key", "code"))),
					alter("n", addKey(foreignKeyTo("n_a", "a_code", "a", "code"))),
				}
			},
			want: []string{"a", "n"},
		},
		{
			name: "a key into a column that needs the new table first",
			nodes: func() []ast.Node {
				return []ast.Node{
					newTable(),
					alter("a", &ast.AddColumnOperation{Column: nullable("code", "n")}),
					alter("a", addKey(uniqueKey("a_code_key", "code"))),
					alter("n", addKey(foreignKeyTo("n_a", "a_code", "a", "code"))),
				}
			},
			want: []string{"n", "a", "n"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(platform.Postgres, test.nodes(), nil)

			c.Assert(tablesOf(got), qt.DeepEquals, test.want)
		})
	}
}

// TestApply_ADroppedTableGoesLast_HappyPath is a table the diff says it read, with nothing
// tying it to the other table, so its drop is free to go where the binary writes it.
func TestApply_ADroppedTableGoesLast_HappyPath(t *testing.T) {
	c := qt.New(t)

	got := planshape.Apply(platform.Postgres, []ast.Node{ast.NewDropTable("gone"), alter("a", addColumn("x"))}, droppedTables("gone"))

	c.Assert(tablesOf(got), qt.DeepEquals, []string{"a", "gone"})
}

// TestApply_KeepsApartWhatTheServerRefusesTogether_HappyPath is a pair of operations of
// one table that cannot share a statement on one engine and can on another.
func TestApply_KeepsApartWhatTheServerRefusesTogether_HappyPath(t *testing.T) {
	selfKey := func() []ast.Node {
		return []ast.Node{
			alter("t", addKey(uniqueKey("t_code_key", "code"))),
			alter("t", addKey(foreignKeyTo("t_fk", "parent_code", "t", "code"))),
		}
	}
	sameName := func() []ast.Node {
		return []ast.Node{alter("c", dropConstraint("fk")), alter("c", addKey(foreignKeyTo("fk", "p_id", "p", "id")))}
	}
	widenedSelfKey := func() []ast.Node {
		return []ast.Node{
			alter("t", dropConstraint("fk")),
			alter("t", modifyColumn("id", "bigint")),
			alter("t", modifyColumn("parent_id", "bigint")),
			alter("t", addKey(foreignKeyTo("fk", "parent_id", "t", "id"))),
		}
	}
	droppedSelfKey := func() []ast.Node {
		return []ast.Node{alter("t", dropConstraint("fk")), alter("t", dropColumn("id"))}
	}
	nameAddedThenDropped := func() []ast.Node {
		return []ast.Node{alter("c", addKey(foreignKeyTo("fk", "p_id", "p", "id"))), alter("c", dropConstraint("fk"))}
	}
	otherName := func() []ast.Node {
		return []ast.Node{alter("c", dropConstraint("fk")), alter("c", addKey(foreignKeyTo("fk2", "p_id", "p", "id")))}
	}
	otherCase := func() []ast.Node {
		return []ast.Node{alter("c", dropConstraint("FK")), alter("c", addKey(foreignKeyTo("fk", "p_id", "p", "id")))}
	}
	nameOfAnotherKind := func() []ast.Node {
		return []ast.Node{alter("c", dropConstraint("fk")), alter("c", addKey(uniqueKey("fk", "code")))}
	}
	keyToAnotherTable := func() []ast.Node {
		return []ast.Node{
			alter("t", addKey(uniqueKey("t_code_key", "code"))),
			alter("t", addKey(foreignKeyTo("t_fk", "parent_code", "p", "code"))),
		}
	}
	uniqueNamedAsTheKey := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{
		changedTable("c", []catalog.Constraint{observedKey("c", "fk", "UNIQUE", "code")}),
	}}
	tests := []struct {
		name    string
		dialect string
		nodes   func() []ast.Node
		diff    *difftypes.SchemaDiff
		want    int
	}{
		{name: "a key to its own table and what it needs on PostgreSQL", dialect: platform.Postgres, nodes: selfKey, want: 1},
		{name: "a key to its own table and what it needs on MySQL", dialect: platform.MySQL, nodes: selfKey, want: 2},
		{name: "a key to its own table and what it needs on MariaDB", dialect: platform.MariaDB, nodes: selfKey, want: 2},
		{name: "one name dropped and added on PostgreSQL", dialect: platform.Postgres, nodes: sameName, diff: keyDiff(), want: 1},
		{name: "one name dropped and added on MySQL", dialect: platform.MySQL, nodes: sameName, diff: keyDiff(), want: 2},
		{name: "one name dropped and added on MariaDB", dialect: platform.MariaDB, nodes: sameName, diff: keyDiff(), want: 1},
		{name: "one name added and dropped on MySQL, the other way round", dialect: platform.MySQL, nodes: nameAddedThenDropped, diff: keyDiff(), want: 2},
		{name: "a key dropped and another added on MySQL", dialect: platform.MySQL, nodes: otherName, diff: keyDiff(), want: 1},
		{name: "one name in two cases on MySQL", dialect: platform.MySQL, nodes: otherCase, diff: keyDiff(), want: 2},
		{name: "a unique constraint dropped and a key of its name added on MySQL", dialect: platform.MySQL, nodes: sameName, diff: uniqueNamedAsTheKey, want: 1},
		{name: "a key dropped and a unique constraint of its name added on MySQL", dialect: platform.MySQL, nodes: nameOfAnotherKind, diff: keyDiff(), want: 1},
		{name: "a key to another table and what its table gains on MariaDB", dialect: platform.MariaDB, nodes: keyToAnotherTable, want: 1},
		{name: "ids widened under a key to its own table on MariaDB", dialect: platform.MariaDB, nodes: widenedSelfKey, diff: selfKeyDiff(), want: 3},
		{name: "a key to its own table dropped with what it needs on MariaDB", dialect: platform.MariaDB, nodes: droppedSelfKey, diff: selfKeyDiff(), want: 2},
		{name: "a key to its own table dropped with what it needs on PostgreSQL", dialect: platform.Postgres, nodes: droppedSelfKey, diff: selfKeyDiff(), want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.Apply(test.dialect, test.nodes(), test.diff)

			c.Assert(got, qt.HasLen, test.want)
		})
	}
}

// TestApply_LeavesWhatItCannotShowIsFree_FailurePath is a statement whose dependencies the
// plan does not settle. It ends the run, so nothing moves over it.
func TestApply_LeavesWhatItCannotShowIsFree_FailurePath(t *testing.T) {
	calling := nullable("n", "int")
	calling.Default = &ast.DefaultValue{Expression: "next_code()"}
	tests := []struct {
		name string
		kept ast.Node
		diff *difftypes.SchemaDiff
	}{
		{name: "a drop of a constraint nothing describes", kept: alter("m", dropConstraint("unknown"))},
		{name: "a table nothing describes", kept: ast.NewDropTable("m")},
		{name: "a dropped index of a table a key ties", kept: ast.NewDropIndex("m_i").SetTable("m"), diff: &difftypes.SchemaDiff{
			DeclaredForeignKeys: []difftypes.ForeignKeyDeclaration{{TableName: "c", Name: "c_m", Columns: []string{"m_id"}, ForeignTable: "m", ForeignColumn: "id"}},
		}},
		{name: "a default calling a declared routine", kept: alter("m", &ast.AddColumnOperation{Column: calling}), diff: &difftypes.SchemaDiff{
			DeclaredFunctions: difftypes.FunctionOrdering{Order: []string{"next_code"}},
		}},
		{name: "a table written as raw SQL", kept: &ast.CreateTableNode{Name: "m", CustomSQL: "CREATE TABLE m ();"}},
		{name: "a table dropped with what depends on it", kept: &ast.DropTableNode{Name: "m", Cascade: true}, diff: droppedTables("m")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes := []ast.Node{alter("z", addColumn("a")), test.kept, alter("a", addColumn("b"))}

			got := planshape.Apply(platform.Postgres, nodes, test.diff)

			c.Assert(got, qt.HasLen, 3)
			c.Assert(tablesOf(got), qt.DeepEquals, tablesOf(nodes))
		})
	}
}

// TestAlignRollback_KeepsWhatItsOwnPlanNeeds_HappyPath is a rollback the forward file read
// backwards would break. Steps that depend on each other keep the order the rollback
// planned, whatever forward's order says.
func TestAlignRollback_KeepsWhatItsOwnPlanNeeds_HappyPath(t *testing.T) {
	childAndParent := func() ([]ast.Node, []ast.Node, *difftypes.SchemaDiff) {
		child := ast.NewCreateTable("child").AddColumn(key("id")).AddColumn(nullable("code", "int")).
			AddConstraint(foreignKeyTo("child_fk", "code", "p", "code"))
		return []ast.Node{alter("p", dropConstraint("p_code_key"), dropColumn("code")), ast.NewDropTable("child")},
			[]ast.Node{alter("p", addColumn("code")), alter("p", addKey(uniqueKey("p_code_key", "code"))), child},
			&difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{changedTable("p", []catalog.Constraint{observedKey("p", "p_code_key", "UNIQUE", "code")})}}
	}
	checkOnNewColumn := func() ([]ast.Node, []ast.Node, *difftypes.SchemaDiff) {
		return []ast.Node{alter("t", addKey(checkConstraint("ck", "n > 0")), addColumn("n"))},
			[]ast.Node{alter("t", dropConstraint("ck")), alter("t", dropColumn("n"))},
			nil
	}
	mixedCaseKey := func() ([]ast.Node, []ast.Node, *difftypes.SchemaDiff) {
		column := nullable("id", "int")
		return []ast.Node{alter(`"Tbl"`, modifyColumn("id", "int"), addKey(&ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Columns: []string{"id"}}))},
			[]ast.Node{alter(`"Tbl"`, dropConstraint("Tbl_pkey")), alter(`"Tbl"`, &ast.ModifyColumnOperation{Column: column})},
			&difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{changedTable("Tbl", []catalog.Constraint{observedKey("Tbl", "Tbl_pkey", "PRIMARY KEY", "id")})}}
	}
	unnamedKey := func() ([]ast.Node, []ast.Node, *difftypes.SchemaDiff) {
		return []ast.Node{alter("t", modifyColumn("id", "int"), addKey(&ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Columns: []string{"id"}}))},
			[]ast.Node{alter("t", dropConstraint("t_pkey")), alter("t", modifyColumn("id", "int"))},
			nil
	}
	movedKey := func() ([]ast.Node, []ast.Node, *difftypes.SchemaDiff) {
		// A diff projected for the rollback may hold the key the forward plan moved off.
		stale := []catalog.Constraint{observedKey("t", "t_pkey", "PRIMARY KEY", "id")}
		return []ast.Node{alter("t", dropConstraint("t_pkey"), addColumn("k"), addKey(&ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Columns: []string{"k"}}))},
			[]ast.Node{alter("t", dropConstraint("t_pkey")), alter("t", dropColumn("k"))},
			&difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{changedTable("t", stale)}}
	}
	columnOrder := func() ([]ast.Node, []ast.Node, *difftypes.SchemaDiff) {
		return []ast.Node{alter("t", dropColumn("a"), dropColumn("b"))},
			[]ast.Node{alter("t", addColumn("a"), addColumn("b"))},
			nil
	}
	tests := []struct {
		name  string
		plans func() ([]ast.Node, []ast.Node, *difftypes.SchemaDiff)
		want  string
	}{
		{
			name: "a dropped child and the parent's column it reads", plans: childAndParent,
			want: "ALTER TABLE \"p\" ADD COLUMN \"code\" integer NULL, ADD CONSTRAINT \"p_code_key\" UNIQUE (\"code\");\n\n" +
				"CREATE TABLE \"child\" (\n  \"id\" integer NOT NULL,\n  \"code\" integer NULL,\n  PRIMARY KEY (\"id\"),\n" +
				"  CONSTRAINT \"child_fk\" FOREIGN KEY (\"code\") REFERENCES \"p\" (\"code\") ON UPDATE NO ACTION ON DELETE NO ACTION\n);\n\n",
		},
		{
			name: "a check over a column the plan adds", plans: checkOnNewColumn,
			want: "ALTER TABLE \"t\" DROP CONSTRAINT \"ck\", DROP COLUMN \"n\";\n\n",
		},
		{
			name: "a primary key of a table whose name holds capitals", plans: mixedCaseKey,
			want: "ALTER TABLE \"Tbl\" DROP CONSTRAINT \"Tbl_pkey\", ALTER COLUMN \"id\" TYPE integer, ALTER COLUMN \"id\" DROP NOT NULL, ALTER COLUMN \"id\" DROP DEFAULT;\n\n",
		},
		{
			name: "a primary key forward adds under no name", plans: unnamedKey,
			want: "ALTER TABLE \"t\" DROP CONSTRAINT \"t_pkey\", ALTER COLUMN \"id\" TYPE integer, ALTER COLUMN \"id\" DROP NOT NULL, ALTER COLUMN \"id\" DROP DEFAULT;\n\n",
		},
		{
			name: "a primary key forward moved onto a column it adds", plans: movedKey,
			want: "ALTER TABLE \"t\" DROP CONSTRAINT \"t_pkey\", DROP COLUMN \"k\";\n\n",
		},
		{
			name: "columns the rollback adds, in the order the table holds them", plans: columnOrder,
			want: "ALTER TABLE \"t\" ADD COLUMN \"a\" integer NULL, ADD COLUMN \"b\" integer NULL;\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			forward, rollback, diff := test.plans()

			got := planshape.AlignRollback(platform.Postgres, forward, rollback, diff)

			c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, got...)), qt.Equals, test.want)
		})
	}
}

// TestAlignRollback_AddedColumnsTakeTheOrderTheTableHeldThem_HappyPath is a rollback that
// adds two columns back on an engine that reads a statement as a whole: inside it the
// other operations are free, and the columns are not.
func TestAlignRollback_AddedColumnsTakeTheOrderTheTableHeldThem_HappyPath(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.AlignRollback(dialect, []ast.Node{alter("t", dropColumn("a"), dropColumn("b"))}, []ast.Node{alter("t", addColumn("a"), addColumn("b"))}, nil)

			c.Assert(must.Must(builtin.RenderSQL(dialect, got...)), qt.Contains, "ADD COLUMN `a` int NULL, ADD COLUMN `b` int NULL")
		})
	}
}

// TestAlignRollback_TheDropOfAKeyInsideANewTable_HappyPath is the drop of a key forward
// wrote inside a table the rollback drops. It goes with the table unless something
// between the two needs it gone: on MySQL an index the key stands on, and on any engine
// the key of another table it references.
func TestAlignRollback_TheDropOfAKeyInsideANewTable_HappyPath(t *testing.T) {
	forward := func() []ast.Node {
		return []ast.Node{
			alter("a", addKey(uniqueKey("a_code_key", "code"))),
			ast.NewCreateTable("n").AddColumn(key("id")).AddColumn(nullable("a_code", "int")).
				AddConstraint(foreignKeyTo("n_a", "a_code", "a", "code")),
			ast.NewIndex("n_i", "n", "a_code"),
		}
	}
	dropKey := func() ast.Node { return alter("n", dropConstraint("n_a")) }
	tests := []struct {
		name     string
		dialect  string
		rollback func() []ast.Node
		want     []string
	}{
		{
			name: "nothing between needs it", dialect: platform.Postgres,
			rollback: func() []ast.Node { return []ast.Node{dropKey(), ast.NewDropTable("n")} },
			want:     []string{"n"},
		},
		{
			name: "a plain index on PostgreSQL", dialect: platform.Postgres,
			rollback: func() []ast.Node {
				return []ast.Node{dropKey(), ast.NewDropIndex("n_i").SetTable("n"), ast.NewDropTable("n")}
			},
			want: []string{"n", "n"},
		},
		{
			name: "an index the key stands on on MySQL", dialect: platform.MySQL,
			rollback: func() []ast.Node {
				return []ast.Node{dropKey(), ast.NewDropIndex("n_i").SetTable("n"), ast.NewDropTable("n")}
			},
			want: []string{"n", "n", "n"},
		},
		{
			name: "the key it references", dialect: platform.Postgres,
			rollback: func() []ast.Node {
				return []ast.Node{dropKey(), alter("a", dropConstraint("a_code_key")), ast.NewDropTable("n")}
			},
			want: []string{"n", "a", "n"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := planshape.AlignRollback(test.dialect, forward(), test.rollback(), nil)

			c.Assert(tablesOf(got), qt.DeepEquals, test.want)
		})
	}
}
