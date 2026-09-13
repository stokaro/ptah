package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

// This file pins the render boundary: the layer that prepares and validates an
// AST node before a dialect renderer sees it, the buffer contract that layer
// implements, and the promise that a render leaves the caller's AST alone.
//
// The renderer [renderer.NewRenderer] returns is a wrapper over a dialect
// renderer. It preflights eight node kinds -- statement list, create table,
// alter table, column, constraint, index, extension and create materialized
// view -- and hands every other kind to the dialect renderer untouched. Every
// AST-level refusal this package makes lives in that preflight, and two of the
// preparations rewrite the node rather than refusing it, so nothing below the
// wrapper reproduces them.
//
// The tests below drive the wrapper through `node.Accept(renderer)`, which is
// the public path that reaches the preflight and nothing else.
// [renderer.RenderSQL] prepares its nodes a second time on the way in, so a
// refusal asserted only there survives a wrapper that stopped preflighting.

// boundaryFixture is one node and the target it is refused on, paired with the
// message that refusal carries. Each row names a different preflighted kind.
type boundaryFixture struct {
	name    string
	dialect string
	node    ast.Node
	wantIs  error
	wantErr string
}

// interceptedRefusals is one row per preflighted node kind, each carrying an
// input that only the preflight refuses.
//
// The materialized-view row is a typed nil because that preparation refuses
// nothing else: it nil-checks and copies. A renderer reached without the
// preflight dereferences the nil instead.
func interceptedRefusals() []boundaryFixture {
	return []boundaryFixture{
		{
			name:    "statement list",
			dialect: platform.Postgres,
			node: &ast.StatementList{Statements: []ast.Node{
				ast.NewCreateSchema("app"),
				ast.NewColumn("", "INTEGER"),
			}},
			wantIs:  ptaherr.ErrInvalidSchemaDiff,
			wantErr: "a column has no name; a column name is not optional, and an empty identifier is not a name this pipeline can address again",
		},
		{
			name:    "create table",
			dialect: platform.Postgres,
			node:    foreignKeyTableMissingItsColumn(),
			wantIs:  ptaherr.ErrInvalidSchemaDiff,
			wantErr: `invalid foreign key: table "orders" has no local foreign-key column "user_id"`,
		},
		{
			name:    "alter table",
			dialect: platform.Postgres,
			node: &ast.AlterTableNode{
				Name:       "products",
				Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: ast.NewColumn("", "INTEGER")}},
			},
			wantIs:  ptaherr.ErrInvalidSchemaDiff,
			wantErr: `table "products" declares a column that has no name; a column name is not optional, and an empty identifier is not a name this pipeline can address again`,
		},
		{
			name:    "column",
			dialect: platform.Postgres,
			node:    ast.NewColumn("", "INTEGER"),
			wantIs:  ptaherr.ErrInvalidSchemaDiff,
			wantErr: "a column has no name; a column name is not optional, and an empty identifier is not a name this pipeline can address again",
		},
		{
			name:    "constraint",
			dialect: platform.MySQL,
			node:    uniqueConstraintWithInclude(),
			wantIs:  ptaherr.ErrUnsupportedFeature,
			wantErr: `mysql does not support INCLUDE columns on UNIQUE constraint "uq_products_sku"; target postgres, yugabytedb, or cockroachdb`,
		},
		{
			name:    "index",
			dialect: platform.MySQL,
			node:    indexWithInclude(),
			wantIs:  ptaherr.ErrUnsupportedFeature,
			wantErr: `mysql does not support INCLUDE columns on index "idx_products_sku"; target postgres, yugabytedb, cockroachdb, or spanner`,
		},
		{
			name:    "extension",
			dialect: platform.CockroachDB,
			node:    &ast.ExtensionNode{Name: "pgcrypto", Schema: "ext"},
			wantIs:  ptaherr.ErrUnsupportedFeature,
			wantErr: `cockroachdb does not support PostgreSQL extension installation schema "ext" for extension "pgcrypto"`,
		},
		{
			name:    "create materialized view",
			dialect: platform.Postgres,
			node:    (*ast.CreateMaterializedViewNode)(nil),
			wantIs:  ptaherr.ErrInvalidSchemaDiff,
			wantErr: "materialized view node is nil",
		},
	}
}

// interceptedAcceptances is the control for interceptedRefusals: the same eight
// kinds, each carrying an input its preflight lets through. A preflight that
// refused everything would pass the failure table on its own.
func interceptedAcceptances() []boundaryFixture {
	return []boundaryFixture{
		{
			name:    "statement list",
			dialect: platform.Postgres,
			node: &ast.StatementList{Statements: []ast.Node{
				ast.NewCreateSchema("app"),
				ast.NewColumn("label", "INTEGER"),
			}},
		},
		{name: "create table", dialect: platform.Postgres, node: foreignKeyTableWithLocalColumn()},
		{
			name:    "alter table",
			dialect: platform.Postgres,
			node: &ast.AlterTableNode{
				Name:       "products",
				Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: ast.NewColumn("label", "INTEGER")}},
			},
		},
		{name: "column", dialect: platform.Postgres, node: ast.NewColumn("label", "INTEGER")},
		{name: "constraint", dialect: platform.Postgres, node: uniqueConstraintWithInclude()},
		{name: "index", dialect: platform.Postgres, node: indexWithInclude()},
		{
			name:    "extension",
			dialect: platform.Postgres,
			node:    &ast.ExtensionNode{Name: "pgcrypto", Schema: "ext"},
		},
		{
			name:    "create materialized view",
			dialect: platform.Postgres,
			node:    &ast.CreateMaterializedViewNode{Name: "mv_products", Body: "SELECT 1"},
		},
	}
}

// foreignKeyTableMissingItsColumn declares a foreign key over a column the
// table does not have. Only the create-table preparation asks that question.
func foreignKeyTableMissingItsColumn() *ast.CreateTableNode {
	return &ast.CreateTableNode{
		Name:    "orders",
		Columns: []*ast.ColumnNode{{Name: "id", Type: "BIGINT"}},
		Constraints: []*ast.ConstraintNode{{
			Type:      ast.ForeignKeyConstraint,
			Name:      "fk_orders_user",
			Columns:   []string{"user_id"},
			Reference: &ast.ForeignKeyRef{Table: "users", Column: "id"},
		}},
	}
}

// foreignKeyTableWithLocalColumn is the same table with the local column present.
func foreignKeyTableWithLocalColumn() *ast.CreateTableNode {
	return &ast.CreateTableNode{
		Name: "orders",
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "BIGINT"},
			{Name: "user_id", Type: "BIGINT", Nullable: true},
		},
		Constraints: []*ast.ConstraintNode{{
			Type:      ast.ForeignKeyConstraint,
			Name:      "fk_orders_user",
			Columns:   []string{"user_id"},
			Reference: &ast.ForeignKeyRef{Table: "users", Column: "id"},
		}},
	}
}

func uniqueConstraintWithInclude() *ast.ConstraintNode {
	return &ast.ConstraintNode{
		Type:           ast.UniqueConstraint,
		Name:           "uq_products_sku",
		Columns:        []string{"sku"},
		IncludeColumns: []string{"label"},
	}
}

func indexWithInclude() *ast.IndexNode {
	return &ast.IndexNode{
		Name:           "idx_products_sku",
		Table:          "products",
		Columns:        []string{"sku"},
		IncludeColumns: []string{"label"},
	}
}

// TestRendererAccept_PreflightedKind_FailurePath drives each preflighted node
// kind through Accept on the renderer and asserts the refusal the preflight
// owns.
//
// These eight rows are the measure of whether the preflight still runs. A
// renderer that dispatched every kind straight to its dialect handler would
// accept all eight inputs and emit DDL for most of them, at exit 0.
func TestRendererAccept_PreflightedKind_FailurePath(t *testing.T) {
	for _, test := range interceptedRefusals() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			r, err := renderer.NewRenderer(test.dialect)
			c.Assert(err, qt.IsNil)

			err = test.node.Accept(r)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(r.Output(), qt.Equals, "")
		})
	}
}

// TestRendererAccept_PreflightedKind_HappyPath is the acceptance control for
// the table above.
func TestRendererAccept_PreflightedKind_HappyPath(t *testing.T) {
	for _, test := range interceptedAcceptances() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			r, err := renderer.NewRenderer(test.dialect)
			c.Assert(err, qt.IsNil)

			err = test.node.Accept(r)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestRenderer_MySQLFamilyForeignKeyTableGetsInnoDB pins the first of the two
// preparations that rewrite a node rather than refusing it.
//
// MySQL and MariaDB enforce a foreign key only under InnoDB, so a table that
// carries one and names no engine has ENGINE=InnoDB written into the copy that
// renders. No dialect renderer does this, so the clause is gone the moment the
// preparation stops running -- and the DDL still applies, which is the part
// that makes the loss silent: the table is created without the constraint
// being enforced.
func TestRenderer_MySQLFamilyForeignKeyTableGetsInnoDB(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			r, err := renderer.NewRenderer(test.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(foreignKeyTableWithLocalColumn().Accept(r), qt.IsNil)
			c.Assert(r.Output(), qt.Contains, ") ENGINE=InnoDB;")

			sql, err := renderer.RenderSQL(test.dialect, foreignKeyTableWithLocalColumn())
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, ") ENGINE=InnoDB;")

			rendered, err := r.Render(foreignKeyTableWithLocalColumn())
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Contains, ") ENGINE=InnoDB;")
		})
	}
}

// TestRenderer_MySQLFamilyEngineIsNotWrittenOverADeclaredOne is the control:
// the write happens only where the author named no engine, and an engine that
// cannot enforce a foreign key is refused rather than replaced.
func TestRenderer_MySQLFamilyEngineIsNotWrittenOverADeclaredOne(t *testing.T) {
	t.Run("a table with no foreign key keeps no engine", func(t *testing.T) {
		c := qt.New(t)

		sql, err := renderer.RenderSQL(platform.MySQL, &ast.CreateTableNode{
			Name:    "users",
			Columns: []*ast.ColumnNode{{Name: "id", Type: "BIGINT"}},
		})

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Not(qt.Contains), "ENGINE=")
	})

	t.Run("a declared engine that cannot enforce the key is refused", func(t *testing.T) {
		c := qt.New(t)

		table := foreignKeyTableWithLocalColumn()
		table.Options = map[string]string{"ENGINE": "MyISAM"}
		sql, err := renderer.RenderSQL(platform.MySQL, table)

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(err, qt.ErrorMatches, `invalid foreign key: table "orders" uses storage engine "MyISAM"; mysql foreign keys require InnoDB`)
		c.Assert(sql, qt.Equals, "")
	})
}

// TestRenderer_SQLServerRestrictBecomesNoAction pins the second rewriting
// preparation.
//
// SQL Server has no RESTRICT: the clause it accepts for the same meaning is NO
// ACTION, so a declared RESTRICT is rewritten on the copy that renders.
// Nothing below the render boundary knows about it, so losing the preparation
// emits `ON DELETE RESTRICT` and the server refuses the statement.
func TestRenderer_SQLServerRestrictBecomesNoAction(t *testing.T) {
	c := qt.New(t)

	r, err := renderer.NewRenderer(platform.SQLServer)
	c.Assert(err, qt.IsNil)
	c.Assert(restrictForeignKeyTable().Accept(r), qt.IsNil)
	c.Assert(r.Output(), qt.Contains, "ON DELETE NO ACTION ON UPDATE NO ACTION")
	c.Assert(r.Output(), qt.Not(qt.Contains), "RESTRICT")

	sql, err := renderer.RenderSQL(platform.SQLServer, restrictForeignKeyTable())
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ON DELETE NO ACTION ON UPDATE NO ACTION")
	c.Assert(sql, qt.Not(qt.Contains), "RESTRICT")
}

// TestRenderer_PostgresKeepsRestrict is the control for the rewrite above: the
// clause is rewritten for the one target that cannot spell it, not folded
// everywhere.
func TestRenderer_PostgresKeepsRestrict(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQL(platform.Postgres, restrictForeignKeyTable())

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ON DELETE RESTRICT ON UPDATE RESTRICT")
}

func restrictForeignKeyTable() *ast.CreateTableNode {
	return &ast.CreateTableNode{
		Name: "orders",
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "BIGINT"},
			{Name: "user_id", Type: "BIGINT", Nullable: true},
		},
		Constraints: []*ast.ConstraintNode{{
			Type:    ast.ForeignKeyConstraint,
			Name:    "fk_orders_user",
			Columns: []string{"user_id"},
			Reference: &ast.ForeignKeyRef{
				Table:    "users",
				Column:   "id",
				OnDelete: "RESTRICT",
				OnUpdate: "RESTRICT",
			},
		}},
	}
}

// partitionTableMissingItsType renders most of a CREATE TABLE and then fails.
//
// The PostgreSQL renderer writes the leading comment, the header and the
// column list into the buffer before it renders the PARTITION BY clause, and a
// partition spec with no type is refused at that point. It is the shortest
// input that makes a preflighted kind fail after it has already written.
func partitionTableMissingItsType() *ast.CreateTableNode {
	return &ast.CreateTableNode{
		Name:      "events",
		Columns:   []*ast.ColumnNode{{Name: "id", Type: "BIGINT"}},
		Partition: &ast.PartitionSpec{Parts: []ast.PartitionPart{{Name: "id"}}},
	}
}

// TestRenderer_BufferContractOnFailure pins what the buffer holds after a node
// fails, and it pins two different answers, because that is what the code
// does today.
//
// A preflighted kind clears the whole buffer -- the failed node's partial
// output and everything an earlier call put there. A pass-through kind clears
// nothing: what it wrote before it failed stays, and so does the output before
// it.
//
// The split is not a design, it is where the clearing happens to be written:
// only the preflighted kinds pass through a wrapper that clears on its error
// arms. Any rework of node dispatch has to make ONE decision here and apply it
// to every node kind. Whichever half it picks, one of these two subtests goes
// red, and that is the point of asserting both.
func TestRenderer_BufferContractOnFailure(t *testing.T) {
	tests := []struct {
		name string
		// failing is a node the PostgreSQL renderer refuses, one that the
		// wrapper refuses during preparation and one the dialect refuses while
		// rendering. Both leave the buffer empty, which is the point: one rule
		// for every node kind rather than one per layer.
		failing func() ast.Node
		wantErr string
	}{
		{
			name:    "the wrapper refuses during preparation",
			failing: func() ast.Node { return partitionTableMissingItsType() },
			wantErr: "postgres partition requires type",
		},
		{
			name:    "the dialect refuses while rendering",
			failing: func() ast.Node { return &ast.UpsertNode{Table: "products"} },
			wantErr: "(?s).*upsert rendering is not implemented.*",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			r, err := renderer.NewRenderer(platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(ast.NewCreateSchema("app").Accept(r), qt.IsNil)
			c.Assert(r.Output(), qt.Equals, "CREATE SCHEMA \"app\";\n")

			err = test.failing().Accept(r)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(r.Output(), qt.Equals, "",
				qt.Commentf("a failed render left output behind"))
		})
	}
}

// TestRenderer_RenderStartsFromAnEmptyBuffer pins the reuse promise
// [renderer.RenderVisitor] makes: Render clears the buffer before it visits,
// so one renderer serves many nodes in sequence and each call returns only
// what it was handed.
//
// The failed-render subtest is the half a single render path owns. The
// successful-render half is implemented twice -- the wrapper clears on entry
// and each dialect's own Render clears again -- so it survives losing either
// site alone and is asserted here for the case where dispatch is rewritten and
// both move.
func TestRenderer_RenderStartsFromAnEmptyBuffer(t *testing.T) {
	t.Run("a failed render discards what was already there", func(t *testing.T) {
		c := qt.New(t)

		r, err := renderer.NewRenderer(platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(ast.NewCreateSchema("app").Accept(r), qt.IsNil)
		c.Assert(r.Output(), qt.Not(qt.Equals), "")

		sql, err := r.Render(foreignKeyTableMissingItsColumn())

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(sql, qt.Equals, "")
		c.Assert(r.Output(), qt.Equals, "")
	})

	t.Run("the renderer still works after a failure", func(t *testing.T) {
		c := qt.New(t)

		r, err := renderer.NewRenderer(platform.Postgres)
		c.Assert(err, qt.IsNil)
		// A node that fails only after the dialect renderer has written part
		// of it, so the next call has something to inherit if the buffer is
		// not cleared.
		_, err = r.Render(partitionTableMissingItsType())
		c.Assert(err, qt.IsNotNil)

		sql, err := r.Render(ast.NewCreateSchema("app"))

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, "CREATE SCHEMA \"app\";\n")
	})

	t.Run("one successful render does not reach the next", func(t *testing.T) {
		c := qt.New(t)

		r, err := renderer.NewRenderer(platform.Postgres)
		c.Assert(err, qt.IsNil)
		first, err := r.Render(ast.NewCreateSchema("app"))
		c.Assert(err, qt.IsNil)
		c.Assert(first, qt.Equals, "CREATE SCHEMA \"app\";\n")

		second, err := r.Render(ast.NewCreateSchema("ops"))

		c.Assert(err, qt.IsNil)
		c.Assert(second, qt.Equals, "CREATE SCHEMA \"ops\";\n")
		c.Assert(r.Output(), qt.Equals, "CREATE SCHEMA \"ops\";\n")
	})
}

// richCreateTable is a table with columns, a column-level foreign key,
// table-level constraints and an option map. It is the shape a preparation has
// most to copy: the create-table preparation clones the node, the column
// slice, the constraint slice and the option map, and each column and
// constraint is prepared into that clone.
//
// The referential actions are RESTRICT and the option map names no engine, so
// the two rewriting preparations both have something to do on the targets
// below. A preparation that wrote into the caller's node instead of its copy
// would be visible on this fixture and on no simpler one.
func richCreateTable() *ast.CreateTableNode {
	return &ast.CreateTableNode{
		Name:        "orders",
		IfNotExists: true,
		Comment:     "customer orders",
		Options:     map[string]string{"COMMENT": "'orders'"},
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "BIGINT", Primary: true},
			{
				Name:     "user_id",
				Type:     "BIGINT",
				Nullable: true,
				ForeignKey: &ast.ForeignKeyRef{
					Table:    "users",
					Column:   "id",
					OnDelete: "RESTRICT",
					OnUpdate: "RESTRICT",
				},
			},
			{Name: "sku", Type: "VARCHAR(64)", Nullable: true},
		},
		Constraints: []*ast.ConstraintNode{
			{Type: ast.UniqueConstraint, Name: "uq_orders_sku", Columns: []string{"sku"}},
			{Type: ast.CheckConstraint, Name: "ck_orders_sku", Expression: "sku <> ''"},
		},
	}
}

// TestRenderer_LeavesTheInputASTUnchanged pins the promise
// [renderer.RenderSQLWithCapabilities] states: the input nodes are never
// mutated.
//
// The comparison is against a second node built from the same constructor, so
// it is a deep copy rather than a field-by-field restatement of what the test
// already believes. The three targets are the ones whose preparations write:
// MySQL adds an engine, SQL Server rewrites both referential actions, and
// PostgreSQL rewrites nothing and is the control that the fixture is not
// simply unreachable.
func TestRenderer_LeavesTheInputASTUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: platform.Postgres},
		{name: "mysql", dialect: platform.MySQL},
		{name: "sqlserver", dialect: platform.SQLServer},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			node := richCreateTable()
			untouched := richCreateTable()

			sql, err := renderer.RenderSQL(test.dialect, node)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Not(qt.Equals), "")
			c.Assert(node, qt.DeepEquals, untouched)

			r, err := renderer.NewRenderer(test.dialect)
			c.Assert(err, qt.IsNil)
			_, err = r.Render(node)
			c.Assert(err, qt.IsNil)
			c.Assert(node, qt.DeepEquals, untouched)

			c.Assert(node.Accept(r), qt.IsNil)
			c.Assert(node, qt.DeepEquals, untouched)
		})
	}
}

// TestStatementList_TheWholeListIsPreparedBeforeAnythingRenders pins the
// property a per-statement dispatch would lose.
//
// A statement list is prepared in full and only then rendered, so a list whose
// LAST statement cannot be prepared emits nothing at all -- the statements
// before it never reach the buffer. Preparing each statement as it is reached
// would write the earlier ones first and report the failure afterwards,
// leaving the caller holding a prefix of a script that was refused.
func TestStatementList_TheWholeListIsPreparedBeforeAnythingRenders(t *testing.T) {
	c := qt.New(t)

	r, err := renderer.NewRenderer(platform.Postgres)
	c.Assert(err, qt.IsNil)

	list := &ast.StatementList{Statements: []ast.Node{
		ast.NewCreateSchema("app"),
		ast.NewCreateSchema("ops"),
		ast.NewColumn("", "INTEGER"),
	}}
	err = list.Accept(r)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, "a column has no name; a column name is not optional, and an empty identifier is not a name this pipeline can address again")
	c.Assert(r.Output(), qt.Equals, "")
}

// TestStatementList_ARenderableListReachesTheBufferInOrder is the control: the
// refusal above is the last statement's, not the list's.
func TestStatementList_ARenderableListReachesTheBufferInOrder(t *testing.T) {
	c := qt.New(t)

	r, err := renderer.NewRenderer(platform.Postgres)
	c.Assert(err, qt.IsNil)

	list := &ast.StatementList{Statements: []ast.Node{
		ast.NewCreateSchema("app"),
		ast.NewCreateSchema("ops"),
	}}
	err = list.Accept(r)

	c.Assert(err, qt.IsNil)
	c.Assert(r.Output(), qt.Equals, "CREATE SCHEMA \"app\";\nCREATE SCHEMA \"ops\";\n")
}

// TestEveryPublicEntryPointReachesTheSameValidation pins that the four ways a
// caller hands an AST node to this package all reach the same preparation.
//
// They take different routes: RenderSQL prepares its node list and then
// dispatches it, Render prepares and hands the prepared node to the dialect
// renderer's own Render, and Accept reaches the preflight and nothing else. A
// refusal that moved to one of those routes would let the others emit DDL for
// an input this package refuses.
func TestEveryPublicEntryPointReachesTheSameValidation(t *testing.T) {
	const wantMessage = `invalid foreign key: table "orders" has no local foreign-key column "user_id"`

	t.Run("RenderSQL", func(t *testing.T) {
		c := qt.New(t)

		sql, err := renderer.RenderSQL(platform.Postgres, foreignKeyTableMissingItsColumn())

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(err, qt.ErrorMatches, wantMessage)
		c.Assert(sql, qt.Equals, "")
	})

	t.Run("RenderSQLWithCapabilities", func(t *testing.T) {
		c := qt.New(t)

		sql, err := renderer.RenderSQLWithCapabilities(
			platform.Postgres,
			capability.ForDialect(platform.Postgres),
			foreignKeyTableMissingItsColumn(),
		)

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(err, qt.ErrorMatches, wantMessage)
		c.Assert(sql, qt.Equals, "")
	})

	t.Run("Render", func(t *testing.T) {
		c := qt.New(t)

		r, err := renderer.NewRenderer(platform.Postgres)
		c.Assert(err, qt.IsNil)

		sql, err := r.Render(foreignKeyTableMissingItsColumn())

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(err, qt.ErrorMatches, wantMessage)
		c.Assert(sql, qt.Equals, "")
		c.Assert(r.Output(), qt.Equals, "")
	})

	t.Run("Render on a capability-aware renderer", func(t *testing.T) {
		c := qt.New(t)

		r, err := renderer.NewRendererWithCapabilities(
			platform.Postgres,
			capability.ForDialect(platform.Postgres),
		)
		c.Assert(err, qt.IsNil)

		sql, err := r.Render(foreignKeyTableMissingItsColumn())

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(err, qt.ErrorMatches, wantMessage)
		c.Assert(sql, qt.Equals, "")
	})

	t.Run("Accept", func(t *testing.T) {
		c := qt.New(t)

		r, err := renderer.NewRenderer(platform.Postgres)
		c.Assert(err, qt.IsNil)

		err = foreignKeyTableMissingItsColumn().Accept(r)

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(err, qt.ErrorMatches, wantMessage)
		c.Assert(r.Output(), qt.Equals, "")
	})
}

// TestEveryPublicEntryPointRendersTheSameSQL is the acceptance control for the
// agreement above: the four routes agree on what they refuse because they
// agree on what they render, not because one of them refuses everything.
func TestEveryPublicEntryPointRendersTheSameSQL(t *testing.T) {
	c := qt.New(t)

	fromRenderSQL, err := renderer.RenderSQL(platform.SQLServer, restrictForeignKeyTable())
	c.Assert(err, qt.IsNil)

	fromCapabilities, err := renderer.RenderSQLWithCapabilities(
		platform.SQLServer,
		capability.ForDialect(platform.SQLServer),
		restrictForeignKeyTable(),
	)
	c.Assert(err, qt.IsNil)

	r, err := renderer.NewRenderer(platform.SQLServer)
	c.Assert(err, qt.IsNil)
	fromRender, err := r.Render(restrictForeignKeyTable())
	c.Assert(err, qt.IsNil)

	accepting, err := renderer.NewRenderer(platform.SQLServer)
	c.Assert(err, qt.IsNil)
	c.Assert(restrictForeignKeyTable().Accept(accepting), qt.IsNil)

	c.Assert(fromRenderSQL, qt.Not(qt.Equals), "")
	c.Assert(fromCapabilities, qt.Equals, fromRenderSQL)
	c.Assert(fromRender, qt.Equals, fromRenderSQL)
	c.Assert(accepting.Output(), qt.Equals, fromRenderSQL)
}

// TestRenderer_NilNodeIsAnsweredByTheWrapper puts the nil answer at the layer
// that owns it.
//
// A dialect dispatcher's type switch matches a non-nil interface holding a nil
// pointer against that kind's case, and hands the handler the nil pointer. Most
// handlers do not check, so a typed nil reaching a dialect renderer directly
// renders nothing and reports nothing. The preparation the wrapper runs is what
// answers first, and it names the kind rather than the absence.
func TestRenderer_NilNodeIsAnsweredByTheWrapper(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "an interface holding a nil pointer",
			node: (*ast.CreateTableNode)(nil),
			want: "create-table node is nil",
		},
		{
			name: "no node at all",
			node: nil,
			want: "cannot render a nil AST node",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			r, err := renderer.NewRenderer("postgresql")
			c.Assert(err, qt.IsNil)

			c.Assert(r.VisitNode(test.node), qt.ErrorMatches, ".*"+test.want+".*")
			c.Assert(r.Output(), qt.Equals, "")
		})
	}
}
