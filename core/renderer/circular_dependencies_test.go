package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
)

func TestGetOrderedCreateStatements_MutualForeignKeysUseTwoPhases(t *testing.T) {
	database := mutualForeignKeyDatabase()

	dialects := []string{"postgres", "cockroachdb", "yugabytedb", "mysql", "mariadb", "sqlserver"}
	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(database, dialect)
			c.Assert(err, qt.IsNil)

			sql := strings.Join(statements, "\n")
			c.Assert(statements, qt.HasLen, 4)
			c.Assert(strings.Count(sql, "CREATE TABLE"), qt.Equals, 2)
			c.Assert(strings.Count(sql, "ALTER TABLE"), qt.Equals, 2)
			c.Assert(strings.Count(sql, "FOREIGN KEY"), qt.Equals, 2)
			c.Assert(statements[0], qt.Not(qt.Contains), "FOREIGN KEY")
			c.Assert(statements[1], qt.Not(qt.Contains), "FOREIGN KEY")
		})
	}
}

func TestGetOrderedCreateStatements_SQLiteKeepsMutualForeignKeysInline(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(mutualForeignKeyDatabase(), "sqlite")

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	sql := strings.Join(statements, "\n")
	c.Assert(strings.Count(sql, "CREATE TABLE"), qt.Equals, 2)
	c.Assert(strings.Count(sql, "REFERENCES"), qt.Equals, 2)
	c.Assert(sql, qt.Not(qt.Contains), "ALTER TABLE")
}

func TestGetOrderedCreateStatements_QualifiesSameSchemaForeignKeys(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Schemas: []goschema.Schema{{Name: "app"}},
		Tables: []goschema.Table{
			{StructName: "User", Schema: "app", Name: "users"},
			{StructName: "Order", Schema: "app", Name: "orders"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Order", Name: "user_id", Type: "INTEGER", Foreign: "users(id)"},
		},
	}
	goschema.Finalize(database)

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `ALTER TABLE "app"."orders"`)
	c.Assert(sql, qt.Contains, `REFERENCES "app"."users"("id")`)
}

func TestGetOrderedCreateStatements_ValidatesMaterializedEmbeddedForeignKey(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Tenant", Name: "tenants"},
			{StructName: "Order", Name: "orders"},
		},
		Fields: []goschema.Field{
			{StructName: "Tenant", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Ownership", Name: "tenant_id", Type: "INTEGER", Foreign: "tenants(id)"},
		},
		EmbeddedFields: []goschema.EmbeddedField{{
			StructName:       "Order",
			EmbeddedTypeName: "Ownership",
			Mode:             "inline",
		}},
	}

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `FOREIGN KEY ("tenant_id") REFERENCES "tenants"("id")`)
}

func TestGetOrderedCreateStatements_CompositeSelfForeignKeyIsEmittedOnce(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Node", Name: "nodes"}},
		Fields: []goschema.Field{
			{StructName: "Node", Name: "tenant_id", Type: "INTEGER", Primary: true},
			{StructName: "Node", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Node", Name: "parent_id", Type: "INTEGER"},
		},
		Constraints: []goschema.Constraint{{
			StructName:     "Node",
			Type:           "FOREIGN KEY",
			Columns:        []string{"tenant_id", "parent_id"},
			ForeignTable:   "nodes",
			ForeignColumns: []string{"tenant_id", "id"},
		}},
	}
	goschema.Finalize(database)

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(strings.Count(sql, "FOREIGN KEY"), qt.Equals, 1)
	c.Assert(sql, qt.Contains, `FOREIGN KEY ("tenant_id", "parent_id") REFERENCES "nodes"("tenant_id", "id")`)
	c.Assert(sql, qt.Not(qt.Contains), `"tenant_id,parent_id"`)
}

func TestGetOrderedCreateStatements_ForeignKeysDisabled_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
	}{
		{name: "postgres", dialect: "postgres", caps: capability.Postgres17().With(capability.ForeignKeysRequireUniqueReference, false).With(capability.ForeignKeys, false)},
		{name: "mysql", dialect: "mysql", caps: capability.MySQL84().With(capability.ForeignKeysRequireUniqueReference, false).With(capability.ForeignKeys, false)},
		{name: "mariadb", dialect: "mariadb", caps: capability.MariaDB1011().With(capability.ForeignKeysRequireIndexedReference, false).With(capability.ForeignKeys, false)},
		{name: "sqlite", dialect: "sqlite", caps: capability.SQLite3().With(capability.ForeignKeysRequireUniqueReference, false).With(capability.ForeignKeys, false)},
		{name: "sqlserver", dialect: "sqlserver", caps: capability.SQLServer2022().With(capability.ForeignKeysRequireUniqueReference, false).With(capability.ForeignKeys, false)},
		{name: "clickhouse", dialect: "clickhouse", caps: capability.ClickHouse24()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(mutualForeignKeyDatabase(), test.dialect, test.caps)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.dialect+` does not support foreign keys`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestGetOrderedCreateStatements_NilDatabase_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(nil, "postgres")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, "cannot render a nil database schema")
	c.Assert(statements, qt.IsNil)
}

func TestValidateSchema_HappyPath(t *testing.T) {
	c := qt.New(t)

	err := renderer.ValidateSchema(mutualForeignKeyDatabase(), "postgres")

	c.Assert(err, qt.IsNil)
}

func TestValidateSchemaWithCapabilities_HappyPath(t *testing.T) {
	c := qt.New(t)

	err := renderer.ValidateSchemaWithCapabilities(
		mutualForeignKeyDatabase(),
		"mysql",
		capability.MySQL84(),
	)

	c.Assert(err, qt.IsNil)
}

func TestValidateSchema_FailurePath(t *testing.T) {
	t.Run("nil database", func(t *testing.T) {
		c := qt.New(t)
		err := renderer.ValidateSchema(nil, "postgres")
		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(err, qt.ErrorMatches, "cannot validate a nil database schema")
	})

	t.Run("unsupported dialect", func(t *testing.T) {
		c := qt.New(t)
		err := renderer.ValidateSchema(mutualForeignKeyDatabase(), "oracle")
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	})

	t.Run("foreign keys disabled", func(t *testing.T) {
		c := qt.New(t)
		err := renderer.ValidateSchemaWithCapabilities(
			mutualForeignKeyDatabase(),
			"postgres",
			capability.Postgres17().
				With(capability.ForeignKeysRequireUniqueReference, false).
				With(capability.ForeignKeys, false),
		)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	})
}

func TestGetOrderedCreateStatements_CompositeForeignKeyCardinalityMismatch_FailurePath(t *testing.T) {
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parents"},
			{StructName: "Child", Name: "children"},
		},
		Constraints: []goschema.Constraint{{
			StructName:     "Child",
			Name:           "fk_children_parents",
			Type:           "FOREIGN KEY",
			Columns:        []string{"tenant_id", "parent_id"},
			ForeignTable:   "parents",
			ForeignColumns: []string{"id"},
		}},
	}

	for _, dialect := range []string{"postgres", "cockroachdb", "yugabytedb", "mysql", "mariadb", "sqlite", "sqlserver", "spanner"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(database, dialect)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `invalid foreign key constraint "fk_children_parents": 2 local columns and 1 referenced columns`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestGetOrderedCreateStatements_ReferentialAction_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		onDelete string
		onUpdate string
		wantErr  string
	}{
		{name: "mysql set default", dialect: "mysql", onDelete: "SET DEFAULT", wantErr: "mysql does not support ON DELETE SET DEFAULT"},
		{name: "mariadb set default", dialect: "mariadb", onUpdate: "SET DEFAULT", wantErr: "mariadb does not support ON UPDATE SET DEFAULT"},
		{name: "spanner update action", dialect: "spanner", onUpdate: "CASCADE", wantErr: "spanner does not support ON UPDATE CASCADE"},
		{name: "spanner set null", dialect: "spanner", onDelete: "SET NULL", wantErr: "spanner does not support ON DELETE SET NULL"},
		{name: "unknown postgres action", dialect: "postgres", onDelete: "ARCHIVE", wantErr: "postgres does not support ON DELETE ARCHIVE"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := foreignKeyActionDatabase(test.onDelete, test.onUpdate)
			statements, err := renderer.GetOrderedCreateStatements(database, test.dialect)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestGetOrderedCreateStatements_SQLServerNormalizesRestrict(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(foreignKeyActionDatabase("RESTRICT", "RESTRICT"), "sqlserver")

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, "ON DELETE NO ACTION")
	c.Assert(sql, qt.Contains, "ON UPDATE NO ACTION")
	c.Assert(sql, qt.Not(qt.Contains), "RESTRICT")
}

func TestGetOrderedCreateStatements_NormalizesAtlasActionTokens(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		foreignKeyActionDatabase("NO_ACTION", "SET_NULL"),
		"postgres",
	)

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, "ON DELETE NO ACTION")
	c.Assert(sql, qt.Contains, "ON UPDATE SET NULL")
	c.Assert(sql, qt.Not(qt.Contains), "NO_ACTION")
	c.Assert(sql, qt.Not(qt.Contains), "SET_NULL")
}

// TestGetOrderedCreateStatements_NormalizesAtlasActionTokensEveryDialect pins the
// underscore spelling an HCL schema file carries (on_delete = SET_NULL) to the
// spaced keywords SQL actually defines, on every dialect that renders a
// referential action rather than on postgres alone.
//
// The spelling matters because neither engine parses the underscore form. Run
// against PostgreSQL 17.10, `ALTER TABLE ... ON DELETE SET_NULL` answers
// `ERROR: syntax error at or near "SET_NULL"`, and against MySQL 9.7.2
// `ON DELETE SET_DEFAULT` answers `ERROR 1064 (42000) ... near 'SET_DEFAULT'`.
// A renderer that passes the token through therefore emits DDL no server
// accepts, which is why the assertion is on the emitted keywords and not merely
// on a successful render.
func TestGetOrderedCreateStatements_NormalizesAtlasActionTokensEveryDialect(t *testing.T) {
	for _, dialect := range []string{"postgres", "cockroachdb", "yugabytedb", "mysql", "mariadb", "sqlite", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(
				foreignKeyActionDatabase("SET_NULL", "NO_ACTION"),
				dialect,
			)

			c.Assert(err, qt.IsNil)
			sql := strings.Join(statements, "\n")
			c.Assert(sql, qt.Contains, "ON DELETE SET NULL")
			c.Assert(sql, qt.Contains, "ON UPDATE NO ACTION")
			c.Assert(sql, qt.Not(qt.Contains), "SET_NULL")
			c.Assert(sql, qt.Not(qt.Contains), "NO_ACTION")
		})
	}
}

// TestGetOrderedCreateStatements_UnderscoreActionReachesDialectGate_FailurePath
// pins that the underscore spelling is normalized BEFORE the per-dialect
// capability gate rather than after it.
//
// Widening the accepted set to include the underscore tokens is the cheaper
// wrong fix for "ptah refuses SET_NULL": it compiles, it clears the reported
// symptom, and it renders at exit 0. What it also does is route SET_DEFAULT
// past the MySQL-family refusal and SET_NULL past the Spanner one, so the
// spelling that no server parses reaches a server that would have refused the
// action even spelled correctly.
func TestGetOrderedCreateStatements_UnderscoreActionReachesDialectGate_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		onDelete string
		onUpdate string
		wantErr  string
	}{
		{name: "mysql set default", dialect: "mysql", onDelete: "SET_DEFAULT", wantErr: "mysql does not support ON DELETE SET DEFAULT"},
		{name: "mariadb set default", dialect: "mariadb", onUpdate: "SET_DEFAULT", wantErr: "mariadb does not support ON UPDATE SET DEFAULT"},
		{name: "spanner set null", dialect: "spanner", onDelete: "SET_NULL", wantErr: "spanner does not support ON DELETE SET NULL"},
		{name: "spanner update cascade", dialect: "spanner", onUpdate: "NO_ACTION", wantErr: "spanner does not support ON UPDATE NO ACTION"},
		{name: "unknown underscore token", dialect: "postgres", onDelete: "SET_ARCHIVE", wantErr: "postgres does not support ON DELETE SET ARCHIVE"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := foreignKeyActionDatabase(test.onDelete, test.onUpdate)

			statements, err := renderer.GetOrderedCreateStatements(database, test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestGetOrderedCreateStatements_SpannerMutualCompositeForeignKeys(t *testing.T) {
	c := qt.New(t)
	database := mutualCompositeForeignKeyDatabase()
	goschema.Finalize(database)

	statements, err := renderer.GetOrderedCreateStatements(database, "spanner")

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 4)
	sql := strings.Join(statements, "\n")
	c.Assert(strings.Count(sql, "CREATE TABLE"), qt.Equals, 2)
	c.Assert(strings.Count(sql, "ALTER TABLE"), qt.Equals, 2)
	c.Assert(strings.Count(sql, "FOREIGN KEY"), qt.Equals, 2)
	c.Assert(sql, qt.Not(qt.Contains), "not supported")
}

func TestGetOrderedCreateStatements_UnnamedForeignKeyNamesAreUnique(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "FirstParent", Name: "first_parents"},
			{StructName: "SecondParent", Name: "second_parents"},
			{StructName: "Child", Name: "children"},
		},
		Fields: []goschema.Field{
			{StructName: "FirstParent", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "SecondParent", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Child", Name: "parent_id", Type: "INTEGER"},
		},
		Constraints: []goschema.Constraint{
			{StructName: "Child", Type: "FOREIGN KEY", Columns: []string{"parent_id"}, ForeignTable: "first_parents", ForeignColumns: []string{"id"}},
			{StructName: "Child", Type: "FOREIGN KEY", Columns: []string{"parent_id"}, ForeignTable: "second_parents", ForeignColumns: []string{"id"}},
		},
	}

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 5)
	firstName := alterConstraintName(statements[3])
	secondName := alterConstraintName(statements[4])
	c.Assert(firstName, qt.Not(qt.Equals), secondName)
	c.Assert(statements[3], qt.Contains, `REFERENCES "first_parents"`)
	c.Assert(statements[4], qt.Contains, `REFERENCES "second_parents"`)
}

func TestGetOrderedCreateStatements_MySQLFamilyIndexesPrecedeForeignKeys(t *testing.T) {
	database := indexedForeignKeyDatabase()
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
	}{
		{name: "mysql 8.0", dialect: "mysql", caps: capability.MySQL8019()},
		{name: "mariadb", dialect: "mariadb", caps: capability.MariaDB1011()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(database, test.dialect, test.caps)
			c.Assert(err, qt.IsNil)

			sql := strings.Join(statements, "\n")
			parentIndex := strings.Index(sql, "idx_parents_tenant_code")
			childIndex := strings.Index(sql, "idx_children_tenant_parent_code")
			foreignKey := strings.Index(sql, "fk_children_parents")
			c.Assert(parentIndex, qt.Not(qt.Equals), -1)
			c.Assert(childIndex, qt.Not(qt.Equals), -1)
			c.Assert(foreignKey, qt.Not(qt.Equals), -1)
			c.Assert(parentIndex < foreignKey, qt.IsTrue)
			c.Assert(childIndex < foreignKey, qt.IsTrue)
		})
	}
}

func TestGetOrderedCreateStatements_MySQL84RejectsNonuniqueReferencedKey(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(indexedForeignKeyDatabase(), "mysql")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `mysql requires referenced columns tenant_id, code on table "parents" to be declared unique`)
	c.Assert(statements, qt.IsNil)
}

func TestRenderSQLWithCapabilities_DirectASTForeignKeysDisabled_FailurePath(t *testing.T) {
	node := foreignKeyAlterNode("CASCADE", "")
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
	}{
		{
			name:    "postgres",
			dialect: "postgres",
			caps: capability.Postgres17().
				With(capability.ForeignKeysRequireUniqueReference, false).
				With(capability.ForeignKeys, false),
		},
		{
			name:    "mysql",
			dialect: "mysql",
			caps: capability.MySQL84().
				With(capability.ForeignKeysRequireUniqueReference, false).
				With(capability.ForeignKeys, false),
		},
		{
			name:    "clickhouse",
			dialect: "clickhouse",
			caps:    capability.ClickHouse24(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := renderer.RenderSQLWithCapabilities(test.dialect, test.caps, node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.dialect+` does not support foreign keys`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

func TestRenderSQLWithCapabilities_DirectASTNormalizesCloneOnly(t *testing.T) {
	c := qt.New(t)
	node := foreignKeyAlterNode("restrict", "restrict")
	operation := node.Operations[0].(*ast.AddConstraintOperation)

	sql, err := renderer.RenderSQLWithCapabilities("sqlserver", capability.SQLServer2022(), node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ON DELETE NO ACTION")
	c.Assert(sql, qt.Contains, "ON UPDATE NO ACTION")
	c.Assert(operation.Constraint.Reference.OnDelete, qt.Equals, "restrict")
	c.Assert(operation.Constraint.Reference.OnUpdate, qt.Equals, "restrict")
}

func TestRenderSQL_DirectColumnSetNullRequiresNullableColumn_FailurePath(t *testing.T) {
	c := qt.New(t)
	column := ast.NewColumn("parent_id", "INTEGER").
		SetNotNull().
		SetForeignKey("parents", "id", "fk_children_parent")
	column.ForeignKey.OnDelete = "set null"

	sql, err := renderer.RenderSQL("postgres", column)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `invalid foreign key: column "parent_id" uses SET NULL but is NOT NULL`)
	c.Assert(sql, qt.Equals, "")
}

func TestRenderSQL_DirectTableConstraintSetNullRequiresNullableColumns_FailurePath(t *testing.T) {
	c := qt.New(t)
	reference := &ast.ForeignKeyRef{Table: "parents", Column: "id", OnUpdate: "SET NULL"}
	table := ast.NewCreateTable("children").
		AddColumn(ast.NewColumn("parent_id", "INTEGER").SetNotNull()).
		AddConstraint(ast.NewForeignKeyConstraint("fk_children_parent", []string{"parent_id"}, reference))

	sql, err := renderer.RenderSQL("postgres", table)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `invalid foreign key: foreign key on "children"\."parent_id" uses SET NULL but the local column is NOT NULL`)
	c.Assert(sql, qt.Equals, "")
}

func TestRenderSQL_DirectTableConstraintSetNullAcceptsNullableColumns(t *testing.T) {
	c := qt.New(t)
	reference := &ast.ForeignKeyRef{Table: "parents", Column: "id", OnDelete: "set null"}
	table := ast.NewCreateTable("children").
		AddColumn(ast.NewColumn("parent_id", "INTEGER")).
		AddConstraint(ast.NewForeignKeyConstraint("fk_children_parent", []string{"parent_id"}, reference))

	sql, err := renderer.RenderSQL("postgres", table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ON DELETE SET NULL")
}

func TestRenderSQLWithCapabilities_DirectASTCardinalityMismatch_FailurePath(t *testing.T) {
	c := qt.New(t)
	node := foreignKeyAlterNode("", "")
	operation := node.Operations[0].(*ast.AddConstraintOperation)
	operation.Constraint.Columns = []string{"tenant_id", "parent_id"}

	sql, err := renderer.RenderSQLWithCapabilities("postgres", capability.Postgres17(), node)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `invalid foreign key: foreign key has 2 local columns and 1 referenced columns`)
	c.Assert(sql, qt.Equals, "")
}

func TestStatementList_DirectAcceptPreflightsNestedNodesAtomically_FailurePath(t *testing.T) {
	c := qt.New(t)
	r, err := renderer.NewRenderer("postgres")
	c.Assert(err, qt.IsNil)
	invalid := foreignKeyAlterNode("ARCHIVE", "")
	list := &ast.StatementList{Statements: []ast.Node{
		ast.NewCreateTable("parents").AddColumn(ast.NewColumn("id", "INTEGER").SetPrimary()),
		&ast.StatementList{Statements: []ast.Node{invalid}},
	}}

	err = list.Accept(r)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "postgres does not support ON DELETE ARCHIVE")
	c.Assert(r.Output(), qt.Equals, "")
}

func TestStatementList_DirectAcceptHonorsForeignKeyCapabilities_FailurePath(t *testing.T) {
	c := qt.New(t)
	caps := capability.Postgres17().
		With(capability.ForeignKeysRequireUniqueReference, false).
		With(capability.ForeignKeys, false)
	r, err := renderer.NewRendererWithCapabilities("postgres", caps)
	c.Assert(err, qt.IsNil)
	list := &ast.StatementList{Statements: []ast.Node{foreignKeyAlterNode("CASCADE", "")}}

	err = list.Accept(r)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "postgres does not support foreign keys")
	c.Assert(r.Output(), qt.Equals, "")
}

func TestStatementList_DirectAcceptNormalizesCloneOnly(t *testing.T) {
	c := qt.New(t)
	node := foreignKeyAlterNode("restrict", "restrict")
	operation := node.Operations[0].(*ast.AddConstraintOperation)
	r, err := renderer.NewRenderer("sqlserver")
	c.Assert(err, qt.IsNil)
	list := &ast.StatementList{Statements: []ast.Node{node}}

	err = list.Accept(r)

	c.Assert(err, qt.IsNil)
	c.Assert(r.Output(), qt.Contains, "ON DELETE NO ACTION")
	c.Assert(r.Output(), qt.Contains, "ON UPDATE NO ACTION")
	c.Assert(operation.Constraint.Reference.OnDelete, qt.Equals, "restrict")
	c.Assert(operation.Constraint.Reference.OnUpdate, qt.Equals, "restrict")
}

func TestRenderSQL_TypedNilForeignKeyContainers_FailurePath(t *testing.T) {
	var createTable *ast.CreateTableNode
	var alterTable *ast.AlterTableNode
	var column *ast.ColumnNode
	var constraint *ast.ConstraintNode
	tests := []struct {
		name    string
		node    ast.Node
		wantErr string
	}{
		{name: "create table", node: createTable, wantErr: "invalid foreign key: create-table node is nil"},
		{name: "alter table", node: alterTable, wantErr: "invalid foreign key: alter-table node is nil"},
		{name: "column", node: column, wantErr: "invalid foreign key: column node is nil"},
		{name: "constraint", node: constraint, wantErr: "invalid foreign key: constraint node is nil"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := renderer.RenderSQL("postgres", test.node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

func TestRenderSQL_NilASTNode_FailurePath(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQL("postgres", nil)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, "invalid foreign key: AST node is nil")
	c.Assert(sql, qt.Equals, "")
}

func TestRenderSQL_TypedNilGenericASTNode_FailurePath(t *testing.T) {
	c := qt.New(t)
	var index *ast.IndexNode

	sql, err := renderer.RenderSQL("postgres", index)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, "index node is nil")
	c.Assert(sql, qt.Equals, "")
}

func TestRenderSQL_NilAlterOperation_FailurePath(t *testing.T) {
	c := qt.New(t)
	node := &ast.AlterTableNode{Name: "children"}
	node.Operations = append(node.Operations, nil)

	sql, err := renderer.RenderSQL("postgres", node)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, "invalid foreign key: alter-table operation is nil")
	c.Assert(sql, qt.Equals, "")
}

func TestRenderSQL_TypedNilAlterOperations_FailurePath(t *testing.T) {
	var dropColumn *ast.DropColumnOperation
	var alterGenerated *ast.AlterGeneratedColumnExpressionOperation
	var dropConstraint *ast.DropConstraintOperation
	var renameColumn *ast.RenameColumnOperation
	var renameTable *ast.RenameTableOperation
	var addSkippingIndex *ast.AddSkippingIndexOperation
	var modifyTTL *ast.ModifyTTLOperation
	tests := []struct {
		name      string
		operation ast.AlterOperation
	}{
		{name: "drop column", operation: dropColumn},
		{name: "alter generated expression", operation: alterGenerated},
		{name: "drop constraint", operation: dropConstraint},
		{name: "rename column", operation: renameColumn},
		{name: "rename table", operation: renameTable},
		{name: "add skipping index", operation: addSkippingIndex},
		{name: "modify ttl", operation: modifyTTL},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.AlterTableNode{Name: "children", Operations: []ast.AlterOperation{test.operation}}
			sql, err := renderer.RenderSQL("postgres", node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, "invalid foreign key: alter-table operation is nil")
			c.Assert(sql, qt.Equals, "")
		})
	}
}

func TestVisitorRenderSQL_FailedRenderClearsPreviousOutput(t *testing.T) {
	c := qt.New(t)
	r, err := renderer.NewRenderer("postgres")
	c.Assert(err, qt.IsNil)
	valid := ast.NewCreateTable("parents").AddColumn(ast.NewColumn("id", "INTEGER").SetPrimary())

	sql, err := renderer.VisitorRenderSQL(r, valid)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE TABLE")

	sql, err = renderer.VisitorRenderSQL(r, nil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(sql, qt.Equals, "")
	c.Assert(r.Output(), qt.Equals, "")
}

func TestRendererRender_FailedValidationClearsPreviousOutput(t *testing.T) {
	c := qt.New(t)
	r, err := renderer.NewRenderer("postgres")
	c.Assert(err, qt.IsNil)
	valid := ast.NewCreateTable("parents").AddColumn(ast.NewColumn("id", "INTEGER").SetPrimary())

	sql, err := r.Render(valid)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE TABLE")

	sql, err = r.Render(nil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(sql, qt.Equals, "")
	c.Assert(r.Output(), qt.Equals, "")
}

func TestVisitorRenderSQL_RendererErrorClearsPartialOutput(t *testing.T) {
	c := qt.New(t)
	r, err := renderer.NewRenderer("postgres")
	c.Assert(err, qt.IsNil)
	valid := ast.NewCreateTable("parents").AddColumn(ast.NewColumn("id", "INTEGER").SetPrimary())
	nullsDistinct := true
	invalid := ast.NewIndex("idx_parents_id", "parents", "id")
	invalid.NullsDistinct = &nullsDistinct

	sql, err := renderer.VisitorRenderSQL(r, valid, invalid)

	c.Assert(err, qt.ErrorMatches, "postgresql NULLS DISTINCT is only valid for unique indexes")
	c.Assert(sql, qt.Equals, "")
	c.Assert(r.Output(), qt.Equals, "")
}

func TestGetOrderedCreateStatements_UnknownForeignKeyTarget_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Child", Name: "children"}},
		Fields: []goschema.Field{
			{StructName: "Child", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Child", Name: "parent_id", Type: "INTEGER", Foreign: "missing(id)"},
		},
	}

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `invalid foreign key: field "parent_id" references unknown table "missing"`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_UnknownForeignKeyColumn_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := mutualForeignKeyDatabase()
	database.Fields[1].Foreign = "right_nodes(missing_id)"

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `invalid foreign key: referenced table "right_nodes" has no column "missing_id"`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_PartialUniqueIndexIsNotReferencedKey_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := indexedForeignKeyDatabase()
	database.Indexes[0].Unique = true
	database.Indexes[0].Condition = "tenant_id > 0"

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `postgres requires referenced columns tenant_id, code on table "parents" to be declared unique`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_MySQLPrefixIndexIsNotReferencedKey_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := indexedForeignKeyDatabase()
	database.Indexes[0].Fields = nil
	database.Indexes[0].Parts = []goschema.IndexPart{
		{Name: "tenant_id"},
		{Name: "code", Prefix: "4"},
	}

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		database,
		"mysql",
		capability.MySQL8019(),
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `mysql requires referenced columns tenant_id, code on table "parents" to be the full leftmost prefix of an index`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_MariaDBAllowsIndexedLeftPrefix(t *testing.T) {
	c := qt.New(t)
	database := indexedForeignKeyDatabase()
	database.Fields = append(database.Fields, goschema.Field{
		StructName: "Parent",
		Name:       "region_id",
		Type:       "INTEGER",
	})
	database.Indexes[0].Fields = []string{"tenant_id", "code", "region_id"}

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		database,
		"mariadb",
		capability.MariaDB1011(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "fk_children_parents")
}

func TestGetOrderedCreateStatements_DuplicateForeignKeyNamesRespectDialectScope_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql database scope", dialect: "mysql"},
		{name: "mariadb database scope", dialect: "mariadb"},
		{name: "sql server schema scope", dialect: "sqlserver"},
		{name: "spanner schema scope", dialect: "spanner"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(
				duplicateForeignKeyNameDatabase(),
				test.dialect,
			)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `invalid foreign key: foreign-key name "fk_shared_parent" is duplicated in .* constraint namespace`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestGetOrderedCreateStatements_PostgresAllowsForeignKeyNamesRepeatedAcrossTables(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(duplicateForeignKeyNameDatabase(), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(strings.Join(statements, "\n"), `CONSTRAINT "fk_shared_parent"`), qt.Equals, 2)
}

func TestGetOrderedCreateStatements_MySQLFamilyRejectsUnsuitableReferencedIndexes_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		caps      capability.Capabilities
		indexType string
		parser    string
	}{
		{name: "mysql fulltext", dialect: "mysql", caps: capability.MySQL8019(), indexType: "FULLTEXT"},
		{name: "mysql spatial", dialect: "mysql", caps: capability.MySQL8019(), indexType: "SPATIAL"},
		{name: "mysql hash", dialect: "mysql", caps: capability.MySQL8019(), indexType: "HASH"},
		{name: "mysql parser", dialect: "mysql", caps: capability.MySQL8019(), parser: "ngram"},
		{name: "mariadb fulltext", dialect: "mariadb", caps: capability.MariaDB1011(), indexType: "FULLTEXT"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := indexedForeignKeyDatabase()
			database.Indexes[0].Type = test.indexType
			database.Indexes[0].Parser = test.parser
			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
				database,
				test.dialect,
				test.caps,
			)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.dialect+` requires referenced columns tenant_id, code on table "parents" to be the full leftmost prefix of an index`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestGetOrderedCreateStatements_MySQLBTREEReferencedIndex(t *testing.T) {
	c := qt.New(t)
	database := indexedForeignKeyDatabase()
	database.Indexes[0].Type = "BTREE"

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		database,
		"mysql",
		capability.MySQL8019(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "fk_children_parents")
}

func TestGetOrderedCreateStatements_SQLiteRejectsUnverifiableStandaloneUniqueIndex_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := simpleForeignKeyDatabase("INTEGER", "INTEGER")
	database.Fields[0].Primary = false
	database.Indexes = []goschema.Index{{
		StructName: "Parent",
		Name:       "uq_parents_id",
		Fields:     []string{"id"},
		Unique:     true,
	}}

	statements, err := renderer.GetOrderedCreateStatements(database, "sqlite")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `sqlite requires referenced columns id on table "parents" to be declared unique`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_SQLiteAcceptsInlineUniqueReferencedColumn(t *testing.T) {
	c := qt.New(t)
	database := simpleForeignKeyDatabase("INTEGER", "INTEGER")
	database.Fields[0].Primary = false
	database.Fields[0].Unique = true

	statements, err := renderer.GetOrderedCreateStatements(database, "sqlite")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `REFERENCES "parents" ("id")`)
}

func TestGetOrderedCreateStatements_IncompatibleForeignKeyTypes_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: "postgres"},
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
		{name: "sql server", dialect: "sqlserver"},
		{name: "spanner", dialect: "spanner"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(
				simpleForeignKeyDatabase("INTEGER", "BIGINT"),
				test.dialect,
			)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `invalid foreign key: foreign-key columns "children"\."parent_id" \(BIGINT\) and "parents"\."id" \(INTEGER\) have incompatible types`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestGetOrderedCreateStatements_SQLiteUsesAffinityForForeignKeyTypes(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		simpleForeignKeyDatabase("TEXT", "INTEGER"),
		"sqlite",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `REFERENCES "parents" ("id")`)
}

func TestGetOrderedCreateStatements_MySQLSignednessMismatch_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		simpleForeignKeyDatabase("BIGINT UNSIGNED", "BIGINT"),
		"mysql",
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*have incompatible types`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_MySQLCollationMismatch_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := simpleForeignKeyDatabase("VARCHAR(64)", "VARCHAR(64)")
	database.Tables[0].Charset = "utf8mb4"
	database.Tables[0].Collate = "utf8mb4_bin"
	database.Tables[1].Charset = "utf8mb4"
	database.Tables[1].Collate = "utf8mb4_unicode_ci"

	statements, err := renderer.GetOrderedCreateStatements(database, "mysql")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*have incompatible collations "utf8mb4_unicode_ci" and "utf8mb4_bin"`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_MySQLIntegerForeignKeyIgnoresTableCharset(t *testing.T) {
	c := qt.New(t)
	database := simpleForeignKeyDatabase("INTEGER", "INTEGER")
	database.Tables[0].Charset = "latin1"
	database.Tables[1].Charset = "utf8mb4"

	statements, err := renderer.GetOrderedCreateStatements(database, "mysql")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "FOREIGN KEY")
}

func TestGetOrderedCreateStatements_MySQLEnumDefinitionsMustMatch_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := simpleForeignKeyDatabase("ENUM", "ENUM")
	database.Fields[0].Enum = []string{"open", "closed"}
	database.Fields[1].Enum = []string{"open", "archived"}

	statements, err := renderer.GetOrderedCreateStatements(database, "mysql")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*have incompatible types`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_MySQLGeneratedForeignKeyColumn_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := simpleForeignKeyDatabase("INTEGER", "INTEGER")
	database.Fields[1].GeneratedExpression = "1"

	statements, err := renderer.GetOrderedCreateStatements(database, "mysql")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `invalid foreign key: generated columns cannot participate in portable mysql foreign keys: .*`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_MySQLStoredGeneratedForeignKeyColumn(t *testing.T) {
	c := qt.New(t)
	database := simpleForeignKeyDatabase("INTEGER", "INTEGER")
	database.Fields[1].GeneratedExpression = "1"
	database.Fields[1].GeneratedKind = "STORED"
	database.Fields[1].OnDelete = "CASCADE"

	statements, err := renderer.GetOrderedCreateStatements(database, "mysql")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "ON DELETE CASCADE")
}

func TestGetOrderedCreateStatements_MySQLStoredGeneratedForeignKeyAction_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := simpleForeignKeyDatabase("INTEGER", "INTEGER")
	database.Fields[1].GeneratedExpression = "1"
	database.Fields[1].GeneratedKind = "STORED"
	database.Fields[1].OnUpdate = "CASCADE"

	statements, err := renderer.GetOrderedCreateStatements(database, "mysql")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `invalid foreign key: stored generated foreign-key columns do not support ON DELETE NO ACTION ON UPDATE CASCADE`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_MySQLStringLengthsMayDiffer(t *testing.T) {
	c := qt.New(t)
	database := simpleForeignKeyDatabase("VARCHAR(64)", "VARCHAR(128)")
	database.Tables[0].Charset = "utf8mb4"
	database.Tables[1].Charset = "utf8mb4"

	statements, err := renderer.GetOrderedCreateStatements(database, "mysql")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "FOREIGN KEY")
}

func TestGetOrderedCreateStatements_MySQLUnindexableForeignKeyType_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		simpleForeignKeyDatabase("TEXT", "TEXT"),
		"mysql",
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*have incompatible types`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_MySQLFamilyNonInnoDB_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		engine  string
	}{
		{name: "mysql MyISAM", dialect: "mysql", engine: "MyISAM"},
		{name: "mariadb Aria", dialect: "mariadb", engine: "Aria"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := simpleForeignKeyDatabase("INTEGER", "INTEGER")
			database.Tables[0].Engine = test.engine
			statements, err := renderer.GetOrderedCreateStatements(database, test.dialect)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `invalid foreign key: table "parents" uses storage engine ".*"; .* foreign keys require InnoDB`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestGetOrderedCreateStatements_MySQLFamilyEmitsExplicitInnoDB(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := simpleForeignKeyDatabase("INTEGER", "INTEGER")
			database.Tables[0].Overrides = map[string]map[string]string{
				test.dialect: {"engine": ""},
			}
			statements, err := renderer.GetOrderedCreateStatements(database, test.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Count(strings.Join(statements, "\n"), "ENGINE=InnoDB"), qt.Equals, 2)
		})
	}
}

func TestRenderSQL_MySQLFamilyInlineForeignKeyEmitsExplicitInnoDB(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("children").
		AddColumn(ast.NewColumn("id", "INTEGER").SetPrimary()).
		AddColumn(ast.NewColumn("parent_id", "INTEGER").SetForeignKey("parents", "id", "fk_parent"))

	sql, err := renderer.RenderSQL("mysql", table)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ENGINE=InnoDB")
	c.Assert(table.Options, qt.DeepEquals, make(map[string]string))
}

func TestRenderSQL_MySQLFamilyInlineForeignKeyNonInnoDB_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		engine  string
	}{
		{name: "mysql MyISAM", dialect: "mysql", engine: "MyISAM"},
		{name: "mariadb Aria", dialect: "mariadb", engine: "Aria"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := ast.NewCreateTable("children").
				AddColumn(ast.NewColumn("parent_id", "INTEGER").SetForeignKey("parents", "id", "fk_parent")).
				SetOption("engine", test.engine)
			sql, err := renderer.RenderSQL(test.dialect, table)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `invalid foreign key: table "children" uses storage engine ".*"; .* foreign keys require InnoDB`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

func TestGetOrderedCreateStatements_SetNullRequiresNullableLocalColumns_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		onDelete string
		onUpdate string
	}{
		{name: "postgres delete", dialect: "postgres", onDelete: "SET NULL"},
		{name: "mysql update", dialect: "mysql", onUpdate: "SET NULL"},
		{name: "mariadb delete", dialect: "mariadb", onDelete: "SET NULL"},
		{name: "sqlite update", dialect: "sqlite", onUpdate: "SET NULL"},
		{name: "sqlserver delete", dialect: "sqlserver", onDelete: "SET NULL"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := foreignKeyActionDatabase(test.onDelete, test.onUpdate)
			database.Fields[1].Nullable = false
			statements, err := renderer.GetOrderedCreateStatements(database, test.dialect)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `invalid foreign key: foreign key on "children"\."parent_id" uses SET NULL but the local column is NOT NULL`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestGetOrderedCreateStatements_CompositeSetNullRequiresAllLocalColumnsNullable_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := indexedForeignKeyDatabase()
	database.Fields[2].Nullable = true
	database.Constraints[0].OnDelete = "SET NULL"

	statements, err := renderer.GetOrderedCreateStatements(database, "mysql")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `invalid foreign key: foreign key on "children"\."parent_code" uses SET NULL but the local column is NOT NULL`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_ExplicitForeignKeyIdentifierLimit_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		fkName  string
		wantErr string
	}{
		{name: "postgres bytes", dialect: "postgres", fkName: strings.Repeat("a", 64), wantErr: `.*exceeds the postgres identifier limit of 63 bytes`},
		{name: "postgres multibyte bytes", dialect: "postgres", fkName: strings.Repeat("é", 32), wantErr: `.*exceeds the postgres identifier limit of 63 bytes`},
		{name: "mysql characters", dialect: "mysql", fkName: strings.Repeat("a", 65), wantErr: `.*exceeds the mysql identifier limit of 64 characters`},
		{name: "mariadb characters", dialect: "mariadb", fkName: strings.Repeat("é", 65), wantErr: `.*exceeds the mariadb identifier limit of 64 characters`},
		{name: "sqlserver characters", dialect: "sqlserver", fkName: strings.Repeat("a", 129), wantErr: `.*exceeds the sqlserver identifier limit of 128 characters`},
		{name: "sqlserver multibyte characters", dialect: "sqlserver", fkName: strings.Repeat("界", 129), wantErr: `.*exceeds the sqlserver identifier limit of 128 characters`},
		{name: "spanner characters", dialect: "spanner", fkName: strings.Repeat("a", 129), wantErr: `.*exceeds the spanner identifier limit of 128 characters`},
		{name: "spanner multibyte characters", dialect: "spanner", fkName: strings.Repeat("界", 129), wantErr: `.*exceeds the spanner identifier limit of 128 characters`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := simpleForeignKeyDatabase("INTEGER", "INTEGER")
			database.Fields[1].ForeignKeyName = test.fkName
			statements, err := renderer.GetOrderedCreateStatements(database, test.dialect)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestGetOrderedCreateStatements_ExplicitForeignKeyIdentifierLimit_HappyPath is
// the control the failure table cannot supply: every row above is refused
// under a byte rule AND under a character rule, so the table passes whichever
// unit the limit is enforced in. These names separate the two. A 100-character
// CJK name is 300 bytes, so a byte rule refuses it on SQL Server and the
// character rule this dialect actually has accepts it; a 31-character accented
// name is 62 bytes, one under PostgreSQL's byte limit, and a character rule
// would accept a 32nd that the byte rule refuses one row above.
//
// SQLite carries no modeled limit at all, and a name no other dialect would
// take proves that is a real absence rather than a table entry nobody reached.
// ClickHouse is the other unlimited dialect and is covered in
// capability.TestIdentifiers_UnlimitedDialects; it has no row here because it
// refuses this fixture's foreign keys outright.
func TestGetOrderedCreateStatements_ExplicitForeignKeyIdentifierLimit_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		fkName  string
	}{
		{name: "sqlserver counts characters not bytes", dialect: "sqlserver", fkName: strings.Repeat("界", 100)},
		{name: "spanner counts characters not bytes", dialect: "spanner", fkName: strings.Repeat("界", 100)},
		{name: "postgres accepts 62 bytes", dialect: "postgres", fkName: strings.Repeat("é", 31)},
		{name: "mysql accepts 64 characters", dialect: "mysql", fkName: strings.Repeat("é", 64)},
		{name: "sqlite models no limit", dialect: "sqlite", fkName: strings.Repeat("a", 300)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := simpleForeignKeyDatabase("INTEGER", "INTEGER")
			database.Fields[1].ForeignKeyName = test.fkName
			statements, err := renderer.GetOrderedCreateStatements(database, test.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.Not(qt.HasLen), 0)
		})
	}
}

func TestGetOrderedCreateStatements_SchemaScopedForeignKeyNamesTreatDefaultSchemaAsExplicit_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		dialect       string
		defaultSchema string
	}{
		{name: "SQL Server", dialect: "sqlserver", defaultSchema: "dbo"},
		{name: "Spanner", dialect: "spanner", defaultSchema: "public"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &goschema.Database{
				Tables: []goschema.Table{
					{StructName: "Parent", Name: "parents"},
					{StructName: "Child", Name: "children"},
					{StructName: "Audit", Schema: test.defaultSchema, Name: "audit_entries"},
				},
				Fields: []goschema.Field{
					{StructName: "Parent", Name: "id", Type: "INTEGER", Primary: true},
					{
						StructName:     "Child",
						Name:           "parent_id",
						Type:           "INTEGER",
						Foreign:        "parents(id)",
						ForeignKeyName: "shared_fk",
					},
					{
						StructName:     "Audit",
						Name:           "parent_id",
						Type:           "INTEGER",
						Foreign:        "parents(id)",
						ForeignKeyName: "SHARED_FK",
					},
				},
			}

			statements, err := renderer.GetOrderedCreateStatements(database, test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*foreign-key name ".*" is duplicated in schema ".*" constraint namespace`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestGetOrderedCreateStatements_SpannerArrayForeignKey_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		simpleForeignKeyDatabase("ARRAY<INT64>", "ARRAY<INT64>"),
		"spanner",
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `invalid foreign key: Spanner type ARRAY<INT64> cannot participate in foreign keys: .*`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_SpannerFloat4ForeignKey_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		simpleForeignKeyDatabase("FLOAT4", "FLOAT4"),
		"spanner",
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `invalid foreign key: Spanner type FLOAT4 cannot participate in foreign keys: .*`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_SQLServerCascadeCycle_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := mutualForeignKeyDatabase()
	database.Fields[1].OnDelete = "CASCADE"
	database.Fields[3].OnDelete = "CASCADE"

	statements, err := renderer.GetOrderedCreateStatements(database, "sqlserver")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `sqlserver does not allow ON DELETE cycles or multiple cascade paths reaching table .*`)
	c.Assert(statements, qt.IsNil)
}

func TestGetOrderedCreateStatements_SQLServerMultipleCascadePaths_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := sqlServerDiamondCascadeDatabase()

	statements, err := renderer.GetOrderedCreateStatements(database, "sqlserver")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `sqlserver does not allow ON DELETE cycles or multiple cascade paths reaching table "leaves"`)
	c.Assert(statements, qt.IsNil)
}

func mutualForeignKeyDatabase() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Left", Name: "left_nodes"},
			{StructName: "Right", Name: "right_nodes"},
		},
		Fields: []goschema.Field{
			{StructName: "Left", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Left", Name: "right_id", Type: "INTEGER", Foreign: "right_nodes(id)", ForeignKeyName: "fk_left_right"},
			{StructName: "Right", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Right", Name: "left_id", Type: "INTEGER", Foreign: "left_nodes(id)", ForeignKeyName: "fk_right_left"},
		},
	}
}

func foreignKeyActionDatabase(onDelete, onUpdate string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parents"},
			{StructName: "Child", Name: "children"},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName:     "Child",
				Name:           "parent_id",
				Type:           "INTEGER",
				Nullable:       true,
				Foreign:        "parents(id)",
				ForeignKeyName: "fk_children_parents",
				OnDelete:       onDelete,
				OnUpdate:       onUpdate,
			},
		},
	}
}

func mutualCompositeForeignKeyDatabase() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Left", Name: "left_nodes", PrimaryKey: []string{"tenant_id", "id"}},
			{StructName: "Right", Name: "right_nodes", PrimaryKey: []string{"tenant_id", "id"}},
		},
		Fields: []goschema.Field{
			{StructName: "Left", Name: "tenant_id", Type: "BIGINT"},
			{StructName: "Left", Name: "id", Type: "BIGINT"},
			{StructName: "Left", Name: "right_id", Type: "BIGINT"},
			{StructName: "Right", Name: "tenant_id", Type: "BIGINT"},
			{StructName: "Right", Name: "id", Type: "BIGINT"},
			{StructName: "Right", Name: "left_id", Type: "BIGINT"},
		},
		Constraints: []goschema.Constraint{
			{StructName: "Left", Name: "fk_left_right", Type: "FOREIGN KEY", Columns: []string{"tenant_id", "right_id"}, ForeignTable: "right_nodes", ForeignColumns: []string{"tenant_id", "id"}},
			{StructName: "Right", Name: "fk_right_left", Type: "FOREIGN KEY", Columns: []string{"tenant_id", "left_id"}, ForeignTable: "left_nodes", ForeignColumns: []string{"tenant_id", "id"}},
		},
	}
}

func indexedForeignKeyDatabase() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parents"},
			{StructName: "Child", Name: "children"},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "tenant_id", Type: "INTEGER"},
			{StructName: "Parent", Name: "code", Type: "INTEGER"},
			{StructName: "Child", Name: "tenant_id", Type: "INTEGER"},
			{StructName: "Child", Name: "parent_code", Type: "INTEGER"},
		},
		Indexes: []goschema.Index{
			{StructName: "Parent", Name: "idx_parents_tenant_code", Fields: []string{"tenant_id", "code"}},
			{StructName: "Child", Name: "idx_children_tenant_parent_code", Fields: []string{"tenant_id", "parent_code"}},
		},
		Constraints: []goschema.Constraint{{
			StructName:     "Child",
			Name:           "fk_children_parents",
			Type:           "FOREIGN KEY",
			Columns:        []string{"tenant_id", "parent_code"},
			ForeignTable:   "parents",
			ForeignColumns: []string{"tenant_id", "code"},
		}},
	}
}

func foreignKeyAlterNode(onDelete, onUpdate string) *ast.AlterTableNode {
	return &ast.AlterTableNode{
		Name: "children",
		Operations: []ast.AlterOperation{&ast.AddConstraintOperation{
			Constraint: ast.NewForeignKeyConstraint(
				"fk_children_parents",
				[]string{"parent_id"},
				&ast.ForeignKeyRef{
					Table:    "parents",
					Column:   "id",
					OnDelete: onDelete,
					OnUpdate: onUpdate,
				},
			),
		}},
	}
}

func sqlServerDiamondCascadeDatabase() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Root", Name: "roots"},
			{StructName: "Left", Name: "left_nodes"},
			{StructName: "Right", Name: "right_nodes"},
			{StructName: "Leaf", Name: "leaves"},
		},
		Fields: []goschema.Field{
			{StructName: "Root", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Left", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Left", Name: "root_id", Type: "INTEGER", Foreign: "roots(id)", OnDelete: "CASCADE"},
			{StructName: "Right", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Right", Name: "root_id", Type: "INTEGER", Foreign: "roots(id)", OnDelete: "CASCADE"},
			{StructName: "Leaf", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Leaf", Name: "left_id", Type: "INTEGER", Foreign: "left_nodes(id)", OnDelete: "CASCADE"},
			{StructName: "Leaf", Name: "right_id", Type: "INTEGER", Foreign: "right_nodes(id)", OnDelete: "CASCADE"},
		},
	}
}

func duplicateForeignKeyNameDatabase() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parents"},
			{StructName: "FirstChild", Name: "first_children"},
			{StructName: "SecondChild", Name: "second_children"},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "FirstChild", Name: "parent_id", Type: "INTEGER", Foreign: "parents(id)", ForeignKeyName: "fk_shared_parent"},
			{StructName: "SecondChild", Name: "parent_id", Type: "INTEGER", Foreign: "parents(id)", ForeignKeyName: "fk_shared_parent"},
		},
	}
}

func simpleForeignKeyDatabase(parentType, childType string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parents"},
			{StructName: "Child", Name: "children"},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "id", Type: parentType, Primary: true},
			{StructName: "Child", Name: "parent_id", Type: childType, Foreign: "parents(id)"},
		},
	}
}

func alterConstraintName(statement string) string {
	_, suffix, _ := strings.Cut(statement, "ADD CONSTRAINT ")
	name, _, _ := strings.Cut(suffix, " ")
	return name
}
