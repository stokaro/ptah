package atlasmigrate_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/atlasmigrate"
)

func TestParseQualifier_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "empty is zero", raw: "", want: ""},
		{name: "blank is zero", raw: "   ", want: ""},
		{name: "plain identifier", raw: "market", want: "market"},
		{name: "trimmed", raw: "  tenant_1  ", want: "tenant_1"},
		{name: "mixed case preserved", raw: "Tenant", want: "Tenant"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			qualifier, err := atlasmigrate.ParseQualifier(test.raw)
			c.Assert(err, qt.IsNil)
			c.Assert(qualifier.Name(), qt.Equals, test.want)
			c.Assert(qualifier.IsZero(), qt.Equals, test.want == "")
		})
	}
}

func TestParseQualifier_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name:    "dot",
			raw:     "bad.name",
			wantErr: `invalid --qualifier "bad\.name": character '\.' is not allowed in a schema qualifier`,
		},
		{
			name:    "double quote",
			raw:     `te"nant`,
			wantErr: `invalid --qualifier "te\\"nant": character '"' is not allowed in a schema qualifier`,
		},
		{
			name:    "backtick",
			raw:     "te`nant",
			wantErr: "invalid --qualifier \"te`nant\": character '`' is not allowed in a schema qualifier",
		},
		{
			name:    "newline",
			raw:     "ten\nant",
			wantErr: `invalid --qualifier "ten\\nant": control characters are not allowed`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			qualifier, err := atlasmigrate.ParseQualifier(test.raw)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(qualifier.IsZero(), qt.IsTrue)
		})
	}
}

func TestQualifierValidateScope_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		schemas []string
	}{
		{name: "postgres no schemas", dialect: platform.Postgres},
		{name: "postgres one schema", dialect: platform.Postgres, schemas: []string{"app"}},
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			qualifier, err := atlasmigrate.ParseQualifier("tenant")
			c.Assert(err, qt.IsNil)
			c.Assert(qualifier.ValidateScope(test.dialect, test.schemas), qt.IsNil)
		})
	}
}

func TestQualifierValidateScope_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("unsupported dialect", func(c *qt.C) {
		qualifier, err := atlasmigrate.ParseQualifier("tenant")
		c.Assert(err, qt.IsNil)
		c.Assert(qualifier.ValidateScope(platform.SQLite, nil), qt.ErrorMatches,
			`atlas migrate diff --qualifier is not supported for dialect "sqlite"`)
	})

	c.Run("multiple schema scope", func(c *qt.C) {
		qualifier, err := atlasmigrate.ParseQualifier("tenant")
		c.Assert(err, qt.IsNil)
		c.Assert(qualifier.ValidateScope(platform.Postgres, []string{"app", "audit"}), qt.ErrorMatches,
			`atlas migrate diff --qualifier "tenant" requires a single schema scope, got --schema "app,audit"`)
	})

	c.Run("zero qualifier skips validation", func(c *qt.C) {
		c.Assert(atlasmigrate.Qualifier{}.ValidateScope(platform.SQLite, []string{"a", "b"}), qt.IsNil)
	})
}

func mustParseQualifier(c *qt.C, raw string) atlasmigrate.Qualifier {
	c.Helper()
	qualifier, err := atlasmigrate.ParseQualifier(raw)
	c.Assert(err, qt.IsNil)
	return qualifier
}

func renderQualified(c *qt.C, dialect string, nodes ...ast.Node) string {
	c.Helper()
	output, err := renderer.RenderSQLWithCapabilities(dialect, capability.ForDialect(dialect), nodes...)
	c.Assert(err, qt.IsNil)
	return output
}

func TestQualifierApplyToPlan_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Run("postgres create table with foreign key", func(c *qt.C) {
		table := ast.NewCreateTable("users").
			AddColumn(ast.NewColumn("id", "SERIAL").SetPrimary()).
			AddColumn(ast.NewColumn("org_id", "INTEGER").SetForeignKey("orgs", "id", "fk_users_org"))
		qualifier := mustParseQualifier(c, "tenant")

		err := qualifier.ApplyToPlan(platform.Postgres, &goschema.Database{}, []ast.Node{table})

		c.Assert(err, qt.IsNil)
		sql := renderQualified(c, platform.Postgres, table)
		c.Assert(sql, qt.Contains, `CREATE TABLE "tenant"."users"`)
		c.Assert(sql, qt.Contains, `REFERENCES "tenant"."orgs"`)
	})

	c.Run("postgres alter table add constraint and index", func(c *qt.C) {
		alter := &ast.AlterTableNode{
			Name: "users",
			Operations: []ast.AlterOperation{
				&ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{
					Type:    ast.ForeignKeyConstraint,
					Name:    "fk_users_org",
					Columns: []string{"org_id"},
					Reference: &ast.ForeignKeyRef{
						Table:  "orgs",
						Column: "id",
					},
				}},
			},
		}
		index := &ast.IndexNode{Name: "idx_users_email", Table: "users", Columns: []string{"email"}}
		qualifier := mustParseQualifier(c, "tenant")

		err := qualifier.ApplyToPlan(platform.Postgres, &goschema.Database{}, []ast.Node{alter, index})

		c.Assert(err, qt.IsNil)
		sql := renderQualified(c, platform.Postgres, alter, index)
		c.Assert(sql, qt.Contains, `ALTER TABLE "tenant"."users"`)
		c.Assert(sql, qt.Contains, `REFERENCES "tenant"."orgs"`)
		c.Assert(sql, qt.Contains, `CREATE INDEX "idx_users_email" ON "tenant"."users"`)
	})

	c.Run("postgres drop index and drop table", func(c *qt.C) {
		dropIndex := ast.NewDropIndex("idx_users_email").SetTable("users")
		dropTable := ast.NewDropTable("orders")
		qualifier := mustParseQualifier(c, "tenant")

		err := qualifier.ApplyToPlan(platform.Postgres, &goschema.Database{}, []ast.Node{dropIndex, dropTable})

		c.Assert(err, qt.IsNil)
		sql := renderQualified(c, platform.Postgres, dropIndex, dropTable)
		c.Assert(sql, qt.Contains, `DROP INDEX "tenant"."idx_users_email"`)
		c.Assert(sql, qt.Contains, `DROP TABLE "tenant"."orders"`)
	})

	c.Run("existing single schema qualification is replaced", func(c *qt.C) {
		table := ast.NewCreateTable("app.users").
			AddColumn(ast.NewColumn("id", "SERIAL").SetPrimary())
		qualifier := mustParseQualifier(c, "tenant")

		err := qualifier.ApplyToPlan(platform.Postgres, &goschema.Database{}, []ast.Node{table})

		c.Assert(err, qt.IsNil)
		sql := renderQualified(c, platform.Postgres, table)
		c.Assert(sql, qt.Contains, `CREATE TABLE "tenant"."users"`)
	})

	c.Run("mysql create index uses backtick quoting", func(c *qt.C) {
		index := &ast.IndexNode{Name: "idx_users_email", Table: "users", Columns: []string{"email"}}
		qualifier := mustParseQualifier(c, "market")

		err := qualifier.ApplyToPlan(platform.MySQL, &goschema.Database{}, []ast.Node{index})

		c.Assert(err, qt.IsNil)
		sql := renderQualified(c, platform.MySQL, index)
		c.Assert(sql, qt.Contains, "ON `market`.`users`")
	})

	c.Run("comments pass through untouched", func(c *qt.C) {
		comment := ast.NewComment("WARNING: destructive operation")
		qualifier := mustParseQualifier(c, "tenant")

		err := qualifier.ApplyToPlan(platform.Postgres, &goschema.Database{}, []ast.Node{comment})

		c.Assert(err, qt.IsNil)
		c.Assert(comment.Text, qt.Equals, "WARNING: destructive operation")
	})

	c.Run("zero qualifier leaves nodes untouched", func(c *qt.C) {
		table := ast.NewCreateTable("users")

		err := atlasmigrate.Qualifier{}.ApplyToPlan(platform.Postgres, &goschema.Database{}, []ast.Node{table})

		c.Assert(err, qt.IsNil)
		c.Assert(table.Name, qt.Equals, "users")
	})
}

func TestQualifierApplyToPlan_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("unsupported dialect", func(c *qt.C) {
		qualifier := mustParseQualifier(c, "tenant")
		err := qualifier.ApplyToPlan(platform.SQLite, &goschema.Database{}, []ast.Node{ast.NewCreateTable("users")})
		c.Assert(err, qt.ErrorMatches, `atlas migrate diff --qualifier is not supported for dialect "sqlite"`)
	})

	c.Run("unsupported statement kind", func(c *qt.C) {
		qualifier := mustParseQualifier(c, "tenant")
		err := qualifier.ApplyToPlan(platform.Postgres, &goschema.Database{}, []ast.Node{ast.NewEnum("status", "a", "b")})
		c.Assert(err, qt.ErrorMatches, `atlas migrate diff --qualifier "tenant" does not support \*ast\.EnumNode statements yet`)
	})

	c.Run("unsupported alter operation kind", func(c *qt.C) {
		qualifier := mustParseQualifier(c, "tenant")
		alter := &ast.AlterTableNode{
			Name:       "users",
			Operations: []ast.AlterOperation{&ast.RenameTableOperation{NewName: "customers"}},
		}
		err := qualifier.ApplyToPlan(platform.Postgres, &goschema.Database{}, []ast.Node{alter})
		c.Assert(err, qt.ErrorMatches, `atlas migrate diff --qualifier "tenant" does not support \*ast\.RenameTableOperation alter operations yet`)
	})

	c.Run("plan spanning several schemas", func(c *qt.C) {
		qualifier := mustParseQualifier(c, "tenant")
		nodes := []ast.Node{
			ast.NewCreateTable("app.users"),
			ast.NewCreateTable("audit.logs"),
		}
		err := qualifier.ApplyToPlan(platform.Postgres, &goschema.Database{}, nodes)
		c.Assert(err, qt.ErrorMatches, `found 2 schemas when migration plan is scoped to one: \["app" "audit"\]`)
	})

	c.Run("enum typed column", func(c *qt.C) {
		qualifier := mustParseQualifier(c, "tenant")
		desired := &goschema.Database{Enums: []goschema.Enum{{Name: "enum_user_status"}}}
		table := ast.NewCreateTable("users").
			AddColumn(ast.NewColumn("status", "enum_user_status"))
		err := qualifier.ApplyToPlan(platform.Postgres, desired, []ast.Node{table})
		c.Assert(err, qt.ErrorMatches,
			`atlas migrate diff --qualifier "tenant": table "users" column "status" uses enum type "enum_user_status"; qualifying enum type references is not supported yet`)
	})

	c.Run("drop index without owning table", func(c *qt.C) {
		qualifier := mustParseQualifier(c, "tenant")
		err := qualifier.ApplyToPlan(platform.Postgres, &goschema.Database{}, []ast.Node{ast.NewDropIndex("idx_orphan")})
		c.Assert(err, qt.ErrorMatches,
			`atlas migrate diff --qualifier "tenant" cannot qualify DROP INDEX "idx_orphan" without its owning table`)
	})
}
