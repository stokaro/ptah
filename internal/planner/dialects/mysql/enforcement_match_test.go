package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mysql"
	"ptah.run/migration/schemadiff/difftypes"
)

// planMySQL renders the plan for diff on MySQL 8.4.
func planMySQL(c *qt.C, diff *difftypes.SchemaDiff) string {
	c.Helper()
	nodes, err := mysql.NewForDialect(platform.MySQL, capability.MySQL84()).GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		diff,
	)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.MySQL, capability.MySQL84(), nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// TestPlanner_ForeignKeysKeepTheirMatchType adds a foreign key with the MATCH
// type it declares on every path the plan builds one: a table it creates, an
// addition the comparison reports, and a key it puts back around a change to
// one of its columns. MySQL 8.4.11 and 9.7.2 keep MATCH FULL and PARTIAL in
// REFERENTIAL_CONSTRAINTS.MATCH_OPTION (stokaro/ptah#3853).
func TestPlanner_ForeignKeysKeepTheirMatchType(t *testing.T) {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users", PrimaryKey: []string{"id"}},
			{StructName: "Post", Name: "posts", PrimaryKey: []string{"id"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INT", Primary: true},
			{StructName: "Post", Name: "id", Type: "INT", Primary: true},
			{StructName: "Post", Name: "user_id", Type: "INT", Nullable: true, Foreign: "users(id)", ForeignKeyMatch: "FULL"},
		},
	}
	typeChange := typeChangeDiff("posts", "user_id", "INT -> BIGINT")
	typeChange.TablesModified[0].ColumnsModified[0].Desired = schemamodel.Field{
		Name: "user_id", Type: "BIGINT", StructName: "Post", Nullable: true,
	}
	typeChange.DeclaredForeignKeys = []difftypes.ForeignKeyDeclaration{{
		TableName: "posts", Name: "fk_posts_user_id", Columns: []string{"user_id"},
		ForeignTable: "users", ForeignColumn: "id", Match: "PARTIAL",
	}}
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "a table the plan creates",
			diff: withDeclaredObjects(&difftypes.SchemaDiff{
				TablesAdded: difftypes.TableCreationsFor(desired, identifier.ForDialect("mysql"), "users", "posts"),
			}, desired),
			want: "FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) MATCH FULL",
		},
		{
			name: "an addition the comparison reports",
			diff: &difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{{
				Name: "posts_user_fk", TableName: "posts", Type: "FOREIGN KEY", Columns: []string{"user_id"},
				ForeignTable: "users", ForeignColumn: "id", Match: "PARTIAL",
			}}},
			want: "FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) MATCH PARTIAL",
		},
		{
			name: "a key put back around a column type change",
			diff: typeChange,
			want: "ADD CONSTRAINT `fk_posts_user_id` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) MATCH PARTIAL",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql := planMySQL(c, test.diff)

			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestPlanner_AnAddedColumnKeepsItsCheckUnenforced adds a column whose CHECK
// the server does not enforce, and the ADD COLUMN says so. MySQL 8.4.11 records
// such a CHECK with ENFORCED = NO (stokaro/ptah#3853).
func TestPlanner_AnAddedColumnKeepsItsCheckUnenforced(t *testing.T) {
	c := qt.New(t)
	score := schemamodel.Field{
		StructName: "Post", Name: "score", Type: "INT", Nullable: true, Check: "score > 0", CheckNotEnforced: true,
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Post", Name: "posts", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{{StructName: "Post", Name: "id", Type: "INT", Primary: true}, score},
	}
	diff := withDeclaredObjects(&difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "posts", ColumnsAdded: difftypes.ColumnChanges{score},
	}}}, desired)

	sql := planMySQL(c, diff)

	c.Assert(sql, qt.Contains, "CHECK (score > 0) NOT ENFORCED")
}
