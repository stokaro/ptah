package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin/internal/dialects/ydb"
)

// accountsTable is a table carrying every column shape and index kind the
// renderer writes. Its rendering on 26.2 was applied to local-ydb 26.2.1.14 one
// statement per query and read back with `scheme describe`: every type, NOT
// NULL, literal default, the key and the three index kinds came back as
// written.
func accountsTable() *ast.CreateTableNode {
	table := ast.NewCreateTable("accounts")
	table.AddColumn(ast.NewColumn("id", "BIGSERIAL").SetPrimary())
	table.AddColumn(ast.NewColumn("email", "VARCHAR(255)").SetNotNull())
	table.AddColumn(ast.NewColumn("name", "TEXT"))
	table.AddColumn(ast.NewColumn("status", "VARCHAR(20)").SetNotNull().SetDefault("active"))
	table.AddColumn(ast.NewColumn("active", "BOOLEAN").SetNotNull().SetDefault("true"))
	table.AddColumn(ast.NewColumn("created_at", "TIMESTAMP").SetNotNull().SetDefault("2026-01-02 03:04:05"))
	table.AddIndex(&ast.IndexNode{Name: "accounts_email_uq", Table: "accounts", Columns: []string{"email"}, Unique: true})
	table.AddIndex(&ast.IndexNode{Name: "accounts_name_async", Table: "accounts", Columns: []string{"name"}, Type: "async"})
	table.AddIndex(&ast.IndexNode{Name: "accounts_status_cover", Table: "accounts", Columns: []string{"status"}, IncludeColumns: []string{"name"}})
	return table
}

// TestRender_CreateTable_HappyPath pins CREATE TABLE on the newest and the
// oldest line: the key as a table-level clause with NOT NULL on its columns,
// every index inside the statement, and the types and literals the line takes.
func TestRender_CreateTable_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "every index kind on 26.2",
			caps: capability.YDB262(),
			node: accountsTable(),
			want: "CREATE TABLE `accounts` (\n" +
				"    `id` BigSerial NOT NULL,\n" +
				"    `email` Utf8 NOT NULL,\n" +
				"    `name` Utf8,\n" +
				"    `status` Utf8 NOT NULL DEFAULT 'active'u,\n" +
				"    `active` Bool NOT NULL DEFAULT true,\n" +
				"    `created_at` Timestamp64 NOT NULL DEFAULT Timestamp64('2026-01-02T03:04:05Z'),\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    INDEX `accounts_email_uq` GLOBAL UNIQUE SYNC ON (`email`),\n" +
				"    INDEX `accounts_name_async` GLOBAL ASYNC ON (`name`),\n" +
				"    INDEX `accounts_status_cover` GLOBAL SYNC ON (`status`) COVER (`name`)\n" +
				");\n",
		},
		{
			name: "the narrow types on 25.1",
			caps: capability.YDB251(),
			node: accountsTable(),
			want: "CREATE TABLE `accounts` (\n" +
				"    `id` BigSerial NOT NULL,\n" +
				"    `email` Utf8 NOT NULL,\n" +
				"    `name` Utf8,\n" +
				"    `status` Utf8 NOT NULL DEFAULT 'active'u,\n" +
				"    `active` Bool NOT NULL DEFAULT true,\n" +
				"    `created_at` Timestamp NOT NULL DEFAULT Timestamp('2026-01-02T03:04:05Z'),\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    INDEX `accounts_email_uq` GLOBAL UNIQUE SYNC ON (`email`),\n" +
				"    INDEX `accounts_name_async` GLOBAL ASYNC ON (`name`),\n" +
				"    INDEX `accounts_status_cover` GLOBAL SYNC ON (`status`) COVER (`name`)\n" +
				");\n",
		},
		{
			// A key column the declaration left nullable is still NOT NULL: a
			// nullable YDB key takes one row whose key is NULL.
			name: "a composite key from a constraint, in a directory",
			caps: capability.YDB262(),
			node: &ast.CreateTableNode{
				Name: "audit.events",
				Columns: []*ast.ColumnNode{
					ast.NewColumn("tenant", "VARCHAR(64)"),
					ast.NewColumn("seq", "INTEGER"),
					ast.NewColumn("payload", "JSONB"),
				},
				Constraints: []*ast.ConstraintNode{{Type: ast.PrimaryKeyConstraint, Columns: []string{"tenant", "seq"}}},
			},
			want: "CREATE TABLE `audit/events` (\n" +
				"    `tenant` Utf8 NOT NULL,\n" +
				"    `seq` Int32 NOT NULL,\n" +
				"    `payload` JsonDocument,\n" +
				"    PRIMARY KEY (`tenant`, `seq`)\n" +
				");\n",
		},
		{
			name: "a guard, a serial from auto-increment, and the author's own clause",
			caps: capability.YDB262(),
			node: &ast.CreateTableNode{
				Name:        "jobs",
				IfNotExists: true,
				Columns: []*ast.ColumnNode{
					{Name: "id", Type: "INTEGER", Primary: true, AutoInc: true},
					{Name: "attempts", Type: "SMALLINT", Nullable: true, Default: &ast.DefaultValue{Value: "0", ValueSet: true}},
				},
				CustomSQL: "WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED)",
			},
			want: "CREATE TABLE IF NOT EXISTS `jobs` (\n" +
				"    `id` Serial NOT NULL,\n" +
				"    `attempts` Int16 DEFAULT 0s,\n" +
				"    PRIMARY KEY (`id`)\n" +
				") WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED);\n",
		},
		{
			// A quoted default from a SQL source is read back to its value
			// before it is written as YQL, and a NULL default on a nullable
			// column writes nothing, which is what YDB stores for it.
			name: "a quoted default and a NULL default",
			caps: capability.YDB262(),
			node: &ast.CreateTableNode{
				Name: "notes",
				Columns: []*ast.ColumnNode{
					ast.NewColumn("id", "UUID").SetPrimary(),
					{Name: "body", Type: "TEXT", Nullable: true, Default: &ast.DefaultValue{Value: `'it''s'`, ValueSet: true}},
					{Name: "extra", Type: "JSON", Nullable: true, Default: &ast.DefaultValue{Value: "NULL", ValueSet: true}},
				},
			},
			want: "CREATE TABLE `notes` (\n" +
				"    `id` Uuid NOT NULL,\n" +
				"    `body` Utf8 DEFAULT 'it\\'s'u,\n" +
				"    `extra` Json,\n" +
				"    PRIMARY KEY (`id`)\n" +
				");\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRender_Changes_HappyPath pins the statements a plan sends to a table
// that exists: one per operation, an index through its table, and only the
// column changes YDB makes in place. Each was applied to local-ydb 26.2.1.14
// and 25.1.4.7 and read back.
func TestRender_Changes_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "an index added to a table that exists",
			caps: capability.YDB251(),
			node: &ast.IndexNode{Name: "events_kind", Table: "audit.events", Columns: []string{"kind", "seq"}, Type: "btree"},
			want: "ALTER TABLE `audit/events` ADD INDEX `events_kind` GLOBAL SYNC ON (`kind`, `seq`);\n",
		},
		{
			name: "an index dropped through its table",
			caps: capability.YDB251(),
			node: &ast.DropIndexNode{Name: "events_kind", Table: "audit.events"},
			want: "ALTER TABLE `audit/events` DROP INDEX `events_kind`;\n",
		},
		{
			name: "one statement per operation",
			caps: capability.YDB251(),
			node: &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{
				&ast.AddColumnOperation{Column: ast.NewColumn("note", "TEXT")},
				&ast.DropColumnOperation{ColumnName: "legacy"},
				&ast.RenameIndexOperation{From: "items_a", To: "items_b"},
				&ast.AddIndexOperation{Index: &ast.IndexNode{Name: "items_note", Columns: []string{"note"}}},
				&ast.RenameTableOperation{NewName: "goods"},
			}},
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` DROP COLUMN `legacy`;\n" +
				"ALTER TABLE `items` RENAME INDEX `items_a` TO `items_b`;\n" +
				"ALTER TABLE `items` ADD INDEX `items_note` GLOBAL SYNC ON (`note`);\n" +
				"ALTER TABLE `items` RENAME TO `goods`;\n",
		},
		{
			name: "a column added with a default where the line fills existing rows",
			caps: capability.YDB261(),
			node: &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{
				&ast.AddColumnOperation{Column: ast.NewColumn("qty", "BIGINT").SetNotNull().SetDefault("7")},
			}},
			want: "ALTER TABLE `items` ADD COLUMN `qty` Int64 NOT NULL DEFAULT 7l;\n",
		},
		{
			name: "a column made nullable",
			caps: capability.YDB251(),
			node: &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{
				&ast.ModifyColumnOperation{
					Column:     ast.NewColumn("label", "TEXT"),
					Changed:    ast.ColumnProperties{Nullability: true},
					HasChanged: true,
				},
			}},
			want: "ALTER TABLE `items` ALTER COLUMN `label` DROP NOT NULL;\n",
		},
		{
			name: "a default set and one dropped where the line changes them in place",
			caps: capability.YDB262(),
			node: &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{
				&ast.ModifyColumnOperation{
					Column:     ast.NewColumn("qty", "INTEGER").SetDefault("2"),
					Changed:    ast.ColumnProperties{Default: true},
					HasChanged: true,
				},
				&ast.ModifyColumnOperation{
					Column:     ast.NewColumn("label", "TEXT"),
					Changed:    ast.ColumnProperties{Default: true},
					HasChanged: true,
				},
			}},
			want: "ALTER TABLE `items` ALTER COLUMN `qty` SET DEFAULT 2;\n" +
				"ALTER TABLE `items` ALTER COLUMN `label` DROP DEFAULT;\n",
		},
		{
			name: "a table dropped with its guard",
			caps: capability.YDB251(),
			node: &ast.DropTableNode{Names: []string{"a", "b"}, IfExists: true},
			want: "DROP TABLE IF EXISTS `a`;\nDROP TABLE IF EXISTS `b`;\n",
		},
		{
			name: "a planner's note",
			caps: capability.YDB262(),
			node: &ast.StatementList{Statements: []ast.Node{ast.NewComment("drop the old table"), ast.NewComment("")}},
			want: "-- drop the old table\n--\n",
		},
		{
			name: "the author's own statement",
			caps: capability.YDB262(),
			node: &ast.RawSQLNode{SQL: "ALTER TABLE `t` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED)  "},
			want: "ALTER TABLE `t` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED);\n",
		},
		{
			// The query goes as written, and the semicolon on a line of its
			// own, after a trailing line comment the query may end with.
			name: "a view in a directory, its query as written",
			caps: capability.YDB251(),
			node: ast.NewCreateView("app.active").SetBody("SELECT id FROM `app/users` -- live ones\n;\n"),
			want: "CREATE VIEW `app/active` WITH (security_invoker = TRUE) AS\n" +
				"SELECT id FROM `app/users` -- live ones\n" +
				";\n",
		},
		{
			name: "a view dropped",
			caps: capability.YDB262(),
			node: ast.NewDropView("app.active"),
			want: "DROP VIEW `app/active`;\n",
		},
		{
			name: "a view dropped with its guard",
			caps: capability.YDB262(),
			node: ast.NewDropView("active").SetIfExists(),
			want: "DROP VIEW IF EXISTS `active`;\n",
		},
		{
			// A Ptah schema is a directory, which the first table path that
			// names it creates: no statement makes one.
			name: "a schema",
			caps: capability.YDB262(),
			node: &ast.CreateSchemaNode{Name: "audit", IfNotExists: true},
			want: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
