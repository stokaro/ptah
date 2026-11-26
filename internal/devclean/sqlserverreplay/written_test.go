package sqlserverreplay_test

import (
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/devclean/sqlserverreplay"
	"ptah.run/internal/sqlserverexec"
)

// rendered is the statement the SQL Server renderer writes for operation on
// table, as a migration file carries it.
func rendered(c *qt.C, table string, operation ast.AlterOperation) string {
	c.Helper()
	sql, err := builtin.RenderSQL(platform.SQLServer, &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{operation}})
	c.Assert(err, qt.IsNil)
	return strings.TrimSpace(sql)
}

func validate(statement string, scope replay.Scope) error {
	return sqlserverreplay.Services()[0].Guard.Validate(statement, scope)
}

func refusal(operation string) string {
	return "sqlserver migration replay rejects " + regexp.QuoteMeta(operation) +
		" because its effects cannot be confined to the disposable database realm"
}

// TestGuard_AcceptsTheStatementsTheRendererWrites replays, without trust, the
// sp_rename calls and the default drop the SQL Server renderer writes. Each is
// an EXEC, which the guard refuses in every other shape.
func TestGuard_AcceptsTheStatementsTheRendererWrites(t *testing.T) {
	tests := []struct {
		name      string
		table     string
		operation ast.AlterOperation
	}{
		{name: "column rename", table: "dbo.users", operation: &ast.RenameColumnOperation{OldName: "email", NewName: "mail"}},
		{name: "column rename on an unqualified table", table: "users", operation: &ast.RenameColumnOperation{OldName: "email", NewName: "mail"}},
		{
			name:      "column rename with brackets, quotes and dots in the names",
			table:     "[dbo].[o]]k]",
			operation: &ast.RenameColumnOperation{OldName: "[it's.a]", NewName: "b]c'd.e"},
		},
		{
			// Read as words, the new name is the AT of a remote EXECUTE.
			name:      "column renamed to at",
			table:     "dbo.users",
			operation: &ast.RenameColumnOperation{OldName: "location", NewName: "at"},
		},
		{name: "table rename", table: "dbo.users", operation: &ast.RenameTableOperation{NewName: "people"}},
		{name: "table rename with a quote and a dot", table: `"app"."it's.t"`, operation: &ast.RenameTableOperation{NewName: "o'k.t"}},
		{name: "default drop", table: "dbo.users", operation: &ast.AlterColumnOperation{ColumnName: "status", Action: ast.AlterColumnDropDefault}},
		{name: "default drop on an unqualified table", table: "users", operation: &ast.AlterColumnOperation{ColumnName: "status", Action: ast.AlterColumnDropDefault}},
		{
			name:      "default drop with a quote and a bracket in the names",
			table:     "dbo.o'k]t",
			operation: &ast.AlterColumnOperation{ColumnName: "it's", Action: ast.AlterColumnDropDefault},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validate(rendered(c, test.table, test.operation), replay.Scope{}), qt.IsNil)
		})
	}
}

// TestGuard_AcceptsTheRenamesTheSharedRenderingWrites holds the sp_rename
// types the renderer writes no statement for today and the guard accepts,
// because each renames an item of the dev database.
func TestGuard_AcceptsTheRenamesTheSharedRenderingWrites(t *testing.T) {
	tests := []struct {
		name   string
		rename sqlserverexec.Rename
	}{
		{name: "index", rename: sqlserverexec.Rename{Name: []string{"dbo", "users", "ix_email"}, NewName: "ix_mail", Type: sqlserverexec.RenameIndex}},
		{name: "untyped", rename: sqlserverexec.Rename{Name: []string{"dbo", "users"}, NewName: "people"}},
		{name: "column named with a #", rename: sqlserverexec.Rename{Name: []string{"dbo", "users", "#c"}, NewName: "#d", Type: sqlserverexec.RenameColumn}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validate(test.rename.Statement(), replay.Scope{}), qt.IsNil)
		})
	}
}

// TestGuard_RefusesWhatTheRendererDoesNotWrite holds statements one change
// away from an accepted one. A statement that is not in the rendered shape is
// judged as every EXEC is; one that is, but reaches past the dev database, is
// refused for that reason.
func TestGuard_RefusesWhatTheRendererDoesNotWrite(t *testing.T) {
	dropDefault := sqlserverexec.DefaultConstraintBatch([]string{"dbo", "users"}, "status", "")
	tests := []struct {
		name      string
		statement string
		operation string
	}{
		{
			name:      "database rename in the rendered shape",
			statement: "EXEC sp_rename N'[a6688_other]', N'escaped', N'DATABASE';",
			operation: "sp_rename @objtype DATABASE",
		},
		{
			name:      "database rename as written by hand",
			statement: "EXEC sp_rename N'a6688_other', N'escaped', N'DATABASE';",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "user data type",
			statement: "EXEC sp_rename N'[dbo].[money_t]', N'cash_t', N'USERDATATYPE';",
			operation: "sp_rename @objtype USERDATATYPE",
		},
		{
			name:      "statistics",
			statement: "EXEC sp_rename N'[dbo].[users].[st]', N'st2', N'STATISTICS';",
			operation: "sp_rename @objtype STATISTICS",
		},
		{
			name:      "lowercase type",
			statement: "EXEC sp_rename N'[dbo].[users].[email]', N'mail', N'column';",
			operation: "sp_rename @objtype column",
		},
		{
			name:      "table in another database",
			statement: "EXEC sp_rename N'[a6688_other].[dbo].[users]', N'people', N'OBJECT';",
			operation: `cross-database target "a6688_other.dbo.users"`,
		},
		{
			name:      "untyped name in another database",
			statement: "EXEC sp_rename N'[a6688_other].[dbo].[users]', N'people';",
			operation: `cross-database target "a6688_other.dbo.users"`,
		},
		{
			name:      "column in another database",
			statement: "EXEC sp_rename N'[a6688_other].[dbo].[users].[email]', N'mail', N'COLUMN';",
			operation: `cross-database target "a6688_other.dbo.users.email"`,
		},
		{
			name:      "index in another database",
			statement: "EXEC sp_rename N'[a6688_other].[dbo].[users].[ix]', N'ix2', N'INDEX';",
			operation: `cross-database target "a6688_other.dbo.users.ix"`,
		},
		{
			name:      "temporary table",
			statement: "EXEC sp_rename N'[#users]', N'people', N'OBJECT';",
			operation: "temporary object",
		},
		{
			name:      "table renamed to a temporary name",
			statement: "EXEC sp_rename N'[dbo].[users]', N'#people', N'OBJECT';",
			operation: "temporary object",
		},
		{
			name:      "column of a temporary table",
			statement: "EXEC sp_rename N'[dbo].[#users].[email]', N'mail', N'COLUMN';",
			operation: "temporary object",
		},
		{
			name:      "procedure in sys",
			statement: "EXEC sys.sp_rename N'[dbo].[users]', N'people', N'OBJECT';",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "procedure in brackets",
			statement: "EXEC [sys].[sp_rename] N'[dbo].[users]', N'people', N'OBJECT';",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "procedure in master",
			statement: "EXEC master.dbo.sp_rename N'[dbo].[users]', N'people', N'OBJECT';",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "procedure in another database",
			statement: "EXEC a6688_other..sp_rename N'[dbo].[users]', N'people', N'OBJECT';",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "argument in a variable",
			statement: "EXEC sp_rename @name, N'people', N'OBJECT';",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "argument built by concatenation",
			statement: "EXEC sp_rename N'[dbo].[users]', N'peo' + N'ple', N'OBJECT';",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "remote",
			statement: "EXEC sp_rename N'[dbo].[users]', N'people', N'OBJECT' AT linked;",
			operation: "remote EXECUTE",
		},
		{
			name:      "default drop with one byte changed",
			statement: strings.Replace(dropDefault, "nvarchar(max)", "nvarchar(4000)", 1),
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "default drop from another table",
			statement: strings.Replace(dropDefault, "ALTER TABLE [dbo].[users]", "ALTER TABLE [dbo].[orders]", 1),
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "default drop that looks up the column of another table",
			statement: strings.Replace(dropDefault, "COLUMNPROPERTY(dc.parent_object_id,", "COLUMNPROPERTY(OBJECT_ID(N''[dbo].[orders]''),", 1),
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "default drop on a table in another database",
			statement: sqlserverexec.DefaultConstraintBatch([]string{"a6688_other", "dbo", "users"}, "status", ""),
			operation: `cross-database target "a6688_other.dbo.users"`,
		},
		{
			name:      "default drop on a temporary table",
			statement: sqlserverexec.DefaultConstraintBatch([]string{"#users"}, "status", ""),
			operation: "temporary object",
		},
		{
			name:      "sp_executesql with other text",
			statement: "EXEC sp_executesql N'CREATE TABLE a6688_other.dbo.escaped (id int)';",
			operation: "EXEC/EXECUTE sublanguage",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := validate(test.statement, replay.Scope{})

			c.Assert(err, qt.ErrorMatches, refusal(test.operation))
		})
	}
}

// TestGuard_AcceptsADefaultReplacement replays, without trust, the batch the
// renderer writes to replace a default, which a migration that drops one
// writes as its down. The new default is one expression, and the ALTER TABLE
// that adds it is a statement the guard accepts on its own.
func TestGuard_AcceptsADefaultReplacement(t *testing.T) {
	tests := []struct {
		name   string
		table  string
		column string
		dflt   *ast.DefaultValue
	}{
		{name: "a literal", table: "dbo.users", column: "status", dflt: &ast.DefaultValue{Value: "active", ValueSet: true}},
		{name: "a literal with a quote", table: "dbo.o'k]t", column: "it's", dflt: &ast.DefaultValue{Value: "it's", ValueSet: true}},
		{name: "a function", table: "dbo.users", column: "created", dflt: &ast.DefaultValue{Expression: "getdate()"}},
		{name: "a sequence", table: "users", column: "id", dflt: &ast.DefaultValue{Expression: "NEXT VALUE FOR dbo.seq"}},
		{name: "a sequence in parentheses", table: "users", column: "id", dflt: &ast.DefaultValue{Expression: "(next value for [dbo].[seq])"}},
		{name: "nested parentheses", table: "users", column: "label", dflt: &ast.DefaultValue{Expression: "(CONVERT(nvarchar(10), 1))"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			replace := rendered(c, test.table, &ast.AlterColumnOperation{
				ColumnName: test.column, Action: ast.AlterColumnSetDefault, Default: test.dflt,
			})

			c.Assert(validate(replace, replay.Scope{}), qt.IsNil)
		})
	}
}

// TestGuard_RefusesADefaultReplacementThatDoesMore holds replacement batches
// in the rendered shape whose new default is not one expression, or whose
// table is outside the dev database.
func TestGuard_RefusesADefaultReplacementThatDoesMore(t *testing.T) {
	users := []string{"dbo", "users"}
	replace := func(table []string, expression string) string {
		return sqlserverexec.DefaultConstraintBatch(table, "status", sqlserverexec.AddDefault(table, "status", expression))
	}
	tests := []struct {
		name      string
		statement string
		operation string
	}{
		{
			name:      "a statement after the ALTER TABLE",
			statement: replace(users, "1 FOR [status] DROP TABLE [dbo].[orders] ALTER TABLE [dbo].[users] ADD DEFAULT 1"),
			operation: `default expression "1 FOR [status] DROP TABLE [dbo].[orders] ALTER TABLE [dbo].[users] ADD DEFAULT 1"`,
		},
		{
			// One expression to every other check: no FOR, no semicolon, and
			// parentheses that close. The server reads two statements.
			name:      "a statement after the expression",
			statement: replace(users, "1 DROP TABLE [dbo].[orders]"),
			operation: `default expression "1 DROP TABLE [dbo].[orders]"`,
		},
		{
			// The batch runs one ALTER TABLE, which the guard accepts written
			// on its own, so only the expression check refuses it: the text
			// adds a second constraint rather than one expression.
			name:      "a second constraint after the expression",
			statement: replace(users, "1 FOR [status], CONSTRAINT [x] DEFAULT 2"),
			operation: `default expression "1 FOR [status], CONSTRAINT [x] DEFAULT 2"`,
		},
		{
			name:      "a semicolon",
			statement: replace(users, "1; DROP TABLE [dbo].[orders]"),
			operation: `default expression "1; DROP TABLE [dbo].[orders]"`,
		},
		{
			name:      "a sequence followed by a statement",
			statement: replace(users, "NEXT VALUE FOR [s] FOR [status] EXEC (N'x') ALTER TABLE [dbo].[users] ADD DEFAULT 1"),
			operation: `default expression "NEXT VALUE FOR [s] FOR [status] EXEC (N'x') ALTER TABLE [dbo].[users] ADD DEFAULT 1"`,
		},
		{
			name:      "a closing parenthesis first",
			statement: replace(users, "1) FOR [status] DROP TABLE [dbo].[orders] ALTER TABLE [dbo].[users] ADD DEFAULT (1"),
			operation: `default expression "1) FOR [status] DROP TABLE [dbo].[orders] ALTER TABLE [dbo].[users] ADD DEFAULT (1"`,
		},
		{
			// The expression is one, and the statement that adds it is one
			// the guard refuses written on its own.
			name:      "an ADD DEFAULT the guard refuses on its own",
			statement: replace(users, "(LEDGER = ON)"),
			operation: "LEDGER table",
		},
		{
			name:      "an unclosed parenthesis",
			statement: replace(users, "(1"),
			operation: `default expression "(1"`,
		},
		{
			name:      "a table in another database",
			statement: replace([]string{"a6688_other", "dbo", "users"}, "1"),
			operation: `cross-database target "a6688_other.dbo.users"`,
		},
		{
			name:      "a temporary table",
			statement: replace([]string{"#users"}, "1"),
			operation: "temporary object",
		},
		{
			name:      "a column added to another table",
			statement: sqlserverexec.DefaultConstraintBatch(users, "status", sqlserverexec.AddDefault([]string{"dbo", "orders"}, "status", "1")),
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "a statement other than ADD DEFAULT",
			statement: sqlserverexec.DefaultConstraintBatch(users, "status", "DROP TABLE [dbo].[orders];"),
			operation: "EXEC/EXECUTE sublanguage",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := validate(test.statement, replay.Scope{})

			c.Assert(err, qt.ErrorMatches, refusal(test.operation))
		})
	}
}

// TestGuard_TrustedSQLRunsWhatTheShapeRefuses is the control for the
// refusals above that exist only for confinement: a run that trusts its SQL
// replays them.
func TestGuard_TrustedSQLRunsWhatTheShapeRefuses(t *testing.T) {
	tests := []string{
		"EXEC sp_rename N'[a6688_other]', N'escaped', N'DATABASE';",
		"EXEC sp_rename N'[a6688_other].[dbo].[users]', N'people', N'OBJECT';",
		"EXEC sp_executesql N'CREATE TABLE a6688_other.dbo.escaped (id int)';",
	}
	for _, statement := range tests {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validate(statement, replay.Scope{TrustedSQL: true}), qt.IsNil)
		})
	}
}
