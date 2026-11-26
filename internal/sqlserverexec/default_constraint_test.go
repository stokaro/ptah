package sqlserverexec_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlserverexec"
)

// dropUsersStatus is the statement that drops the default of dbo.users.status.
const dropUsersStatus = "EXEC sp_executesql N'DECLARE @drop nvarchar(max) = (SELECT N''ALTER TABLE [dbo].[users] DROP CONSTRAINT '' + QUOTENAME(dc.name)" +
	" FROM sys.default_constraints AS dc WHERE dc.parent_object_id = OBJECT_ID(N''[dbo].[users]'')" +
	" AND dc.parent_column_id = COLUMNPROPERTY(dc.parent_object_id, N''status'', ''ColumnId'')); EXEC (@drop);';"

func TestDefaultConstraintBatch(t *testing.T) {
	tests := []struct {
		name   string
		table  []string
		column string
		add    string
		want   string
	}{
		{
			name:  "drop",
			table: []string{"dbo", "users"}, column: "status",
			want: dropUsersStatus,
		},
		{
			name:  "replace",
			table: []string{"dbo", "users"}, column: "status", add: "ALTER TABLE [dbo].[users] ADD DEFAULT 'active' FOR [status];",
			want: strings.TrimSuffix(dropUsersStatus, "';") + " ALTER TABLE [dbo].[users] ADD DEFAULT ''active'' FOR [status];';",
		},
		{
			name:  "a quote and a bracket in the names",
			table: []string{"dbo", "o'k]t"}, column: "it's",
			want: "EXEC sp_executesql N'DECLARE @drop nvarchar(max) = (SELECT N''ALTER TABLE [dbo].[o''''k]]t] DROP CONSTRAINT '' + QUOTENAME(dc.name)" +
				" FROM sys.default_constraints AS dc WHERE dc.parent_object_id = OBJECT_ID(N''[dbo].[o''''k]]t]'')" +
				" AND dc.parent_column_id = COLUMNPROPERTY(dc.parent_object_id, N''it''''s'', ''ColumnId'')); EXEC (@drop);';",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlserverexec.DefaultConstraintBatch(test.table, test.column, test.add), qt.Equals, test.want)
		})
	}
}

// TestReadDefaultDrop_HappyPath reads the column back out of every drop
// DefaultConstraintBatch writes.
func TestReadDefaultDrop_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		table  []string
		column string
	}{
		{name: "schema and table", table: []string{"dbo", "users"}, column: "status"},
		{name: "table alone", table: []string{"users"}, column: "status"},
		{name: "quotes and brackets", table: []string{"dbo", "o'k]t"}, column: "it's]"},
		{name: "dots", table: []string{"app.v2", "users.old"}, column: "a.b"},
		{name: "names outside the code page", table: []string{"dbo", "größe"}, column: "注文"},
		{name: "three parts", table: []string{"other", "dbo", "users"}, column: "status"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := sqlserverexec.ReadDefaultDrop(sqlserverexec.DefaultConstraintBatch(test.table, test.column, ""))

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, sqlserverexec.DefaultDrop{Table: test.table, Column: test.column})
		})
	}
}

// TestReadDefaultDrop_FailurePath holds the statements that read as no drop of
// a default. Most are dropUsersStatus with one change, so the catalog lookup
// and the table it drops from have to agree with the column and table read out
// of it, byte for byte. The column's name alone is not among them: it appears
// once, in the lookup, and another name there is the drop of that column's
// default, which TestReadDefaultDrop_HappyPath reads.
func TestReadDefaultDrop_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "one byte changed", statement: strings.Replace(dropUsersStatus, "nvarchar(max)", "nvarchar(4000)", 1)},
		{name: "another table in ALTER TABLE", statement: strings.Replace(dropUsersStatus, "ALTER TABLE [dbo].[users]", "ALTER TABLE [dbo].[orders]", 1)},
		{name: "another database in ALTER TABLE", statement: strings.Replace(dropUsersStatus, "ALTER TABLE [dbo].[users]", "ALTER TABLE [other].[dbo].[users]", 1)},
		{name: "another table in the lookup", statement: strings.Replace(dropUsersStatus, "OBJECT_ID(N''[dbo].[users]'')", "OBJECT_ID(N''[dbo].[orders]'')", 1)},
		{
			name:      "the column of another table in the lookup",
			statement: strings.Replace(dropUsersStatus, "COLUMNPROPERTY(dc.parent_object_id,", "COLUMNPROPERTY(OBJECT_ID(N''[dbo].[orders]''),", 1),
		},
		{
			name:      "a lookup not scoped to the column",
			statement: strings.Replace(dropUsersStatus, " AND dc.parent_column_id = COLUMNPROPERTY(dc.parent_object_id, N''status'', ''ColumnId'')", "", 1),
		},
		{name: "another catalog view", statement: strings.Replace(dropUsersStatus, "sys.default_constraints", "sys.check_constraints", 1)},
		{name: "DROP TABLE for DROP CONSTRAINT", statement: strings.Replace(dropUsersStatus, "DROP CONSTRAINT", "DROP TABLE", 1)},
		{name: "a statement added to the batch", statement: strings.Replace(dropUsersStatus, "EXEC (@drop);", "EXEC (@drop); DROP TABLE [dbo].[orders];", 1)},
		{name: "a default added after the drop", statement: strings.TrimSuffix(dropUsersStatus, "';") + " ALTER TABLE [dbo].[users] ADD DEFAULT ''active'' FOR [status];';"},
		{name: "a statement after it", statement: dropUsersStatus + " DROP TABLE [dbo].[orders];"},
		{name: "a plain literal", statement: strings.Replace(dropUsersStatus, "sp_executesql N'", "sp_executesql '", 1)},
		{name: "EXECUTE", statement: strings.Replace(dropUsersStatus, "EXEC sp_executesql", "EXECUTE sp_executesql", 1)},
		{name: "no EXEC", statement: strings.TrimPrefix(dropUsersStatus, "EXEC ")},
		{name: "procedure in sys", statement: strings.Replace(dropUsersStatus, "EXEC sp_executesql", "EXEC sys.sp_executesql", 1)},
		{name: "procedure in another database", statement: strings.Replace(dropUsersStatus, "EXEC sp_executesql", "EXEC other..sp_executesql", 1)},
		{name: "parameters", statement: strings.TrimSuffix(dropUsersStatus, ";") + ", N'@x int', @x = 1;"},
		{name: "variable", statement: "EXEC sp_executesql @batch;"},
		{name: "unquoted table in the lookup", statement: strings.Replace(dropUsersStatus, "OBJECT_ID(N''[dbo].[users]'')", "OBJECT_ID(N''dbo.users'')", 1)},
		{name: "another batch", statement: "EXEC sp_executesql N'CREATE TABLE other.dbo.escaped (id int)';"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := sqlserverexec.ReadDefaultDrop(test.statement)

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.DeepEquals, sqlserverexec.DefaultDrop{})
		})
	}
}

func TestAddDefault(t *testing.T) {
	tests := []struct {
		name       string
		table      []string
		column     string
		expression string
		want       string
	}{
		{
			name:  "a literal",
			table: []string{"dbo", "users"}, column: "status", expression: "'active'",
			want: "ALTER TABLE [dbo].[users] ADD DEFAULT 'active' FOR [status];",
		},
		{
			name:  "brackets in the names",
			table: []string{"o]k"}, column: "it]s", expression: "getdate()",
			want: "ALTER TABLE [o]]k] ADD DEFAULT getdate() FOR [it]]s];",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlserverexec.AddDefault(test.table, test.column, test.expression), qt.Equals, test.want)
		})
	}
}

// TestReadDefaultReplace_HappyPath reads the column and the expression back
// out of every replacement DefaultConstraintBatch and AddDefault write. The
// expression is read as text, whatever it holds.
func TestReadDefaultReplace_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		table      []string
		column     string
		expression string
	}{
		{name: "a literal", table: []string{"dbo", "users"}, column: "status", expression: "'active'"},
		{name: "a function", table: []string{"users"}, column: "created", expression: "getdate()"},
		{name: "a sequence", table: []string{"dbo", "users"}, column: "id", expression: "NEXT VALUE FOR [dbo].[seq]"},
		{name: "quotes and brackets", table: []string{"dbo", "o'k]t"}, column: "it's]", expression: "N'it''s FOR [x]'"},
		{name: "a statement after the ALTER TABLE", table: []string{"dbo", "users"}, column: "status", expression: "1 FOR [status] DROP TABLE [t] ALTER TABLE [dbo].[users] ADD DEFAULT 1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statement := sqlserverexec.DefaultConstraintBatch(test.table, test.column, sqlserverexec.AddDefault(test.table, test.column, test.expression))

			got, ok := sqlserverexec.ReadDefaultReplace(statement)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, sqlserverexec.DefaultReplace{Table: test.table, Column: test.column, Expression: test.expression})
		})
	}
}

// TestReadDefaultReplace_FailurePath holds the batches that read as no
// replacement: the drop alone, and a drop followed by anything but the
// AddDefault of the same table and column.
func TestReadDefaultReplace_FailurePath(t *testing.T) {
	users := []string{"dbo", "users"}
	replaceActive := sqlserverexec.DefaultConstraintBatch(users, "status", sqlserverexec.AddDefault(users, "status", "'active'"))
	tests := []struct {
		name      string
		statement string
	}{
		{name: "the drop alone", statement: dropUsersStatus},
		{name: "a default for another table", statement: sqlserverexec.DefaultConstraintBatch(users, "status", sqlserverexec.AddDefault([]string{"dbo", "orders"}, "status", "1"))},
		{name: "a default for another column", statement: sqlserverexec.DefaultConstraintBatch(users, "status", sqlserverexec.AddDefault(users, "state", "1"))},
		{name: "another statement after the drop", statement: sqlserverexec.DefaultConstraintBatch(users, "status", "DROP TABLE [dbo].[orders];")},
		{name: "no expression", statement: sqlserverexec.DefaultConstraintBatch(users, "status", "ALTER TABLE [dbo].[users] ADD DEFAULT  FOR [status];")},
		{name: "no semicolon after the ALTER TABLE", statement: strings.Replace(replaceActive, "FOR [status];'", "FOR [status]'", 1)},
		{name: "text after the ALTER TABLE", statement: strings.Replace(replaceActive, "FOR [status];'", "FOR [status]; DROP TABLE [dbo].[orders];'", 1)},
		{name: "one byte changed in the drop", statement: strings.Replace(replaceActive, "nvarchar(max)", "nvarchar(4000)", 1)},
		{name: "another table in the drop", statement: strings.Replace(replaceActive, "DROP CONSTRAINT", "DROP TABLE", 1)},
		{name: "a statement after it", statement: replaceActive + " DROP TABLE [dbo].[orders];"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := sqlserverexec.ReadDefaultReplace(test.statement)

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.DeepEquals, sqlserverexec.DefaultReplace{})
		})
	}
}
