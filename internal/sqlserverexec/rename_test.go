package sqlserverexec_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlserverexec"
)

func TestRenameStatement(t *testing.T) {
	tests := []struct {
		name   string
		rename sqlserverexec.Rename
		want   string
	}{
		{
			name:   "column",
			rename: sqlserverexec.Rename{Name: []string{"dbo", "users", "email"}, NewName: "mail", Type: sqlserverexec.RenameColumn},
			want:   "EXEC sp_rename N'[dbo].[users].[email]', N'mail', N'COLUMN';",
		},
		{
			name:   "table",
			rename: sqlserverexec.Rename{Name: []string{"users"}, NewName: "people", Type: sqlserverexec.RenameObject},
			want:   "EXEC sp_rename N'[users]', N'people', N'OBJECT';",
		},
		{
			name:   "index",
			rename: sqlserverexec.Rename{Name: []string{"app", "users", "ix_email"}, NewName: "ix_mail", Type: sqlserverexec.RenameIndex},
			want:   "EXEC sp_rename N'[app].[users].[ix_email]', N'ix_mail', N'INDEX';",
		},
		{
			name:   "untyped",
			rename: sqlserverexec.Rename{Name: []string{"dbo", "users"}, NewName: "people"},
			want:   "EXEC sp_rename N'[dbo].[users]', N'people';",
		},
		{
			name:   "brackets, quotes and dots in the names",
			rename: sqlserverexec.Rename{Name: []string{"dbo", "o]k", "it's.a"}, NewName: "a]b'c.d", Type: sqlserverexec.RenameColumn},
			want:   "EXEC sp_rename N'[dbo].[o]]k].[it''s.a]', N'a]b''c.d', N'COLUMN';",
		},
		{
			name:   "a name outside the code page",
			rename: sqlserverexec.Rename{Name: []string{"dbo", "größe"}, NewName: "注文", Type: sqlserverexec.RenameObject},
			want:   "EXEC sp_rename N'[dbo].[größe]', N'注文', N'OBJECT';",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.rename.Statement(), qt.Equals, test.want)
		})
	}
}

// TestReadRename_HappyPath reads back every statement Statement writes, and
// one spaced and commented differently, which reads as the same tokens.
func TestReadRename_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      sqlserverexec.Rename
	}{
		{
			name:      "column",
			statement: "EXEC sp_rename N'[dbo].[users].[email]', N'mail', N'COLUMN';",
			want:      sqlserverexec.Rename{Name: []string{"dbo", "users", "email"}, NewName: "mail", Type: sqlserverexec.RenameColumn},
		},
		{
			name:      "table",
			statement: "EXEC sp_rename N'[users]', N'people', N'OBJECT';",
			want:      sqlserverexec.Rename{Name: []string{"users"}, NewName: "people", Type: sqlserverexec.RenameObject},
		},
		{
			name:      "index",
			statement: "EXEC sp_rename N'[app].[users].[ix_email]', N'ix_mail', N'INDEX';",
			want:      sqlserverexec.Rename{Name: []string{"app", "users", "ix_email"}, NewName: "ix_mail", Type: sqlserverexec.RenameIndex},
		},
		{
			name:      "untyped",
			statement: "EXEC sp_rename N'[dbo].[users]', N'people';",
			want:      sqlserverexec.Rename{Name: []string{"dbo", "users"}, NewName: "people"},
		},
		{
			name:      "brackets, quotes and dots in the names",
			statement: "EXEC sp_rename N'[dbo].[o]]k].[it''s.a]', N'a]b''c.d', N'COLUMN';",
			want:      sqlserverexec.Rename{Name: []string{"dbo", "o]k", "it's.a"}, NewName: "a]b'c.d", Type: sqlserverexec.RenameColumn},
		},
		{
			name:      "a type the renderer does not write, read as spelled",
			statement: "EXEC sp_rename N'[a6688_other]', N'escaped', N'DATABASE';",
			want:      sqlserverexec.Rename{Name: []string{"a6688_other"}, NewName: "escaped", Type: "DATABASE"},
		},
		{
			name:      "four parts",
			statement: "EXEC sp_rename N'[other].[dbo].[users].[email]', N'mail', N'COLUMN';",
			want:      sqlserverexec.Rename{Name: []string{"other", "dbo", "users", "email"}, NewName: "mail", Type: sqlserverexec.RenameColumn},
		},
		{
			name:      "spacing, a comment and no semicolon",
			statement: "EXEC  sp_rename\n\tN'[users]' /* old */ ,N'people'  -- new\n",
			want:      sqlserverexec.Rename{Name: []string{"users"}, NewName: "people"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := sqlserverexec.ReadRename(test.statement)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestReadRename_FailurePath holds the statements that read as no rename. Each
// is one change away from a statement TestReadRename_HappyPath reads.
func TestReadRename_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "EXECUTE", statement: "EXECUTE sp_rename N'[users]', N'people', N'OBJECT';"},
		{name: "no EXEC", statement: "sp_rename N'[users]', N'people', N'OBJECT';"},
		{name: "lowercase exec", statement: "exec sp_rename N'[users]', N'people', N'OBJECT';"},
		{name: "procedure in sys", statement: "EXEC sys.sp_rename N'[users]', N'people', N'OBJECT';"},
		{name: "procedure in brackets", statement: "EXEC [sp_rename] N'[users]', N'people', N'OBJECT';"},
		{name: "procedure in another database", statement: "EXEC other..sp_rename N'[users]', N'people', N'OBJECT';"},
		{name: "procedure in master", statement: "EXEC master.dbo.sp_rename N'[users]', N'people', N'OBJECT';"},
		{name: "another procedure", statement: "EXEC sp_renamedb N'[users]', N'people';"},
		{name: "plain literal", statement: "EXEC sp_rename '[users]', N'people', N'OBJECT';"},
		{name: "plain literal for the new name", statement: "EXEC sp_rename N'[users]', 'people', N'OBJECT';"},
		{name: "plain literal for the type", statement: "EXEC sp_rename N'[users]', N'people', 'OBJECT';"},
		{name: "lowercase N", statement: "EXEC sp_rename n'[users]', N'people', N'OBJECT';"},
		{name: "unquoted part", statement: "EXEC sp_rename N'dbo.users', N'people', N'OBJECT';"},
		{name: "one unquoted part", statement: "EXEC sp_rename N'[dbo].users', N'people', N'OBJECT';"},
		{name: "double-quoted part", statement: `EXEC sp_rename N'"dbo".[users]', N'people', N'OBJECT';`},
		{name: "empty part", statement: "EXEC sp_rename N'[other]..[users]', N'people', N'OBJECT';"},
		{name: "unclosed bracket", statement: "EXEC sp_rename N'[users', N'people', N'OBJECT';"},
		{name: "single closing bracket inside", statement: "EXEC sp_rename N'[o]k]', N'people', N'OBJECT';"},
		{name: "space after a part", statement: "EXEC sp_rename N'[dbo]. [users]', N'people', N'OBJECT';"},
		{name: "empty name", statement: "EXEC sp_rename N'', N'people', N'OBJECT';"},
		{name: "variable", statement: "EXEC sp_rename @name, N'people', N'OBJECT';"},
		{name: "variable for the new name", statement: "EXEC sp_rename N'[users]', @new, N'OBJECT';"},
		{name: "concatenation", statement: "EXEC sp_rename N'[users]', N'peo' + N'ple', N'OBJECT';"},
		{name: "function call", statement: "EXEC sp_rename N'[users]', CONCAT(N'peo', N'ple'), N'OBJECT';"},
		{name: "named arguments", statement: "EXEC sp_rename @objname = N'[users]', @newname = N'people', @objtype = N'OBJECT';"},
		{name: "one argument", statement: "EXEC sp_rename N'[users]';"},
		{name: "four arguments", statement: "EXEC sp_rename N'[users]', N'people', N'OBJECT', N'x';"},
		{name: "trailing comma", statement: "EXEC sp_rename N'[users]', N'people', N'OBJECT',;"},
		{name: "missing comma", statement: "EXEC sp_rename N'[users]' N'people', N'OBJECT';"},
		{name: "two commas", statement: "EXEC sp_rename N'[users]',, N'people';"},
		{name: "another statement after it", statement: "EXEC sp_rename N'[users]', N'people', N'OBJECT'; DROP TABLE x;"},
		{name: "a second rename after it", statement: "EXEC sp_rename N'[a]', N'b'; EXEC sp_rename N'[c]', N'd';"},
		{name: "return status", statement: "EXEC @status = sp_rename N'[users]', N'people', N'OBJECT';"},
		{name: "remote", statement: "EXEC sp_rename N'[users]', N'people', N'OBJECT' AT linked;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := sqlserverexec.ReadRename(test.statement)

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.DeepEquals, sqlserverexec.Rename{})
		})
	}
}
