package onlineddl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/onlineddl"
)

func TestParseAlterTable(t *testing.T) {
	tests := []struct {
		name string
		stmt string
		want onlineddl.AlterTarget
		ok   bool
	}{
		{
			name: "plain alter",
			stmt: "ALTER TABLE users ADD COLUMN bio TEXT",
			want: onlineddl.AlterTarget{Table: "users", Clause: "ADD COLUMN bio TEXT"},
			ok:   true,
		},
		{
			name: "lowercase keywords and multiline clause",
			stmt: "alter table users\n  modify column bio VARCHAR(500)\n  NOT NULL",
			want: onlineddl.AlterTarget{Table: "users", Clause: "modify column bio VARCHAR(500)\n  NOT NULL"},
			ok:   true,
		},
		{
			name: "backtick-quoted table",
			stmt: "ALTER TABLE `order``s` ADD INDEX idx (a)",
			want: onlineddl.AlterTarget{Table: "order`s", Clause: "ADD INDEX idx (a)"},
			ok:   true,
		},
		{
			name: "schema-qualified reference",
			stmt: "ALTER TABLE shop.users DROP COLUMN legacy",
			want: onlineddl.AlterTarget{Schema: "shop", Table: "users", Clause: "DROP COLUMN legacy"},
			ok:   true,
		},
		{
			name: "quoted schema-qualified reference with spaces",
			stmt: "ALTER TABLE `shop` . `users` ADD COLUMN a INT",
			want: onlineddl.AlterTarget{Schema: "shop", Table: "users", Clause: "ADD COLUMN a INT"},
			ok:   true,
		},
		{
			name: "mariadb if exists",
			stmt: "ALTER TABLE IF EXISTS users ADD COLUMN a INT",
			want: onlineddl.AlterTarget{Table: "users", Clause: "ADD COLUMN a INT"},
			ok:   true,
		},
		{
			name: "mariadb alter online table",
			stmt: "ALTER ONLINE TABLE users ADD COLUMN a INT",
			want: onlineddl.AlterTarget{Table: "users", Clause: "ADD COLUMN a INT"},
			ok:   true,
		},
		{
			name: "mariadb alter ignore table",
			stmt: "ALTER IGNORE TABLE users DROP COLUMN legacy",
			want: onlineddl.AlterTarget{Table: "users", Clause: "DROP COLUMN legacy"},
			ok:   true,
		},
		{name: "not an alter", stmt: "CREATE TABLE users (id INT)", ok: false},
		{name: "alter of a non-table object", stmt: "ALTER VIEW v AS SELECT 1", ok: false},
		{name: "bare alter table without clause", stmt: "ALTER TABLE users", ok: false},
		{name: "unterminated quoted identifier", stmt: "ALTER TABLE `users ADD COLUMN a INT", ok: false},
		{name: "empty statement", stmt: "", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := onlineddl.ParseAlterTable(tt.stmt)
			c.Assert(ok, qt.Equals, tt.ok, qt.Commentf("stmt: %s", tt.stmt))
			if tt.ok {
				c.Assert(got, qt.DeepEquals, tt.want)
			}
		})
	}
}
