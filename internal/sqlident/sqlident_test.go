package sqlident_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlident"
)

func TestQuote(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		ident   string
		want    string
	}{
		{name: "postgres double quotes", dialect: "postgres", ident: "users", want: `"users"`},
		{name: "sqlite double quotes", dialect: "sqlite", ident: "users", want: `"users"`},
		{name: "cockroachdb falls back to double quotes", dialect: "cockroachdb", ident: "users", want: `"users"`},
		{name: "yugabytedb falls back to double quotes", dialect: "yugabytedb", ident: "users", want: `"users"`},
		{name: "unknown dialect falls back to double quotes", dialect: "duckdb", ident: "users", want: `"users"`},
		{name: "mysql backticks", dialect: "mysql", ident: "users", want: "`users`"},
		{name: "mariadb backticks", dialect: "mariadb", ident: "users", want: "`users`"},
		{name: "clickhouse backticks", dialect: "clickhouse", ident: "users", want: "`users`"},
		{name: "sqlserver brackets", dialect: "sqlserver", ident: "users", want: "[users]"},
		// Every documented spelling of an engine has to pick that engine's quote
		// style. Reverting Quote to matching raw strings makes the four rows
		// below fail with `"users"` (SQL Server) and `"users"` (ClickHouse):
		// the raw switch listed only sqlserver/mssql and clickhouse, so tsql,
		// sql-server, sql_server and ch fell through to the default arm and
		// produced PostgreSQL quoting for a SQL Server / ClickHouse identifier.
		{name: "mssql alias brackets", dialect: "mssql", ident: "users", want: "[users]"},
		{name: "tsql alias brackets", dialect: "tsql", ident: "users", want: "[users]"},
		{name: "sql-server alias brackets", dialect: "sql-server", ident: "users", want: "[users]"},
		{name: "sql_server alias brackets", dialect: "sql_server", ident: "users", want: "[users]"},
		{name: "ch alias backticks", dialect: "ch", ident: "users", want: "`users`"},
		{name: "pgx alias double quotes", dialect: "pgx", ident: "users", want: `"users"`},
		{name: "sqlite3 alias double quotes", dialect: "sqlite3", ident: "users", want: `"users"`},
		{name: "dialect is case and space insensitive", dialect: "  MySQL ", ident: "users", want: "`users`"},
		{name: "postgres escapes embedded double quote", dialect: "postgres", ident: `a"b`, want: `"a""b"`},
		{name: "mysql escapes embedded backtick", dialect: "mysql", ident: "a`b", want: "`a``b`"},
		{name: "sqlserver escapes embedded bracket", dialect: "sqlserver", ident: "a]b", want: "[a]]b]"},
		// YQL: SELECT 1 AS `a\\b`, 2 AS `c\`d`, 3 AS `e\\\`f` answered the
		// columns a\b, c`d and e\`f on YDB 26.2.1.14.
		{name: "ydb backticks", dialect: "ydb", ident: "users", want: "`users`"},
		{name: "ydbs alias backticks", dialect: "ydbs", ident: "Users", want: "`Users`"},
		{name: "ydb escapes a backtick with a backslash", dialect: "ydb", ident: "c`d", want: "`c\\`d`"},
		{name: "ydb doubles a backslash", dialect: "ydb", ident: `a\b`, want: "`a\\\\b`"},
		{name: "ydb escapes both", dialect: "ydb", ident: "e\\`f", want: "`e\\\\\\`f`"},
		{name: "ydb keeps a path whole", dialect: "ydb", ident: "dir/sub/t", want: "`dir/sub/t`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlident.Quote(tt.dialect, tt.ident), qt.Equals, tt.want)
		})
	}
}

func TestQualified(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		schema  string
		ident   string
		want    string
	}{
		{name: "postgres schema qualified", dialect: "postgres", schema: "public", ident: "users", want: `"public"."users"`},
		{name: "empty schema yields bare name", dialect: "postgres", schema: "", ident: "users", want: `"users"`},
		{name: "whitespace schema treated as empty", dialect: "postgres", schema: "  ", ident: " users ", want: `" users "`},
		{name: "schema and name bytes are preserved", dialect: "mysql", schema: " app ", ident: " users ", want: "` app `.` users `"},
		{name: "sqlserver schema qualified", dialect: "sqlserver", schema: "dbo", ident: "users", want: "[dbo].[users]"},
		// A YDB schema is a directory, and the qualified name is one path.
		{name: "ydb schema is a directory", dialect: "ydb", schema: "dir/sub", ident: "t", want: "`dir/sub/t`"},
		{name: "ydb trailing slash names the same directory", dialect: "ydb", schema: "dir/", ident: "t", want: "`dir/t`"},
		{name: "ydb root is the empty schema", dialect: "ydb", schema: "", ident: "t", want: "`t`"},
		{name: "ydb escapes inside the path", dialect: "ydbs", schema: "d`x", ident: "t", want: "`d\\`x/t`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlident.Qualified(tt.dialect, tt.schema, tt.ident), qt.Equals, tt.want)
		})
	}
}

// TestBareOrQuoted pins the spelling two callers have to agree about: the
// SQLite renderer writes a virtual table's module with it, and the inspection
// check looks for exactly what the renderer wrote. When they disagreed, a SQL
// document carrying `USING "fts-5"` was reported as lossy and refused under
// strict compatibility. See stokaro/ptah#1028.
func TestBareOrQuoted(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		input   string
		want    string
	}{
		{name: "a plain lowercase name stays bare", dialect: "sqlite", input: "fts5", want: "fts5"},
		{name: "digits after the first byte are plain", dialect: "sqlite", input: "rtree_i32", want: "rtree_i32"},
		{name: "a leading underscore is plain", dialect: "sqlite", input: "_mod", want: "_mod"},
		{name: "mixed case stays bare", dialect: "sqlite", input: "VirtualShape", want: "VirtualShape"},
		{name: "a SQLite keyword is quoted", dialect: "sqlite", input: "select", want: `"select"`},
		{name: "a mixed-case SQLite keyword is quoted", dialect: "sqlite", input: "SeLeCt", want: `"SeLeCt"`},
		{name: "another SQLite keyword is quoted", dialect: "sqlite", input: "table", want: `"table"`},
		{name: "a plain name on another dialect stays bare", dialect: "mysql", input: "select", want: "select"},
		{name: "a hyphen forces quoting", dialect: "sqlite", input: "fts-5", want: `"fts-5"`},
		{name: "a space forces quoting", dialect: "sqlite", input: "my module", want: `"my module"`},
		{name: "a leading digit forces quoting", dialect: "sqlite", input: "5fts", want: `"5fts"`},
		{name: "a dot forces quoting", dialect: "sqlite", input: "a.b", want: `"a.b"`},
		{name: "non-ASCII forces quoting", dialect: "sqlite", input: "modü", want: `"modü"`},
		{name: "an embedded quote is doubled", dialect: "sqlite", input: `a"b`, want: `"a""b"`},
		{name: "the empty name is quoted", dialect: "sqlite", input: "", want: `""`},
		{name: "the dialect selects the quote style", dialect: "mysql", input: "fts-5", want: "`fts-5`"},
		{name: "a plain YDB name is quoted", dialect: "ydb", input: "users", want: "`users`"},
		{name: "a YDB keyword is quoted", dialect: "ydb", input: "select", want: "`select`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlident.BareOrQuoted(tt.dialect, tt.input), qt.Equals, tt.want)
		})
	}
}

// TestOracleReservedWordsAreQuoted pins the words Oracle refuses as a bare
// identifier, because the Oracle renderer writes names WITHOUT quotes and this
// list is the only thing standing between a declaration and ORA-03050.
//
// The reserved cases are not a sample. Each one answered ORA-03050 on a live
// 23.26 server while the ordinary controls beside them created their table.
// They span both flags the server sets -- `size` and `resource` are
// `reserved='Y'`, the rest are `res_semi='Y'` -- because a list built from
// either flag alone covers only part of the set.
func TestOracleReservedWordsAreQuoted(t *testing.T) {
	tests := []struct {
		name  string
		ident string
		want  string
	}{
		{name: "comment is reserved", ident: "comment", want: `"comment"`},
		{name: "level is reserved", ident: "level", want: `"level"`},
		{name: "session is reserved", ident: "session", want: `"session"`},
		{name: "user is reserved", ident: "user", want: `"user"`},
		{name: "row is reserved", ident: "row", want: `"row"`},
		{name: "rows is reserved", ident: "rows", want: `"rows"`},
		{name: "access is reserved", ident: "access", want: `"access"`},
		{name: "add is reserved", ident: "add", want: `"add"`},
		{name: "column is reserved", ident: "column", want: `"column"`},
		{name: "file is reserved", ident: "file", want: `"file"`},
		{name: "current is reserved", ident: "current", want: `"current"`},
		{name: "uid is reserved", ident: "uid", want: `"uid"`},
		{name: "sysdate is reserved", ident: "sysdate", want: `"sysdate"`},
		{name: "rowid is reserved", ident: "rowid", want: `"rowid"`},
		{name: "rownum is reserved", ident: "rownum", want: `"rownum"`},
		{name: "audit is reserved", ident: "audit", want: `"audit"`},
		{name: "online is reserved", ident: "online", want: `"online"`},
		{name: "offline is reserved", ident: "offline", want: `"offline"`},
		{name: "validate is reserved", ident: "validate", want: `"validate"`},
		{name: "whenever is reserved", ident: "whenever", want: `"whenever"`},
		{name: "immediate is reserved", ident: "immediate", want: `"immediate"`},
		{name: "increment is reserved", ident: "increment", want: `"increment"`},
		{name: "initial is reserved", ident: "initial", want: `"initial"`},
		{name: "modify is reserved", ident: "modify", want: `"modify"`},
		{name: "size was already on the list", ident: "size", want: `"size"`},
		{name: "resource was already on the list", ident: "resource", want: `"resource"`},
		// Controls: an ordinary name must stay bare, or an over-broad list
		// would undo the renderer's whole bare-identifier decision.
		{name: "description is ordinary", ident: "description", want: "description"},
		{name: "status is ordinary", ident: "status", want: "status"},
		{name: "title is ordinary", ident: "title", want: "title"},
		{name: "amount is ordinary", ident: "amount", want: "amount"},
		{name: "email is ordinary", ident: "email", want: "email"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlident.Ident("oracle", tt.ident), qt.Equals, tt.want)
		})
	}
}

// TestQualifiedIdent pins the spelling a row statement shares with the Oracle
// DDL renderer: bare where Oracle takes a bare name, quoted where it does not,
// and the plain quoted form on every other dialect. Written with Qualified, an
// Oracle statement names "ora_flags", which is not the ORA_FLAGS table a bare
// CREATE TABLE made.
func TestQualifiedIdent(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		schema  string
		ident   string
		want    string
	}{
		{name: "oracle plain name stays bare", dialect: "oracle", schema: "", ident: "ora_flags", want: "ora_flags"},
		{name: "oracle schema qualified", dialect: "oracle", schema: "app", ident: "ora_flags", want: "app.ora_flags"},
		{name: "oracle reserved word is quoted", dialect: "oracle", schema: "", ident: "comment", want: `"comment"`},
		{name: "oracle name that is not plain is quoted", dialect: "oracle", schema: "", ident: "ora-flags", want: `"ora-flags"`},
		{name: "blank schema yields the name alone", dialect: "oracle", schema: "  ", ident: "flags", want: "flags"},
		{name: "postgres stays quoted", dialect: "postgres", schema: "app", ident: "flags", want: `"app"."flags"`},
		{name: "ydb qualifies with one path", dialect: "ydb", schema: "app", ident: "flags", want: "`app/flags`"},
		{name: "ydb at the root", dialect: "ydb", schema: "", ident: "Flags", want: "`Flags`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlident.QualifiedIdent(tt.dialect, tt.schema, tt.ident), qt.Equals, tt.want)
		})
	}
}

func TestSplitQualified(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{name: "one part", in: "users", want: []string{"users"}},
		{name: "schema and table", in: "app.users", want: []string{"app", "users"}},
		{name: "a dot inside quotes", in: `"a.b"."c"`, want: []string{`"a.b"`, `"c"`}},
		{name: "a doubled quote does not end the part", in: `"a"".b".c`, want: []string{`"a"".b"`, "c"}},
		{name: "non-ASCII bytes come back as they arrived", in: "Größe.Maß", want: []string{"Größe", "Maß"}},
		{name: "bytes that are not UTF-8 come back as they arrived", in: "caf\xe9.t", want: []string{"caf\xe9", "t"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlident.SplitQualified(test.in), qt.DeepEquals, test.want)
		})
	}
}

func TestUnquoteDoubleQuoted(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "a bare part", in: "users", want: "users"},
		{name: "a quoted part", in: `"Users"`, want: "Users"},
		{name: "a doubled quote inside", in: `"odd""name"`, want: `odd"name`},
		{name: "one quote is not a quoted part", in: `"`, want: `"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlident.UnquoteDoubleQuoted(test.in), qt.Equals, test.want)
		})
	}
}

func TestQuotePostgresQualified(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "schema and table", in: "app.users", want: `"app"."users"`},
		{name: "an already quoted part is not quoted twice", in: `"a.b"."c"`, want: `"a.b"."c"`},
		{name: "an embedded quote is doubled", in: `odd"name`, want: `"odd""name"`},
		{name: "a non-ASCII name", in: "Größe", want: `"Größe"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlident.QuotePostgresQualified(test.in), qt.Equals, test.want)
		})
	}
}

// TestQuotePostgresQualifiedSpellsARegclassLiteral pins the spelling a
// PostgreSQL-family statement and a REGCLASS literal share: each part quoted
// once, a part already in double quotes not quoted twice, and a dot inside a
// quoted part kept in it.
func TestQuotePostgresQualifiedSpellsARegclassLiteral(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "bare", in: "readings", want: `"readings"`},
		{name: "mixed case", in: "Readings", want: `"Readings"`},
		{name: "qualified", in: "app.Readings", want: `"app"."Readings"`},
		{name: "already quoted", in: `"app"."Readings"`, want: `"app"."Readings"`},
		{name: "a dot inside a quoted part", in: `app."odd.name"`, want: `"app"."odd.name"`},
		{name: "a doubled quote", in: `"odd""name"`, want: `"odd""name"`},
		{name: "non-ASCII bytes", in: "app.Äpfel", want: `"app"."Äpfel"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlident.QuotePostgresQualified(test.in), qt.Equals, test.want)
		})
	}
}

// TestStringLiteral pins the standard escape: a single quote is doubled.
func TestStringLiteral(t *testing.T) {
	c := qt.New(t)
	c.Assert(sqlident.StringLiteral(`"odd"."it's"`), qt.Equals, `'"odd"."it''s"'`)
}
