package deporder_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/deporder"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// name is a use of a name that stands nowhere in particular.
func name(parts ...string) deporder.NameUse {
	return deporder.NameUse{Parts: parts}
}

// call is a use of a name an opening parenthesis follows.
func call(parts ...string) deporder.NameUse {
	return deporder.NameUse{Parts: parts, Call: true}
}

// relation is a use of a name where SQL names a relation.
func relation(parts ...string) deporder.NameUse {
	return deporder.NameUse{Parts: parts, Relation: true}
}

// relationCall is a use of a name where SQL names a relation that an opening
// parenthesis follows, as a column list follows a table.
func relationCall(parts ...string) deporder.NameUse {
	return deporder.NameUse{Parts: parts, Call: true, Relation: true}
}

// TestCodeNameUses_HappyPath pins what a reader of the names of a body gets:
// the dotted names of the code, folded, with quotes taken off and nothing from
// a literal or a comment, each with whether a call names it and whether it
// stands where SQL names a relation.
func TestCodeNameUses_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []deporder.NameUse
	}{
		{
			name: "qualified call and bare column",
			body: `USING ((owner = auth.uid()))`,
			want: []deporder.NameUse{call("using"), name("owner"), call("auth", "uid")},
		},
		{
			name: "quoted parts lose their quotes and fold",
			body: `REFERENCES "Rebase"."Users" ("id")`,
			want: []deporder.NameUse{name("references"), relationCall("rebase", "users"), name("id")},
		},
		{
			name: "literals and comments name nothing",
			body: "SELECT 'auth.uid' -- rebase.roles\n/* app.f */ FROM $$ x.y $$",
			want: []deporder.NameUse{name("select"), name("from")},
		},
		{
			name: "a star is an empty part",
			body: `SELECT t.* FROM t`,
			want: []deporder.NameUse{name("select"), name("t", ""), name("from"), relation("t")},
		},
		{
			name: "a column and a call of the same name",
			body: `SELECT title, title () FROM notes`,
			want: []deporder.NameUse{name("select"), name("title"), call("title"), name("from"), relation("notes")},
		},
		{
			name: "a FROM list with aliases",
			body: `SELECT 1 FROM a x, b AS y, c WHERE x.id = y.id`,
			want: []deporder.NameUse{
				name("select"), name("from"), relation("a"), name("x"), relation("b"), name("as"), name("y"),
				relation("c"), name("where"), name("x", "id"), name("y", "id"),
			},
		},
		{
			name: "a join as PostgreSQL prints one",
			body: `FROM ((a JOIN b ON ((a.id = b.id))) JOIN c ON true)`,
			want: []deporder.NameUse{
				call("from"), relation("a"), name("join"), relation("b"), call("on"), name("a", "id"), name("b", "id"),
				name("join"), relation("c"), name("on"), relation("true"),
			},
		},
		{
			name: "a column list and a values list",
			body: `INSERT INTO t (a, b) VALUES (a, b)`,
			want: []deporder.NameUse{
				name("insert"), name("into"), relationCall("t"), name("a"), name("b"), call("values"), name("a"), name("b"),
			},
		},
		{
			name: "a column a plan adds",
			body: `ALTER TABLE "public"."notes" ADD COLUMN "items" jsonb NULL, ADD COLUMN title text`,
			want: []deporder.NameUse{
				name("alter"), name("table"), relation("public", "notes"), name("add"), name("column"), name("items"),
				name("jsonb"), name("null"), relation("add"), name("column"), name("title"), name("text"),
			},
		},
		{
			name: "a row type",
			body: `DECLARE r items%ROWTYPE;`,
			want: []deporder.NameUse{name("declare"), name("r"), relation("items"), name("rowtype")},
		},
		{
			name: "the parents of a table",
			body: `CREATE TABLE c (id int) INHERITS (p1, p2)`,
			want: []deporder.NameUse{
				name("create"), name("table"), relationCall("c"), name("id"), name("int"), call("inherits"),
				relation("p1"), relation("p2"),
			},
		},
		{
			name: "a select list",
			body: `SELECT a, b, count(*) FROM t GROUP BY a, b`,
			want: []deporder.NameUse{
				name("select"), name("a"), name("b"), call("count"), name("from"), relation("t"), name("group"),
				name("by"), name("a"), name("b"),
			},
		},
		{
			name: "a table after IF EXISTS",
			body: `DROP TABLE IF EXISTS a, b`,
			want: []deporder.NameUse{name("drop"), name("table"), relation("if"), name("exists"), relation("a"), relation("b")},
		},
		{
			name: "a partition and its parent",
			body: `CREATE TABLE c PARTITION OF p FOR VALUES IN (1); ALTER TABLE p ATTACH PARTITION d DEFAULT`,
			want: []deporder.NameUse{
				name("create"), name("table"), relation("c"), name("partition"), relation("of"), relation("p"), name("for"),
				name("values"), call("in"), name("alter"), name("table"), relation("p"), name("attach"), name("partition"),
				relation("d"), name("default"),
			},
		},
		{
			name: "a routine named without its argument list",
			body: `COMMENT ON FUNCTION uid IS 'x'; GRANT EXECUTE ON PROCEDURE touch TO app`,
			want: []deporder.NameUse{
				name("comment"), name("on"), relation("function"), call("uid"), name("is"), name("grant"), name("execute"),
				name("on"), relation("procedure"), call("touch"), name("to"), name("app"),
			},
		},
		{
			name: "a routine named after IF EXISTS",
			body: `DROP FUNCTION IF EXISTS uid`,
			want: []deporder.NameUse{
				name("drop"), name("function"), call("if"), name("exists"), {Parts: []string{"uid"}, Call: true, Relation: true},
			},
		},
		{
			name: "a trigger and the function it executes",
			body: `CREATE TRIGGER touch BEFORE INSERT ON tasks FOR EACH ROW EXECUTE FUNCTION touch()`,
			want: []deporder.NameUse{
				name("create"), name("trigger"), name("touch"), name("before"), name("insert"), name("on"),
				relation("tasks"), name("for"), name("each"), name("row"), name("execute"), name("function"),
				call("touch"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(deporder.CodeNameUses(test.body, "postgres"), qt.DeepEquals, test.want)
		})
	}
}

// TestCodeNameUses_FailurePath is a body that names nothing.
func TestCodeNameUses_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(deporder.CodeNameUses(" \n\t", "postgres"), qt.IsNil)
}

// TestCodeNameUses_ReadsTheDialectsDollarTags reads the names of a body with
// the dollar-quote tags of its dialect, the tags the migrator's lexer reads.
// PostgreSQL 19 reads a tag outside ASCII, such as the euro sign, and reads
// `a$b$` as one name. ClickHouse 26.10 reads `$1abc$` as a heredoc tag, and a
// tag nothing closes as part of a name, which a name read here does not start
// with, and the code goes on after it.
func TestCodeNameUses_ReadsTheDialectsDollarTags(t *testing.T) {
	tests := []struct {
		name, dialect, body string
		want                []deporder.NameUse
	}{
		{
			name:    "a PostgreSQL tag outside ASCII",
			dialect: "postgres",
			body:    "SELECT $€$ FROM app.b $€$ AS label",
			want:    []deporder.NameUse{name("select"), name("as"), name("label")},
		},
		{
			name:    "a dollar sign inside a PostgreSQL name",
			dialect: "postgres",
			body:    "SELECT a$b$ FROM app.t -- $b$",
			want:    []deporder.NameUse{name("select"), name("a$b$"), name("from"), relation("app", "t")},
		},
		{
			name:    "a ClickHouse tag that starts with a digit",
			dialect: "clickhouse",
			body:    "SELECT $1abc$ FROM app.b $1abc$ AS label",
			want:    []deporder.NameUse{name("select"), name("as"), name("label")},
		},
		{
			name:    "a ClickHouse tag nothing closes",
			dialect: "clickhouse",
			body:    "SELECT 1 AS $ab$ FROM app.t",
			want:    []deporder.NameUse{name("select"), name("as"), name("from"), relation("app", "t")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(deporder.CodeNameUses(test.body, test.dialect), qt.DeepEquals, test.want)
		})
	}
}

// lexerReadsAString reports whether the migrator's lexer for dialect reads the
// byte at offset of text as part of a string.
func lexerReadsAString(text, dialect string, offset int) bool {
	var tokens []lexer.Token
	scan := lexer.NewLexerWithOptions(text, dialectlexer.Options(dialect))
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		tokens = append(tokens, token)
	}
	return slices.ContainsFunc(tokens, func(token lexer.Token) bool {
		return token.Type == lexer.TokenString && token.Start <= offset && offset < token.End
	})
}

// TestCodeNameUses_ReadsADollarSignAsTheLexerDoes reads the names of a body
// where the migrator's lexer reads code. A dollar sign inside a name, as in
// a1$b$ or _1$b$, opens nothing, and one right after a number or a closing tag
// can open a string: PostgreSQL 19 reads 1$a$ as the number 1 and a string,
// ClickHouse 26.10 and 24.10 read a heredoc there, and ClickHouse 26.10 reads
// $a$p$a$$b$q$b$ as two heredocs. ClickHouse 26.10 reads $1$b$, which nothing
// closes, as one name.
func TestCodeNameUses_ReadsADollarSignAsTheLexerDoes(t *testing.T) {
	tests := []struct {
		name, dialect, body string
		want                []deporder.NameUse
	}{
		{
			name:    "a PostgreSQL string after a number",
			dialect: "postgres",
			body:    "SELECT 1$a$ FROM app.t $a$ AS label",
			want:    []deporder.NameUse{name("select"), name("as"), name("label")},
		},
		{
			name:    "a ClickHouse heredoc after a number",
			dialect: "clickhouse",
			body:    "SELECT 1$a$ FROM app.t $a$ AS label",
			want:    []deporder.NameUse{name("select"), name("as"), name("label")},
		},
		{
			name:    "a ClickHouse heredoc after another",
			dialect: "clickhouse",
			body:    "SELECT $a$p$a$$b$ FROM app.t $b$ AS label",
			want:    []deporder.NameUse{name("select"), name("as"), name("label")},
		},
		{
			name:    "a digit inside a name",
			dialect: "postgres",
			body:    "SELECT a1$b$ FROM app.t -- $b$",
			want:    []deporder.NameUse{name("select"), name("a1$b$"), name("from"), relation("app", "t")},
		},
		{
			name:    "a name that begins with an underscore",
			dialect: "postgres",
			body:    "SELECT _1$b$ FROM app.t -- $b$",
			want:    []deporder.NameUse{name("select"), name("_1$b$"), name("from"), relation("app", "t")},
		},
		{
			name:    "a ClickHouse name that begins at a sign",
			dialect: "clickhouse",
			body:    "SELECT $1$b$ FROM app.t $b$ AS label",
			want:    []deporder.NameUse{name("select"), name("from"), relation("app", "t"), name("as"), name("label")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := deporder.CodeNameUses(test.body, test.dialect)
			c.Assert(got, qt.DeepEquals, test.want)
			usesTable := slices.ContainsFunc(got, func(use deporder.NameUse) bool {
				return slices.Equal(use.Parts, []string{"app", "t"})
			})
			c.Assert(lexerReadsAString(test.body, test.dialect, strings.Index(test.body, "app.t")), qt.Equals, !usesTable)
		})
	}
}
