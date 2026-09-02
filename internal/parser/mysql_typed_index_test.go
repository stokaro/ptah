package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/parser"
)

// TestParse_MySQLTypedIndexAcceptsIndexAndKey_HappyPath covers
// stokaro/ptah#2747.
//
// The grammar is `{SPATIAL|FULLTEXT} [INDEX|KEY] [name] (key_part,...)`, so a
// table body has six spellings for these indexes and this table covers the four
// that name a keyword. Three of those four were refused: `SPATIAL KEY` demanded
// INDEX, and FULLTEXT was in no keyword list at all, so the element fell
// through to column parsing and the diagnostic named the INDEX NAME as an
// unsupported column attribute.
//
// The other two omit the keyword, which both servers also accept;
// TestParse_MySQLTypedIndexOmitsTheKeyword covers those, separately because no
// dumper ever writes them and they are therefore the half a reader forgets.
//
// The refused spellings are the ones that matter most, because both dumpers
// NORMALIZE to them: a table created with `SPATIAL INDEX` comes back out of
// mysqldump 26.7 and mariadb-dump 12.3 as `SPATIAL KEY`. No dump of any MySQL
// or MariaDB database holding either index type could be read.
//
// The access method travels as ast.IndexNode.Type because that is what the
// renderer emits from; Unique is asserted false on every row because the old
// SPATIAL reader assigned ast.UniqueConstraint, which is a promise no such
// declaration makes.
func TestParse_MySQLTypedIndexAcceptsIndexAndKey_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantName    string
		wantType    string
		wantColumns []string
	}{
		{
			name:        "SPATIAL INDEX",
			sql:         "CREATE TABLE places (id BIGINT PRIMARY KEY, geom GEOMETRY NOT NULL, SPATIAL INDEX sp_g (geom));",
			wantName:    "sp_g",
			wantType:    "SPATIAL",
			wantColumns: []string{"geom"},
		},
		{
			name:        "SPATIAL KEY",
			sql:         "CREATE TABLE places (id BIGINT PRIMARY KEY, geom GEOMETRY NOT NULL, SPATIAL KEY sp_g (geom));",
			wantName:    "sp_g",
			wantType:    "SPATIAL",
			wantColumns: []string{"geom"},
		},
		{
			name:        "FULLTEXT INDEX",
			sql:         "CREATE TABLE places (id BIGINT PRIMARY KEY, bio TEXT, FULLTEXT INDEX ft_b (bio));",
			wantName:    "ft_b",
			wantType:    "FULLTEXT",
			wantColumns: []string{"bio"},
		},
		{
			name:        "FULLTEXT KEY",
			sql:         "CREATE TABLE places (id BIGINT PRIMARY KEY, bio TEXT, FULLTEXT KEY ft_b (bio));",
			wantName:    "ft_b",
			wantType:    "FULLTEXT",
			wantColumns: []string{"bio"},
		},
		{
			// The empty name is deliberate and is not a name yet. MySQL and
			// MariaDB name such an index after its first key part, and
			// stokaro/ptah#2713 put that rule in toschema, where the dialect is
			// known; inventing one here would put a parser's guess where the
			// server's answer belongs.
			name:        "unnamed SPATIAL KEY",
			sql:         "CREATE TABLE places (id BIGINT PRIMARY KEY, geom GEOMETRY NOT NULL, SPATIAL KEY (geom));",
			wantName:    "",
			wantType:    "SPATIAL",
			wantColumns: []string{"geom"},
		},
		{
			name:        "unnamed FULLTEXT KEY",
			sql:         "CREATE TABLE places (id BIGINT PRIMARY KEY, bio TEXT, FULLTEXT KEY (bio));",
			wantName:    "",
			wantType:    "FULLTEXT",
			wantColumns: []string{"bio"},
		},
		{
			// Both servers accept a full-text index over several columns, and
			// this is the row that separates a reader carrying the whole key
			// from one that kept its first part.
			name:        "multi-column FULLTEXT KEY",
			sql:         "CREATE TABLE places (id BIGINT PRIMARY KEY, title VARCHAR(100), bio TEXT, FULLTEXT KEY ft_b (title, bio));",
			wantName:    "ft_b",
			wantType:    "FULLTEXT",
			wantColumns: []string{"title", "bio"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := parsedTable(c, test.sql)

			c.Assert(indexNames(table), qt.DeepEquals, []string{test.wantName})
			c.Assert(table.Indexes[0].Type, qt.Equals, test.wantType)
			c.Assert(table.Indexes[0].Columns, qt.DeepEquals, test.wantColumns)
			c.Assert(table.Indexes[0].Unique, qt.IsFalse)
			// The primary key is a column attribute here, so the declaration
			// makes no table constraint at all.
			c.Assert(constraintTypes(table), qt.HasLen, 0)
		})
	}
}

// TestParse_MySQLTypedIndexKeepsItsKeyParts establishes that the per-part
// attributes a column-name list cannot carry survive the typed spellings too.
//
// SPATIAL and FULLTEXT reach ast.IndexNode through the same key-part conversion
// an ordinary KEY does, so a reader that built the index from Columns alone
// would keep every index above and silently flatten `ft_b (title, bio(10)
// DESC)` into `ft_b (title, bio)` -- a different index that applies cleanly.
//
// The two rows are not worth the same, and which is which was measured.
// `FULLTEXT KEY ft_b (title, bio(10) DESC)` is accepted by MariaDB 12.3 and
// refused by MySQL 26.7 with `ERROR 1221 Incorrect usage of
// spatial/fulltext/hash index and explicit index order`; drop the DESC and both
// accept it, and both then report SUB_PART NULL, having normalized the prefix
// away. `SPATIAL KEY sp_g (geom(4))` is accepted by NEITHER -- MySQL answers
// `ERROR 1089 Incorrect prefix key` and MariaDB a syntax error near `(4)))`.
//
// That row is here because of the refusal rather than in spite of it. A reader
// that dropped the prefix would turn a declaration both servers reject into a
// different index they both accept, which is the looser half of the
// compatibility policy; the refusal has to come from the server.
//
// So what is asserted here is that the parser reproduces the document it was
// handed, which is the only thing a reader can promise before a dialect has had
// its say.
func TestParse_MySQLTypedIndexKeepsItsKeyParts(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantParts []ast.IndexPart
	}{
		{
			name: "FULLTEXT KEY with a prefix length and DESC",
			sql: "CREATE TABLE places (id BIGINT PRIMARY KEY, title VARCHAR(100), bio TEXT, " +
				"FULLTEXT KEY ft_b (title, bio(10) DESC));",
			wantParts: []ast.IndexPart{
				{Name: "title"},
				{Name: "bio", Prefix: "10", Desc: true},
			},
		},
		{
			name: "SPATIAL KEY with a prefix length",
			sql: "CREATE TABLE places (id BIGINT PRIMARY KEY, geom GEOMETRY NOT NULL, " +
				"SPATIAL KEY sp_g (geom(4)));",
			wantParts: []ast.IndexPart{
				{Name: "geom", Prefix: "4"},
			},
		},
		{
			// The control: an ordinary part carries empty attribute fields
			// rather than invented ones.
			name: "SPATIAL KEY with a plain column",
			sql: "CREATE TABLE places (id BIGINT PRIMARY KEY, geom GEOMETRY NOT NULL, " +
				"SPATIAL KEY sp_g (geom));",
			wantParts: []ast.IndexPart{
				{Name: "geom"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := parsedTable(c, test.sql)

			c.Assert(table.Indexes, qt.HasLen, 1)
			c.Assert(table.Indexes[0].Parts, qt.DeepEquals, test.wantParts)
		})
	}
}

// TestParse_MySQLTypedIndexReadsWithParser establishes that `WITH PARSER
// <name>` reaches ast.IndexNode.Parser, and that an index that declares none
// carries the empty string rather than a default someone chose here.
//
// The clause names the tokenizer, and the difference between the default and
// ngram is whether CJK text is indexed at all. An index that lost it is one
// Ptah reports as matching while it tokenizes differently, which is a silent
// wrong answer rather than a missing feature.
//
// Only the bare spelling is covered. mysqldump writes the clause inside an
// executable comment, /*!50100 WITH PARSER `ngram` */, and the parser's lexer
// does not enable MySQL executable comments, so that spelling still loses the
// name; it is filed separately rather than asserted here.
//
// The AST is also as far as the clause is followed anywhere. No live test
// declares it, because MariaDB 12.3 answers `ERROR 1128 Function 'ngram' is not
// defined` and the round trip in integration/ runs one fixture against both
// engines; what reaches a server is measured for the access method alone.
func TestParse_MySQLTypedIndexReadsWithParser(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantParser string
	}{
		{
			name: "WITH PARSER ngram",
			sql: "CREATE TABLE places (id BIGINT PRIMARY KEY, bio TEXT, " +
				"FULLTEXT KEY ft_b (bio) WITH PARSER ngram);",
			wantParser: "ngram",
		},
		{
			name: "no clause",
			sql: "CREATE TABLE places (id BIGINT PRIMARY KEY, bio TEXT, " +
				"FULLTEXT KEY ft_b (bio));",
			wantParser: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := parsedTable(c, test.sql)

			c.Assert(table.Indexes, qt.HasLen, 1)
			c.Assert(table.Indexes[0].Parser, qt.Equals, test.wantParser)
			c.Assert(table.Indexes[0].Name, qt.Equals, "ft_b")
			c.Assert(table.Indexes[0].Type, qt.Equals, "FULLTEXT")
		})
	}
}

// TestParse_MySQLTypedIndexOmitsTheKeyword covers the spelling no dumper
// writes and both servers accept.
//
// MySQL's grammar is `{SPATIAL|FULLTEXT} [INDEX|KEY] [name] (key_part, ...)`,
// and the keyword is optional. Measured on MySQL 26.7 and MariaDB 12.3:
// `CREATE TABLE bare_ft (id BIGINT PRIMARY KEY, bio TEXT, FULLTEXT ft_b (bio))`
// is accepted by both, information_schema reports INDEX_TYPE FULLTEXT, and
// SHOW CREATE TABLE prints it back as `FULLTEXT KEY`; `SPATIAL sp_g (geom)`
// behaves the same way.
//
// It is easy to leave out precisely because no dump contains it, so it is here
// as its own test rather than as two more rows above: this is the half of the
// grammar that only a hand-written document exercises, and refusing it would
// make this reader stricter than either engine on SQL a user can legitimately
// write.
func TestParse_MySQLTypedIndexOmitsTheKeyword(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantName    string
		wantType    string
		wantColumns []string
	}{
		{
			name:        "FULLTEXT with a name and no keyword",
			sql:         "CREATE TABLE places (id BIGINT PRIMARY KEY, bio TEXT, FULLTEXT ft_b (bio));",
			wantName:    "ft_b",
			wantType:    "FULLTEXT",
			wantColumns: []string{"bio"},
		},
		{
			name:        "SPATIAL with a name and no keyword",
			sql:         "CREATE TABLE places (id BIGINT PRIMARY KEY, geom GEOMETRY NOT NULL, SPATIAL sp_g (geom));",
			wantName:    "sp_g",
			wantType:    "SPATIAL",
			wantColumns: []string{"geom"},
		},
		{
			name:        "FULLTEXT with neither a keyword nor a name",
			sql:         "CREATE TABLE places (id BIGINT PRIMARY KEY, bio TEXT, FULLTEXT (bio));",
			wantName:    "",
			wantType:    "FULLTEXT",
			wantColumns: []string{"bio"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := parsedTable(c, test.sql)

			c.Assert(table.Indexes, qt.HasLen, 1)
			c.Assert(table.Indexes[0].Name, qt.Equals, test.wantName)
			c.Assert(table.Indexes[0].Type, qt.Equals, test.wantType)
			c.Assert(table.Indexes[0].Columns, qt.DeepEquals, test.wantColumns)
			c.Assert(table.Indexes[0].Unique, qt.IsFalse)
		})
	}
}

// TestParse_MySQLIndexParserBelongsToFulltext_FailurePath keeps the clause with
// the index type that has it.
//
// `WITH PARSER` is FULLTEXT's alone. Measured on MySQL 26.7 and MariaDB 12.3,
// both answer `ERROR 1064` to `KEY k (a) WITH PARSER ngram`, so reading it
// anywhere else would accept a document neither engine takes -- and because
// the renderer emits the clause from a non-empty Parser without asking about
// the type, the run would then write DDL neither engine takes either. Ignoring
// the clause instead of refusing it would be the same defect one layer quieter.
func TestParse_MySQLIndexParserBelongsToFulltext_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "an ordinary index",
			sql:     "CREATE TABLE t (id BIGINT PRIMARY KEY, a VARCHAR(50), KEY k (a) WITH PARSER ngram);",
			wantErr: `WITH PARSER belongs to a FULLTEXT index, and this one is an ordinary index`,
		},
		{
			name:    "a spatial index",
			sql:     "CREATE TABLE t (id BIGINT PRIMARY KEY, g GEOMETRY NOT NULL, SPATIAL KEY k (g) WITH PARSER ngram);",
			wantErr: `WITH PARSER belongs to a FULLTEXT index, and this one is SPATIAL`,
		},
		{
			name:    "a unique constraint",
			sql:     "CREATE TABLE t (id BIGINT PRIMARY KEY, a VARCHAR(50), UNIQUE KEY k (a) WITH PARSER ngram);",
			wantErr: `WITH PARSER belongs to a FULLTEXT index, and this one is an ordinary index`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(test.sql, parser.WithDialect(platform.MySQL)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
