package parser_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// everyStatementGrammar is each dialect the rows below are measured on. Each row reads
// on the dialects it names and is refused on every other: the servers took and refused
// them as the rows say, on PostgreSQL 18, YugabyteDB 2026.1, CockroachDB 26.3, MySQL
// 26.10, MariaDB 12.3, ClickHouse 26.10, SQL Server 2025, SQLite 3.51 and the Spanner
// emulator behind PGAdapter 0.56.
var everyStatementGrammar = []string{
	platform.Postgres, platform.YugabyteDB, platform.CockroachDB, platform.MySQL, platform.MariaDB,
	platform.ClickHouse, platform.SQLServer, platform.SQLite, platform.Spanner,
}

var (
	postgresFamily = []string{platform.Postgres, platform.YugabyteDB, platform.CockroachDB}
	mysqlFamily    = []string{platform.MySQL, platform.MariaDB}
)

// together joins lists of dialects.
func together(lists ...[]string) []string {
	var dialects []string
	for _, list := range lists {
		dialects = append(dialects, list...)
	}
	return dialects
}

var statementGrammarRows = []struct {
	sql   string
	reads []string
}{
	{sql: "DROP VIEW v", reads: everyStatementGrammar},
	{sql: "DROP VIEW IF EXISTS v", reads: everyStatementGrammar},
	{sql: "DROP VIEW s.v", reads: everyStatementGrammar},
	{sql: "DROP VIEW v, w", reads: together([]string{platform.Postgres, platform.CockroachDB, platform.ClickHouse, platform.SQLServer}, mysqlFamily)},
	{sql: "DROP VIEW v CASCADE", reads: together(postgresFamily, mysqlFamily)},
	{sql: "DROP VIEW v RESTRICT", reads: together(postgresFamily, mysqlFamily)},
	{sql: "DROP VIEW v SYNC", reads: []string{platform.ClickHouse}},
	{sql: "DROP MATERIALIZED VIEW v", reads: postgresFamily},
	{sql: "DROP MATERIALIZED VIEW IF EXISTS v CASCADE", reads: postgresFamily},
	{sql: "DROP TRIGGER t", reads: []string{platform.MySQL, platform.MariaDB, platform.SQLServer, platform.SQLite}},
	{sql: "DROP TRIGGER IF EXISTS t", reads: []string{platform.MySQL, platform.MariaDB, platform.SQLServer, platform.SQLite}},
	{sql: "DROP TRIGGER t, u", reads: []string{platform.SQLServer}},
	{sql: "DROP TRIGGER t ON u", reads: postgresFamily},
	{sql: "DROP TRIGGER IF EXISTS t ON u CASCADE", reads: postgresFamily},
	{sql: "DROP FUNCTION f", reads: together(postgresFamily, mysqlFamily, []string{platform.ClickHouse, platform.SQLServer, platform.Spanner})},
	{sql: "DROP FUNCTION f()", reads: postgresFamily},
	{sql: "DROP FUNCTION f(int, text), g()", reads: postgresFamily},
	{sql: "DROP FUNCTION f, g", reads: together(postgresFamily, []string{platform.SQLServer})},
	{sql: "DROP FUNCTION f() CASCADE", reads: []string{platform.Postgres, platform.YugabyteDB}},
	{sql: "DROP PROCEDURE p", reads: together(postgresFamily, mysqlFamily, []string{platform.SQLServer})},
	{sql: "DROP PROCEDURE p, q", reads: together(postgresFamily, []string{platform.SQLServer})},
	{sql: "DROP PROC p", reads: []string{platform.SQLServer}},
	{sql: "DROP SEQUENCE s", reads: together(postgresFamily, []string{platform.MariaDB, platform.SQLServer, platform.Spanner})},
	{sql: "DROP SEQUENCE s, u", reads: []string{platform.Postgres, platform.CockroachDB, platform.MariaDB, platform.SQLServer}},
	{sql: "DROP SEQUENCE s CASCADE", reads: postgresFamily},
	{sql: "DROP TYPE e", reads: together(postgresFamily, []string{platform.SQLServer})},
	{sql: "DROP TYPE e, u", reads: postgresFamily},
	{sql: "DROP TYPE e CASCADE", reads: []string{platform.Postgres, platform.YugabyteDB}},
	{sql: "DROP TABLE t SYNC", reads: []string{platform.ClickHouse}},
	{sql: "TRUNCATE t", reads: together(postgresFamily, mysqlFamily, []string{platform.ClickHouse, platform.Spanner})},
	{sql: "TRUNCATE TABLE t", reads: together(postgresFamily, mysqlFamily, []string{platform.ClickHouse, platform.SQLServer, platform.Spanner})},
	{sql: "TRUNCATE t, u", reads: postgresFamily},
	{sql: "TRUNCATE ONLY t", reads: postgresFamily},
	{sql: "TRUNCATE TABLE t CASCADE", reads: postgresFamily},
	{sql: "TRUNCATE t RESTART IDENTITY", reads: []string{platform.Postgres, platform.YugabyteDB}},
	{sql: "TRUNCATE TABLE ONLY t, u RESTART IDENTITY CASCADE", reads: []string{platform.Postgres, platform.YugabyteDB}},
	{sql: "TRUNCATE TABLE IF EXISTS t SYNC", reads: []string{platform.ClickHouse}},
	{sql: "SAVEPOINT s", reads: together(postgresFamily, mysqlFamily, []string{platform.SQLite, platform.Spanner})},
	{sql: "RELEASE SAVEPOINT s", reads: together(postgresFamily, mysqlFamily, []string{platform.SQLite, platform.Spanner})},
	{sql: "RELEASE s", reads: together(postgresFamily, []string{platform.SQLite, platform.Spanner})},
	{sql: "SAVE TRANSACTION s", reads: []string{platform.SQLServer}},
	{sql: "SAVE TRAN s", reads: []string{platform.SQLServer}},
	{sql: "LOCK t", reads: []string{platform.Postgres, platform.YugabyteDB}},
	{sql: "LOCK TABLE t IN ACCESS SHARE MODE NOWAIT", reads: []string{platform.Postgres, platform.YugabyteDB}},
	{sql: "LOCK TABLE ONLY t, u IN SHARE UPDATE EXCLUSIVE MODE", reads: []string{platform.Postgres, platform.YugabyteDB}},
	{sql: "LOCK TABLES t WRITE", reads: mysqlFamily},
	{sql: "LOCK TABLE t READ LOCAL", reads: mysqlFamily},
	{sql: "LOCK TABLES t AS x READ, u WRITE", reads: mysqlFamily},
	{sql: "LOCK TABLES t LOW_PRIORITY WRITE", reads: []string{platform.MariaDB}},
	{sql: "UNLOCK TABLES", reads: mysqlFamily},
	{sql: "OPTIMIZE TABLE t", reads: together(mysqlFamily, []string{platform.ClickHouse})},
	{sql: "OPTIMIZE TABLE t, u", reads: mysqlFamily},
	{sql: "OPTIMIZE NO_WRITE_TO_BINLOG TABLE t", reads: mysqlFamily},
	{sql: "OPTIMIZE TABLE t FINAL DEDUPLICATE", reads: []string{platform.ClickHouse}},
	{sql: "RENAME TABLE t TO u", reads: together(mysqlFamily, []string{platform.ClickHouse})},
	{sql: "RENAME TABLE t TO u, v TO w", reads: together(mysqlFamily, []string{platform.ClickHouse})},
	{sql: "REPLACE INTO t (id) VALUES (1)", reads: together(mysqlFamily, []string{platform.SQLite})},
	{sql: "UPSERT INTO t (id) VALUES (1)", reads: []string{platform.CockroachDB}},
	{sql: "CREATE INDEX i ON t (a) STORING (b)", reads: []string{platform.CockroachDB}},
	{sql: "CREATE UNIQUE INDEX i ON t (a) STORING (b)", reads: []string{platform.CockroachDB}},
	{sql: "CREATE NONCLUSTERED INDEX i ON t (a)", reads: []string{platform.SQLServer}},
	{sql: "CREATE UNIQUE NONCLUSTERED INDEX i ON t (a)", reads: []string{platform.SQLServer}},
}

// TestParser_StatementGrammar_HappyPath holds each statement to the dialects whose
// servers take it. A dialect that is left out refuses it in the next test.
func TestParser_StatementGrammar_HappyPath(t *testing.T) {
	for _, row := range statementGrammarRows {
		for _, dialect := range row.reads {
			t.Run(dialect+"/"+row.sql, func(t *testing.T) {
				c := qt.New(t)

				_, err := parser.NewParser(row.sql+";", parser.WithDialect(dialect)).Parse()

				c.Assert(err, qt.IsNil)
			})
		}
	}
}

// TestParser_StatementGrammar_FailurePath is the other half of the same rows: a dialect
// whose server refuses a statement is not told it read.
func TestParser_StatementGrammar_FailurePath(t *testing.T) {
	for _, row := range statementGrammarRows {
		for _, dialect := range missingFrom(everyStatementGrammar, row.reads) {
			t.Run(dialect+"/"+row.sql, func(t *testing.T) {
				c := qt.New(t)

				_, err := parser.NewParser(row.sql+";", parser.WithDialect(dialect)).Parse()

				c.Assert(err, qt.IsNotNil)
			})
		}
	}
}

// missingFrom is the entries of all that are not in some.
func missingFrom(all, some []string) []string {
	set := make(map[string]bool, len(some))
	for _, entry := range some {
		set[entry] = true
	}
	var missing []string
	for _, entry := range all {
		if !set[entry] {
			missing = append(missing, entry)
		}
	}
	return missing
}

// TestParser_ANewStatementIsReadToItsEnd holds a statement this grammar reads to what
// the form says: text after it, or inside it, is refused, where a statement the parser
// steps over would have let it through.
func TestParser_ANewStatementIsReadToItsEnd(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		dialect string
		wantErr string
	}{
		{name: "a word after a view", sql: "DROP VIEW v BOGUS;", dialect: platform.SQLite, wantErr: `.*unsupported DROP VIEW clause: BOGUS`},
		{name: "a clause another engine has", sql: "DROP VIEW v SYNC;", dialect: platform.Postgres, wantErr: `.*unsupported DROP VIEW clause: SYNC`},
		{name: "a list where one name is taken", sql: "DROP VIEW v, w;", dialect: platform.SQLite, wantErr: `.*expected the end of DROP VIEW, got Operator`},
		{name: "no name", sql: "DROP VIEW;", dialect: platform.Postgres, wantErr: `.*expected view name.*`},
		{name: "no EXISTS", sql: "DROP VIEW IF v;", dialect: platform.Postgres, wantErr: `.*expected EXISTS after DROP VIEW IF.*`},
		{name: "punctuation for a name", sql: "DROP VIEW @@@;", dialect: platform.Postgres, wantErr: `.*expected view name.*`},
		{name: "a trigger with no table", sql: "DROP TRIGGER t;", dialect: platform.Postgres, wantErr: `.*expected ON after DROP TRIGGER t.*`},
		{name: "a word after a truncate", sql: "TRUNCATE t BOGUS;", dialect: platform.MySQL, wantErr: `.*unsupported TRUNCATE clause: BOGUS`},
		{name: "no table to truncate", sql: "TRUNCATE;", dialect: platform.MySQL, wantErr: `.*expected name.*`},
		{name: "punctuation for a table", sql: "TRUNCATE (((;", dialect: platform.Postgres, wantErr: `.*expected name.*`},
		{name: "T-SQL's TABLE left out", sql: "TRUNCATE t;", dialect: platform.SQLServer, wantErr: `.*unsupported TRUNCATE clause.*`},
		{name: "a lock mode that is no lock mode", sql: "LOCK TABLE t IN SOME MODE;", dialect: platform.Postgres, wantErr: `.*unsupported LOCK clause: SOME`},
		{name: "a lock with no kind", sql: "LOCK TABLES t;", dialect: platform.MySQL, wantErr: `.*unsupported LOCK clause.*`},
		{name: "a rename with no target", sql: "RENAME TABLE t;", dialect: platform.MySQL, wantErr: `.*expected TO after RENAME TABLE t.*`},
		{name: "a word after a rename", sql: "RENAME TABLE t TO u BOGUS;", dialect: platform.MySQL, wantErr: `.*unsupported RENAME TABLE clause: BOGUS`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestParser_ADropBuildsItsNodes holds what a read records: each name its own node, so
// a consumer that counts or scopes objects sees each one.
func TestParser_ADropBuildsItsNodes(t *testing.T) {
	arguments := "int, text"
	none := ""
	tests := []struct {
		name    string
		sql     string
		dialect string
		want    []ast.Node
	}{
		{
			name: "a view", sql: "DROP VIEW IF EXISTS s.v CASCADE;", dialect: platform.Postgres,
			want: []ast.Node{&ast.DropViewNode{Name: "s.v", IfExists: true, Cascade: true}},
		},
		{
			name: "views", sql: "DROP VIEW v, w;", dialect: platform.MySQL,
			want: []ast.Node{&ast.DropViewNode{Name: "v"}, &ast.DropViewNode{Name: "w"}},
		},
		{
			name: "a materialized view", sql: "DROP MATERIALIZED VIEW m;", dialect: platform.Postgres,
			want: []ast.Node{&ast.DropMaterializedViewNode{Name: "m"}},
		},
		{
			name: "a trigger on its table", sql: "DROP TRIGGER t ON s.u;", dialect: platform.Postgres,
			want: []ast.Node{&ast.DropTriggerNode{Name: "t", Table: "s.u"}},
		},
		{
			name: "a trigger with no table", sql: "DROP TRIGGER t;", dialect: platform.SQLite,
			want: []ast.Node{&ast.DropTriggerNode{Name: "t"}},
		},
		{
			name: "a sequence in a schema", sql: "DROP SEQUENCE s.q;", dialect: platform.Postgres,
			want: []ast.Node{&ast.DropSequenceNode{Schema: "s", Name: "q"}},
		},
		{
			name: "a type", sql: "DROP TYPE e;", dialect: platform.Postgres,
			want: []ast.Node{&ast.DropTypeNode{Name: "e"}},
		},
		{
			name: "routines and the arguments that address them", sql: "DROP FUNCTION f(int, text), g(), h;", dialect: platform.Postgres,
			want: []ast.Node{
				&ast.DropFunctionNode{Name: "f", Kind: string(ast.RoutineKindFunction), Parameters: &arguments},
				&ast.DropFunctionNode{Name: "g", Kind: string(ast.RoutineKindFunction), Parameters: &none},
				&ast.DropFunctionNode{Name: "h", Kind: string(ast.RoutineKindFunction)},
			},
		},
		{
			name: "T-SQL's short spelling of a procedure", sql: "DROP PROC p;", dialect: platform.SQLServer,
			want: []ast.Node{&ast.DropFunctionNode{Name: "p", Kind: string(ast.RoutineKindProcedure)}},
		},
		{
			name: "renames, in order", sql: "RENAME TABLE a TO b, b TO c;", dialect: platform.MySQL,
			want: []ast.Node{
				&ast.AlterTableNode{Name: "a", Operations: []ast.AlterOperation{&ast.RenameTableOperation{NewName: "b"}}},
				&ast.AlterTableNode{Name: "b", Operations: []ast.AlterOperation{&ast.RenameTableOperation{NewName: "c"}}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(got.Statements, qt.DeepEquals, test.want)
		})
	}
}

// TestParser_AStatementThatChangesNoSchemaContributesNoNode is the contract of a read
// of a schema document: an operation is read, and declares nothing.
func TestParser_AStatementThatChangesNoSchemaContributesNoNode(t *testing.T) {
	c := qt.New(t)

	got, err := parser.NewParser("TRUNCATE t; SAVEPOINT s; LOCK TABLE t; RELEASE s;", parser.WithDialect(platform.Postgres)).Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(got.Statements, qt.HasLen, 0)
}

// TestParser_ADocumentOfNoDialectStepsOverAnOperation is the permissive reading: nothing
// says which engine's form the statement is written in, so none is held against it.
func TestParser_ADocumentOfNoDialectStepsOverAnOperation(t *testing.T) {
	c := qt.New(t)

	got, err := parser.NewParser("TRUNCATE TABLE IF EXISTS t SYNC; LOCK TABLES t WRITE; UPSERT INTO t VALUES (1); DROP VIEW v;").Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(got.Statements, qt.DeepEquals, []ast.Node{&ast.DropViewNode{Name: "v"}})
}

// TestSkipsUnread is the one answer to which statements nothing reads, which a report
// on the text asks for rather than listing them itself.
func TestSkipsUnread(t *testing.T) {
	tests := []struct {
		keyword string
		dialect string
		want    bool
	}{
		{keyword: "SELECT", dialect: platform.Postgres, want: true},
		{keyword: "INSERT", dialect: platform.SQLite, want: true},
		{keyword: "REPLACE", dialect: platform.MySQL, want: true},
		{keyword: "REPLACE", dialect: platform.Postgres, want: false},
		{keyword: "UPSERT", dialect: platform.CockroachDB, want: true},
		{keyword: "UPSERT", dialect: platform.MySQL, want: false},
		{keyword: "TRUNCATE", dialect: platform.Postgres, want: false},
		{keyword: "DROP", dialect: platform.Postgres, want: false},
		{keyword: "TRUNCATE", dialect: "", want: true},
		{keyword: "UPSERT", dialect: "", want: true},
		{keyword: "DROP", dialect: "", want: false},
		{keyword: "FOO", dialect: "", want: false},
	}
	for _, test := range tests {
		t.Run(test.keyword+" on "+test.dialect, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(parser.SkipsUnread(test.keyword, test.dialect), qt.Equals, test.want)
		})
	}
}

// TestParser_OpeningWordsNoEngineHas is the one refusal the parser can say is about the
// text, and the controls that it does not say so of anything else.
func TestParser_OpeningWordsNoEngineHas(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantHead string
	}{
		{name: "a statement", sql: "FOO BAR;", wantHead: "FOO"},
		{name: "a misspelled statement", sql: "SELEC 1;", wantHead: "SELEC"},
		{name: "a misspelled DROP target", sql: "DROP TABEL t;", wantHead: "DROP TABEL"},
		{name: "a misspelled CREATE target", sql: "CREATE TABEL t (id int);", wantHead: "CREATE TABEL"},
		{name: "a misspelled ALTER target", sql: "ALTER TABEL t ADD a int;", wantHead: "ALTER TABEL"},
		{name: "a misspelled target after UNIQUE", sql: "CREATE UNIQUE INDX i ON t (a);", wantHead: "CREATE UNIQUE INDX"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			refusal, known := errors.AsType[*parser.UnknownStatementError](err)
			c.Assert(known, qt.IsTrue, qt.Commentf("%v", err))
			c.Assert(refusal.Head, qt.Equals, test.wantHead)
		})
	}

	refused := []struct {
		name string
		sql  string
	}{
		{name: "a statement no reading holds here", sql: "EXPLAIN SELECT 1;"},
		{name: "a DROP target an engine has", sql: "DROP AGGREGATE a(int);"},
		{name: "a CREATE target an engine has", sql: "CREATE COLLATION c (locale = 'C');"},
		{name: "an ALTER target an engine has", sql: "ALTER SYSTEM SET a = 1;"},
		{name: "a word inside a statement the parser reads", sql: "CREATE TABLE b BOGUS;"},
		{name: "a word after a statement with no semicolon", sql: "DROP TABLE t ON CLUSTER c;"},
		{name: "a form another dialect takes", sql: "OPTIMIZE TABLE t;"},
	}
	for _, test := range refused {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.IsNotNil)
			_, known := errors.AsType[*parser.UnknownStatementError](err)
			c.Assert(known, qt.IsFalse, qt.Commentf("%v", err))
		})
	}
}
