package migrator

// White-box testing required: what decides how a YDB body runs -- the units it
// is split into, the source each unit is digested from, the refusal of a body
// that cannot be split, and the hook that commits a data query with its
// checkpoint -- is package-local, and the exported path that reaches it needs a
// live YDB connection. The live tests under integration/dbschema/ydb drive that
// path; these pin each decision on its own.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

const ydbBody = "CREATE TABLE `t` (id Int64 NOT NULL, PRIMARY KEY (id));\n" +
	"$v = 1l;\nUPSERT INTO `t` (id) VALUES ($v);\nUPSERT INTO `t` (id) VALUES ($v + 1);\n" +
	"ALTER TABLE `t` ADD COLUMN `name` Utf8;\n"

// On YDB the units are the queries the body runs as; elsewhere they stay
// statements.
func TestSplitSQLStatementsForDialect_YDBRunsQueries(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{
			name:    "ydb",
			dialect: platform.YDB,
			want: []string{
				"CREATE TABLE `t` (id Int64 NOT NULL, PRIMARY KEY (id))",
				"$v = 1l;\nUPSERT INTO `t` (id) VALUES ($v);\nUPSERT INTO `t` (id) VALUES ($v + 1)",
				"$v = 1l;\nALTER TABLE `t` ADD COLUMN `name` Utf8",
			},
		},
		{
			name:    "postgres keeps statements",
			dialect: platform.Postgres,
			want: []string{
				"CREATE TABLE `t` (id Int64 NOT NULL, PRIMARY KEY (id))",
				"$v = 1l",
				"UPSERT INTO `t` (id) VALUES ($v)",
				"UPSERT INTO `t` (id) VALUES ($v + 1)",
				"ALTER TABLE `t` ADD COLUMN `name` Utf8",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(splitSQLStatementsForDialect(ydbBody, test.dialect), qt.DeepEquals, test.want)
			c.Assert(migrationStatementCountForDialect(ydbBody, test.dialect), qt.Equals, len(test.want))
		})
	}
}

// A unit's source is what partial hashes digest, so the source units have to
// line up with the executed ones: one each, in order.
func TestSourceStatementsForDialect_FollowTheUnits(t *testing.T) {
	c := qt.New(t)

	sources := sourceStatementsForDialect(ydbBody, platform.YDB)

	texts := make([]string, 0, len(sources))
	for _, source := range sources {
		texts = append(texts, source.Text)
	}
	c.Assert(texts, qt.DeepEquals, []string{
		"CREATE TABLE `t` (id Int64 NOT NULL, PRIMARY KEY (id));",
		"$v = 1l;\nUPSERT INTO `t` (id) VALUES ($v);\nUPSERT INTO `t` (id) VALUES ($v + 1);",
		"ALTER TABLE `t` ADD COLUMN `name` Utf8;",
	})
	c.Assert(len(sources), qt.Equals, len(splitSQLStatementsForDialect(ydbBody, platform.YDB)))
}

func TestRefuseUnsplittableSQL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{name: "a client delimiter", body: "DELIMITER //\nDROP TABLE `t` //",
			wantErr: `client delimiter directive in YQL: "DELIMITER //" .*`},
		{name: "a translation setting Ptah cannot split by", body: "--!ansi_lexer\nDROP TABLE `t`;",
			wantErr: `unsupported YQL translation setting "--!ansi_lexer": .*`},
		{name: "a commit inside a data run", body: "UPSERT INTO `t` (id) VALUES (1l);\nCOMMIT;\nUPSERT INTO `t` (id) VALUES (2l);",
			wantErr: `"COMMIT" controls a transaction, and on YDB the migrator runs each data query in a transaction of its own; remove it`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(refuseUnsplittableSQL(platform.YDB, test.body), qt.ErrorMatches, test.wantErr)
		})
	}
}

// The control: the same bodies pass on a dialect whose splitter honors or
// ignores them, and a plain YDB body passes.
func TestRefuseUnsplittableSQL_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		body    string
	}{
		{name: "a YDB body", dialect: platform.YDB, body: ydbBody},
		{name: "a YDB body under the supported setting", dialect: platform.YDB, body: "--!syntax_v1\n" + ydbBody},
		{name: "a client delimiter on MySQL", dialect: platform.MySQL, body: "DELIMITER //\nDROP TABLE t //"},
		{name: "a commit on PostgreSQL", dialect: platform.Postgres, body: "COMMIT;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(refuseUnsplittableSQL(test.dialect, test.body), qt.IsNil)
		})
	}
}

// A committed YDB query leaves no session state to replay: every query
// carries the definitions it needs.
func TestNoTransactionResumeAction_YDBQueriesAreDurable(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "a pragma-headed scheme query", statement: "PRAGMA TablePathPrefix = \"/local/app\";\nCREATE TABLE a (id Int64 NOT NULL, PRIMARY KEY (id))"},
		{name: "a data query", statement: "$v = 1l;\nUPSERT INTO a (id) VALUES ($v)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(noTransactionResumeAction(test.statement, platform.YDB), qt.Equals, noTransactionPrefixDurable)
		})
	}
}

func TestIsYDBDataQuery(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "a data query", query: "$v = 1l;\nUPSERT INTO a (id) VALUES ($v)", want: true},
		{name: "a scheme query", query: "$v = 1l;\nDROP TABLE a", want: false},
		{name: "two queries are not one data query", query: "UPSERT INTO a (id) VALUES (1l); DROP TABLE a;", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(isYDBDataQuery(test.query), qt.Equals, test.want)
		})
	}
}

// The loop asks the committer first, and a statement it claims is run and
// recorded by it alone; one it does not claim is left to the loop.
func TestCommitStatement(t *testing.T) {
	failure := errors.New("refused")
	var ran []string
	ctx := withStatementCommitter(context.Background(), statementCommitter{
		claims: isYDBDataQuery,
		run: func(_ context.Context, event StatementEvent) error {
			ran = append(ran, event.Statement)
			return failure
		},
	})
	tests := []struct {
		name          string
		statement     string
		wantCommitted bool
		wantErr       error
	}{
		{name: "a data query is committed", statement: "UPSERT INTO a (id) VALUES (1l)", wantCommitted: true, wantErr: failure},
		{name: "a scheme query is left to the loop", statement: "DROP TABLE a", wantCommitted: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			committed, err := commitStatement(ctx, StatementEvent{Statement: test.statement, Index: 1, Total: 1})
			c.Assert(committed, qt.Equals, test.wantCommitted)
			c.Assert(errors.Is(err, test.wantErr), qt.IsTrue)
		})
	}
	c := qt.New(t)
	c.Assert(ran, qt.DeepEquals, []string{"UPSERT INTO a (id) VALUES (1l)"})
	committed, err := commitStatement(context.Background(), StatementEvent{Statement: "UPSERT INTO a (id) VALUES (1l)"})
	c.Assert(committed, qt.IsFalse)
	c.Assert(err, qt.IsNil)
}
