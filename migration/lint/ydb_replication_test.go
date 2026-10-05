package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

// connection is a WITH clause naming a database and no credential.
const connection = "CONNECTION_STRING = 'grpc://primary:2136/?database=/prod'"

// Each row is a migration directory whose replication drop YD115 reports, or
// whose secret in clear YD116 reports, at the statement that does it. A
// replication created again after a failover is one that was not failed over.
func TestYDBRules_ReportReplicationTraps(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{
			name: "a replication dropped without failover",
			files: map[string]string{
				"0001_r.up.sql": "CREATE ASYNC REPLICATION mirror FOR a AS ra WITH (" + connection + ");\n",
				"0002_r.up.sql": "DROP ASYNC REPLICATION mirror;\n",
			},
			want: []string{"0002_r.up.sql:1:YD115"},
		},
		{
			name: "a failover an earlier file undid by creating the replication again",
			files: map[string]string{
				"0001_r.up.sql": "ALTER ASYNC REPLICATION mirror SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE');\n",
				"0002_r.up.sql": "DROP ASYNC REPLICATION mirror CASCADE;\n" +
					"CREATE ASYNC REPLICATION mirror FOR a AS ra WITH (" + connection + ");\n",
				"0003_r.up.sql": "DROP ASYNC REPLICATION mirror;\n",
			},
			want: []string{"0003_r.up.sql:1:YD115"},
		},
		{
			name: "a password in clear",
			files: map[string]string{
				"0001_r.up.sql": "CREATE ASYNC REPLICATION mirror FOR a AS ra WITH (" + connection +
					", USER = 'u', PASSWORD = 'secret');\n",
			},
			want: []string{"0001_r.up.sql:1:YD116"},
		},
		{
			name: "a token in clear on a transfer, after its lambda",
			files: map[string]string{
				"0001_t.up.sql": "CREATE TRANSFER ingest FROM tp TO t USING ($m) -> { return [<| a: 1 |>]; } WITH (" +
					connection + ", TOKEN = 'secret');\n",
			},
			want: []string{"0001_t.up.sql:1:YD116"},
		},
		{
			name: "a password set later",
			files: map[string]string{
				"0001_r.up.sql": "ALTER ASYNC REPLICATION mirror SET (PASSWORD = 'secret');\n",
			},
			want: []string{"0001_r.up.sql:1:YD116"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, test.files, "")), qt.DeepEquals, test.want)
		})
	}
}

// Each row is the remedy for a row above: a drop after a failover, in the same
// file or an earlier one, and a credential named by its secret.
func TestYDBRules_LeaveReplicationStatementsYDBKeeps(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
	}{
		{
			name: "a drop after a failover in an earlier file",
			files: map[string]string{
				"0001_r.up.sql": "ALTER ASYNC REPLICATION mirror SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE');\n",
				"0002_r.up.sql": "DROP ASYNC REPLICATION mirror;\n",
			},
		},
		{
			name: "a drop after a failover written in lower case, in the same file",
			files: map[string]string{
				"0001_r.up.sql": "ALTER ASYNC REPLICATION mirror SET (state = 'done', failover_mode = 'force');\n" +
					"DROP ASYNC REPLICATION mirror;\n",
			},
		},
		{
			name: "secrets named",
			files: map[string]string{
				"0001_r.up.sql": "CREATE ASYNC REPLICATION mirror FOR a AS ra WITH (" + connection +
					", USER = 'u', PASSWORD_SECRET_NAME = 'pw');\n" +
					"CREATE TRANSFER ingest FROM tp TO t USING ($m) -> { return []; } WITH (" + connection +
					", TOKEN_SECRET_PATH = 'secrets/token');\n",
			},
		},
		{
			name: "a lambda whose own text names a password",
			files: map[string]string{
				"0001_t.up.sql": "CREATE TRANSFER ingest FROM tp TO t USING ($m) -> { return [<| PASSWORD: 'x' |>]; };\n",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, test.files, "")), qt.HasLen, 0)
		})
	}
}

// TestYDBRules_SayWhatAReplicationStatementLeaves pins the messages, which name
// the remedy.
func TestYDBRules_SayWhatAReplicationStatementLeaves(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "drop", sql: "DROP ASYNC REPLICATION `dr/mirror`;",
			want: "DROP ASYNC REPLICATION dr/mirror keeps its replica tables, and YDB keeps a replica of a replication " +
				"that was not failed over read-only for good (path is an async replica table); fail it over first " +
				"with ALTER ASYNC REPLICATION dr/mirror SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE') to keep " +
				"writable tables, or drop it with CASCADE to drop them"},
		{name: "secret", sql: "CREATE TRANSFER ingest FROM tp TO t USING ($m) -> { return []; } WITH (" + connection +
			", TOKEN = 'secret');",
			want: "TOKEN of ingest is written in clear: the migration file now holds the secret, and YDB keeps it " +
				"without reading it back; put it in a secret and name it with TOKEN_SECRET_NAME or TOKEN_SECRET_PATH"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, err := lint.ResolveTarget("ydb", "")
			c.Assert(err, qt.IsNil)

			findings, err := lint.LintFS(fixture(map[string]string{"0001_t.up.sql": test.sql + "\n"}),
				lint.Options{Dialect: "ydb", Target: target})

			c.Assert(err, qt.IsNil)
			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Message, qt.Equals, test.want)
		})
	}
}

// DS107 reports DROP TRANSFER, which drops the consumer YDB created for the
// transfer with its position, and DROP ASYNC REPLICATION ... CASCADE, which
// drops the replica tables; a plain drop it leaves to YD115.
func TestYDBRules_DS107ReportsADroppedTransferOrCascade(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{name: "a transfer", sql: "DROP TRANSFER `shop/ingest`;", want: []string{"0001_t.up.sql:1:DS107"}},
		{name: "a replication with CASCADE", sql: "DROP ASYNC REPLICATION mirror CASCADE;",
			want: []string{"0001_t.up.sql:1:DS107"}},
		{name: "a replication without CASCADE", sql: "DROP ASYNC REPLICATION mirror;",
			want: []string{"0001_t.up.sql:1:YD115"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbLint(c, map[string]string{"0001_t.up.sql": test.sql + "\n"}, ""), qt.DeepEquals, test.want)
		})
	}
}
