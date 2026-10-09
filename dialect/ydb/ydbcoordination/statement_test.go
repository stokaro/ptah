package ydbcoordination_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbcoordination"
)

func TestStatementText_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement ydbcoordination.Statement
		want      string
	}{
		{
			name:      "a node with the defaults",
			statement: ydbcoordination.Statement{Verb: ydbcoordination.Create, Path: "locks"},
			want:      "CREATE COORDINATION NODE `locks`",
		},
		{
			name: "a node in a directory with every setting",
			statement: ydbcoordination.Statement{Verb: ydbcoordination.Create, Path: "app/locks",
				Spec: ydbcoordination.Spec{
					SelfCheckPeriodMillis: 2500, SessionGracePeriodMillis: 15000,
					ReadConsistencyMode: "strict", AttachConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed",
				}},
			want: "CREATE COORDINATION NODE `app/locks` WITH (self_check_period = Interval('PT2.5S'), " +
				"session_grace_period = Interval('PT15S'), read_consistency_mode = 'strict', " +
				"attach_consistency_mode = 'relaxed', rate_limiter_counters_mode = 'detailed')",
		},
		{
			name: "a change of one setting",
			statement: ydbcoordination.Statement{Verb: ydbcoordination.Alter, Path: "locks",
				Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}},
			want: "ALTER COORDINATION NODE `locks` SET (read_consistency_mode = 'strict')",
		},
		{
			name:      "a drop of a name that needs escaping",
			statement: ydbcoordination.Statement{Verb: ydbcoordination.Drop, Path: "dir/tick`name"},
			want:      "DROP COORDINATION NODE `dir/tick\\`name`",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := test.statement.Text()
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestStatementText_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement ydbcoordination.Statement
		wantErr   string
	}{
		{name: "no verb", statement: ydbcoordination.Statement{Path: "locks"},
			wantErr: `invalid coordination node statement: unknown verb 0`},
		{name: "a change of nothing", statement: ydbcoordination.Statement{Verb: ydbcoordination.Alter, Path: "locks"},
			wantErr: `invalid coordination node statement: ALTER COORDINATION NODE locks names no setting to change`},
		{name: "a drop with settings", statement: ydbcoordination.Statement{Verb: ydbcoordination.Drop, Path: "locks",
			Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}},
			wantErr: `invalid coordination node statement: DROP COORDINATION NODE locks takes no setting`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := test.statement.Text()
			c.Assert(err, qt.ErrorIs, ydbcoordination.ErrStatement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// Each statement Text writes is recognized as the statement it was written
// from, which is what lets a plan carry it as text.
func TestRecognize_ReadsWhatTextWrites(t *testing.T) {
	statements := []ydbcoordination.Statement{
		{Verb: ydbcoordination.Create, Path: "locks"},
		{Verb: ydbcoordination.Create, Path: "app/sub/locks", Spec: ydbcoordination.Spec{
			SelfCheckPeriodMillis: 750, SessionGracePeriodMillis: 30000,
			ReadConsistencyMode: "strict", AttachConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed",
		}},
		{Verb: ydbcoordination.Alter, Path: "a.b", Spec: ydbcoordination.Spec{SessionGracePeriodMillis: 12000}},
		{Verb: ydbcoordination.Drop, Path: "tick`back\\slash"},
	}
	for _, statement := range statements {
		t.Run(statement.Path, func(t *testing.T) {
			c := qt.New(t)
			text, err := statement.Text()
			c.Assert(err, qt.IsNil)
			got, recognized, err := ydbcoordination.Recognize(text + ";")
			c.Assert(err, qt.IsNil)
			c.Assert(recognized, qt.IsTrue)
			c.Assert(got, qt.Equals, ydbcoordination.Query{Statement: statement})
		})
	}
}

func TestRecognize_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want ydbcoordination.Query
	}{
		{
			name: "keywords in any case and a bare name",
			text: "create Coordination node locks",
			want: ydbcoordination.Query{Statement: ydbcoordination.Statement{Verb: ydbcoordination.Create, Path: "locks"}},
		},
		{
			name: "settings in any case, double quotes and the Utf8 suffix",
			text: `ALTER COORDINATION NODE ` + "`l`" + ` SET (Read_Consistency_Mode = "Strict"u, ` +
				`SELF_CHECK_PERIOD = interval("PT2S"))`,
			want: ydbcoordination.Query{Statement: ydbcoordination.Statement{Verb: ydbcoordination.Alter, Path: "l",
				Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict", SelfCheckPeriodMillis: 2000}}},
		},
		{
			name: "the head a migration query carries",
			text: "--!syntax_v1\n-- the lock nodes\nPRAGMA TablePathPrefix(\"/local/app\");\n$x = 1;\n" +
				"DECLARE $p AS Int64;\nDROP COORDINATION NODE `locks` /* gone */;",
			want: ydbcoordination.Query{PathPrefix: "/local/app",
				Statement: ydbcoordination.Statement{Verb: ydbcoordination.Drop, Path: "locks"}},
		},
		{
			name: "the assignment form of the prefix pragma, the last one winning",
			text: "PRAGMA TablePathPrefix('/local/a'); PRAGMA tablepathprefix = '/local/b'; CREATE COORDINATION NODE n",
			want: ydbcoordination.Query{PathPrefix: "/local/b",
				Statement: ydbcoordination.Statement{Verb: ydbcoordination.Create, Path: "n"}},
		},
		{
			name: "an absolute path",
			text: "DROP COORDINATION NODE `/local/app/locks`",
			want: ydbcoordination.Query{Statement: ydbcoordination.Statement{Verb: ydbcoordination.Drop,
				Path: "/local/app/locks"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, recognized, err := ydbcoordination.Recognize(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(recognized, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// Text that only mentions the statement is not one: Ptah's connection sends
// it to YDB as it is.
func TestRecognize_LeavesOtherTextAlone(t *testing.T) {
	texts := []string{
		"SELECT 1",
		"SELECT 'CREATE COORDINATION NODE x'",
		"-- CREATE COORDINATION NODE x\nSELECT 1",
		"CREATE TABLE `coordination` (id Int64 NOT NULL, PRIMARY KEY (id))",
		"CREATE TABLE coordination_node (id Int64 NOT NULL, PRIMARY KEY (id))",
		"UPSERT INTO t (note) VALUES (@@DROP COORDINATION NODE n@@)",
		"CREATE TOPIC coordination",
	}
	for _, text := range texts {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			got, recognized, err := ydbcoordination.Recognize(text)
			c.Assert(err, qt.IsNil)
			c.Assert(recognized, qt.IsFalse)
			c.Assert(got, qt.Equals, ydbcoordination.Query{})
		})
	}
}

func TestRecognize_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		text    string
		wantErr string
	}{
		{name: "two statements",
			text:    "CREATE COORDINATION NODE a; DROP COORDINATION NODE b",
			wantErr: `invalid coordination node statement: a query runs one coordination node statement, .*`},
		{name: "a statement after it",
			text:    "DROP COORDINATION NODE a; SELECT 1",
			wantErr: `invalid coordination node statement: DROP COORDINATION NODE is followed by another statement .*`},
		{name: "a statement before it",
			text:    "CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)); CREATE COORDINATION NODE a",
			wantErr: `invalid coordination node statement: CREATE COORDINATION NODE shares its query with CREATE TABLE T, .*`},
		{name: "a prefix pragma Ptah cannot read",
			text:    "PRAGMA TablePathPrefix('cluster', '/local/a'); CREATE COORDINATION NODE a",
			wantErr: `invalid coordination node statement: CREATE COORDINATION NODE follows a TablePathPrefix pragma Ptah cannot read; .*`},
		{name: "a prefix that is not a plain string",
			text:    "PRAGMA TablePathPrefix($p); CREATE COORDINATION NODE a",
			wantErr: `invalid coordination node statement: CREATE COORDINATION NODE follows a TablePathPrefix pragma whose path is not a plain string`},
		{name: "no name", text: "DROP COORDINATION NODE",
			wantErr: `invalid coordination node statement: DROP COORDINATION NODE names no node`},
		{name: "a string for a name", text: "DROP COORDINATION NODE 'a'",
			wantErr: `invalid coordination node statement: DROP COORDINATION NODE 'a' does not name a node; .*`},
		{name: "a named expression for a name", text: "DROP COORDINATION NODE $n",
			wantErr: `invalid coordination node statement: DROP COORDINATION NODE \$n does not name a node; .*`},
		{name: "an alter of nothing", text: "ALTER COORDINATION NODE a",
			wantErr: `invalid coordination node statement: ALTER COORDINATION NODE a names no setting; write SET \(...\)`},
		{name: "a drop with settings", text: "DROP COORDINATION NODE a WITH (read_consistency_mode = 'strict')",
			wantErr: `invalid coordination node statement: DROP COORDINATION NODE a is followed by WITH, which Ptah does not read`},
		{name: "a create with SET", text: "CREATE COORDINATION NODE a SET (read_consistency_mode = 'strict')",
			wantErr: `invalid coordination node statement: CREATE COORDINATION NODE a is followed by SET, .*`},
		{name: "no parentheses", text: "CREATE COORDINATION NODE a WITH read_consistency_mode = 'strict'",
			wantErr: `invalid coordination node statement: CREATE COORDINATION NODE a: the settings are written in parentheses`},
		{name: "an unknown setting", text: "CREATE COORDINATION NODE a WITH (session_timeout = Interval('PT1S'))",
			wantErr: `invalid coordination node statement: CREATE COORDINATION NODE a: session_timeout is not a setting; .*`},
		{name: "a setting twice",
			text:    "ALTER COORDINATION NODE a SET (read_consistency_mode = 'strict', read_consistency_mode = 'relaxed')",
			wantErr: `invalid coordination node statement: ALTER COORDINATION NODE a: read_consistency_mode is set twice`},
		{name: "a missing equals sign", text: "ALTER COORDINATION NODE a SET (read_consistency_mode 'strict')",
			wantErr: `invalid coordination node statement: ALTER COORDINATION NODE a: read_consistency_mode is followed by 'strict', not =`},
		{name: "a period without Interval", text: "ALTER COORDINATION NODE a SET (self_check_period = 'PT1S')",
			wantErr: `invalid coordination node statement: ALTER COORDINATION NODE a: self_check_period takes an interval such as Interval\('PT1S'\)`},
		{name: "a mode that is not a string", text: "ALTER COORDINATION NODE a SET (read_consistency_mode = strict)",
			wantErr: `invalid coordination node statement: ALTER COORDINATION NODE a: read_consistency_mode takes a string such as 'strict', not strict`},
		{name: "an escaped string", text: `ALTER COORDINATION NODE a SET (read_consistency_mode = 'str\x69ct')`,
			wantErr: `invalid coordination node statement: ALTER COORDINATION NODE a: read_consistency_mode takes a string .*`},
		{name: "an unknown mode", text: "ALTER COORDINATION NODE a SET (attach_consistency_mode = 'eventual')",
			wantErr: `invalid coordination node statement: ALTER COORDINATION NODE a: attach_consistency_mode "eventual": .*`},
		{name: "a self-check period out of range",
			text:    "ALTER COORDINATION NODE a SET (self_check_period = Interval('PT0.1S'))",
			wantErr: `invalid coordination node statement: ALTER COORDINATION NODE a: self_check_period PT0.1S: .*`},
		{name: "a grace period out of range",
			text:    "ALTER COORDINATION NODE a SET (session_grace_period = Interval('PT1M'))",
			wantErr: `invalid coordination node statement: ALTER COORDINATION NODE a: session_grace_period PT1M: .*`},
		{name: "a created node the server would not run as written",
			text:    "CREATE COORDINATION NODE a WITH (self_check_period = Interval('PT10S'))",
			wantErr: `invalid coordination node statement: CREATE COORDINATION NODE a: session_grace_period PT10S: .*`},
		{name: "a trailing comma", text: "ALTER COORDINATION NODE a SET (read_consistency_mode = 'strict',)",
			wantErr: `invalid coordination node statement: ALTER COORDINATION NODE a: the settings end early; .*`},
		{name: "text after the settings",
			text:    "ALTER COORDINATION NODE a SET (read_consistency_mode = 'strict') NOW",
			wantErr: `invalid coordination node statement: ALTER COORDINATION NODE a: the settings end with \) and nothing follows it`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, recognized, err := ydbcoordination.Recognize(test.text)
			c.Assert(err, qt.ErrorIs, ydbcoordination.ErrStatement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(recognized, qt.IsTrue)
			c.Assert(got, qt.Equals, ydbcoordination.Query{})
		})
	}
}

func TestQueryAbsolute_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		query ydbcoordination.Query
		want  string
	}{
		{name: "relative to the database root",
			query: ydbcoordination.Query{Statement: ydbcoordination.Statement{Path: "app/locks"}}, want: "/local/app/locks"},
		{name: "relative to the prefix", query: ydbcoordination.Query{PathPrefix: "/local/probe",
			Statement: ydbcoordination.Statement{Path: "locks"}}, want: "/local/probe/locks"},
		{name: "absolute whatever the prefix", query: ydbcoordination.Query{PathPrefix: "/local/probe",
			Statement: ydbcoordination.Statement{Path: "/local/x/../locks"}}, want: "/local/locks"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := test.query.Absolute("/local/")
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestQueryAbsolute_FailurePath(t *testing.T) {
	c := qt.New(t)
	query := ydbcoordination.Query{PathPrefix: "relp", Statement: ydbcoordination.Statement{Path: "locks"}}

	got, err := query.Absolute("/local")

	c.Assert(err, qt.ErrorIs, ydbcoordination.ErrStatement)
	c.Assert(err, qt.ErrorMatches, `invalid coordination node statement: path relp/locks is not in database /local: `+
		`the TablePathPrefix "relp" is relative, and YDB reads a prefix only as an absolute path`)
	c.Assert(got, qt.Equals, "")
}
