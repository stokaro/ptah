package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

func TestParseChecks_MySQLBackslashEscaping(t *testing.T) {
	c := qt.New(t)

	checks, err := migrator.ParseChecks(
		`-- +ptah check name="escaped" assert="SELECT 'it\'s;ok'"`,
		platform.MySQL,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(checks, qt.DeepEquals, []migrator.Check{{
		Name:   "escaped",
		Assert: `SELECT 'it\'s;ok'`,
		OnFail: migrator.OnFailAbort, Phase: migrator.CheckPhaseBefore,
	}})
}

func TestParseChecks_ClickHouseBackslashEscaping(t *testing.T) {
	c := qt.New(t)

	checks, err := migrator.ParseChecks(
		`-- +ptah check name="escaped" assert="SELECT 'it\'s;ok'"`,
		platform.ClickHouse,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(checks, qt.DeepEquals, []migrator.Check{{
		Name:   "escaped",
		Assert: `SELECT 'it\'s;ok'`,
		OnFail: migrator.OnFailAbort, Phase: migrator.CheckPhaseBefore,
	}})
}

func TestParseChecks_PostgresBackslashLiteralBeforeDirective(t *testing.T) {
	c := qt.New(t)

	checks, err := migrator.ParseChecks(`SELECT '\'::text;
-- +ptah check name="after_literal" assert="SELECT 1"
DROP TABLE users;
`, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(checks, qt.DeepEquals, []migrator.Check{{
		Name:   "after_literal",
		Assert: "SELECT 1",
		OnFail: migrator.OnFailAbort, Phase: migrator.CheckPhaseBefore,
	}})
}

func TestParseChecks_PostgresEscapeStringBeforeDirective(t *testing.T) {
	c := qt.New(t)

	checks, err := migrator.ParseChecks(`SELECT E'it\'s still one literal';
-- +ptah check name="after_escape_literal" assert="SELECT 1"
DROP TABLE users;
`, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(checks, qt.DeepEquals, []migrator.Check{{
		Name:   "after_escape_literal",
		Assert: "SELECT 1",
		OnFail: migrator.OnFailAbort, Phase: migrator.CheckPhaseBefore,
	}})
}

func TestParseChecks_PostgresFamilyEscapeStringBeforeDirective(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "CockroachDB", dialect: platform.CockroachDB},
		{name: "YugabyteDB", dialect: platform.YugabyteDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			checks, err := migrator.ParseChecks(`SELECT E'it\'s still one literal';
-- +ptah check name="after_escape_literal" assert="SELECT 1"
DROP TABLE users;
`, test.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(checks, qt.DeepEquals, []migrator.Check{{
				Name:   "after_escape_literal",
				Assert: "SELECT 1",
				OnFail: migrator.OnFailAbort, Phase: migrator.CheckPhaseBefore,
			}})
		})
	}
}

func TestParseChecks_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []migrator.Check
	}{
		{
			name: "single check with spaces and equals in assert",
			sql:  `-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort` + "\nDROP TABLE users;\n",
			want: []migrator.Check{{Name: "users_empty", Assert: "SELECT count(*) = 0 FROM users", OnFail: migrator.OnFailAbort, Phase: migrator.CheckPhaseBefore}},
		},
		{
			name: "on_fail defaults to abort",
			sql:  `-- +ptah check name="x" assert="SELECT true"` + "\nSELECT 1;\n",
			want: []migrator.Check{{Name: "x", Assert: "SELECT true", OnFail: migrator.OnFailAbort, Phase: migrator.CheckPhaseBefore}},
		},
		{
			name: "multiple checks run in file order",
			sql: `-- +ptah check name="a" assert="SELECT 1"` + "\n" +
				`-- +ptah check name="b" assert="SELECT 2"` + "\nDROP TABLE t;\n",
			want: []migrator.Check{
				{Name: "a", Assert: "SELECT 1", OnFail: migrator.OnFailAbort, Phase: migrator.CheckPhaseBefore},
				{Name: "b", Assert: "SELECT 2", OnFail: migrator.OnFailAbort, Phase: migrator.CheckPhaseBefore},
			},
		},
		{
			name: "no checks",
			sql:  "DROP TABLE users;\n",
			want: nil,
		},
		{
			name: "other +ptah directives are ignored",
			sql:  "-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY idx ON t (c);\n",
			want: nil,
		},
		{
			name: "check text inside a string literal is not parsed",
			sql:  `INSERT INTO log (msg) VALUES ('-- +ptah check name="x" assert="SELECT 1"');` + "\n",
			want: nil,
		},
		{
			name: "trailing comment is not a check",
			sql:  `DROP TABLE users; -- +ptah check name="x" assert="SELECT 1"` + "\n",
			want: nil,
		},
		{
			name: "doubled quotes escape a double quote in the assert",
			sql:  `-- +ptah check name="q" assert="SELECT count(*) = 0 FROM ""My Table"""` + "\nSELECT 1;\n",
			want: []migrator.Check{{Name: "q", Assert: `SELECT count(*) = 0 FROM "My Table"`, OnFail: migrator.OnFailAbort, Phase: migrator.CheckPhaseBefore}},
		},
		{
			name: "a kind",
			sql:  `-- +ptah check kind=rounded-values name="public.t.c" assert="SELECT 1"` + "\nALTER TABLE t ALTER COLUMN c TYPE int;\n",
			want: []migrator.Check{{
				Name: "public.t.c", Assert: "SELECT 1", OnFail: migrator.OnFailAbort, Phase: migrator.CheckPhaseBefore,
				Kind: migrator.CheckKindRoundedValues,
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := migrator.ParseChecks(tt.sql, "")
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

func TestParseChecks_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "missing assert",
			sql:     `-- +ptah check name="x"` + "\nSELECT 1;\n",
			wantErr: `\+ptah check requires a non-empty assert predicate`,
		},
		{
			name:    "unknown key",
			sql:     `-- +ptah check name="x" assert="SELECT 1" bogus=1` + "\nSELECT 1;\n",
			wantErr: `unknown \+ptah check key "bogus" \(want name, assert, on_fail, phase, kind\)`,
		},
		{
			name:    "unknown kind",
			sql:     `-- +ptah check kind=rounded name="x" assert="SELECT 1"` + "\nSELECT 1;\n",
			wantErr: `unsupported \+ptah check kind="rounded" \(want rounded-values\)`,
		},
		{
			name:    "empty kind",
			sql:     `-- +ptah check kind="" name="x" assert="SELECT 1"` + "\nSELECT 1;\n",
			wantErr: `unsupported \+ptah check kind="" \(want rounded-values\)`,
		},
		{
			name:    "rounded values after the body",
			sql:     `-- +ptah check kind=rounded-values phase=after name="x" assert="SELECT 1"` + "\nSELECT 1;\n",
			wantErr: `\+ptah check kind=rounded-values reads the rows before the body and takes no phase=after`,
		},
		{
			name:    "unsupported on_fail",
			sql:     `-- +ptah check name="x" assert="SELECT 1" on_fail=warn` + "\nSELECT 1;\n",
			wantErr: `unsupported \+ptah check on_fail="warn" \(only abort is supported\)`,
		},
		{
			name:    "unterminated quote",
			sql:     `-- +ptah check name="x" assert="SELECT 1` + "\nSELECT 1;\n",
			wantErr: `unterminated quote in \+ptah check directive`,
		},
		{
			name:    "multi-statement assert",
			sql:     `-- +ptah check name="x" assert="SELECT 1; DROP TABLE t"` + "\nSELECT 1;\n",
			wantErr: `\+ptah check assert must be a single statement, got 2`,
		},
		{
			name:    "duplicate key",
			sql:     `-- +ptah check name="x" name="y" assert="SELECT 1"` + "\nSELECT 1;\n",
			wantErr: `duplicate \+ptah check key "name"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := migrator.ParseChecks(tt.sql, "")
			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestParseFileDirectivesIgnoresChecks proves a check line does not pollute the
// merged directive map that ParseFileDirectives returns.
func TestParseFileDirectivesIgnoresChecks(t *testing.T) {
	c := qt.New(t)
	sql := `-- +ptah check name="x" assert="SELECT count(*) = 0 FROM users"` + "\n" +
		"-- +ptah no_transaction\nDROP TABLE users;\n"
	directives := migrationfile.ParseDirectives(sql)
	c.Assert(directives, qt.DeepEquals, map[string]string{"no_transaction": "true"})
}

// TestCheckDirective_RoundTrip writes each check as a directive and reads it back: the
// line [migrator.ParseChecks] parses is the check that wrote it, with a double quote in
// a name or an assertion doubled and the defaults a directive may omit filled in.
func TestCheckDirective_RoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		check migrator.Check
		line  string
		phase migrator.CheckPhase
	}{
		{
			name:  "an ordinary check",
			check: migrator.Check{Name: "users_empty", Assert: "SELECT count(*) = 0 FROM users"},
			line:  `-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users"`,
			phase: migrator.CheckPhaseBefore,
		},
		{
			name: "a rounded-values check with quoted names",
			check: migrator.Check{
				Kind: migrator.CheckKindRoundedValues, Name: `public."odd ""t""".c`,
				Assert: `SELECT CASE WHEN EXISTS (SELECT 1 FROM "public"."odd ""t""" WHERE "c" = 1) THEN 0 ELSE 1 END`,
			},
			line: `-- +ptah check kind=rounded-values name="public.""odd """"t"""""".c" ` +
				`assert="SELECT CASE WHEN EXISTS (SELECT 1 FROM ""public"".""odd """"t"""""" WHERE ""c"" = 1) THEN 0 ELSE 1 END"`,
			phase: migrator.CheckPhaseBefore,
		},
		{
			name:  "a check after the body",
			check: migrator.Check{Name: "seeded", Assert: "SELECT count(*) > 0 FROM t", Phase: migrator.CheckPhaseAfter},
			line:  `-- +ptah check name="seeded" phase=after assert="SELECT count(*) > 0 FROM t"`,
			phase: migrator.CheckPhaseAfter,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			line, err := test.check.Directive()
			c.Assert(err, qt.IsNil)
			c.Assert(line, qt.Equals, test.line)

			parsed, err := migrator.ParseChecks(line+"\nSELECT 1;\n", "postgres")
			c.Assert(err, qt.IsNil)
			c.Assert(parsed, qt.HasLen, 1)
			c.Assert(parsed[0].Kind, qt.Equals, test.check.Kind)
			c.Assert(parsed[0].Name, qt.Equals, test.check.Name)
			c.Assert(parsed[0].Assert, qt.Equals, test.check.Assert)
			c.Assert(parsed[0].Phase, qt.Equals, test.phase)
		})
	}
}

// TestCheckDirective_FailurePath refuses a check that one line cannot carry: a line break
// in the name or the assertion would end the directive and leave the rest as SQL.
func TestCheckDirective_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		check migrator.Check
	}{
		{"a line break in the assert", migrator.Check{Name: "x", Assert: "SELECT 1\nDROP TABLE t"}},
		{"a carriage return in the name", migrator.Check{Name: "x\ry", Assert: "SELECT 1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			line, err := test.check.Directive()

			c.Assert(err, qt.ErrorMatches, `a \+ptah check directive is one line, and its name or assert holds a line break`)
			c.Assert(line, qt.Equals, "")
		})
	}
}
