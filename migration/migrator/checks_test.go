package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/migration/migrator"
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
		OnFail: migrator.OnFailAbort,
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
		OnFail: migrator.OnFailAbort,
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
		OnFail: migrator.OnFailAbort,
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
		OnFail: migrator.OnFailAbort,
	}})
}

func TestParseChecks_PostgresFamilyEscapeStringBeforeDirective(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "CockroachDB", dialect: platform.CockroachDB},
		{name: "YugabyteDB", dialect: platform.YugabyteDB},
		{name: "Spanner", dialect: platform.Spanner},
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
				OnFail: migrator.OnFailAbort,
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
			want: []migrator.Check{{Name: "users_empty", Assert: "SELECT count(*) = 0 FROM users", OnFail: migrator.OnFailAbort}},
		},
		{
			name: "on_fail defaults to abort",
			sql:  `-- +ptah check name="x" assert="SELECT true"` + "\nSELECT 1;\n",
			want: []migrator.Check{{Name: "x", Assert: "SELECT true", OnFail: migrator.OnFailAbort}},
		},
		{
			name: "multiple checks run in file order",
			sql: `-- +ptah check name="a" assert="SELECT 1"` + "\n" +
				`-- +ptah check name="b" assert="SELECT 2"` + "\nDROP TABLE t;\n",
			want: []migrator.Check{
				{Name: "a", Assert: "SELECT 1", OnFail: migrator.OnFailAbort},
				{Name: "b", Assert: "SELECT 2", OnFail: migrator.OnFailAbort},
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
			want: []migrator.Check{{Name: "q", Assert: `SELECT count(*) = 0 FROM "My Table"`, OnFail: migrator.OnFailAbort}},
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
			wantErr: `unknown \+ptah check key "bogus" \(want name, assert, on_fail\)`,
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
	directives := migrator.ParseFileDirectives(sql)
	c.Assert(directives, qt.DeepEquals, map[string]string{"no_transaction": "true"})
}
