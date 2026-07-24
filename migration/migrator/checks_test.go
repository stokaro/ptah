package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/migrator"
)

func TestParseChecks(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    []migrator.Check
		wantErr bool
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
		{
			name:    "missing assert is an error",
			sql:     `-- +ptah check name="x"` + "\nSELECT 1;\n",
			wantErr: true,
		},
		{
			name:    "unknown key is an error",
			sql:     `-- +ptah check name="x" assert="SELECT 1" bogus=1` + "\nSELECT 1;\n",
			wantErr: true,
		},
		{
			name:    "unsupported on_fail is an error",
			sql:     `-- +ptah check name="x" assert="SELECT 1" on_fail=warn` + "\nSELECT 1;\n",
			wantErr: true,
		},
		{
			name:    "unterminated quote is an error",
			sql:     `-- +ptah check name="x" assert="SELECT 1` + "\nSELECT 1;\n",
			wantErr: true,
		},
		{
			name:    "multi-statement assert is an error",
			sql:     `-- +ptah check name="x" assert="SELECT 1; DROP TABLE t"` + "\nSELECT 1;\n",
			wantErr: true,
		},
		{
			name:    "duplicate key is an error",
			sql:     `-- +ptah check name="x" name="y" assert="SELECT 1"` + "\nSELECT 1;\n",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := migrator.ParseChecks(tt.sql)
			c.Assert(err != nil, qt.Equals, tt.wantErr, qt.Commentf("err=%v", err))
			c.Assert(got, qt.DeepEquals, tt.want)
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
