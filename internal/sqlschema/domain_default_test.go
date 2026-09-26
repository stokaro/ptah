package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlschema"
)

// A domain's DEFAULT reaches the model the way the model keeps it: a string
// constant as the text PostgreSQL 18.6 stores for it, whichever spelling the
// file used, and anything else as the SQL the file wrote. The text is what the
// planner quotes, so a literal kept with its own quotes is quoted twice and the
// plan sets the default to the quotes and all (stokaro/ptah#3740). DEFAULT NULL
// is no default, which is what the server stores for it.
func TestRead_DomainDefault_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		domain      string
		wantDefault string
		wantExpr    string
	}{
		{name: "a string", domain: "CREATE DOMAIN dm AS text DEFAULT 'ab';", wantDefault: "ab"},
		{name: "a doubled quote", domain: "CREATE DOMAIN dm AS text DEFAULT 'it''s';", wantDefault: "it's"},
		{name: "a backslash", domain: `CREATE DOMAIN dm AS text DEFAULT 'a\b';`, wantDefault: `a\b`},
		{name: "a continued string", domain: "CREATE DOMAIN dm AS text DEFAULT 'a'\n'b';", wantDefault: "ab"},
		{name: "a dollar-quoted string", domain: "CREATE DOMAIN dm AS text DEFAULT $$ab$$;", wantDefault: "ab"},
		{name: "an escape string", domain: `CREATE DOMAIN dm AS text DEFAULT E'a\x62';`, wantDefault: "ab"},
		{name: "a Unicode escape string", domain: `CREATE DOMAIN dm AS text DEFAULT U&'\0061b';`, wantDefault: "ab"},
		{name: "a string on a varchar domain", domain: "CREATE DOMAIN dm AS varchar(10) DEFAULT 'ab';", wantDefault: "ab"},
		{name: "a string with a cast", domain: "CREATE DOMAIN dm AS text DEFAULT 'ab'::text;", wantExpr: "'ab'::text"},
		{name: "a number", domain: "CREATE DOMAIN dm AS int DEFAULT 42;", wantExpr: "42"},
		{name: "a negative number", domain: "CREATE DOMAIN dm AS int DEFAULT -1;", wantExpr: "-1"},
		{name: "a boolean", domain: "CREATE DOMAIN dm AS boolean DEFAULT true;", wantExpr: "true"},
		{name: "a call", domain: "CREATE DOMAIN dm AS timestamptz DEFAULT now();", wantExpr: "now()"},
		// A double-quoted name is a column reference, which PostgreSQL refuses
		// in a DEFAULT. It stays SQL, so the server says so, rather than
		// becoming the string 'x'.
		{name: "a quoted identifier", domain: `CREATE DOMAIN dm AS text DEFAULT "x";`, wantExpr: `"x"`},
		{name: "NULL", domain: "CREATE DOMAIN dm AS text DEFAULT NULL;"},
		{name: "no default", domain: "CREATE DOMAIN dm AS text;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.domain), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(database.Domains, qt.HasLen, 1)
			c.Assert(database.Domains[0].Default, qt.Equals, test.wantDefault)
			c.Assert(database.Domains[0].DefaultExpr, qt.Equals, test.wantExpr)
		})
	}
}

// A domain DEFAULT the server refuses is refused here too, rather than kept as
// some other text.
func TestRead_DomainDefault_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		domain  string
		wantErr string
	}{
		{
			name:    "an escape naming no character",
			domain:  `CREATE DOMAIN dm AS text DEFAULT U&'\0000';`,
			wantErr: `expected domain default value: U&'\\0000' is not a string constant the server reads, for the default value at position \d+`,
		},
		{
			name:    "two strings on one line",
			domain:  "CREATE DOMAIN dm AS text DEFAULT 'a' 'b';",
			wantErr: `expected SQL keyword, got String at position \d+`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.domain), "postgres")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Domains, qt.HasLen, 0)
		})
	}
}
