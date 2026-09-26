//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// A `--dev-url` made of spaces is a value, not an absent flag. The pinned
// community binary v1.3.0 opens it and answers the missing driver, measured on
// PostgreSQL 18 on 2026-09-26, including on the argv that opens no dev database
// when the flag is absent: `schema diff` between two databases and
// `schema apply` to a database. Treated as absent, both run to completion here
// (stokaro/ptah#3680).

// blankDevURLMissingDriver is the pinned binary's whole output for each row.
const blankDevURLMissingDriver = "Error: sql/sqlclient: missing driver. See: https://atlasgo.io/url\n"

func TestCompatBlankDevURLE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		args func(db string) []string
	}{
		{
			name: "schema diff between two databases",
			args: func(db string) []string {
				return []string{"schema", "diff", "--from", db, "--to", db, "--dev-url", " "}
			},
		},
		{
			name: "schema apply to a database",
			args: func(db string) []string {
				return []string{"schema", "apply", "--url", db, "--to", db, "--dev-url", " ", "--auto-approve"}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			db := databaseBuiltFrom(c, "CREATE TABLE widgets (id bigint PRIMARY KEY);")

			out, err := runCompatVerb(tt.args(db)...)

			c.Assert(err, qt.ErrorMatches, `sql/sqlclient: missing driver\. See: https://atlasgo\.io/url`)
			c.Assert(out, qt.Equals, blankDevURLMissingDriver)
		})
	}
}

// TestCompatBlankDevURLE2E_HappyPath is the control: `schema inspect` of a
// database never opens the dev database, on either binary, so the blank value
// is not judged there and the schema is printed.
func TestCompatBlankDevURLE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := databaseBuiltFrom(c, "CREATE TABLE widgets (id bigint PRIMARY KEY);")

	out, err := runCompatVerb("schema", "inspect", "--url", db, "--dev-url", " ")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `table "widgets"`)
}
