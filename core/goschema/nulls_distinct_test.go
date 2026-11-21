package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
)

// nullsDistinctSource declares a unique constraint and a unique index, each
// carrying attribute after its other attributes.
func nullsDistinctSource(attribute string) string {
	return `package test

//ptah:schema:table name="users"
//ptah:schema:constraint name="users_email_uq" type="UNIQUE" columns="email" ` + attribute + `
type User struct {
	//ptah:schema:field name="email" type="TEXT"
	//ptah:schema:index name="users_email_idx" fields="email" unique ` + attribute + `
	Email string
}
`
}

// TestParseSource_NullsDistinct_HappyPath reads nulls_distinct on a constraint
// and on an index. A bare word reads as true, the way primary and not_null
// read, and an absent attribute leaves the engine's default.
func TestParseSource_NullsDistinct_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		attribute string
		want      *bool
	}{
		{name: "bare", attribute: "nulls_distinct", want: new(true)},
		{name: "true", attribute: `nulls_distinct="true"`, want: new(true)},
		{name: "false", attribute: `nulls_distinct="false"`, want: new(false)},
		{name: "absent", attribute: "", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db := mustParseSource(c, "users.go", nullsDistinctSource(test.attribute))

			c.Assert(db.Constraints, qt.HasLen, 1)
			c.Assert(db.Constraints[0].NullsDistinct, qt.DeepEquals, test.want)
			c.Assert(db.Indexes, qt.HasLen, 1)
			c.Assert(db.Indexes[0].NullsDistinct, qt.DeepEquals, test.want)
		})
	}
}

// TestParseSource_NullsDistinct_FailurePath refuses a value that is not exactly
// true or false. Read as false, it would declare NULLS NOT DISTINCT, which
// refuses rows its author meant to allow.
func TestParseSource_NullsDistinct_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   string
	}{
		{
			name: "a constraint value in another case",
			source: "package test\n\n" +
				`//ptah:schema:constraint name="u" type="UNIQUE" columns="email" nulls_distinct="TRUE"` + "\ntype T struct{}\n",
			want: `(?s).*invalid "nulls_distinct" value "TRUE" on //ptah:schema:\w+ at \w+: not a boolean; write true or false.*`,
		},
		{
			name: "an empty constraint value",
			source: "package test\n\n" +
				`//ptah:schema:constraint name="u" type="UNIQUE" columns="email" nulls_distinct=""` + "\ntype T struct{}\n",
			want: `(?s).*invalid "nulls_distinct" value "" on //ptah:schema:\w+ at \w+: not a boolean; write true or false.*`,
		},
		{
			name: "an index value that is not a boolean",
			source: "package test\n\n//ptah:schema:table name=\"users\"\ntype User struct {\n" +
				"\t//ptah:schema:field name=\"email\" type=\"TEXT\"\n" +
				"\t//ptah:schema:index name=\"users_email_idx\" fields=\"email\" unique nulls_distinct=\"no\"\n" +
				"\tEmail string\n}\n",
			want: `(?s).*invalid "nulls_distinct" value "no" on //ptah:schema:\w+ at \w+: not a boolean; write true or false.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := goschema.ParseSource(rowSecurityOwners, "bad.go", test.source)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
