package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// qualifierDocument renders a table in schema "app" carrying the given lines.
func qualifierDocument(lines string) []byte {
	return []byte(`
schema "app" {}

table "users" {
  schema = schema.app
  ` + lines + `
  column "id" {
    type = int
  }
}
`)
}

// TestParseTableQualifierNamingItsSchema_HappyPath pins that a qualifier
// repeating the table's schema is accepted.
//
// The attribute disambiguates two tables that share a name, and Ptah addresses
// a table by its schema: `table.app.users` already resolves to `app.users`, and
// the dependency map, the comparator and every renderer key on that qualified
// name. A qualifier naming that same schema says what `schema` says, so the
// document it appears in has no reason to fail (stokaro/ptah#3113).
func TestParseTableQualifierNamingItsSchema_HappyPath(t *testing.T) {
	rows := []struct {
		name  string
		lines string
	}{
		{name: "stated", lines: `qualifier = "app"`},
		{name: "absent", lines: ``},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(qualifierDocument(row.lines), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 1)
			c.Assert(db.Tables[0].QualifiedName(), qt.Equals, "app.users")
			sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
			c.Assert(sql, qt.Contains, `CREATE TABLE "app"."users"`)
		})
	}
}

// TestParseTableQualifier_FailurePath pins that a qualifier asking for a second
// address is refused rather than dropped.
//
// Nothing in the model can express one: a table reached under a name Ptah never
// uses is a table the plan would create twice.
func TestParseTableQualifier_FailurePath(t *testing.T) {
	rows := []struct {
		name  string
		lines string
		want  string
	}{
		{
			name:  "a qualifier that is not the schema",
			lines: `qualifier = "other"`,
			want:  `table "users" qualifier "other" does not name its schema "app"`,
		},
		{
			name:  "an empty qualifier",
			lines: `qualifier = ""`,
			want:  `table "users" qualifier names nothing`,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(qualifierDocument(row.lines), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, `(?s).*`+row.want+`.*`)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseTableQualifiedReferenceAlreadyAddressesASchema_HappyPath is the
// measurement behind the decision above.
//
// Two tables share a name in different schemas, and a reference from one of
// them reaches the other by naming its schema. That is the capability the
// qualifier exists to provide, and it is already here, which is why the
// attribute adds an address rather than a capability.
func TestParseTableQualifiedReferenceAlreadyAddressesASchema_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "a" {}
schema "b" {}

table "users" {
  schema = schema.a
  column "id" { type = int }
  primary_key {
    columns = [column.id]
  }
}

table "users" {
  schema = schema.b
  column "id" { type = int }
  primary_key {
    columns = [column.id]
  }
}

table "orders" {
  schema = schema.b
  column "id" { type = int }
  column "owner" { type = int }
  foreign_key "fk" {
    columns     = [column.owner]
    ref_columns = [table.a.users.column.id]
  }
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
	c.Assert(sql, qt.Contains, `REFERENCES "a"."users"("id")`)
}
