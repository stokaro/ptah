package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
)

// TestParseSynonymAndExtendedProperty reads the two blocks stokaro/ptah#1031
// added, in the four addresses SQL Server has for a property: database, schema,
// table and column.
//
// The owner is read as plain strings rather than as block references, matching
// what the render writes and what sp_addextendedproperty carries -- the
// statement passes `@level1name = N'users'`, a name, and a reference would
// resolve to nothing once selection removed the table.
func TestParseSynonymAndExtendedProperty(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "dbo" {
}

synonym "s_users" {
  schema  = schema.dbo
  target  = "other.dbo.users"
  comment = "the remote users"
}

extended_property "ptah_flag" {
  value = "database scope"
}

extended_property "schema_note" {
  schema = schema.dbo
  value  = "schema scope"
}

extended_property "MS_Description" {
  schema = schema.dbo
  table  = "users"
  column = "title"
  value  = "the title"
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Synonyms, qt.DeepEquals, []schemamodel.Synonym{{
		Name: "s_users", Schema: "dbo", Target: "other.dbo.users",
		Comment: "the remote users",
	}})
	c.Assert(db.ExtendedProperties, qt.DeepEquals, []schemamodel.ExtendedProperty{
		{Name: "ptah_flag", Value: "database scope"},
		{Name: "schema_note", Schema: "dbo", Value: "schema scope"},
		{
			Name: "MS_Description", Schema: "dbo", Table: "users",
			Column: "title", Value: "the title",
		},
	})
}

// TestParseSynonymAcceptsTheSchemaLabel pins the two-label spelling every other
// schema-scoped block accepts, so the synonym is not the one that needs the
// attribute.
func TestParseSynonymAcceptsTheSchemaLabel(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
synonym "reporting" "s_users" {
  target = "other.dbo.users"
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Synonyms, qt.DeepEquals, []schemamodel.Synonym{{
		Name: "s_users", Schema: "reporting", Target: "other.dbo.users",
	}})
}

// TestParseRefusesAnAddressThatNamesNothing pins the refusals, and pins them
// where the SQL Server renderer already makes them.
//
// Each level of an extended property's address is passed BY NAME, so a level
// whose parent is missing addresses nothing: `@level1name = N'users'` with no
// `@level0name` is a table in no schema. The renderer refuses both, and
// refusing here as well means the message names the block rather than arriving
// once a plan is being written.
func TestParseRefusesAnAddressThatNamesNothing(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{
			name: "a table with no schema",
			document: `
extended_property "MS_Description" {
  table = "users"
  value = "x"
}
`,
			want: "names table \"users\" and no schema",
		},
		{
			name: "a column with no table",
			document: `
extended_property "MS_Description" {
  schema = schema.dbo
  column = "title"
  value  = "x"
}
`,
			want: "names column \"title\" and no table",
		},
		{
			name: "a synonym that stands for nothing",
			document: `
synonym "s_users" {
  schema = schema.dbo
}
`,
			want: "requires a target",
		},
		{
			name: "an attribute neither block has",
			document: `
synonym "s_users" {
  target  = "other.dbo.users"
  charset = "utf8"
}
`,
			want: "charset",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlashcl.Parse([]byte(test.document), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, ".*"+test.want+".*")
		})
	}
}
