package sqlschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlext"
	"ptah.run/internal/sqlschema"
)

const keyDeclaredByAnExport = `CREATE TABLE owners (id bigint PRIMARY KEY);
CREATE TABLE items (id bigint PRIMARY KEY, owner_id bigint,
  CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id));
`

// redeclarations reads sql as PostgreSQL and returns what it reports as
// redeclared.
func redeclarations(c *qt.C, sql string) []sqlschema.Redeclaration {
	c.Helper()
	var reported []sqlschema.Redeclaration
	_, _, err := sqlschema.ReadWith(context.Background(), []byte(sql), platform.Postgres, nil, sqlschema.Options{
		Owners:     sqlext.None(),
		Redeclared: func(r sqlschema.Redeclaration) { reported = append(reported, r) },
	})
	c.Assert(err, qt.IsNil)
	return reported
}

// A key dropped and added again with another definition is reported, with
// what changed; the reading is the later statement's either way.
func TestRedeclared_ReportsAKeyAddedAgainDifferently(t *testing.T) {
	tests := []struct {
		name    string
		added   string
		changed []string
	}{
		{"only NOT VALID", "REFERENCES owners (id) NOT VALID", []string{"not valid"}},
		{"another action", "REFERENCES owners (id) ON DELETE CASCADE", []string{"on delete"}},
		{"another action and NOT VALID", "REFERENCES owners (id) ON DELETE CASCADE NOT VALID", []string{"on delete", "not valid"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reported := redeclarations(c, keyDeclaredByAnExport+
				"ALTER TABLE items DROP CONSTRAINT items_owner_fk;\n"+
				"ALTER TABLE items ADD CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) "+test.added+";\n")

			c.Assert(reported, qt.DeepEquals, []sqlschema.Redeclaration{
				{Kind: "foreign key", Table: "items", Name: "items_owner_fk", Changed: test.changed},
			})
		})
	}
}

// The same definition again, in any spelling that means the same, and a key
// added under a name nothing dropped, are not redeclarations of another
// definition.
func TestRedeclared_IgnoresTheSameDefinition(t *testing.T) {
	tests := []struct{ name, sql string }{
		{"the same words", "ALTER TABLE items DROP CONSTRAINT items_owner_fk;\n" +
			"ALTER TABLE items ADD CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id);\n"},
		{"the default action written out", "ALTER TABLE items DROP CONSTRAINT items_owner_fk, " +
			"ADD CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id) MATCH SIMPLE ON DELETE NO ACTION;\n"},
		{"nothing dropped", "ALTER TABLE items ADD CONSTRAINT items_other_fk FOREIGN KEY (owner_id) REFERENCES owners (id) NOT VALID;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(redeclarations(c, keyDeclaredByAnExport+test.sql), qt.HasLen, 0)
		})
	}
}

func TestRedeclaration_StringPointsAtTheDirectiveForNotValidAlone(t *testing.T) {
	c := qt.New(t)

	c.Assert(sqlschema.Redeclaration{Kind: "foreign key", Table: "items", Name: "items_owner_fk", Changed: []string{"not valid"}}.String(), qt.Equals,
		`foreign key "items_owner_fk" on table "items" is dropped and added again only to allow NOT VALID; `+
			`-- ptah:allow-not-valid CONSTRAINT "items_owner_fk" ON "items" allows it without declaring the key a second time`)
	c.Assert(sqlschema.Redeclaration{Kind: "foreign key", Table: "items", Name: "items_owner_fk", Changed: []string{"on delete", "not valid"}}.String(), qt.Equals,
		`foreign key "items_owner_fk" on table "items" is dropped and added again with another on delete, not valid; the later declaration decides it`)
}
