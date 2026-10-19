package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

// A DO block of a dialect whose descriptor names no DO-block language is
// refused with the dialects that read one, as the registry states them, and
// a block written in another language than the dialect's is refused with the
// name of the dialect's language, as its reader gives it.
func TestRoleBootstrap_RefusalText(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		dialect string
		wantErr string
	}{
		{
			name:    "a dialect whose grammar has DO blocks and whose descriptor names no language",
			sql:     `DO $$ BEGIN CREATE ROLE reader; END $$;`,
			dialect: "cockroachdb",
			wantErr: `the schema model has no place for this statement: procedural role declarations require one of the dialects \["postgres"\]`,
		},
		{
			name:    "another language",
			sql:     `DO $$ BEGIN NULL; END $$ LANGUAGE sql;`,
			dialect: "postgres",
			wantErr: `the schema model has no place for this statement: desired-state DO blocks require the supported PL/pgSQL role-bootstrap form`,
		},
		{
			name:    "a refusal of the language's reader",
			sql:     `DO $$ BEGIN PERFORM 1; END $$;`,
			dialect: "postgres",
			wantErr: `the schema model has no place for this statement: desired-state DO block at body byte 7: ` +
				`only CREATE ROLE, NULL, BEGIN, and supported IF conditions are allowed; ` +
				`use supported role-bootstrap declarations or an explicit migration`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, _, err := sqlschema.Read([]byte(test.sql), test.dialect)
			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// A conversion with no dialect reads a DO block as the first dialect whose
// language has a reader, PostgreSQL, which folds an unquoted role name.
func TestRoleBootstrap_NoDialect(t *testing.T) {
	c := qt.New(t)
	statements, err := parser.NewParser(`DO $$ BEGIN CREATE ROLE Reader LOGIN; END $$;`, parser.WithDialect("postgres")).Parse()
	c.Assert(err, qt.IsNil)
	db, err := sqlschema.ToDatabase(statements, "")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Roles, qt.DeepEquals, []schemamodel.Role{{Name: "reader", Login: true, Inherit: true}})
}
