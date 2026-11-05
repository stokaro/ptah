package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// The rendered schema names each declared password by the variable its role
// reads it from, and holds no password or hash anywhere; ADR 0071. YDB, the
// other engine that sets a password, renders through its provider.
func TestGetOrderedCreateStatementsNamesAPasswordByItsVariable_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		role    schemamodel.Role
		want    string
	}{
		{name: "a PostgreSQL role", dialect: "postgres", role: schemamodel.Role{Name: "app_user", Login: true, Password: "pw-SENTINEL"},
			want: `CREATE ROLE "app_user" WITH LOGIN PASSWORD $PTAH_SECRET_PASSWORD_APP_USER`},
		{name: "a PostgreSQL role with a SCRAM hash", dialect: "postgres", role: schemamodel.Role{Name: "ops", Password: "SCRAM-SHA-256$4096:SENTINEL"}, // #nosec G101 -- a made-up value, not a credential
			want: `CREATE ROLE "ops" WITH NOLOGIN PASSWORD $PTAH_SECRET_PASSWORD_OPS`},
		{name: "a PostgreSQL role whose password names its variable", dialect: "postgres",
			role: schemamodel.Role{Name: "app", Login: true, Password: "$PTAH_SECRET_APP_PW"}, // #nosec G101 -- a variable name, not a credential
			want: `CREATE ROLE "app" WITH LOGIN PASSWORD $PTAH_SECRET_APP_PW`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(&schemamodel.Database{Roles: []schemamodel.Role{test.role}}, test.dialect)

			c.Assert(err, qt.IsNil)
			rendered := strings.Join(statements, "\n")
			c.Assert(rendered, qt.Contains, test.want)
			c.Assert(rendered, qt.Not(qt.Contains), "SENTINEL")
			c.Assert(rendered, qt.Not(qt.Contains), "WARNING")
		})
	}
}

// Two roles whose names give one variable would be set to one password
// wherever the statements run, so the schema is refused before any statement
// exists; the refusal names the roles and never a password.
func TestValidateSchemaRefusesRolesThatShareAPasswordVariable_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := builtin.ValidateSchema(&schemamodel.Database{Roles: []schemamodel.Role{
		{Name: "App", Login: true, Password: "one-SENTINEL"},
		{Name: "app", Login: true, Password: "two-SENTINEL"},
	}}, "postgres")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*roles "App" and "app" read their passwords from one environment variable, PTAH_SECRET_PASSWORD_APP; rename one of them.*`)
	c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*SENTINEL.*`)
}
