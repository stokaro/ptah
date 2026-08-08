package reservedrole_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/reservedrole"
)

// TestIsRecognizesBothSpellingsTheServerRefuses covers the two names that fail
// for different reasons. A check that knew only one of them would let the other
// through, which is the half stokaro/ptah#1312 names.
//
// The negative rows are the ones stokaro/ptah#1291 had to buy back: pgbouncer,
// pgadmin and pgpool are ordinary user roles, and a test that treated the
// underscore as a wildcard would refuse all three.
func TestIsRecognizesBothSpellingsTheServerRefuses(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		role string
		want bool
	}{
		{name: "the reserved prefix itself", role: "pg_", want: true},
		{name: "a reserved system role", role: "pg_monitor", want: true},
		{name: "a name nobody has used yet under the prefix", role: "pg_whatever", want: true},
		{name: "the bootstrap superuser", role: "postgres", want: true},
		{name: "pgbouncer is an ordinary role", role: "pgbouncer", want: false},
		{name: "pgadmin is an ordinary role", role: "pgadmin", want: false},
		{name: "pgpool is an ordinary role", role: "pgpool", want: false},
		{name: "the prefix uppercased is not reserved", role: "PG_upper", want: false},
		{name: "the prefix in the middle is not reserved", role: "app_pg_user", want: false},
		{name: "a name the superuser shares a stem with", role: "postgres_admin", want: false},
		{name: "an ordinary application role", role: "app_user", want: false},
		{name: "the empty name", role: "", want: false},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(reservedrole.Is(test.role), qt.Equals, test.want)
		})
	}
}

// TestExcludeSQLEscapesTheUnderscore pins the rendered fragment byte for byte.
//
// This is the half the PostgreSQL reader embeds, and the reason the fragment is
// rendered rather than written out at each read site: an unescaped underscore
// is a single-character wildcard, so 'pg_%' matches pgbouncer and drops it from
// both role reads at once (stokaro/ptah#1291). Pinning the text is what keeps
// the SQL exclusion and [reservedrole.Is] from drifting apart.
func TestExcludeSQLEscapesTheUnderscore(t *testing.T) {
	c := qt.New(t)

	got := reservedrole.ExcludeSQL("r.rolname")

	c.Assert(got, qt.Equals, `r.rolname NOT LIKE 'pg\_%' ESCAPE '\' AND r.rolname != 'postgres'`)
	c.Assert(got, qt.Not(qt.Contains), `NOT LIKE 'pg_%'`)
}

// TestValidateDeclaredHappyPath is the control the refusal has to leave alone.
// A schema whose roles are ordinary is planned exactly as before, and so is one
// declaring a reserved name for a target whose reader never excluded it.
func TestValidateDeclaredHappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		roles   []goschema.Role
	}{
		{
			name:    "an ordinary role on postgres",
			dialect: "postgres",
			roles:   []goschema.Role{{Name: "app_user", Login: true}},
		},
		{
			name:    "roles whose names merely start like the reserved ones",
			dialect: "postgres",
			roles:   []goschema.Role{{Name: "pgbouncer"}, {Name: "postgres_admin"}},
		},
		{
			name:    "no roles at all",
			dialect: "postgres",
			roles:   nil,
		},
		{
			name:    "a reserved name on mysql, whose reader excludes nothing",
			dialect: "mysql",
			roles:   []goschema.Role{{Name: "pg_monitor"}},
		},
		{
			name:    "a reserved name with no dialect resolved",
			dialect: "",
			roles:   []goschema.Role{{Name: "postgres"}},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(reservedrole.ValidateDeclared(test.dialect, test.roles), qt.IsNil)
		})
	}
}

// TestValidateDeclaredFailurePath is the refusal itself, on every
// PostgreSQL-family dialect whose reader excludes these names from both of its
// role reads.
func TestValidateDeclaredFailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		roles   []goschema.Role
		wantErr string
	}{
		{
			name:    "the reserved prefix on postgres",
			dialect: "postgres",
			roles:   []goschema.Role{{Name: "pg_monitor"}},
			wantErr: `.*declares reserved PostgreSQL role "pg_monitor" ` +
				`\(PostgreSQL reserves the "pg_" prefix for system roles and refuses ` +
				`CREATE ROLE at SQLSTATE 42939\).*`,
		},
		{
			name:    "the bootstrap superuser on postgres",
			dialect: "postgres",
			roles:   []goschema.Role{{Name: "postgres"}},
			wantErr: `.*declares reserved PostgreSQL role "postgres" ` +
				`\(the bootstrap superuser is not a role Ptah manages, and CREATE ROLE ` +
				`for a role that already exists fails at SQLSTATE 42710\).*`,
		},
		{
			name:    "both spellings are named at once",
			dialect: "postgres",
			roles: []goschema.Role{
				{Name: "app_user"},
				{Name: "pg_monitor"},
				{Name: "postgres"},
			},
			wantErr: `.*declares reserved PostgreSQL roles "pg_monitor" \(.*\), "postgres" \(.*\).*`,
		},
		{
			name:    "cockroachdb reads the same catalog",
			dialect: "cockroachdb",
			roles:   []goschema.Role{{Name: "pg_monitor"}},
			wantErr: `.*declares reserved PostgreSQL role "pg_monitor".*`,
		},
		{
			name:    "yugabytedb reads the same catalog",
			dialect: "yugabytedb",
			roles:   []goschema.Role{{Name: "postgres"}},
			wantErr: `.*declares reserved PostgreSQL role "postgres".*`,
		},
		{
			name:    "the alias spelling resolves to the same dialect",
			dialect: "postgresql",
			roles:   []goschema.Role{{Name: "pg_monitor"}},
			wantErr: `.*declares reserved PostgreSQL role "pg_monitor".*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := reservedrole.ValidateDeclared(test.dialect, test.roles)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err.Error(), qt.Contains, reservedrole.AllowEnvVar)
		})
	}
}

// TestValidateDeclaredOptInRestoresTheOlderBehavior is the capability half of
// AGENTS.md: the refusal removes something a user could do, so the fuller
// behavior stays reachable on the same surface behind a PTAH_ variable rather
// than being deleted.
//
// Measured on PostgreSQL 17.10: on a cluster bootstrapped as "admin" rather
// than "postgres", CREATE ROLE "postgres" succeeds and the role appears in
// pg_roles. Refusing it unconditionally would take that away.
func TestValidateDeclaredOptInRestoresTheOlderBehavior(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		value string
	}{
		{name: "1", value: "1"},
		{name: "true", value: "true"},
		{name: "TRUE", value: "TRUE"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Setenv(reservedrole.AllowEnvVar, test.value)

			err := reservedrole.ValidateDeclared("postgres", []goschema.Role{{Name: "postgres"}})

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestValidateDeclaredOptInKeepsTheRefusalForEveryOtherValue mirrors how the
// other PTAH_ opt-ins read themselves: unset, empty, false and unparsable all
// keep the default.
func TestValidateDeclaredOptInKeepsTheRefusalForEveryOtherValue(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "zero", value: "0"},
		{name: "false", value: "false"},
		{name: "unparsable", value: "yes please"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Setenv(reservedrole.AllowEnvVar, test.value)

			err := reservedrole.ValidateDeclared("postgres", []goschema.Role{{Name: "postgres"}})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		})
	}
}
