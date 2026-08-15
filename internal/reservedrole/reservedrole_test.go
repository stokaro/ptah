package reservedrole_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(reservedrole.ValidateDeclared(test.dialect, test.roles), qt.IsNil)
		})
	}
}

// TestValidateDeclaredFailurePath is the refusal itself, on every
// PostgreSQL-family dialect whose reader excludes these names from both of its
// role reads.
func TestValidateDeclaredFailurePath(t *testing.T) {

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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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

	tests := []struct {
		name  string
		value string
	}{
		{name: "1", value: "1"},
		{name: "true", value: "true"},
		{name: "TRUE", value: "TRUE"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv(reservedrole.AllowEnvVar, test.value)

			err := reservedrole.ValidateDeclared("postgres", []goschema.Role{{Name: "postgres"}})

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestValidateDeclaredOptInKeepsTheRefusalForAValidFalse mirrors how the other
// PTAH_ opt-ins read themselves: absence and a valid false keep the default.
func TestValidateDeclaredOptInKeepsTheRefusalForAValidFalse(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		env  func(testing.TB)
	}{
		{name: "unset", env: envbooltest.Unset(reservedrole.AllowEnvVar)},
		{name: "zero", env: envbooltest.Set(reservedrole.AllowEnvVar, "0")},
		{name: "false", env: envbooltest.Set(reservedrole.AllowEnvVar, "false")},
		{name: "FALSE", env: envbooltest.Set(reservedrole.AllowEnvVar, "FALSE")},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			test.env(c)

			err := reservedrole.ValidateDeclared("postgres", []goschema.Role{{Name: "postgres"}})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		})
	}
}

// TestValidateDeclaredRefusesAMalformedOptIn is the state split stokaro/ptah#1334
// introduced: a value that is neither absent nor a boolean is a configuration
// error, and the refusal it produces is NOT the schema-diff refusal.
//
// The two are asserted apart on purpose. Before this, `yes please` produced the
// reserved-role refusal, which reads to the operator as "the opt-in did not
// apply to this role" rather than "the opt-in was never read".
func TestValidateDeclaredRefusesAMalformedOptIn(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name        string
		env         func(testing.TB)
		wantMessage string
	}{
		{
			name:        "unparsable",
			env:         envbooltest.Set(reservedrole.AllowEnvVar, "yes please"),
			wantMessage: `invalid boolean value "yes please" for PTAH_ALLOW_RESERVED_ROLE_NAMES`,
		},
		{
			name:        "an exported empty value",
			env:         envbooltest.Set(reservedrole.AllowEnvVar, ""),
			wantMessage: `invalid boolean value "" for PTAH_ALLOW_RESERVED_ROLE_NAMES`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			test.env(c)

			err := reservedrole.ValidateDeclared("postgres", []goschema.Role{{Name: "postgres"}})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.wantMessage)
			c.Assert(err, qt.Not(qt.ErrorIs), ptaherr.ErrInvalidSchemaDiff)
		})
	}
}

// TestValidateDeclaredRefusesAMalformedOptInWithNoReservedRole is the
// discriminating case for "validate before control-flow shortcuts".
//
// A desired schema declaring no reserved role at all is the whole of a healthy
// pipeline, and it is the only shape most runs ever have. Resolving the variable
// beside the refusal left a typo dormant on every one of them and surfaced it
// only on the run the operator had already set the variable to change, which is
// the run they would have noticed anyway.
func TestValidateDeclaredRefusesAMalformedOptInWithNoReservedRole(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(reservedrole.AllowEnvVar, "yes please")(t)

	err := reservedrole.ValidateDeclared("postgres", []goschema.Role{{Name: "app_user"}})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, `invalid boolean value "yes please" for PTAH_ALLOW_RESERVED_ROLE_NAMES`)
}

// TestValidateDeclaredLeavesAnUnrelatedDialectAlone is the other side of the
// same boundary, and it is the half that keeps the rule from becoming a global
// environment validation.
//
// A MySQL render declares nothing about PostgreSQL role names, so this subsystem
// does not recognize the variable on that invocation and must not fail for it.
func TestValidateDeclaredLeavesAnUnrelatedDialectAlone(t *testing.T) {
	envbooltest.Set(reservedrole.AllowEnvVar, "yes please")(t)

	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
		{name: "sqlite", dialect: "sqlite"},
		{name: "an empty dialect", dialect: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := reservedrole.ValidateDeclared(test.dialect, []goschema.Role{{Name: "postgres"}})

			c.Assert(err, qt.IsNil)
		})
	}
}
