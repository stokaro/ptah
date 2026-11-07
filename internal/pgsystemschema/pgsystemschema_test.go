package pgsystemschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/pgsystemschema"
)

// TestPostgresDescribedSchemasPredicate_DropsTheSchemasAnExtensionOwns pins the
// arm and the gate on it.
//
// The extension arm reads pg_depend, so it may only be asked of a catalog that
// has one: a server without it answers `relation "pg_depend" does not exist`
// to the whole statement, which would turn every realm read there into a
// failure rather than a narrower description.
func TestPostgresDescribedSchemasPredicate_DropsTheSchemasAnExtensionOwns(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		caps       capability.Capabilities
		wantDepend bool
		wantCrdb   bool
	}{
		{name: "postgres reads pg_depend", dialect: "postgres", caps: capability.ForDialect("postgres"), wantDepend: true},
		{name: "cockroachdb reads pg_depend", dialect: "cockroachdb", caps: capability.ForDialect("cockroachdb"), wantDepend: true, wantCrdb: true},
		{name: "yugabytedb reads pg_depend", dialect: "yugabytedb", caps: capability.ForDialect("yugabytedb"), wantDepend: true},
		{name: "a catalog without pg_depend", dialect: "postgres",
			caps: capability.ForDialect("postgres").With(capability.CatalogDependencies, false), wantDepend: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			predicate := pgsystemschema.PostgresDescribedSchemasPredicate(test.dialect, test.caps)

			c.Assert(predicate, qt.Contains, "n.nspname <> 'information_schema'")
			c.Assert(strings.Contains(predicate, "d.deptype = 'e'"), qt.Equals, test.wantDepend)
			c.Assert(strings.Contains(predicate, "crdb_internal"), qt.Equals, test.wantCrdb)
		})
	}
}

// TestPostgresDescribedSchemasPredicate_ExtendsRatherThanReplaces holds the two
// predicates together: the described one is the non-system one plus an arm, so
// a system schema cannot be described because the extension arm was added.
func TestPostgresDescribedSchemasPredicate_ExtendsRatherThanReplaces(t *testing.T) {
	c := qt.New(t)
	base := pgsystemschema.PostgresNonSystemSchemasPredicate("postgres")
	described := pgsystemschema.PostgresDescribedSchemasPredicate(
		"postgres", capability.ForDialect("postgres"))

	c.Assert(described, qt.Contains, base)
}

func TestPostgresNonSystemSchemasPredicate_DerivesSystemSchemasFromDialect(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		c := qt.New(t)
		predicate := pgsystemschema.PostgresNonSystemSchemasPredicate("postgres")

		c.Assert(predicate, qt.Contains, "n.nspname <> 'information_schema'")
		c.Assert(predicate, qt.Not(qt.Contains), "crdb_internal")
		c.Assert(predicate, qt.Contains, "n.nspname NOT LIKE 'pg\\_%' ESCAPE '\\'")
	})

	t.Run("cockroachdb", func(t *testing.T) {
		c := qt.New(t)
		predicate := pgsystemschema.PostgresNonSystemSchemasPredicate("cockroachdb")

		c.Assert(predicate, qt.Contains, "n.nspname <> 'information_schema'")
		c.Assert(predicate, qt.Contains, "n.nspname <> 'crdb_internal'")
		c.Assert(predicate, qt.Contains, "n.nspname NOT LIKE 'pg\\_%' ESCAPE '\\'")
	})

	t.Run("any member leaves out every member's catalog", func(t *testing.T) {
		c := qt.New(t)
		predicate := pgsystemschema.PostgresNonSystemSchemasPredicate(pgsystemschema.AnyMember)

		c.Assert(predicate, qt.Equals, "n.nspname <> 'information_schema'\n\t\t\t  AND n.nspname <> 'crdb_internal'"+
			"\n\t\t\t  AND n.nspname NOT LIKE 'pg\\_%' ESCAPE '\\'")
	})
}

func TestIsPostgresSystemSchemaPreservesQuotedIdentifierIdentity(t *testing.T) {
	c := qt.New(t)

	for _, name := range []string{"pg_catalog", "pg_toast", "information_schema"} {
		c.Assert(pgsystemschema.IsPostgresSystemSchema(name), qt.IsTrue, qt.Commentf("name: %q", name))
	}
	for _, name := range []string{"public", "extensions", "PG_CATALOG", " pg_catalog ", " "} {
		c.Assert(pgsystemschema.IsPostgresSystemSchema(name), qt.IsFalse, qt.Commentf("name: %q", name))
	}
}

// TestIsPostgresFamilySystemSchemaAddsTheReservedSchemas covers the schemas a
// dialect's descriptor reserves: crdb_internal on CockroachDB, owned by that
// engine alone, and by a caller that cannot tell which member it talks to.
func TestIsPostgresFamilySystemSchemaAddsTheReservedSchemas(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		schema  string
		want    bool
	}{
		{name: "Cockroach internal", dialect: "cockroachdb", schema: "crdb_internal", want: true},
		{name: "Postgres does not own it", dialect: "postgres", schema: "crdb_internal", want: false},
		{name: "quoted lookalike", dialect: "cockroachdb", schema: "CRDB_INTERNAL", want: false},
		{name: "common catalog", dialect: "cockroachdb", schema: "pg_catalog", want: true},
		{name: "any member owns Cockroach internal", dialect: pgsystemschema.AnyMember, schema: "crdb_internal", want: true},
		{name: "any member leaves public to the user", dialect: pgsystemschema.AnyMember, schema: "public", want: false},
		{name: "any member owns the common catalog", dialect: pgsystemschema.AnyMember, schema: "information_schema", want: true},
		{name: "PostgreSQL leaves a variant of the information schema to the user", dialect: "postgres", schema: "INFORMATION_SCHEMA", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				pgsystemschema.IsPostgresFamilySystemSchema(test.dialect, test.schema),
				qt.Equals,
				test.want,
			)
		})
	}
}

func TestValidateDeclaredPostgresSystemSchemasRefusesServerNamespaces(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		schema  string
	}{
		{name: "PostgreSQL catalog", dialect: "postgres", schema: "pg_catalog"},
		{name: "PostgreSQL reserved prefix", dialect: "yugabytedb", schema: "pg_app"},
		{name: "CockroachDB internal", dialect: "cockroachdb", schema: "crdb_internal"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := pgsystemschema.ValidateDeclaredPostgresSystemSchemas(
				test.dialect,
				[]schemamodel.Schema{{Name: test.schema}},
			)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches,
				`.*declares server-owned PostgreSQL schema "`+test.schema+`".*`)
		})
	}
}

func TestValidateDeclaredPostgresSystemSchemasKeepsUserNamespaces(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		schema  string
	}{
		{name: "quoted catalog lookalike", dialect: "postgres", schema: "PG_CATALOG"},
		{name: "CockroachDB lookalike", dialect: "cockroachdb", schema: "CRDB_INTERNAL"},
		{name: "CockroachDB namespace on PostgreSQL", dialect: "postgres", schema: "crdb_internal"},
		{name: "non PostgreSQL target", dialect: "mysql", schema: "pg_catalog"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := pgsystemschema.ValidateDeclaredPostgresSystemSchemas(
				test.dialect,
				[]schemamodel.Schema{{Name: test.schema}},
			)
			c.Assert(err, qt.IsNil)
		})
	}
}

// A schema the target owns cannot be created, and one it leaves to the user
// can, including the public schema every family member creates.
func TestIsUncreatableSchemaCoversTheNamesATargetOwns(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		schema  string
		want    bool
	}{
		{name: "PostgreSQL creates public", dialect: "postgres", schema: "public", want: false},
		{name: "CockroachDB creates public", dialect: "cockroachdb", schema: "public", want: false},
		{name: "the common catalog is owned everywhere", dialect: "yugabytedb", schema: "pg_catalog", want: true},
		{name: "Cockroach internal is still owned", dialect: "cockroachdb", schema: "crdb_internal", want: true},
		{name: "a quoted lookalike is a user schema", dialect: "postgres", schema: "PUBLIC", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				pgsystemschema.IsUncreatableSchema(test.dialect, test.schema),
				qt.Equals,
				test.want,
			)
		})
	}
}
