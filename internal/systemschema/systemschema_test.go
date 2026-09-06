package systemschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/systemschema"
)

// TestPostgresDescribedSchemasPredicate_DropsTheSchemasAnExtensionOwns pins the
// arm and the gate on it.
//
// The extension arm reads pg_depend, so it may only be asked of a catalog that
// has one. Measured: the Cloud Spanner emulator through PGAdapter 0.55.2
// answers `relation "pg_depend" does not exist` to the whole statement, which
// would turn every realm read there into a failure rather than a narrower
// description. Omitting it costs that target nothing -- it has no CREATE
// EXTENSION to own a schema with.
func TestPostgresDescribedSchemasPredicate_DropsTheSchemasAnExtensionOwns(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		wantDepend bool
		wantCrdb   bool
	}{
		{name: "postgres reads pg_depend", dialect: "postgres", wantDepend: true},
		{name: "cockroachdb reads pg_depend", dialect: "cockroachdb", wantDepend: true, wantCrdb: true},
		{name: "yugabytedb reads pg_depend", dialect: "yugabytedb", wantDepend: true},
		{name: "spanner has no pg_depend", dialect: "spanner", wantDepend: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			predicate := systemschema.PostgresDescribedSchemasPredicate(
				test.dialect, capability.ForDialect(test.dialect))

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
	base := systemschema.PostgresNonSystemSchemasPredicate("postgres")
	described := systemschema.PostgresDescribedSchemasPredicate(
		"postgres", capability.ForDialect("postgres"))

	c.Assert(described, qt.Contains, base)
}

func TestPostgresNonSystemSchemasPredicate_DerivesSystemSchemasFromDialect(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		c := qt.New(t)
		predicate := systemschema.PostgresNonSystemSchemasPredicate("postgres")

		c.Assert(predicate, qt.Contains, "n.nspname <> 'information_schema'")
		c.Assert(predicate, qt.Not(qt.Contains), "crdb_internal")
		c.Assert(predicate, qt.Contains, "n.nspname NOT LIKE 'pg\\_%' ESCAPE '\\'")
	})

	t.Run("cockroachdb", func(t *testing.T) {
		c := qt.New(t)
		predicate := systemschema.PostgresNonSystemSchemasPredicate("cockroachdb")

		c.Assert(predicate, qt.Contains, "n.nspname <> 'information_schema'")
		c.Assert(predicate, qt.Contains, "n.nspname <> 'crdb_internal'")
		c.Assert(predicate, qt.Contains, "n.nspname NOT LIKE 'pg\\_%' ESCAPE '\\'")
	})
}

func TestIsPostgresSystemSchemaPreservesQuotedIdentifierIdentity(t *testing.T) {
	c := qt.New(t)

	for _, name := range []string{"pg_catalog", "pg_toast", "information_schema"} {
		c.Assert(systemschema.IsPostgresSystemSchema(name), qt.IsTrue, qt.Commentf("name: %q", name))
	}
	for _, name := range []string{"public", "extensions", "PG_CATALOG", " pg_catalog ", " "} {
		c.Assert(systemschema.IsPostgresSystemSchema(name), qt.IsFalse, qt.Commentf("name: %q", name))
	}
}

func TestIsPostgresFamilySystemSchemaAddsOnlyCockroachDBInternal(t *testing.T) {
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				systemschema.IsPostgresFamilySystemSchema(test.dialect, test.schema),
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
		{name: "information schema", dialect: "spanner", schema: "information_schema"},
		{name: "CockroachDB internal", dialect: "cockroachdb", schema: "crdb_internal"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := systemschema.ValidateDeclaredPostgresSystemSchemas(
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
			err := systemschema.ValidateDeclaredPostgresSystemSchemas(
				test.dialect,
				[]schemamodel.Schema{{Name: test.schema}},
			)
			c.Assert(err, qt.IsNil)
		})
	}
}

// Spanner's `public` is not a catalog namespace -- it holds the user's own
// tables -- and it still cannot be created. Measured on the PGAdapter emulator
// v0.55.2, `CREATE SCHEMA IF NOT EXISTS "public"` is refused with
// `Schema name not valid: public`, and the IF NOT EXISTS does not help because
// the refusal is about the name (stokaro/ptah#2072).
func TestIsUncreatableSchemaCoversTheNamesATargetOwns(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		schema  string
		want    bool
	}{
		{name: "Spanner owns public", dialect: "spanner", schema: "public", want: true},
		{name: "Spanner owns its catalog", dialect: "spanner", schema: "spanner_sys", want: true},
		{name: "Spanner does not own a user schema", dialect: "spanner", schema: "app", want: false},
		{name: "PostgreSQL creates public", dialect: "postgres", schema: "public", want: false},
		{name: "CockroachDB creates public", dialect: "cockroachdb", schema: "public", want: false},
		{name: "the common catalog is owned everywhere", dialect: "spanner", schema: "pg_catalog", want: true},
		{name: "Cockroach internal is still owned", dialect: "cockroachdb", schema: "crdb_internal", want: true},
		{name: "a quoted lookalike is a user schema", dialect: "spanner", schema: "PUBLIC", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				systemschema.IsUncreatableSchema(test.dialect, test.schema),
				qt.Equals,
				test.want,
			)
		})
	}
}
