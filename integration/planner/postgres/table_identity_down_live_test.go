//go:build integration

package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestDownMigrationRestoresDroppedColumnLive executes both directions.
//
// Rendering is not applying, and a rollback that is never executed proves
// nothing about whether the column comes back. These rows run the forward plan
// against a real PostgreSQL, then the reverse plan, and ask
// information_schema.columns what the database holds afterwards. Without the
// identity-aware lookup the reverse plan is empty for the mismatched spellings
// and the column stays dropped -- with a rollback that exits 0.
func TestDownMigrationRestoresDroppedColumnLive(t *testing.T) {
	c := qt.New(t)
	adminURL := livePostgresURLForRLSEnable(t)

	tests := []struct {
		name         string
		targetSchema string
		dbSchema     string
	}{
		{
			name:         "both sides spell the table the same way",
			targetSchema: "",
			dbSchema:     "",
		},
		{
			name:         "the target qualifies public and the database reports it bare",
			targetSchema: "public",
			dbSchema:     "",
		},
		{
			name:         "the target is bare and the database reports public",
			targetSchema: "",
			dbSchema:     "public",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dbURL := createRLSEnableDatabase(c.TB, adminURL)
			executeSQL(c.TB, dbURL, []string{
				`CREATE TABLE users (id integer PRIMARY KEY, email text, legacy_note text)`,
				`INSERT INTO users (id, email, legacy_note) VALUES (1, 'a@example.com', 'keep')`,
			})

			generated := downColumnTarget(test.targetSchema)
			database := downColumnDatabase(test.dbSchema)

			forward := schemadiff.CompareWithDialect(generated, database, "postgres")
			up, err := planner.GenerateSchemaDiffSQLStatements(forward, generated, "postgres")
			c.Assert(err, qt.IsNil)
			executeSQL(c.TB, dbURL, up)
			c.Assert(usersColumns(c.TB, dbURL), qt.DeepEquals, []string{"email", "id"})

			down := planDownStatements(c, generated, database)
			c.Logf("down plan:\n%s", strings.Join(down, "\n"))
			executeSQL(c.TB, dbURL, down)
			c.Assert(usersColumns(c.TB, dbURL), qt.DeepEquals, []string{"email", "id", "legacy_note"})
		})
	}
}

// usersColumns reports the columns public.users holds, sorted by name.
func usersColumns(tb testing.TB, dbURL string) []string {
	c := qt.New(tb)
	c.Helper()
	return queryStrings(c.TB, dbURL,
		`SELECT column_name FROM information_schema.columns
		  WHERE table_schema = 'public' AND table_name = 'users'
		  ORDER BY column_name`)
}

// TestModifiedUserTypeDropWithoutRecreateLive executes the user-type pair.
//
// The claim is about what pg_type holds after the plan runs, which no assertion
// on the SQL text can settle: a DROP with no CREATE renders as one perfectly
// valid statement and applies cleanly. These rows apply the plan and read the
// catalog.
func TestModifiedUserTypeDropWithoutRecreateLive(t *testing.T) {
	c := qt.New(t)
	adminURL := livePostgresURLForRLSEnable(t)

	tests := []struct {
		name      string
		generated *goschema.Database
		wantTypes []string
	}{
		{
			// Control: the definition resolves, so the pair runs and the domain
			// is present afterwards with its new base type.
			name: "a resolvable modification leaves the domain in place",
			generated: &goschema.Database{
				Domains: []goschema.Domain{{Name: "zip", Schema: "app", BaseType: "VARCHAR(10)"}},
			},
			wantTypes: []string{"app.zip"},
		},
		{
			// The definition does not resolve. Nothing is dropped, because
			// nothing could be put back.
			name: "an unresolvable modification leaves the domain in place",
			generated: &goschema.Database{
				Domains: []goschema.Domain{{Name: "other", Schema: "app", BaseType: "TEXT"}},
			},
			wantTypes: []string{"app.zip"},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dbURL := createRLSEnableDatabase(c.TB, adminURL)
			executeSQL(c.TB, dbURL, []string{
				`CREATE SCHEMA app`,
				`CREATE DOMAIN app.zip AS varchar(5)`,
			})
			c.Assert(domainNames(c.TB, dbURL), qt.DeepEquals, []string{"app.zip"})

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				modifiedZipDomainDiff(),
				test.generated,
				"postgres",
			)
			c.Assert(err, qt.IsNil)
			c.Logf("plan:\n%s", strings.Join(statements, "\n"))
			executeSQL(c.TB, dbURL, statements)
			c.Assert(domainNames(c.TB, dbURL), qt.DeepEquals, test.wantTypes)
		})
	}
}

// domainNames reports every user-schema domain in pg_type as `schema.name`.
func domainNames(tb testing.TB, dbURL string) []string {
	c := qt.New(tb)
	c.Helper()
	return queryStrings(c.TB, dbURL,
		`SELECT n.nspname || '.' || t.typname
		   FROM pg_type t
		   JOIN pg_namespace n ON n.oid = t.typnamespace
		  WHERE t.typtype = 'd'
		    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		  ORDER BY 1`)
}

// modifiedZipDomainDiff is the comparator's verdict on a widened app.zip: the
// shape a drop-and-recreate is planned from.
func modifiedZipDomainDiff() *types.SchemaDiff {
	return &types.SchemaDiff{DomainsModified: []types.DomainDiff{{
		DomainName:      "app.zip",
		Changes:         map[string]string{"type": "character varying(5) -> VARCHAR(10)"},
		CurrentBaseType: "character varying(5)",
	}}}
}
