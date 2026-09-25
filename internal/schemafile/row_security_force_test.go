package schemafile_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemafile"
)

// These tests cover the two row-level security clauses a multi-tenant schema
// leans on: FORCE, which binds the table's owner to its policies, and a
// restrictive policy, which narrows what the permissive ones admit. Both have a
// place in the model; a SQL schema file has to reach it.

func loadPostgresSchema(c *qt.C, body string) (*schemamodel.Database, error) {
	dir := c.TempDir()
	path := writeSchemaFile(c, dir, "schema.sql", "CREATE TABLE t1 (id BIGINT PRIMARY KEY);\n"+body+"\n")
	return schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: platform.Postgres})
}

// TestLoadAll_ForceRowLevelSecurity_HappyPath reads ENABLE and FORCE as one
// enablement, in either order. They are two statements in the file and one
// declaration in the model, because FORCE qualifies the enablement.
func TestLoadAll_ForceRowLevelSecurity_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{{
		name: "enable then force",
		body: "ALTER TABLE t1 ENABLE ROW LEVEL SECURITY;\nALTER TABLE t1 FORCE ROW LEVEL SECURITY;",
	}, {
		name: "force then enable",
		body: "ALTER TABLE t1 FORCE ROW LEVEL SECURITY;\nALTER TABLE t1 ENABLE ROW LEVEL SECURITY;",
	}, {
		name: "only and quoted spellings",
		body: "ALTER TABLE ONLY \"t1\" FORCE ROW LEVEL SECURITY;\nALTER TABLE public.t1 ENABLE ROW LEVEL SECURITY;",
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := loadPostgresSchema(c, tc.body)
			c.Assert(err, qt.IsNil)
			c.Assert(db.RLSEnabledTables, qt.HasLen, 1)
			c.Assert(db.RLSEnabledTables[0].Forced, qt.IsTrue)
			rendered := strings.Join(renderPostgres(c, db), "\n")
			c.Assert(rendered, qt.Contains, "ENABLE ROW LEVEL SECURITY;")
			c.Assert(rendered, qt.Contains, "FORCE ROW LEVEL SECURITY;")
		})
	}
}

// TestLoadAll_EnableWithoutForceStaysUnforced is the control for the test
// above: the flag comes from the FORCE statement and nothing else.
func TestLoadAll_EnableWithoutForceStaysUnforced(t *testing.T) {
	c := qt.New(t)
	db, err := loadPostgresSchema(c, "ALTER TABLE t1 ENABLE ROW LEVEL SECURITY;")
	c.Assert(err, qt.IsNil)
	c.Assert(db.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(db.RLSEnabledTables[0].Forced, qt.IsFalse)
	c.Assert(strings.Join(renderPostgres(c, db), "\n"), qt.Not(qt.Contains), "FORCE ROW LEVEL SECURITY")
}

// TestLoadAll_ForceRowLevelSecurity_FailurePath refuses a FORCE for a table
// the file never enables. PostgreSQL accepts it and it changes nothing, and
// the model has no state for a forced table that is not enabled.
func TestLoadAll_ForceRowLevelSecurity_FailurePath(t *testing.T) {
	c := qt.New(t)
	db, err := loadPostgresSchema(c, "ALTER TABLE t1 FORCE ROW LEVEL SECURITY;")
	c.Assert(err, qt.ErrorMatches, `(?s).*ALTER TABLE t1 FORCE ROW LEVEL SECURITY has no effect until the table `+
		`enables row-level security; declare ALTER TABLE t1 ENABLE ROW LEVEL SECURITY too.*`)
	c.Assert(db, qt.IsNil)
}

// TestLoadAll_PolicyKind_HappyPath reads the AS clause. RESTRICTIVE reaches
// the model and the rendered SQL; PERMISSIVE is PostgreSQL's default and
// renders as a policy with no AS clause, which is the same policy.
func TestLoadAll_PolicyKind_HappyPath(t *testing.T) {
	tests := []struct {
		name            string
		statement       string
		wantRestrictive bool
		wantRendered    string
	}{{
		name:            "restrictive",
		statement:       `CREATE POLICY p1 ON t1 AS RESTRICTIVE FOR SELECT USING (id > 0);`,
		wantRestrictive: true,
		wantRendered:    `CREATE POLICY "p1" ON "t1" AS RESTRICTIVE FOR SELECT`,
	}, {
		name:            "restrictive in lower case after the command",
		statement:       `CREATE POLICY p1 ON t1 FOR SELECT as restrictive USING (id > 0);`,
		wantRestrictive: true,
		wantRendered:    `CREATE POLICY "p1" ON "t1" AS RESTRICTIVE FOR SELECT`,
	}, {
		name:            "permissive",
		statement:       `CREATE POLICY p1 ON t1 AS PERMISSIVE FOR SELECT USING (id > 0);`,
		wantRestrictive: false,
		wantRendered:    `CREATE POLICY "p1" ON "t1" FOR SELECT`,
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := loadPostgresSchema(c, "ALTER TABLE t1 ENABLE ROW LEVEL SECURITY;\n"+tc.statement)
			c.Assert(err, qt.IsNil)
			c.Assert(db.RLSPolicies, qt.HasLen, 1)
			c.Assert(db.RLSPolicies[0].Restrictive, qt.Equals, tc.wantRestrictive)
			c.Assert(strings.Join(renderPostgres(c, db), "\n"), qt.Contains, tc.wantRendered)
		})
	}
}
