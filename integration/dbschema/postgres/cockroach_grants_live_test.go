//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/dbtarget"
)

// TestCockroachGrants_LiveReadsWhatWasGranted grants privileges on a table, a
// sequence, a schema and a routine, and reads them back.
//
// v25.4.16 and v26.2.7 leave the ACL columns NULL, so a read built on them
// found none of these grants and left the grantee out of the description
// (stokaro/ptah#3815). The read takes them from information_schema on every
// line. What nobody granted stays out: admin and root, which hold ALL on every
// object, and the owner of the table the fixture hands to another role, whose
// ALL on it is no grant here although its grants on the schema are. The
// routine whose PUBLIC EXECUTE was revoked is described with the revoke. CI
// runs one CockroachDB line; the others were run by hand, v25.4.16, v26.2.7
// and v26.3.1 among them.
//
// The expected grants are the ones the test made.
func TestCockroachGrants_LiveReadsWhatWasGranted(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
	defer cancel()
	fixture := newCockroachGrantFixture(c, ctx)

	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.CockroachDB))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{fixture.schema})

	c.Assert(err, qt.IsNil)
	c.Assert(grantedPrivileges(live.Grants), qt.DeepEquals, []string{
		fixture.owner + " CREATE on SCHEMA " + fixture.schema,
		fixture.owner + " USAGE on SCHEMA " + fixture.schema,
		fixture.reader + " EXECUTE on FUNCTION " + fixture.schema + ".f",
		fixture.reader + " INSERT on TABLE " + fixture.schema + ".t with grant option",
		fixture.reader + " SELECT on TABLE " + fixture.schema + ".t",
		fixture.reader + " USAGE on SCHEMA " + fixture.schema,
		fixture.reader + " USAGE on SEQUENCE " + fixture.schema + ".seq",
	})
	c.Assert(roleNamesOf(live.Roles), qt.DeepEquals, []string{fixture.owner, fixture.reader})
	c.Assert(revokedRoutines(must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), live, "cockroachdb", must.Must(builtin.New()))).RevokedGrants),
		qt.DeepEquals, []string{"PUBLIC EXECUTE on " + fixture.schema + ".locked"})
}

// cockroachGrantFixture is a schema holding one object of each kind a grant
// names, the roles the grants go to, and a table another role owns.
type cockroachGrantFixture struct {
	schema string
	owner  string
	reader string
}

// newCockroachGrantFixture creates the fixture and registers its removal. The
// names are unique to the run because roles are cluster-wide.
func newCockroachGrantFixture(c *qt.C, ctx context.Context) cockroachGrantFixture {
	c.Helper()
	db, err := sql.Open("pgx", requirePostgresWriterFamilyLiveURL(c, dbtarget.CockroachDB))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })

	stamp := time.Now().UnixNano()
	fixture := cockroachGrantFixture{
		schema: fmt.Sprintf("ptah_crdbgrant_%d", stamp),
		owner:  fmt.Sprintf("ptah_crdbgrant_owner_%d", stamp),
		reader: fmt.Sprintf("ptah_crdbgrant_reader_%d", stamp),
	}
	schema := pgx.Identifier{fixture.schema}.Sanitize()
	owner := pgx.Identifier{fixture.owner}.Sanitize()
	reader := pgx.Identifier{fixture.reader}.Sanitize()
	c.Cleanup(func() {
		for _, statement := range []string{
			"DROP SCHEMA IF EXISTS " + schema + " CASCADE",
			"DROP ROLE IF EXISTS " + reader,
			"DROP ROLE IF EXISTS " + owner,
		} {
			_, cleanupErr := db.ExecContext(context.Background(), statement)
			c.Check(cleanupErr, qt.IsNil, qt.Commentf("statement: %s", statement))
		}
	})
	for _, statement := range []string{
		"CREATE ROLE " + owner,
		"CREATE ROLE " + reader,
		"CREATE SCHEMA " + schema,
		"GRANT USAGE ON SCHEMA " + schema + " TO " + reader,
		"CREATE TABLE " + schema + ".t (id INT8 PRIMARY KEY)",
		"GRANT SELECT ON TABLE " + schema + ".t TO " + reader,
		"GRANT INSERT ON TABLE " + schema + ".t TO " + reader + " WITH GRANT OPTION",
		"GRANT CREATE, USAGE ON SCHEMA " + schema + " TO " + owner,
		"CREATE TABLE " + schema + ".owned (id INT8 PRIMARY KEY)",
		"ALTER TABLE " + schema + ".owned OWNER TO " + owner,
		"CREATE SEQUENCE " + schema + ".seq",
		"GRANT USAGE ON SEQUENCE " + schema + ".seq TO " + reader,
		"CREATE FUNCTION " + schema + ".f() RETURNS INT8 LANGUAGE SQL AS 'SELECT 1'",
		"GRANT EXECUTE ON FUNCTION " + schema + ".f() TO " + reader,
		"CREATE FUNCTION " + schema + ".locked() RETURNS INT8 LANGUAGE SQL AS 'SELECT 1'",
		"REVOKE EXECUTE ON FUNCTION " + schema + ".locked() FROM public",
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	return fixture
}

// grantedPrivileges lists the grants a description carries -- the implicit
// ones left out -- one line each, sorted.
func grantedPrivileges(grants []catalog.Grant) []string {
	var lines []string
	for _, grant := range grants {
		if grant.Implicit {
			continue
		}
		line := grant.Role + " " + grant.Privilege + " on " + grant.ObjectType + " " + grant.QualifiedTarget()
		if grant.WithOption {
			line += " with grant option"
		}
		lines = append(lines, line)
	}
	slices.Sort(lines)
	return lines
}

// roleNamesOf lists the names of roles, sorted.
func roleNamesOf(roles []catalog.Role) []string {
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	slices.Sort(names)
	return names
}

// revokedRoutines lists the revoked grants a description carries, one line
// each, sorted.
func revokedRoutines(grants []schemamodel.Grant) []string {
	var lines []string
	for _, grant := range grants {
		lines = append(lines, grant.Role+" "+strings.Join(grant.Privileges, ",")+" on "+grant.OnRoutine)
	}
	slices.Sort(lines)
	return lines
}
