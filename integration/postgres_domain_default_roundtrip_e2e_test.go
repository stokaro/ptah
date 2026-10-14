//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/config"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// TestPostgresDomainDefaultConvergesE2E is the live acceptance for
// stokaro/ptah#2037, and it is live because the failure only exists once a
// server has answered.
//
// PostgreSQL reports a domain's default as an EXPRESSION -- a declared
// `DEFAULT 'x@y.z'` on a varchar domain comes back as
// `'x@y.z'::character varying`. Carried into the description as a literal, that
// text was written as a quoted string, read back as a 26-character value, and
// planned as `SET DEFAULT ”'x@y.z”::character varying'`. Applying it made the
// domain's default the SOURCE of the old expression, so a column of that type
// defaulted to that source; each further inspect-and-apply cycle wrapped it
// again, 26 to 49 to 76 to 111 characters.
//
// The assertion is convergence rather than the spelling of the description,
// because the spelling is only wrong in what it makes the next comparison do.
func TestPostgresDomainDefaultConvergesE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_domain_default_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

	scopedURL := replaceDatabaseName(c, dbURL, testDBName)
	setupDB, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	defer setupDB.Close()
	_, err = setupDB.ExecContext(ctx,
		"CREATE DOMAIN email AS varchar(120) NOT NULL DEFAULT 'x@y.z' CHECK (VALUE LIKE '%@%')")
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, scopedURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// The catalog's answer is an expression, and asserting that first is what
	// makes the convergence below mean something: a server reporting a bare
	// literal would converge whichever field the conversion used.
	c.Assert(liveDomainDefault(c, ctx, setupDB, "email"), qt.Contains, "::character varying")

	// The description of the database compared against the database it
	// describes. Nothing about it should differ.
	described := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), read, "postgres", must.Must(builtin.New())))
	// The description has to hold the domain, or the comparison below has
	// nothing to converge and passes on any default.
	domain := describedDomain(c, described, "email")
	diff, err := schemadiff.CompareWithDatabase(
		ctx, conn, described, read, config.DefaultCompareOptions(), must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(domainChanges(diff.FeatureChanges), qt.HasLen, 0,
		qt.Commentf("described default (expression %q, literal %q) against catalog %q",
			domain.DefaultExpr, domain.Default, liveDomainDefault(c, ctx, setupDB, "email")))
}

// describedDomain is the description's one domain called name. The pgtype
// owner reports domains as feature objects.
func describedDomain(c *qt.C, described *schemamodel.Database, name string) *pgtype.DesiredDomain {
	c.Helper()
	found, err := described.FeatureObjects.Select(func(ref objectidentity.ID) bool {
		return ref.Kind == objectidentity.Kind(pgtype.DomainKind) && ref.Name.Source == name
	}).All()
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.HasLen, 1, qt.Commentf("the description has no domain %q", name))
	domain, ok := found[0].Value.(*pgtype.DesiredDomain)
	c.Assert(ok, qt.IsTrue, qt.Commentf("domain %q is %T", name, found[0].Value))
	return domain
}

// domainChanges keeps the changes a comparison planned for domains.
func domainChanges(changes []schemaext.ChangeRecord) []schemaext.ChangeRecord {
	return slices.DeleteFunc(slices.Clone(changes), func(change schemaext.ChangeRecord) bool {
		return change.Subject.Kind != objectidentity.Kind(pgtype.DomainKind)
	})
}

// liveDomainDefault asks the server what the domain's default is now.
func liveDomainDefault(c *qt.C, ctx context.Context, db *sql.DB, name string) string {
	c.Helper()
	var stored string
	err := db.QueryRowContext(ctx,
		"SELECT COALESCE(typdefault, '') FROM pg_type WHERE typname = $1", name).Scan(&stored)
	c.Assert(err, qt.IsNil)
	return stored
}
