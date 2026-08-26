//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
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
	described := dbschematogo.ConvertCatalogToSchema(read)
	diff, err := schemadiff.CompareWithDatabase(
		ctx, conn, described, read, config.DefaultCompareOptions())

	c.Assert(err, qt.IsNil)
	c.Assert(diff.DomainsModified, qt.HasLen, 0,
		qt.Commentf("described default %q against catalog %q",
			describedDomainDefault(described, "email"),
			liveDomainDefault(c, ctx, setupDB, "email")))
}

// describedDomainDefault returns whichever of the two default fields the
// description filled, so a failure message names the value rather than a field.
func describedDomainDefault(described *schemamodel.Database, name string) string {
	for _, domain := range described.Domains {
		if domain.Name != name {
			continue
		}
		if domain.DefaultExpr != "" {
			return "expression " + domain.DefaultExpr
		}
		return "literal " + domain.Default
	}
	return ""
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
