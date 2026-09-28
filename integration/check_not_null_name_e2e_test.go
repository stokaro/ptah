//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// checkNamedLikeANotNull is a table with a CHECK its author named the way
// PostgreSQL names a NOT NULL, beside a column that is NOT NULL. PostgreSQL 17
// lists the NOT NULL as a CHECK named `..._not_null` holding `q IS NOT NULL`,
// and PostgreSQL 18 keeps it as a NOT NULL constraint; both are the column's
// nullability. The author's CHECK is a CHECK: taken for a NOT NULL by its name,
// it was left out of the comparison and every plan added it again, which the
// apply then refused because the constraint exists (stokaro/ptah#3935).
const checkNamedLikeANotNull = `CREATE TABLE c (id int PRIMARY KEY, p int, q int NOT NULL, CONSTRAINT p_not_null CHECK (p > 0));`

// TestSchemaApplyFindsACheckNamedLikeANotNullSyncedE2E applies the file to the
// database it built, natively and through ptah-compat: both are synced.
func TestSchemaApplyFindsACheckNamedLikeANotNullSyncedE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, checkNamedLikeANotNull, checkNamedLikeANotNull)
	target := postgresUniqueEngine.builtDB(c, checkNamedLikeANotNull)

	native := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
	compat, err := runCompatVerb("schema", "apply", "-u", target, "--to", "file://"+schema,
		"--dev-url", postgresUniqueEngine.emptyDB(c), "--dry-run")

	c.Assert(native, qt.Contains, "Schema is synced")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", compat))
	c.Assert(compat, qt.Contains, "Schema is synced")
}
