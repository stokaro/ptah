//go:build integration

package ydb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// The directory the binary's external object run writes into, and the
// variable and value of the secret its data source names.
const (
	externalE2EDir   = "ptah_ydb_external_e2e"
	externalE2EEnv   = "PTAH_SECRET_E2E_WAREHOUSE" // #nosec G101 -- a variable name, not a credential
	externalE2EValue = "warehouse-SENTINEL"        // #nosec G101 -- a made-up value every output is searched for
)

// externalE2EEntities declares a secret, a PostgreSQL source that names it by
// its path, an object storage source and an external table over it.
const externalE2EEntities = `package entities

//ptah:schema:secret name="pg_password" schema="` + externalE2EDir + `" value_env="` + externalE2EEnv + `"
//ptah:schema:externaldatasource name="warehouse" schema="` + externalE2EDir + `" source_type="PostgreSQL" location="pg.invalid:5432" auth_method="BASIC" options="DATABASE_NAME=app;LOGIN=reader;PASSWORD_SECRET_PATH=` + externalE2EDir + `/pg_password"
//ptah:schema:externaldatasource name="bucket" schema="` + externalE2EDir + `" source_type="ObjectStorage" location="https://storage.invalid/events/" auth_method="NONE"
type Warehouse struct{}

//ptah:schema:externaltable name="events" schema="` + externalE2EDir + `" data_source="` + externalE2EDir + `/bucket" location="2026/" columns="id Int64 NOT NULL, kind Utf8" options="FORMAT=csv_with_names;CSV_DELIMITER=\;"
type Event struct{}
`

// TestYDBBinary_AppliesExternalObjects drives the shipped binary over Go
// annotations that declare external objects on a cluster with external data
// sources on: schema apply creates the secret first, then the sources and the
// table, schema compare finds nothing to change, and no output holds the
// secret's value.
func TestYDBBinary_AppliesExternalObjects(t *testing.T) {
	t.Setenv(externalE2EEnv, externalE2EValue)
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	line := lineNamed(c, "26.2")
	setClusterFlags(c, line, externalSourcesOn)
	url := dbtarget.URL(c, line.engine)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	conn := openYDB(c, line)
	dropper, ok := conn.SchemaWriter().(interface {
		DropDirectory(ctx context.Context, dir string) error
	})
	c.Assert(ok, qt.IsTrue)
	c.Cleanup(func() { c.Check(dropper.DropDirectory(context.Background(), externalE2EDir), qt.IsNil) })
	entities := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(entities, "schema.go"), []byte(externalE2EEntities), 0o600), qt.IsNil)
	scope := []string{"--db-url", url, "--schemas", externalE2EDir}

	applied, applyErr := runBinary(ctx, binary, append([]string{"schema", "apply", "--root-dir", entities,
		"--auto-approve"}, scope...)...)
	compared, compareErr := runBinary(ctx, binary, append([]string{"schema", "compare", "--root-dir", entities,
		"--exit-code"}, scope...)...)

	c.Assert(applyErr, qt.IsNil, qt.Commentf("schema apply:\n%s", applied))
	c.Assert(compareErr, qt.IsNil, qt.Commentf("schema compare:\n%s", compared))
	c.Assert(applied, qt.Contains, "CREATE SECRET `"+externalE2EDir+"/pg_password`")
	c.Assert(applied, qt.Contains, "CREATE EXTERNAL DATA SOURCE `"+externalE2EDir+"/warehouse`")
	c.Assert(applied, qt.Contains, "CREATE EXTERNAL TABLE `"+externalE2EDir+"/events`")
	c.Assert(applied, qt.Not(qt.Contains), "SENTINEL")
	c.Assert(compared, qt.Not(qt.Contains), "SENTINEL")
	live := readScoped(c, conn, []string{externalE2EDir})
	c.Assert(live.ExternalDataSources, qt.HasLen, 2)
	c.Assert(live.ExternalTables, qt.HasLen, 1)
	c.Assert(live.ExternalTables[0].Options["CSV_DELIMITER"], qt.Equals, ";")
}
