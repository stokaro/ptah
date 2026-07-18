//go:build integration

package generator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestConstraintDriftGenerateRoundTrip_Integration(t *testing.T) {
	dbURL := os.Getenv("POSTGRES_URL")
	if dbURL == "" {
		t.Skip("skipping PostgreSQL constraint drift integration: POSTGRES_URL not set")
	}

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping PostgreSQL constraint drift integration: cannot connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cleanupConstraintDriftTable(conn)
	t.Cleanup(func() { cleanupConstraintDriftTable(conn) })

	_, err = conn.ExecContext(ctx, `
CREATE TABLE ptah_constraint_drift (
  id integer PRIMARY KEY,
  price integer NOT NULL,
  sku text NOT NULL,
  region text NOT NULL,
  category text NOT NULL,
  CONSTRAINT ptah_constraint_price_check CHECK (price > 0),
  CONSTRAINT ptah_constraint_unique UNIQUE (sku, region)
)`)
	c.Assert(err, qt.IsNil)

	root := c.TempDir()
	entitiesDir := filepath.Join(root, "entities")
	migrationsDir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(entitiesDir, 0755), qt.IsNil)
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(entitiesDir, "schema.go"), []byte(`package entities

//migrator:schema:table name="ptah_constraint_drift"
//migrator:schema:constraint name="ptah_constraint_price_check" type="CHECK" check="price >= 0"
//migrator:schema:constraint name="ptah_constraint_unique" type="UNIQUE" columns="sku,region,category"
type Product struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int

	//migrator:schema:field name="price" type="INTEGER" not_null="true"
	Price int

	//migrator:schema:field name="sku" type="TEXT" not_null="true"
	SKU string

	//migrator:schema:field name="region" type="TEXT" not_null="true"
	Region string

	//migrator:schema:field name="category" type="TEXT" not_null="true"
	Category string
}
`), 0600), qt.IsNil)

	files, err := GenerateMigration(ctx, GenerateMigrationOptions{
		GoEntitiesDir: entitiesDir,
		DBConn:        conn,
		MigrationName: "constraint_drift",
		OutputDir:     migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)

	upSQLBytes, err := os.ReadFile(files.UpFile)
	c.Assert(err, qt.IsNil)
	upSQL := string(upSQLBytes)
	c.Assert(upSQL, qt.Contains, "DROP CONSTRAINT")
	c.Assert(strings.Count(upSQL, "ADD CONSTRAINT ptah_constraint_price_check"), qt.Equals, 1)
	c.Assert(strings.Count(upSQL, "ADD CONSTRAINT ptah_constraint_unique"), qt.Equals, 1)

	execScript(c, conn, upSQL, "UP")

	desired, err := goschema.ParseFS(os.DirFS(root), "entities")
	c.Assert(err, qt.IsNil)
	dbAfter, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	diff := schemadiff.CompareWithDialect(desired, dbAfter, "postgres")
	c.Assert(diff.HasChanges(), qt.IsFalse,
		qt.Commentf("post-migration diff must be clean; added=%v removed=%v modified=%v",
			diff.ConstraintsAdded, diff.ConstraintsRemoved, diff.TablesModified))
}

func TestUniqueConstraintDriftGenerateRoundTrip_Integration(t *testing.T) {
	cases := []struct {
		dialect string
		envKey  string
	}{
		{"postgres", "POSTGRES_URL"},
		{"mysql", "MYSQL_URL"},
		{"mariadb", "MARIADB_URL"},
	}

	for _, tc := range cases {
		t.Run(tc.dialect, func(t *testing.T) {
			dbURL := os.Getenv(tc.envKey)
			if dbURL == "" {
				t.Skipf("skipping %s unique constraint drift integration: %s not set", tc.dialect, tc.envKey)
			}

			c := qt.New(t)
			ctx := context.Background()
			conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
			if err != nil {
				t.Skipf("skipping %s unique constraint drift integration: cannot connect: %v", tc.dialect, err)
			}
			t.Cleanup(func() { _ = conn.Close() })

			dialect := conn.Info().Dialect
			cleanupUniqueConstraintDriftTable(conn, dialect)
			t.Cleanup(func() { cleanupUniqueConstraintDriftTable(conn, dialect) })

			_, err = conn.ExecContext(ctx, `
CREATE TABLE ptah_unique_constraint_drift (
  id integer PRIMARY KEY,
  sku varchar(255) NOT NULL,
  region varchar(255) NOT NULL,
  category varchar(255) NOT NULL,
  CONSTRAINT ptah_unique_constraint_unique UNIQUE (sku, region)
)`)
			c.Assert(err, qt.IsNil)

			root := c.TempDir()
			entitiesDir := filepath.Join(root, "entities")
			migrationsDir := filepath.Join(root, "migrations")
			c.Assert(os.MkdirAll(entitiesDir, 0755), qt.IsNil)
			c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(entitiesDir, "schema.go"), []byte(`package entities

//migrator:schema:table name="ptah_unique_constraint_drift"
//migrator:schema:constraint name="ptah_unique_constraint_unique" type="UNIQUE" columns="sku,region,category"
type Product struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true"
	ID int

	//migrator:schema:field name="sku" type="VARCHAR(255)" not_null="true"
	SKU string

	//migrator:schema:field name="region" type="VARCHAR(255)" not_null="true"
	Region string

	//migrator:schema:field name="category" type="VARCHAR(255)" not_null="true"
	Category string
}
`), 0600), qt.IsNil)

			files, err := GenerateMigration(ctx, GenerateMigrationOptions{
				GoEntitiesDir: entitiesDir,
				DBConn:        conn,
				MigrationName: "unique_constraint_drift",
				OutputDir:     migrationsDir,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.IsNotNil)

			upSQLBytes, err := os.ReadFile(files.UpFile)
			c.Assert(err, qt.IsNil)
			upSQL := string(upSQLBytes)
			c.Assert(strings.Count(upSQL, "ADD CONSTRAINT ptah_unique_constraint_unique"), qt.Equals, 1,
				qt.Commentf("[%s] generated UP must re-add the changed UNIQUE constraint:\n%s", dialect, upSQL))

			execScript(c, conn, upSQL, "UP")

			desired, err := goschema.ParseFS(os.DirFS(root), "entities")
			c.Assert(err, qt.IsNil)
			dbAfter, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			diff := schemadiff.CompareWithDialect(desired, dbAfter, dialect)
			c.Assert(diff.HasChanges(), qt.IsFalse,
				qt.Commentf("[%s] post-migration diff must be clean; added=%v removed=%v modified=%v",
					dialect, diff.ConstraintsAdded, diff.ConstraintsRemoved, diff.TablesModified))
		})
	}
}

func cleanupConstraintDriftTable(conn *dbschema.DatabaseConnection) {
	_, _ = conn.Exec("DROP TABLE IF EXISTS ptah_constraint_drift CASCADE")
}

func cleanupUniqueConstraintDriftTable(conn *dbschema.DatabaseConnection, dialect string) {
	_, _ = conn.Exec(dropTableSQL(dialect, "ptah_unique_constraint_drift"))
}
