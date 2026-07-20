//go:build integration

package gonative_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/stokaro/ptah/cmd/root"
	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func openPostgres(t *testing.T, dsn string) (*sql.DB, error) {
	t.Helper()
	return sql.Open("pgx", dsn)
}

func TestIntrospectCommand_PostgresBrownfieldGoRoundTrip(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	schemaName := fmt.Sprintf("ptah_introspect_%d", time.Now().UnixNano())

	db, err := openPostgres(t, dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	defer func() {
		_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + schemaName + ` CASCADE`)
	}()

	_, err = db.Exec(`DROP SCHEMA IF EXISTS ` + schemaName + ` CASCADE`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`CREATE SCHEMA ` + schemaName)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(introspectBrownfieldSQL(schemaName))
	c.Assert(err, qt.IsNil)

	outDir := filepath.Join(t.TempDir(), "models")
	cmd := root.NewRootCommand()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"introspect",
		"--db-url", dsn,
		"--schemas", schemaName,
		"--out", outDir,
		"--package", "models",
		"--add-json-tags",
		"--add-db-tags",
	})

	err = cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String()))
	c.Assert(stdout.String(), qt.Contains, "Imported 10 table(s)")

	c.Assert(os.WriteFile(filepath.Join(outDir, "go.mod"), []byte("module introspected_models\n\ngo 1.24\n"), 0o600), qt.IsNil)
	goTest := exec.Command("go", "test", ".")
	goTest.Dir = outDir
	goTest.Env = append(os.Environ(),
		"GO111MODULE=on",
		"GOWORK=off",
		"GOCACHE="+filepath.Join(t.TempDir(), "go-build-cache"),
	)
	output, err := goTest.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("generated package go test:\n%s", output))

	generated, err := goschema.ParseDir(outDir)
	c.Assert(err, qt.IsNil)
	c.Assert(generated.Tables, qt.HasLen, 10)
	c.Assert(generated.Enums, qt.HasLen, 1)
	c.Assert(generated.Functions, qt.HasLen, 1)
	c.Assert(generated.RLSPolicies, qt.HasLen, 1)
	c.Assert(generated.RLSEnabledTables, qt.HasLen, 1)

	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	compareOpts := config.DefaultCompareOptions()
	compareOpts.Dialect = conn.Info().Dialect
	diff := schemadiff.CompareWithOptions(generated, live, compareOpts)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func introspectBrownfieldSQL(schemaName string) string {
	q := func(name string) string {
		return schemaName + "." + name
	}
	return fmt.Sprintf(`
CREATE TYPE %[1]s.status_type AS ENUM ('active', 'inactive', 'archived');

CREATE FUNCTION %[1]s.touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
	NEW.updated_at = now();
	RETURN NEW;
END;
$$;

CREATE TABLE %[2]s (
	id integer PRIMARY KEY,
	slug varchar(64) NOT NULL UNIQUE,
	created_at timestamp with time zone NOT NULL DEFAULT now(),
	CONSTRAINT tenants_slug_check CHECK (slug <> '')
);

CREATE TABLE %[3]s (
	id integer PRIMARY KEY,
	tenant_id integer NOT NULL REFERENCES %[2]s(id) ON DELETE CASCADE,
	email varchar(255) NOT NULL,
	status %[1]s.status_type NOT NULL DEFAULT 'active',
	created_at timestamp with time zone NOT NULL DEFAULT now(),
	updated_at timestamp with time zone NOT NULL DEFAULT now(),
	CONSTRAINT users_tenant_email_unique UNIQUE (tenant_id, email)
);

CREATE TABLE %[4]s (
	id integer PRIMARY KEY,
	tenant_id integer NOT NULL REFERENCES %[2]s(id) ON DELETE CASCADE,
	sku varchar(64) NOT NULL,
	price numeric(10,2) NOT NULL,
	status %[1]s.status_type NOT NULL DEFAULT 'active',
	CONSTRAINT products_price_check CHECK (price >= 0),
	CONSTRAINT products_tenant_sku_unique UNIQUE (tenant_id, sku),
	CONSTRAINT products_tenant_id_unique UNIQUE (tenant_id, id)
);

CREATE TABLE %[5]s (
	id integer PRIMARY KEY,
	tenant_id integer NOT NULL REFERENCES %[2]s(id) ON DELETE CASCADE,
	parent_id integer REFERENCES %[5]s(id) ON DELETE SET NULL,
	name varchar(128) NOT NULL
);

CREATE TABLE %[6]s (
	id integer PRIMARY KEY,
	tenant_id integer NOT NULL REFERENCES %[2]s(id) ON DELETE CASCADE,
	user_id integer NOT NULL REFERENCES %[3]s(id) ON DELETE RESTRICT,
	status %[1]s.status_type NOT NULL DEFAULT 'active',
	created_at timestamp with time zone NOT NULL DEFAULT now(),
	CONSTRAINT orders_tenant_id_unique UNIQUE (tenant_id, id)
);

CREATE TABLE %[7]s (
	tenant_id integer NOT NULL,
	order_id integer NOT NULL,
	product_id integer NOT NULL,
	quantity integer NOT NULL,
	PRIMARY KEY (tenant_id, order_id, product_id),
	CONSTRAINT order_items_quantity_check CHECK (quantity > 0),
	CONSTRAINT order_items_order_fk FOREIGN KEY (tenant_id, order_id) REFERENCES %[6]s(tenant_id, id) ON DELETE CASCADE,
	CONSTRAINT order_items_product_fk FOREIGN KEY (tenant_id, product_id) REFERENCES %[4]s(tenant_id, id) ON DELETE RESTRICT
);

CREATE TABLE %[8]s (
	id integer PRIMARY KEY,
	tenant_id integer NOT NULL REFERENCES %[2]s(id) ON DELETE CASCADE,
	order_id integer NOT NULL REFERENCES %[6]s(id) ON DELETE CASCADE,
	total numeric(10,2) NOT NULL CHECK (total >= 0)
);

CREATE TABLE %[9]s (
	id integer PRIMARY KEY,
	tenant_id integer NOT NULL REFERENCES %[2]s(id) ON DELETE CASCADE,
	invoice_id integer NOT NULL REFERENCES %[8]s(id) ON DELETE CASCADE,
	amount numeric(10,2) NOT NULL CHECK (amount >= 0)
);

CREATE TABLE %[10]s (
	product_id integer NOT NULL REFERENCES %[4]s(id) ON DELETE CASCADE,
	category_id integer NOT NULL REFERENCES %[5]s(id) ON DELETE CASCADE,
	PRIMARY KEY (product_id, category_id)
);

CREATE TABLE %[11]s (
	id integer PRIMARY KEY,
	tenant_id integer NOT NULL REFERENCES %[2]s(id) ON DELETE CASCADE,
	entity varchar(64) NOT NULL,
	action varchar(64) NOT NULL,
	updated_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX idx_users_active_email ON %[3]s(email) WHERE status = 'active';
CREATE INDEX idx_products_tenant_status ON %[4]s(tenant_id, status);

ALTER TABLE %[6]s ENABLE ROW LEVEL SECURITY;
CREATE POLICY orders_tenant_policy ON %[6]s
	FOR ALL
	TO PUBLIC
	USING (tenant_id > 0);

CREATE TRIGGER audit_logs_touch_updated_at
	BEFORE UPDATE ON %[11]s
	FOR EACH ROW
	EXECUTE FUNCTION %[1]s.touch_updated_at();

CREATE VIEW %[1]s.active_users AS
	SELECT id, tenant_id, email FROM %[3]s WHERE status = 'active';
`, schemaName,
		q("tenants"),
		q("users"),
		q("products"),
		q("categories"),
		q("orders"),
		q("order_items"),
		q("invoices"),
		q("payments"),
		q("product_categories"),
		q("audit_logs"),
	)
}
