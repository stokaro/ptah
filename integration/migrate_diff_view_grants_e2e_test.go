//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/internal/dbtarget"
)

// TestMigrateDiffViewGrantsE2E_SecondRunIsInSync grants on a view to PUBLIC
// and to a role the schema does not declare, and on a materialized view, and
// runs `migrate diff` twice. The first run writes the grants, and applying the
// directory gives each relation the privileges it declares. The second run
// replays that directory and finds it in sync. PostgreSQL reports a grant on a
// view as a TABLE grant. Scoped by the kept tables alone, the scope filter
// `migrate diff` puts on the replayed state drops such a grant, and the second
// run writes every grant again (stokaro/ptah#5047, stokaro/ptah#5016).
func TestMigrateDiffViewGrantsE2E_SecondRunIsInSync(t *testing.T) {
	c := qt.New(t)
	admin, err := sql.Open("pgx", dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	role := fmt.Sprintf("ptah_view_grant_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+pgx.Identifier{role}.Sanitize())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+pgx.Identifier{role}.Sanitize())
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_view_grant_dev"), "public")
	target := postgresScratchDevURL(c, "ptah_view_grant_target")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(fmt.Sprintf(`CREATE TABLE public.orders (id integer PRIMARY KEY, amount numeric NOT NULL);
CREATE VIEW public.big_orders AS SELECT id, amount FROM public.orders WHERE amount > 100;
CREATE MATERIALIZED VIEW public.order_totals AS SELECT sum(amount) AS total FROM public.orders;
GRANT SELECT ON public.big_orders TO PUBLIC;
GRANT SELECT ON public.big_orders TO %[1]s;
GRANT SELECT ON public.order_totals TO %[1]s;
`, pgx.Identifier{role}.Sanitize())), 0o600), qt.IsNil)
	dir := c.TempDir()
	diff := func(name string) []string {
		return []string{"migrate", "diff", name, "--dir", "file://" + filepath.ToSlash(dir),
			"--to", "file://" + filepath.ToSlash(schema), "--dev-url", dev}
	}

	first, err := runCompatVerb(diff("init")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", first))
	applied, err := runCompatVerb("migrate", "apply", "--url", target, "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	second, err := runCompatVerb(diff("second")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
	c.Assert(second, qt.Contains, "The migration directory is synced with the desired state")
	written, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)
	c.Assert(viewGrantsOf(c, target), qt.Equals, fmt.Sprintf(
		"big_orders=PUBLIC:SELECT big_orders=%[1]s:SELECT order_totals=%[1]s:SELECT", role))
}

// viewGrantsOf reads back the privileges granted on the views and
// materialized views of the database at rawURL, as `relation=grantee:privilege`,
// leaving out the owner's.
func viewGrantsOf(c *qt.C, rawURL string) string {
	c.Helper()
	db, err := sql.Open("pgx", rawURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var grants string
	c.Assert(db.QueryRowContext(c.Context(), `
		SELECT coalesce(string_agg(
			c.relname || '=' || coalesce(r.rolname, 'PUBLIC') || ':' || acl.privilege_type,
			' ' ORDER BY c.relname, coalesce(r.rolname, 'PUBLIC') COLLATE "C", acl.privilege_type), '')
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL aclexplode(c.relacl) acl
		LEFT JOIN pg_roles r ON r.oid = acl.grantee
		WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm') AND acl.grantee <> c.relowner`).Scan(&grants), qt.IsNil)
	return grants
}
