//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// TestMigrateDiffViewTriggerE2E_SecondRunIsInSync declares an INSTEAD OF
// trigger on a view that reads a materialized view, and runs `migrate diff`
// twice. The first run creates the trigger, and applying the directory puts it
// on the view. The second run replays that directory and finds it in sync.
// Scoped by the kept tables alone, the scope filter `migrate diff` puts on the
// replayed state drops the trigger, the second run writes CREATE TRIGGER
// again, and applying it fails because the trigger exists (stokaro/ptah#5015).
func TestMigrateDiffViewTriggerE2E_SecondRunIsInSync(t *testing.T) {
	c := qt.New(t)
	// The replay creates the trigger's function, which a dev server that is
	// not declared disposable refuses.
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
	dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_view_trigger_dev"), "public")
	target := postgresScratchDevURL(c, "ptah_view_trigger_target")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(`CREATE TABLE public.orders (id integer PRIMARY KEY, amount numeric NOT NULL);
CREATE MATERIALIZED VIEW public.order_totals AS SELECT sum(amount) AS total FROM public.orders;
CREATE VIEW public.order_report AS SELECT total FROM public.order_totals;
CREATE FUNCTION public.order_report_ins() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER order_report_ins INSTEAD OF INSERT ON public.order_report FOR EACH ROW EXECUTE FUNCTION public.order_report_ins();
`), 0o600), qt.IsNil)
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
	c.Assert(viewTriggersOf(c, target), qt.Equals, "order_report.order_report_ins")
}

// viewTriggersOf reads back the INSTEAD OF triggers on the views of the
// database at rawURL, as `view.trigger`.
func viewTriggersOf(c *qt.C, rawURL string) string {
	c.Helper()
	db, err := sql.Open("pgx", rawURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var triggers string
	// Bit 6 of tgtype (64) is TRIGGER_TYPE_INSTEAD.
	c.Assert(db.QueryRowContext(c.Context(), `
		SELECT coalesce(string_agg(c.relname || '.' || t.tgname, ' ' ORDER BY c.relname, t.tgname), '')
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind = 'v' AND NOT t.tgisinternal AND t.tgtype & 64 <> 0`).Scan(&triggers), qt.IsNil)
	return triggers
}
