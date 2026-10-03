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

// TestMigrateDiffSequenceGrantsE2E_SecondRunIsInSync grants on a serial
// column's sequence and on a standalone sequence, and runs `migrate diff`
// twice. The first run writes both grants; the second finds the directory in
// sync. The scope filter `migrate diff` puts on the replayed state kept a grant
// only on a table it kept, so it dropped every sequence grant from the current
// side, and the second run wrote both grants again (stokaro/ptah#4085).
func TestMigrateDiffSequenceGrantsE2E_SecondRunIsInSync(t *testing.T) {
	c := qt.New(t)
	admin, err := sql.Open("pgx", dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	role := fmt.Sprintf("ptah_seq_grant_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+pgx.Identifier{role}.Sanitize())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+pgx.Identifier{role}.Sanitize())
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	dev := pinnedDevURL(c, postgresScratchDevURL(c, "ptah_seq_grant_dev"), "public")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(fmt.Sprintf(`CREATE TABLE public.todos (id bigserial PRIMARY KEY);
CREATE SEQUENCE public.counter;
GRANT USAGE, SELECT ON SEQUENCE public.todos_id_seq TO %[1]s;
GRANT USAGE ON SEQUENCE public.counter TO %[1]s;
`, pgx.Identifier{role}.Sanitize())), 0o600), qt.IsNil)
	dir := c.TempDir()
	diff := func(name string) []string {
		return []string{"migrate", "diff", name, "--dir", "file://" + filepath.ToSlash(dir),
			"--to", "file://" + filepath.ToSlash(schema), "--dev-url", dev}
	}

	first, err := runCompatVerb(diff("init")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", first))
	second, err := runCompatVerb(diff("second")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
	c.Assert(second, qt.Contains, "The migration directory is synced with the desired state")
	written, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)
	body, err := os.ReadFile(written[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(body), qt.Contains, `GRANT USAGE ON SEQUENCE "public"."todos_id_seq"`)
	c.Assert(string(body), qt.Contains, `GRANT USAGE ON SEQUENCE "public"."counter"`)
}
