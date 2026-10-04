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

// TestYDBBinary_DBVerify runs `db verify` against a live YDB database: an
// assertion that holds is verified and the run exits 0; one that does not hold
// fails, and a write is refused before it runs and reported as errored, which
// is the exit status a fault gets.
func TestYDBBinary_DBVerify(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			dir := c.TempDir()
			holds := filepath.Join(dir, "holds.sql")
			mixed := filepath.Join(dir, "mixed.sql")
			c.Assert(os.WriteFile(holds, []byte(`-- +ptah check name="holds" assert="SELECT 1 = 1"`+"\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(mixed, []byte(`-- +ptah check name="holds" assert="SELECT 1 = 1"`+"\n"+
				`-- +ptah check name="fails" assert="SELECT 1 = 2"`+"\n"+
				"-- +ptah check name=\"write\" assert=\"UPSERT INTO `ptah_ydb_verify` (`id`) VALUES (1)\"\n"), 0o600), qt.IsNil)

			verified, verifiedErr := runBinary(ctx, binary, "db", "verify", "--db-url", url, "--checks", holds)
			reported, reportedErr := runBinary(ctx, binary, "db", "verify", "--db-url", url, "--checks", mixed)

			c.Assert(verifiedErr, qt.IsNil, qt.Commentf("%s", verified))
			c.Assert(verified, qt.Contains, "Verdict: verified (1 verified, 0 failed, 0 errored of 1)")
			c.Assert(reportedErr, qt.ErrorMatches, "exit status 2")
			c.Assert(reported, qt.Contains, "Verdict: errored (1 verified, 1 failed, 1 errored of 3)")
			c.Assert(reported, qt.Contains, "reason: check assertion must be a read-only SELECT statement")
			c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), "ptah_ydb_verify")
		})
	}
}
