//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
)

// TestPreparePlanFileNamesTheContainerItDoesNotStartLive is the warning row of
// TestPreparePlanFileNamesTheContainerItDoesNotStart. A saved plan reads local
// desired-state files and starts no container, so a `docker://` dev URL is
// answered with a sentence saying so (stokaro/ptah#1635). The dev URL must
// name an engine that speaks the target's dialect, which is why the row runs
// against PostgreSQL; nothing is started either way.
func TestPreparePlanFileNamesTheContainerItDoesNotStartLive(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	desired := filepath.Join(c.TempDir(), "desired.sql")
	c.Assert(os.WriteFile(desired, []byte("CREATE TABLE plan_warning_probe (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)
	var diagnostics bytes.Buffer

	_, err = atlasschema.PreparePlanFile(t.Context(), conn, atlasschema.PlanFileOptions{
		DevURL:      "docker://postgres/16/dev",
		ToURLs:      []string{"file://" + desired},
		Diagnostics: &diagnostics,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.String(), qt.Contains, "schema plan starts none")
}
