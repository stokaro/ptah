package migratedown_test

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationfailure"
	"ptah.run/migration/migrator"
)

// A rollback whose down file fails reports it the way `migrations up` does:
// after the error line, and beside the failure record, naming the down file.
func TestMigrateDownServerDetails_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newDocumentRollback(c)
	c.Assert(os.WriteFile(filepath.Join(f.dir, "0000000002_orders.down.sql"), []byte("DELETE FROM missing;\n"), 0o600), qt.IsNil)

	stdout, stderr, err := f.down("--target", "0", "--confirm", "--json", "--server-details")

	c.Assert(err, qt.IsNotNil)
	decoder := json.NewDecoder(strings.NewReader(stdout))
	decoder.DisallowUnknownFields()
	var document struct {
		migrator.RunResult
		ServerDetails *migrationfailure.Details `json:"server_details"`
	}
	c.Assert(decoder.Decode(&document), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
	var trailing json.RawMessage
	c.Assert(decoder.Decode(&trailing), qt.Equals, io.EOF)
	c.Assert(document.Failure, qt.DeepEquals, &migrator.RunFailure{Version: 2, VersionKey: "2", Statement: 1, Statements: 1})
	c.Assert(document.ServerDetails, qt.DeepEquals, &migrationfailure.Details{File: "0000000002_orders.down.sql"})
	c.Assert(stderr, qt.Contains, "\nserver details (may contain application data):\n"+
		"  failed at: statement 1 of 1 of migration 2 (0000000002_orders.down.sql)\n")
}

// Without the flag neither output carries them.
func TestMigrateDownWithoutServerDetails(t *testing.T) {
	c := qt.New(t)
	f := newDocumentRollback(c)
	c.Assert(os.WriteFile(filepath.Join(f.dir, "0000000002_orders.down.sql"), []byte("DELETE FROM missing;\n"), 0o600), qt.IsNil)

	stdout, stderr, err := f.down("--target", "0", "--confirm", "--json")

	c.Assert(err, qt.IsNotNil)
	c.Assert(decodeRunDocument(c, stdout).Failure, qt.DeepEquals, &migrator.RunFailure{Version: 2, VersionKey: "2", Statement: 1, Statements: 1})
	c.Assert(stderr, qt.Not(qt.Contains), "server details")
}
