package dbschema_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dburldisplay"
)

// TestConnectToDatabase_FillsTheRedactedURL pins the field a marshalled DBInfo
// serializes under `url` to the connection it describes. Without this the
// exclusion of the credential-bearing URL would leave `url` empty on every
// connection, which is a different defect from the one it fixes.
func TestConnectToDatabase_FillsTheRedactedURL(t *testing.T) {
	c := qt.New(t)

	dbURL := "sqlite://" + filepath.Join(c.TempDir(), "redacted.db")
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	info := conn.Info()
	c.Assert(info.URL, qt.Equals, dbURL)
	c.Assert(info.RedactedURL, qt.Equals, dburldisplay.Format(dbURL))
	c.Assert(info.RedactedURL, qt.Not(qt.Equals), "")
}
