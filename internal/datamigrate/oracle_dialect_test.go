package datamigrate_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/datamigrate"
)

// TestGenerate_OracleDialectWritesWhatOracleAccepts renders a data migration for
// Oracle from a declaration whose moment is quoted text.
//
// The statements name the table and columns bare, as the Oracle DDL renderer
// created them; a BOOLEAN column is NUMBER(1) and takes 1; and a TIMESTAMP
// column takes a typed literal, because Oracle refuses the plain string with
// ORA-01843. The column type comes from the declaration, and it has to survive
// the per-phase split the migration body renders through, so the down script
// is asserted as well as the up one.
func TestGenerate_OracleDialectWritesWhatOracleAccepts(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	_, err = conn.ExecContext(ctx, `CREATE TABLE events (id TEXT PRIMARY KEY, seen_at TIMESTAMP, enabled BOOLEAN)`)
	c.Assert(err, qt.IsNil)

	root := t.TempDir()
	goSrc := `package fixture

//ptah:schema:table name="events"
//ptah:schema:data table="events" key="id" file="events.yaml"
type Event struct {
	//ptah:schema:field name="id" type="VARCHAR(32)" primary="true"
	ID string

	//ptah:schema:field name="seen_at" type="TIMESTAMP"
	SeenAt string

	//ptah:schema:field name="enabled" type="BOOLEAN"
	Enabled bool
}
`
	rows := "- id: A\n  seen_at: \"2024-03-01 12:30:45\"\n  enabled: true\n"
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(goSrc), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "events.yaml"), []byte(rows), 0o600), qt.IsNil)

	up, down, err := datamigrate.Generate(ctx, conn, datamigrate.Options{RootDir: root, Dialect: "oracle"})
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains,
		`INSERT INTO events (enabled, id, seen_at) VALUES (1, 'A', TIMESTAMP '2024-03-01 12:30:45+00:00');`)
	c.Assert(down, qt.Contains, `DELETE FROM events WHERE id = 'A';`)
}
