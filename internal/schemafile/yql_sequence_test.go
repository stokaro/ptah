package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/schemafile"
)

func TestYQLSerialSettingsAcrossFiles(t *testing.T) {
	c := qt.New(t)
	directory := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(directory, "01.sql"), []byte("CREATE TABLE `app/orders` (id BigSerial NOT NULL,PRIMARY KEY(id)); ALTER SEQUENCE `/Root/db/app/orders/_serial_column_id` START 100;"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(directory, "02.sql"), []byte("ALTER SEQUENCE `/Root/db/app/orders/_serial_column_id` INCREMENT BY 5;"), 0o600), qt.IsNil)
	database, err := schemafile.LoadPath(directory, schemafile.Options{Dialect: "ydb", DatabaseURL: "ydb://localhost:2136/Root/db"})
	c.Assert(err, qt.IsNil)
	c.Assert(database.Fields, qt.HasLen, 1)
	c.Assert(database.Fields[0].IdentityStart, qt.Equals, "100")
	c.Assert(database.Fields[0].IdentityIncrement, qt.Equals, "5")
	c.Assert(database.DatabasePath, qt.Equals, "")
}
