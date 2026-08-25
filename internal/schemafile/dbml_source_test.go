package schemafile_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemafile"
)

// TestLoadPath_ReadsADBMLDocument pins that a `.dbml` file is a schema source
// like any other, so every workflow that already consumes `--schema-file`
// reaches it without a second code path (stokaro/ptah#2065).
func TestLoadPath_ReadsADBMLDocument(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.dbml")
	document := "Table public.users {\n  id bigint [pk, increment]\n  email text [not null, unique]\n}\n"
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)

	db, err := schemafile.LoadPath(path, schemafile.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
	c.Assert(db.Tables[0].Schema, qt.Equals, "public")
	c.Assert(columnNames(db), qt.DeepEquals, []string{"id", "email"})
}

// TestLoadPath_ADBMLSyntaxErrorNamesTheFile pins that the diagnostic a reader
// gets carries the path they passed, not just a line and a column.
func TestLoadPath_ADBMLSyntaxErrorNamesTheFile(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "broken.dbml")
	c.Assert(os.WriteFile(path, []byte("Table t {\n  a int [nope]\n}\n"), 0o600), qt.IsNil)

	_, err := schemafile.LoadPath(path, schemafile.Options{})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "broken.dbml:")
	c.Assert(err.Error(), qt.Contains, `unsupported column setting "nope"`)
}

// TestLoadPath_AnUnknownExtensionNamesDBMLAmongTheSupportedOnes keeps the
// refusal message in step with what the loader accepts. A format that was wired
// in and left out of the message reads as unsupported to whoever hits the error.
func TestLoadPath_AnUnknownExtensionNamesDBMLAmongTheSupportedOnes(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.txt")
	c.Assert(os.WriteFile(path, []byte("nothing\n"), 0o600), qt.IsNil)

	_, err := schemafile.LoadPath(path, schemafile.Options{})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, ".dbml")
}

// columnNames lists the loaded columns in declaration order.
func columnNames(db *goschema.Database) []string {
	names := make([]string, 0, len(db.Fields))
	for _, field := range db.Fields {
		names = append(names, field.Name)
	}
	return names
}

// TestLoadPath_ADBMLLossReachesTheReportIgnoredChannel pins that a construct
// the document declares and the schema model cannot carry is reported through
// the channel that already exists for one, rather than through a second one or
// through silence.
func TestLoadPath_ADBMLLossReachesTheReportIgnoredChannel(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.dbml")
	document := "Project shop {\n  database_type: 'PostgreSQL'\n}\n\nTable t {\n  a int\n}\n"
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	var reported bytes.Buffer

	db, err := schemafile.LoadPath(path, schemafile.Options{ReportIgnored: &reported})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(reported.String(), qt.Contains, "warning: schema file")
	c.Assert(reported.String(), qt.Contains, "describes the diagram")
	c.Assert(reported.String(), qt.Contains, "schema.dbml:")
}

// TestLoadPath_ADBMLDocumentWithNothingToLoseReportsNothing is the control.
//
// Without it a loader that wrote a warning for every DBML file would satisfy
// the test above, and every ordinary document would carry a line nobody can act
// on.
func TestLoadPath_ADBMLDocumentWithNothingToLoseReportsNothing(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.dbml")
	c.Assert(os.WriteFile(path, []byte("Table t {\n  a int [pk]\n}\n"), 0o600), qt.IsNil)
	var reported bytes.Buffer

	_, err := schemafile.LoadPath(path, schemafile.Options{ReportIgnored: &reported})

	c.Assert(err, qt.IsNil)
	c.Assert(reported.String(), qt.Equals, "")
}
