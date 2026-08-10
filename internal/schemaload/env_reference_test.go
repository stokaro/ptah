package schemaload_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/schemaload"
)

// An env:// --schema-file used to be handed to filepath.Abs, so it failed on
// its empty extension: the message named neither env:// nor the attribute, and
// a valid attribute and a misspelled one produced byte-identical output. These
// tests hold each half of that split.

func TestLoad_EnvReferenceWithSupportedAttributeNamesEnvScheme(t *testing.T) {
	c := qt.New(t)

	_, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{"env://src"}})

	c.Assert(err, qt.ErrorMatches, `--schema-file "env://src": env:// references are not resolved by ptah; pass the schema file itself, or use ptah-compat, whose --to and --from accept env://src`)
	c.Assert(err.Error(), qt.Not(qt.Contains), `unsupported schema file extension`)
}

func TestLoad_EnvReferenceWithUnknownAttributeNamesTheAttribute(t *testing.T) {
	c := qt.New(t)

	_, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{"env://bogus"}})

	c.Assert(err, qt.ErrorMatches, `--schema-file "env://bogus": unsupported env:// attribute "bogus": supported attributes are src, schema.src, url, dev, migration, and migration.dir`)
	c.Assert(err.Error(), qt.Not(qt.Contains), `unsupported schema file extension`)
}

// TestLoad_EnvReferencesDifferByAttributeValidity is the point of the split:
// the two messages must not be interchangeable, because the actions they call
// for are different.
func TestLoad_EnvReferencesDifferByAttributeValidity(t *testing.T) {
	c := qt.New(t)

	_, supported := schemaload.Load(schemaload.Options{SchemaFiles: []string{"env://src"}})
	_, unknown := schemaload.Load(schemaload.Options{SchemaFiles: []string{"env://bogus"}})

	c.Assert(supported, qt.IsNotNil)
	c.Assert(unknown, qt.IsNotNil)
	c.Assert(supported.Error(), qt.Not(qt.Equals), unknown.Error())
	// Differing only by the attribute echoed back would satisfy the line above
	// while still telling a user with a typo to go and use another binary, so
	// each message is also held to the phrase the other must not carry.
	c.Assert(supported.Error(), qt.Not(qt.Contains), "unsupported env:// attribute")
	c.Assert(unknown.Error(), qt.Not(qt.Contains), "are not resolved by ptah")
}

func TestLoad_EnvReferenceWithoutAttributeIsNamed(t *testing.T) {
	c := qt.New(t)

	_, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{"env://"}})

	c.Assert(err, qt.ErrorMatches, `--schema-file "env://": env:// desired-state reference is missing the env attribute \(for example env://src\)`)
}

// TestLoad_UnsupportedExtensionSurvivesEnvRejection is the negative control:
// the extension error must still fire for a real extension problem. Without
// it, deleting the extension check entirely would pass the tests above.
func TestLoad_UnsupportedExtensionSurvivesEnvRejection(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.txt")
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE t (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	_, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})

	c.Assert(err, qt.ErrorMatches, `unsupported schema file extension "\.txt": only \.yaml, \.yml, \.hcl, and \.sql are supported`)
}

// TestLoad_PlainFileNamedLikeEnvIsStillAFile is the other negative control: the
// rejection keys on the env:// scheme, not on the substring "env", so an
// ordinary file whose name contains it still loads.
func TestLoad_PlainFileNamedLikeEnvIsStillAFile(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "env-src.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE t (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
}
