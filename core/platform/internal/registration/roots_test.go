package registration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/schemafile"
)

// TestRegister_RefusesTheRootOfAnEngineNoServiceReads registers an engine
// whose schema documents name objects by absolute paths under the database
// root its URL names, and that no URL service reads a root for. Before the
// registration the engine states nothing, so its documents name no root and
// no reader is asked for. After it, the root is required, and the engine is
// refused as one no service reads a root for, rather than having its URLs
// read as YDB's, though nothing names the engine. YDB's reader is the
// control on both sides.
func TestRegister_RefusesTheRootOfAnEngineNoServiceReads(t *testing.T) {
	c := qt.New(t)
	reader, err := schemafile.RootReader("rootdb")
	c.Assert(err, qt.IsNil)
	c.Assert(reader, qt.IsNil)
	ydbReader, err := schemafile.RootReader(platform.YDB)
	c.Assert(err, qt.IsNil)
	c.Assert(ydbReader, qt.IsNotNil)

	engine := described("rooted", "", "rootdb")
	engine.Conversion.DatabaseRootPaths = true
	c.Assert(platform.Register(engine), qt.IsNil)

	reader, err = schemafile.RootReader("rootdb")
	c.Assert(err, qt.ErrorMatches, `dialect "rootdb" names objects under a database root, and no service reads the root its URLs name`)
	c.Assert(reader, qt.IsNil)
	root, err := schemafile.SourceRoot("rootdb", "rootdb://example/db")
	c.Assert(err, qt.ErrorMatches, `dialect "rootdb" names objects under a database root, and no service reads the root its URLs name`)
	c.Assert(root, qt.Equals, "")
	ydbReader, err = schemafile.RootReader(platform.YDB)
	c.Assert(err, qt.IsNil)
	c.Assert(ydbReader, qt.IsNotNil)
}
