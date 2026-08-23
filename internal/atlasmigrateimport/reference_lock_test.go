package atlasmigrateimport_test

import (
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

const differentialReferenceVersion = "atlas community version v1.3.0"

func TestAtlasCEReferenceLockMatchesDifferentialFuzz(t *testing.T) {
	c := qt.New(t)

	lock, err := os.ReadFile("../../scripts/atlas-ce-reference.lock")
	c.Assert(err, qt.IsNil)
	version, found := strings.CutPrefix(differentialReferenceVersion, "atlas community version ")
	c.Assert(found, qt.IsTrue)
	c.Assert(string(lock), qt.Contains, "\nversion "+version+"\n")
}
