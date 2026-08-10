package atlasmigrateimport_test

import (
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

const differentialOracleVersion = "atlas community version v1.3.0"

func TestAtlasCEOracleLockMatchesDifferentialFuzz(t *testing.T) {
	c := qt.New(t)

	lock, err := os.ReadFile("../../scripts/atlas-ce-oracle.lock")
	c.Assert(err, qt.IsNil)
	version, found := strings.CutPrefix(differentialOracleVersion, "atlas community version ")
	c.Assert(found, qt.IsTrue)
	c.Assert(string(lock), qt.Contains, "\nversion "+version+"\n")
}
