package builtindialects_test

import (
	"encoding/json"
	"go/build"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestDescriptors_NameTheConstants pins the seed against the names engine
// packages compare with: the descriptors are in the order the constants are
// declared, one each, so a constant without a descriptor or a descriptor
// without a constant fails here rather than as an unknown dialect elsewhere.
func TestDescriptors_NameTheConstants(t *testing.T) {
	c := qt.New(t)
	var names []string
	for _, described := range builtindialects.Descriptors() {
		names = append(names, described.Name)
	}

	c.Assert(names, qt.DeepEquals, []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.ClickHouse, platform.SQLite, platform.SQLServer,
		platform.CockroachDB, platform.YugabyteDB, platform.Spanner, platform.Oracle, platform.YDB,
	})
}

func TestDescriptors_FormOneRegistry(t *testing.T) {
	c := qt.New(t)

	registry, err := descriptor.NewRegistry(builtindialects.Descriptors()...)

	c.Assert(err, qt.IsNil)
	c.Assert(registry.Dialects(), qt.DeepEquals, builtindialects.Descriptors())
	c.Assert(platform.Dialects().Dialects(), qt.DeepEquals, builtindialects.Descriptors())
}

// TestDescriptors_RoundTripThroughJSON pins that every built-in descriptor
// is data a provider in another process could have sent.
func TestDescriptors_RoundTripThroughJSON(t *testing.T) {
	for _, sent := range builtindialects.Descriptors() {
		t.Run(sent.Name, func(t *testing.T) {
			c := qt.New(t)
			encoded, err := json.Marshal(sent)
			c.Assert(err, qt.IsNil)
			var received descriptor.Dialect
			c.Assert(json.Unmarshal(encoded, &received), qt.IsNil)
			c.Assert(received, qt.DeepEquals, sent)
		})
	}
}

// TestDescriptors_EachCallReturnsNewValues pins that a caller cannot change
// what the next caller, such as the process registry's seed, receives.
func TestDescriptors_EachCallReturnsNewValues(t *testing.T) {
	c := qt.New(t)
	first := builtindialects.Descriptors()

	first[0].Aliases[0] = "changed"

	c.Assert(builtindialects.Descriptors()[0].Aliases[0], qt.Equals, "pgx")
}

// TestPackage_ImportsOnlyTheContract pins what lets core/platform import the
// seed: it is data that links the descriptor contract and nothing else, so no
// shared package reaches an engine's code through it.
func TestPackage_ImportsOnlyTheContract(t *testing.T) {
	c := qt.New(t)

	pkg, err := build.ImportDir(".", 0)

	c.Assert(err, qt.IsNil)
	c.Assert(len(pkg.GoFiles) >= 12, qt.IsTrue, qt.Commentf("files %q", pkg.GoFiles))
	c.Assert(pkg.Imports, qt.DeepEquals, []string{"ptah.run/core/platform/descriptor"})
}
