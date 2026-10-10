// Package builtintest gives the tests of a test binary one bundled runtime to
// share.
//
// builtin.New assembles every bundled provider and codec: about 1.5 ms and
// 2 MB each time, and three times as long under -race. A test that builds one
// per table row or per iteration spends much of its time there. A runtime is
// frozen when New returns and is safe for concurrent use, so a test that does
// not need a runtime of its own can take this one. A test about what New
// builds, or one that samples independently built runtimes, keeps calling New.
//
// It is library code only tests import. It sits outside engine/builtin so the
// shared runtime stays out of that package's public API.
package builtintest

import (
	"sync"

	"github.com/go-extras/go-kit/must"

	"ptah.run/engine"
	"ptah.run/engine/builtin"
)

// shared is built on first use and lives as long as the test binary.
var shared = sync.OnceValues(builtin.New)

// Runtime returns the bundled runtime every caller in this test binary shares.
// It panics when the bundled providers do not assemble, which every other use
// of builtin.New in the binary would report as well.
func Runtime() *engine.Runtime {
	return must.Must(shared())
}
