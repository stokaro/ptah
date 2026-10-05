//go:build integration

package ydb_test

import (
	"context"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// ydbLine is one YDB server the contour starts. Every test in this package
// runs once against each, as a subtest named for the line.
type ydbLine struct {
	// name is the release line the server runs.
	name string
	// engine is where the server's address comes from.
	engine dbtarget.Engine
	// preset is the capability set the server's version resolves to. A
	// target that reaches a server on another line fails
	// TestYDBConnection_DescribesTheServer instead of measuring that line
	// under this one's name.
	preset func() capability.Capabilities
}

// ydbLines are the lines whose capability cells are certified: 26.2, the
// current release, and 25.1, the one line with a published support date.
// The package runs whole on both. Which test meets a difference between the
// lines is not something reading predicts: the aborted-transaction test
// looked line-independent until 25.1 answered the abort at the write rather
// than at the commit.
var ydbLines = []ydbLine{
	{name: "26.2", engine: dbtarget.YDB, preset: capability.YDB262},
	{name: "25.1", engine: dbtarget.YDB251, preset: capability.YDB251},
}

// contourCapabilities is what a connection to the line's server in the
// integration contour resolves to: the line's preset, refined by the feature
// flags the contour turns on. Of those, EnableResourcePools is the one that
// decides a capability (see .github/workflows/go-integration-tests.yml), so
// the set is the preset with resource_pools on.
func contourCapabilities(line ydbLine) capability.Capabilities {
	return withContourFlags(line.preset())
}

// withContourFlags is preset on a server of the contour, whose feature flags
// turn resource_pools on.
func withContourFlags(preset capability.Capabilities) capability.Capabilities {
	return preset.With(capability.ResourcePools, true)
}

// openYDB connects to the line's database.
func openYDB(c *qt.C, line ydbLine) *dbschema.DatabaseConnection {
	c.Helper()
	url := dbtarget.URL(c, line.engine)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}
