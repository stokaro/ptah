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
	// flagged are the keys the contour's server holds beyond its line's
	// defaults, through the feature flags go-integration-tests.yml starts it
	// with, which the connection reads from the monitoring endpoint.
	flagged []capability.Capability
}

// capabilities is the set a connection to the line's server reads: its
// preset, with the keys its flags turn on.
func (l ydbLine) capabilities() capability.Capabilities {
	caps := l.preset()
	for _, key := range l.flagged {
		caps = caps.With(key, true)
	}
	return caps
}

// ydbLines are the lines whose capability cells are certified: 26.2, the
// current release, and 25.1, the one line with a published support date.
// The package runs whole on both. Which test meets a difference between the
// lines is not something reading predicts: the aborted-transaction test
// looked line-independent until 25.1 answered the abort at the write rather
// than at the commit.
var ydbLines = []ydbLine{
	{name: "26.2", engine: dbtarget.YDB, preset: capability.YDB262, flagged: []capability.Capability{capability.ResourcePools}},
	// Started with EnableVectorIndex, which 25.1 keeps off by default.
	{name: "25.1", engine: dbtarget.YDB251, preset: capability.YDB251, flagged: []capability.Capability{capability.VectorIndexes, capability.ResourcePools}},
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
