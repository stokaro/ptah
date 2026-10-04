//go:build integration

package ydb_test

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/ydburl"
)

// monitoredTarget is the line's URL and its reading. The URL names the
// server's monitoring endpoint, so the flags are read wherever the server's
// operator published it rather than at a port this package assumes.
func monitoredTarget(c *qt.C, line ydbLine) (string, ydburl.URL) {
	c.Helper()
	raw := dbtarget.URL(c, line.engine)
	parsed, err := ydburl.Parse(raw)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Monitoring, qt.IsNotNil, qt.Commentf(
		"%s names no monitoring endpoint: add monitoring=http://host:port, where the server publishes it",
		line.engine))
	return raw, parsed
}

// withMonitoring replaces the monitoring endpoint a YDB URL names.
func withMonitoring(c *qt.C, raw, endpoint string) string {
	c.Helper()
	parsed, err := url.Parse(raw)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set(ydburl.MonitoringParameter, endpoint)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// The connection reads the cluster's feature flags from the monitoring
// endpoint. The servers the contour starts leave every flag that decides a
// capability at its line's default, so the set the flags refine the preset
// into is the preset itself; the unit tests pin what each flag does when it
// is set. The 25.1 server turns EnableVectorIndex on, which decides no
// capability.
func TestYDBConnection_ReadsTheClusterFeatureFlags_HappyPath(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			target, _ := monitoredTarget(c, line)

			conn, err := dbschema.ConnectToDatabase(ctx, target)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

			info := conn.Info()
			c.Assert(info.Capabilities, qt.DeepEquals, capability.ForServerVersion(platform.YDB, info.Version))
			var session capability.Capabilities
			c.Assert(conn.WithSession(ctx, func(scoped *dbschema.DatabaseConnection) error {
				session = scoped.Info().Capabilities
				return nil
			}), qt.IsNil)
			c.Assert(session, qt.DeepEquals, info.Capabilities)
		})
	}
}

// A monitoring endpoint that does not answer fails the connection: the
// operator asked for the cluster's flags, and a plan made without them would
// be made for a cluster nobody described. The control above shows the same
// URL with the endpoint that does answer connects; here only the port moves,
// to one nothing listens on.
func TestYDBConnection_ReadsTheClusterFeatureFlags_FailurePath(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			target, parsed := monitoredTarget(c, line)
			silent := "http://" + net.JoinHostPort(parsed.Monitoring.Hostname(), "1")

			conn, err := dbschema.ConnectToDatabase(ctx, withMonitoring(c, target, silent))

			c.Assert(err, qt.ErrorMatches,
				`failed to get database info: read YDB feature flags from http://.*:1/viewer/json/feature_flags\?database=[^ ]*: .*`)
			c.Assert(conn, qt.IsNil)
		})
	}
}
