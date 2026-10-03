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

// withMonitoring adds the monitoring parameter to a YDB URL, naming port on
// the host the URL connects to: local-ydb serves its monitoring endpoint
// beside its gRPC one, and go-integration-tests.yml publishes both.
func withMonitoring(c *qt.C, raw, port string) string {
	c.Helper()
	parsed, err := url.Parse(raw)
	c.Assert(err, qt.IsNil)
	ydb, err := ydburl.FromURL(parsed)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set(ydburl.MonitoringParameter, "http://"+net.JoinHostPort(ydb.Host, port))
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// The connection reads the cluster's feature flags from the monitoring
// endpoint. The server the contour starts runs with its line's default
// flags, so the set they refine the preset into is the preset itself; the
// unit tests pin what each flag does when it is set.
func TestYDBConnection_ReadsTheClusterFeatureFlags_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, withMonitoring(c, dbtarget.URL(c, dbtarget.YDB), "8765"))
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
}

// A monitoring endpoint that does not answer fails the connection: the
// operator asked for the cluster's flags, and a plan made without them would
// be made for a cluster nobody described. The control above shows the same
// URL with the endpoint that does answer connects.
func TestYDBConnection_ReadsTheClusterFeatureFlags_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, withMonitoring(c, dbtarget.URL(c, dbtarget.YDB), "1"))

	c.Assert(err, qt.ErrorMatches,
		`failed to get database info: read YDB feature flags from http://.*:1/viewer/json/feature_flags\?database=[^ ]*: .*`)
	c.Assert(conn, qt.IsNil)
}
