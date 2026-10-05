//go:build integration

package ydb_test

import (
	"context"
	"net"
	"net/url"
	"slices"
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
// capability at its line's default but EnableResourcePools, so the set the
// flags refine the preset into is the preset with resource_pools on; the unit
// tests pin what each flag does when it is set. The 25.1 server turns
// EnableVectorIndex on too, which decides no capability.
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
			c.Assert(info.Capabilities, qt.DeepEquals,
				withContourFlags(capability.ForServerVersion(platform.YDB, info.Version)))
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

// connectingUser is a user a test creates on the line's server, with the
// rights rights names on the database, and drops again when it ends.
func connectingUser(c *qt.C, line ydbLine, user, password string, rights ...string) {
	c.Helper()
	admin := openYDB(c, line)
	c.Assert(admin.Writer().ExecuteSQL(c.Context(), "DROP USER IF EXISTS "+user), qt.IsNil)
	c.Assert(admin.Writer().ExecuteSQL(c.Context(), "CREATE USER "+user+" PASSWORD '"+password+"'"), qt.IsNil)
	c.Cleanup(func() { _ = admin.Writer().ExecuteSQL(context.Background(), "DROP USER IF EXISTS "+user) })
	for _, right := range rights {
		c.Assert(admin.Writer().ExecuteSQL(c.Context(), "GRANT "+right+" ON `/local` TO "+user), qt.IsNil)
	}
}

// asUser is the line's URL, monitoring endpoint included, logging in as user.
func asUser(c *qt.C, line ydbLine, user, password string) string {
	c.Helper()
	target, _ := monitoredTarget(c, line)
	parsed, err := url.Parse(target)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(user, password)
	return parsed.String()
}

// lineNamed is the certified line called name.
func lineNamed(c *qt.C, name string) ydbLine {
	c.Helper()
	index := slices.IndexFunc(ydbLines, func(line ydbLine) bool { return line.name == name })
	c.Assert(index, qt.Not(qt.Equals), -1, qt.Commentf("no certified line %s", name))
	return ydbLines[index]
}

// The flags page is read as the user the URL connects as. A user who may
// describe the database reads it on both lines; 25.1 serves it to a user
// who may not, because that line's endpoint checks no right beyond the login.
// The failure path below is what tells the user's token was sent: an
// anonymous read is served on both lines.
func TestYDBConnection_ReadsTheFlagsAsTheConnectingUser_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		line   string
		rights []string
	}{
		{name: "26.2, a user who may describe the database", line: "26.2", rights: []string{"CONNECT", "DESCRIBE SCHEMA"}},
		{name: "25.1, a user who may describe the database", line: "25.1", rights: []string{"CONNECT", "DESCRIBE SCHEMA"}},
		{name: "25.1, a user who may only connect", line: "25.1", rights: []string{"CONNECT"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			line := lineNamed(c, test.line)
			const user, password = "ptahflagreader", "flagreader1"
			connectingUser(c, line, user, password, test.rights...)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			conn, err := dbschema.ConnectToDatabase(ctx, asUser(c, line, user, password))

			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			c.Assert(conn.Info().Capabilities, qt.DeepEquals,
				withContourFlags(capability.ForServerVersion(platform.YDB, conn.Info().Version)))
		})
	}
}

// 26.2 serves the flags page only to a user with DESCRIBE SCHEMA on the
// database, and answers an anonymous read on a cluster that does not enforce
// authentication. So a user who may only connect is refused here only because
// the page was read with that user's token, and the error says which right is
// missing.
func TestYDBConnection_ReadsTheFlagsAsTheConnectingUser_FailurePath(t *testing.T) {
	c := qt.New(t)
	line := lineNamed(c, "26.2")
	const user, password = "ptahflagstranger", "flagstranger1"
	connectingUser(c, line, user, password, "CONNECT")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, asUser(c, line, user, password))

	c.Assert(err, qt.ErrorMatches, `failed to get database info: read YDB feature flags from http://\S+: `+
		`400 Bad Request: Failed to resolve database; the page is read as the connection's user, `+
		`who needs DESCRIBE SCHEMA on /local`)
	c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*`+password+`.*`)
	c.Assert(conn, qt.IsNil)
}
