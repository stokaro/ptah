package dbschema

// White-box testing required: a YDB cluster's feature flags reach the
// capability set inside getDatabaseInfoWithCapabilities, and the public path to
// it, ConnectToDatabase, dials the cluster through ydb-go-sdk before it reads a
// flag. Driving that function with an in-memory database that answers the
// version query reaches every step after the dial -- the URL parse, the flag
// read from the monitoring endpoint and the refinement -- with the endpoint
// serving a page recorded from a live cluster.

import (
	"database/sql/driver"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
)

// ydbVersionDB answers the one query getDatabaseInfo sends a YDB connection.
func ydbVersionDB(c *qt.C, version string) *dbtest.DB {
	return dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		c.Assert(query, qt.Equals, "SELECT Version()")
		return dbtest.QueryResult{Columns: []string{"column0"}, Rows: [][]driver.Value{{version}}}, nil
	})
}

// monitoringServing serves the recorded flags page at the path ydbflags reads,
// and answers status with no page anywhere else.
func monitoringServing(c *qt.C, page string, status int) *httptest.Server {
	body, err := os.ReadFile(page)
	c.Assert(err, qt.IsNil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /viewer/json/feature_flags", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
	server := httptest.NewServer(mux)
	c.Cleanup(server.Close)
	return server
}

// infoFor runs the capability assembly over a 26.2.1.14 server at dbURL.
func infoFor(c *qt.C, dbURL string) (catalog.ServerInfo, capability.VersionResolution, error) {
	parsed, err := parseDatabaseURL(dbURL)
	c.Assert(err, qt.IsNil)
	return getDatabaseInfoWithCapabilities(
		c.Context(), ydbVersionDB(c, "26.2.1.14").SQL, platform.YDB, parsed, dbURL, resolveSchemaFromSession,
	)
}

// On 26.2.1.14 the preset says a unique index cannot be added to a table that
// holds rows, and a cluster started with enable_add_unique_index can: the page
// recorded from that cluster turns the key on in the set Ptah plans with. The
// page recorded at the line's default flags, and a URL naming no monitoring
// endpoint, both leave the preset's answer.
func TestGetDatabaseInfoWithCapabilities_FeatureFlags_HappyPath(t *testing.T) {
	for _, tc := range []struct {
		name string
		page string
		want bool
	}{{
		name: "a cluster started with enable_add_unique_index",
		page: "../internal/ydbflags/testdata/local-ydb-26.2.1.14-add-unique-index.json",
		want: true,
	}, {
		name: "a cluster at the line's default flags",
		page: "../internal/ydbflags/testdata/local-ydb-26.2.1.14.json",
		want: false,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			server := monitoringServing(c, tc.page, http.StatusOK)

			info, resolution, err := infoFor(c, "ydb://localhost:2136/local?monitoring="+server.URL)

			c.Assert(err, qt.IsNil)
			c.Assert(resolution.Capabilities.Has(capability.UniqueIndexOnExistingTable), qt.IsFalse)
			c.Assert(info.Capabilities.Has(capability.UniqueIndexOnExistingTable), qt.Equals, tc.want)
		})
	}
}

// Without the monitoring parameter nothing is read, and the preset stands.
func TestGetDatabaseInfoWithCapabilities_NoMonitoringEndpoint_HappyPath(t *testing.T) {
	c := qt.New(t)

	info, resolution, err := infoFor(c, "ydb://localhost:2136/local")

	c.Assert(err, qt.IsNil)
	c.Assert(info.Capabilities, qt.DeepEquals, resolution.Capabilities)
}

// The operator asked for the cluster's answer, so a page that cannot be read
// fails the connection rather than planning with the preset.
func TestGetDatabaseInfoWithCapabilities_FeatureFlags_FailurePath(t *testing.T) {
	c := qt.New(t)
	server := monitoringServing(c, "../internal/ydbflags/testdata/local-ydb-26.2.1.14.json",
		http.StatusServiceUnavailable)

	info, _, err := infoFor(c, "ydb://localhost:2136/local?monitoring="+server.URL)

	c.Assert(err, qt.ErrorMatches, `(?s).*503 Service Unavailable.*`)
	c.Assert(info, qt.DeepEquals, catalog.ServerInfo{})
}
