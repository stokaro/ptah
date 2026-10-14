package ydbconnect

// White-box testing required: a YDB cluster's feature flags reach the
// capability set in Service.Refine, and the public path to it,
// dbschema.ConnectToDatabase, dials the cluster through ydb-go-sdk before it
// reads a flag. Driving Refine with a pool whose credential source the test
// supplies reaches every step after the dial -- the URL parse, the flag read
// from the monitoring endpoint and the refinement -- with the endpoint serving
// a page recorded from a live cluster.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/connector"
)

// monitoring is a stand-in for a cluster's monitoring endpoint.
type monitoring struct {
	*httptest.Server
	// authorization holds the Authorization header of each request the
	// endpoint answered, in order.
	authorization []string
}

// monitoringServing serves the recorded flags page at the path ydbflags reads,
// and answers status with no page anywhere else.
func monitoringServing(c *qt.C, page string, status int) *monitoring {
	body, err := os.ReadFile(page)
	c.Assert(err, qt.IsNil)
	endpoint := &monitoring{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /viewer/json/feature_flags", func(w http.ResponseWriter, r *http.Request) {
		endpoint.authorization = append(endpoint.authorization, r.Header.Get("Authorization"))
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
	endpoint.Server = httptest.NewServer(mux)
	c.Cleanup(endpoint.Close)
	return endpoint
}

// presents is a connection that presents ticket to its server.
func presents(ticket string) ticketSource {
	return func(context.Context) (string, error) { return ticket, nil }
}

// infoFor refines the preset of a 26.2.1.14 server at dbURL, for a connection
// whose credential ticket hands over, and returns the refined set beside the
// preset's resolution.
func infoFor(c *qt.C, dbURL string, ticket ticketSource) (capability.Capabilities, capability.VersionResolution, error) {
	const version = "26.2.1.14"
	resolution := capability.ResolveServerVersion(platform.YDB, version)
	caps, err := Service{}.Refine(
		c.Context(),
		&connector.Pool{Native: pool{ticket: ticket}},
		connector.Request{URL: dbURL},
		catalog.ServerInfo{Dialect: platform.YDB, Version: version},
		resolution.Capabilities,
	)
	return caps, resolution, err
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
		page: "../../ydbflags/testdata/local-ydb-26.2.1.14-add-unique-index.json",
		want: true,
	}, {
		name: "a cluster at the line's default flags",
		page: "../../ydbflags/testdata/local-ydb-26.2.1.14.json",
		want: false,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			server := monitoringServing(c, tc.page, http.StatusOK)

			caps, resolution, err := infoFor(c, "ydb://localhost:2136/local?monitoring="+server.URL, nil)

			c.Assert(err, qt.IsNil)
			c.Assert(resolution.Capabilities.Has(capability.UniqueIndexOnExistingTable), qt.IsFalse)
			c.Assert(caps.Has(capability.UniqueIndexOnExistingTable), qt.Equals, tc.want)
		})
	}
}

// The page is read with the credential the connection presents, and with none
// for an anonymous one. A TLS connection's credential goes to an https://
// endpoint; anonymous, the same connection reads a plain http:// one.
func TestGetDatabaseInfoWithCapabilities_FeatureFlagsCredential_HappyPath(t *testing.T) {
	for _, tc := range []struct {
		name   string
		scheme string
		ticket ticketSource
		want   []string
	}{
		{name: "an anonymous connection", scheme: "ydb", ticket: nil, want: []string{""}},
		{name: "a connection with a credential", scheme: "ydb", ticket: presents("t0k3n"), want: []string{"t0k3n"}},
		{name: "an anonymous TLS connection", scheme: "ydbs", ticket: presents(""), want: []string{""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			server := monitoringServing(c, "../../ydbflags/testdata/local-ydb-26.2.1.14.json", http.StatusOK)

			_, _, err := infoFor(c, tc.scheme+"://localhost:2136/local?monitoring="+server.URL, tc.ticket)

			c.Assert(err, qt.IsNil)
			c.Assert(server.authorization, qt.DeepEquals, tc.want)
		})
	}
}

// A credential the connection carries over TLS is not sent to a plain http://
// endpoint, and the refusal names neither the credential nor anything but the
// endpoint; a connection whose credential cannot be read fails rather than
// reading the page anonymously.
func TestGetDatabaseInfoWithCapabilities_FeatureFlagsCredential_FailurePath(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ticket  ticketSource
		wantErr string
	}{
		{
			name:   "a TLS connection's credential and a plain endpoint",
			ticket: presents("t0k3n"),
			wantErr: `read YDB feature flags from http://127\.0\.0\.1:\d+: the connection's credential travels ` +
				`over TLS, and Ptah does not send it to a plain http:// endpoint; ` +
				`name the endpoint as https://127\.0\.0\.1:\d+`,
		},
		{
			name:    "a credential that cannot be read",
			ticket:  func(context.Context) (string, error) { return "", errors.New("the server is gone") },
			wantErr: `read YDB feature flags: the server is gone`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			server := monitoringServing(c, "../../ydbflags/testdata/local-ydb-26.2.1.14.json", http.StatusOK)

			caps, _, err := infoFor(c, "ydbs://localhost:2135/local?monitoring="+server.URL, tc.ticket)

			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(caps, qt.DeepEquals, capability.Capabilities{})
			c.Assert(server.authorization, qt.HasLen, 0)
		})
	}
}

// Without the monitoring parameter nothing is read, the connection is not
// asked for its credential, and the preset stands.
func TestGetDatabaseInfoWithCapabilities_NoMonitoringEndpoint_HappyPath(t *testing.T) {
	c := qt.New(t)
	unasked := func(context.Context) (string, error) { return "", errors.New("the credential was asked for") }

	caps, resolution, err := infoFor(c, "ydb://localhost:2136/local", unasked)

	c.Assert(err, qt.IsNil)
	c.Assert(caps, qt.DeepEquals, resolution.Capabilities)
}

// The operator asked for the cluster's answer, so a page that cannot be read
// fails the connection rather than planning with the preset.
func TestGetDatabaseInfoWithCapabilities_FeatureFlags_FailurePath(t *testing.T) {
	c := qt.New(t)
	server := monitoringServing(c, "../../ydbflags/testdata/local-ydb-26.2.1.14.json",
		http.StatusServiceUnavailable)

	caps, _, err := infoFor(c, "ydb://localhost:2136/local?monitoring="+server.URL, nil)

	c.Assert(err, qt.ErrorMatches, `(?s).*503 Service Unavailable.*`)
	c.Assert(caps, qt.DeepEquals, capability.Capabilities{})
}
