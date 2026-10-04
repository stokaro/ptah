package ydburl_test

import (
	"net/url"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydburl"
)

func TestParse_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		raw          string
		wantSecure   bool
		wantEndpoint string
		wantDatabase string
		// wantMonitoring is the monitoring endpoint, empty for none.
		wantMonitoring string
		wantQuery      url.Values
	}{
		{
			name:         "plaintext defaults to 2136",
			raw:          "ydb://localhost/local",
			wantEndpoint: "localhost:2136",
			wantDatabase: "/local",
			wantQuery:    url.Values{},
		},
		{
			name:         "TLS defaults to 2135",
			raw:          "ydbs://ydb.example/ru-central1/b1g/etn",
			wantSecure:   true,
			wantEndpoint: "ydb.example:2135",
			wantDatabase: "/ru-central1/b1g/etn",
			wantQuery:    url.Values{},
		},
		{
			name:         "an explicit port and an IPv6 host",
			raw:          "ydb://[::1]:45136/local/",
			wantEndpoint: "[::1]:45136",
			wantDatabase: "/local",
			wantQuery:    url.Values{},
		},
		{
			name:         "the database parameter is taken out of the query",
			raw:          "ydb://h:2136/?database=local&go_balancer=disable",
			wantEndpoint: "h:2136",
			wantDatabase: "/local",
			wantQuery:    url.Values{"go_balancer": {"disable"}},
		},
		{
			name:         "the path and the parameter may name the same database",
			raw:          "YDB://h/local?database=/local/",
			wantEndpoint: "h:2136",
			wantDatabase: "/local",
			wantQuery:    url.Values{},
		},
		{
			name:         "no database at all",
			raw:          "ydb://h:2136",
			wantEndpoint: "h:2136",
			wantDatabase: "",
			wantQuery:    url.Values{},
		},
		{
			name:           "the monitoring endpoint is taken out of the query",
			raw:            "ydb://h:2136/local?monitoring=http://h:8765&go_balancer=disable",
			wantEndpoint:   "h:2136",
			wantDatabase:   "/local",
			wantMonitoring: "http://h:8765",
			wantQuery:      url.Values{"go_balancer": {"disable"}},
		},
		{
			name:           "a TLS monitoring endpoint keeps its scheme and drops a trailing slash",
			raw:            "ydbs://h/local?monitoring=https://mon.example:8765/",
			wantSecure:     true,
			wantEndpoint:   "h:2135",
			wantDatabase:   "/local",
			wantMonitoring: "https://mon.example:8765",
			wantQuery:      url.Values{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydburl.Parse(test.raw)

			c.Assert(err, qt.IsNil)
			c.Assert(got.Secure, qt.Equals, test.wantSecure)
			c.Assert(got.Endpoint(), qt.Equals, test.wantEndpoint)
			c.Assert(got.Database, qt.Equals, test.wantDatabase)
			c.Assert(monitoring(got), qt.Equals, test.wantMonitoring)
			c.Assert(got.Query, qt.DeepEquals, test.wantQuery)
		})
	}
}

// monitoring spells the parsed monitoring endpoint, or "" for none.
func monitoring(u ydburl.URL) string {
	if u.Monitoring == nil {
		return ""
	}
	return u.Monitoring.String()
}

// A realm is taken out of the query, and it moves the root a connection reads
// and writes into its directory under the database; without one the root is
// the database.
func TestParse_Realm_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		raw           string
		wantRealm     string
		wantRealmPath string
		wantRoot      string
		wantQuery     url.Values
	}{
		{
			name:          "a realm in the path's database",
			raw:           "ydb://h:2136/local?dev_realm=k3j9&go_balancer=disable",
			wantRealm:     "k3j9",
			wantRealmPath: "ptah_dev/k3j9",
			wantRoot:      "/local/ptah_dev/k3j9",
			wantQuery:     url.Values{"go_balancer": {"disable"}},
		},
		{
			name:          "a realm in the parameter's database",
			raw:           "ydbs://h/?database=/Root/app&dev_realm=run_2",
			wantRealm:     "run_2",
			wantRealmPath: "ptah_dev/run_2",
			wantRoot:      "/Root/app/ptah_dev/run_2",
			wantQuery:     url.Values{},
		},
		{
			name:          "no realm",
			raw:           "ydb://h/Root/app",
			wantRealm:     "",
			wantRealmPath: "",
			wantRoot:      "/Root/app",
			wantQuery:     url.Values{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydburl.Parse(test.raw)

			c.Assert(err, qt.IsNil)
			c.Assert(got.Realm, qt.Equals, test.wantRealm)
			c.Assert(got.RealmPath(), qt.Equals, test.wantRealmPath)
			c.Assert(got.Root(), qt.Equals, test.wantRoot)
			c.Assert(got.Query, qt.DeepEquals, test.wantQuery)
		})
	}
}

// WithRealm names the realm in place of any the URL names, and keeps the
// user, the database and every other parameter.
func TestWithRealm_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		// #nosec G101 -- a fixture with a made-up password, not credentials
		{
			name: "a URL with no realm",
			raw:  "ydb://alice:s3cret@h:2136/local?go_balancer=disable",
			want: "ydb://alice:s3cret@h:2136/local?dev_realm=r1&go_balancer=disable",
		},
		{
			name: "a URL with another realm",
			raw:  "ydbs://h/?database=/Root/app&dev_realm=old",
			want: "ydbs://h/?database=%2FRoot%2Fapp&dev_realm=r1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydburl.WithRealm(test.raw, "r1")

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A realm name that is not one, and a URL that cannot hold a realm, are
// refused rather than written.
func TestWithRealm_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		realm   string
		wantErr string
	}{
		{
			name:    "a realm that names a path",
			raw:     "ydb://h/local",
			realm:   "../x",
			wantErr: `the dev_realm parameter "../x" is not a realm name: use 1 to 64 lowercase letters, digits and underscores`,
		},
		{
			name:    "a URL with no database",
			raw:     "ydb://h:2136",
			realm:   "r1",
			wantErr: `the URL names a dev realm and no database to hold it`,
		},
		{
			name:    "not a YDB URL",
			raw:     "postgres://h/app",
			realm:   "r1",
			wantErr: `not a ydb:// or ydbs:// URL: "postgres"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydburl.WithRealm(test.raw, test.realm)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// WithoutRealm names the database that holds the realm, and leaves a URL that
// names none as it is written.
func TestWithoutRealm_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		// #nosec G101 -- a fixture with a made-up password, not credentials
		{
			name: "a URL with a realm",
			raw:  "ydb://alice:s3cret@h:2136/local?dev_realm=r1&go_balancer=disable",
			want: "ydb://alice:s3cret@h:2136/local?go_balancer=disable",
		},
		{
			name: "a URL with none",
			raw:  "ydbs://h/?database=/Root/app&go_balancer=disable",
			want: "ydbs://h/?database=/Root/app&go_balancer=disable",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydburl.WithoutRealm(test.raw)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A URL that is not a YDB one is refused rather than rewritten.
func TestWithoutRealm_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := ydburl.WithoutRealm("postgres://h/app?dev_realm=r1")

	c.Assert(err, qt.ErrorMatches, `not a ydb:// or ydbs:// URL: "postgres"`)
	c.Assert(got, qt.Equals, "")
}

func TestParse_KeepsTheUser(t *testing.T) {
	c := qt.New(t)

	got, err := ydburl.Parse("ydb://alice:s3cret@h:2136/local")

	c.Assert(err, qt.IsNil)
	c.Assert(got.User.Username(), qt.Equals, "alice")
	password, set := got.User.Password()
	c.Assert(set, qt.IsTrue)
	c.Assert(password, qt.Equals, "s3cret")
}

func TestParse_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name:    "grpc is not a YDB scheme",
			raw:     "grpc://h:2136/local",
			wantErr: `not a ydb:// or ydbs:// URL: "grpc"`,
		},
		{
			name:    "no host",
			raw:     "ydb:///local",
			wantErr: `a YDB URL needs a host: write ydb://host:2136/database`,
		},
		{
			name:    "an opaque URL has no host",
			raw:     "ydb:local",
			wantErr: `a YDB URL needs a host: write ydb://host:2136/database`,
		},
		{
			name:    "the path and the parameter disagree",
			raw:     "ydb://h/local?database=/dev",
			wantErr: `the URL names database /local in its path and /dev in the database parameter; name it once`,
		},
		{
			name:    "the parameter is given twice",
			raw:     "ydb://h/?database=/a&database=/a",
			wantErr: `the database parameter is given more than once`,
		},
		{
			name:    "the parameter is empty",
			raw:     "ydb://h/?database=/",
			wantErr: `the database parameter is empty`,
		},
		{
			name:    "the monitoring parameter is empty",
			raw:     "ydb://h/local?monitoring=",
			wantErr: `the monitoring parameter is empty: write monitoring=http://host:8765`,
		},
		{
			name:    "the monitoring parameter is given twice",
			raw:     "ydb://h/local?monitoring=http://h:8765&monitoring=http://h:8765",
			wantErr: `the monitoring parameter is given more than once`,
		},
		{
			name:    "the monitoring parameter names no scheme",
			raw:     "ydb://h/local?monitoring=h:8765",
			wantErr: `the monitoring parameter names no http:// or https:// endpoint: write monitoring=http://host:8765`,
		},
		{
			name:    "the monitoring parameter names the gRPC endpoint",
			raw:     "ydb://h/local?monitoring=grpc://h:2136",
			wantErr: `the monitoring parameter names no http:// or https:// endpoint: write monitoring=http://host:8765`,
		},
		{
			name:    "the monitoring parameter names no host",
			raw:     "ydb://h/local?monitoring=http:///viewer",
			wantErr: `the monitoring parameter names no host: write monitoring=http://host:8765`,
		},
		{
			name: "the monitoring parameter carries a user",
			raw:  "ydb://h/local?monitoring=http://viewer@h:8765",
			wantErr: `the monitoring parameter for http://h:8765 carries a user; Ptah reads that endpoint with ` +
				`the connection's own credential, so name the endpoint only, as monitoring=http://h:8765`,
		},
		{
			name: "the monitoring parameter names the page rather than the endpoint",
			raw:  "ydb://h/local?monitoring=http://h:8765/viewer/json/feature_flags",
			wantErr: `the monitoring parameter for http://h:8765 names a page; ` +
				`name the endpoint only, as monitoring=http://h:8765`,
		},
		{
			name: "the monitoring parameter names an escaped slash as its page",
			raw:  "ydb://h/local?monitoring=http://h:8765/%252F",
			wantErr: `the monitoring parameter for http://h:8765 names a page; ` +
				`name the endpoint only, as monitoring=http://h:8765`,
		},
		{
			name:    "the monitoring parameter is not a URL",
			raw:     "ydb://h/local?monitoring=http://h:87%2565",
			wantErr: `the monitoring parameter is not a URL: write monitoring=http://host:8765`,
		},
		{
			name:    "the realm parameter is empty",
			raw:     "ydb://h/local?dev_realm=",
			wantErr: `the dev_realm parameter "" is not a realm name: use 1 to 64 lowercase letters, digits and underscores`,
		},
		{
			name:    "the realm parameter names a path",
			raw:     "ydb://h/local?dev_realm=a/b",
			wantErr: `the dev_realm parameter "a/b" is not a realm name: use 1 to 64 lowercase letters, digits and underscores`,
		},
		{
			name:    "the realm parameter leaves the realm directory",
			raw:     "ydb://h/local?dev_realm=..",
			wantErr: `the dev_realm parameter ".." is not a realm name: use 1 to 64 lowercase letters, digits and underscores`,
		},
		{
			name:    "the realm parameter is not lowercase",
			raw:     "ydb://h/local?dev_realm=Run1",
			wantErr: `the dev_realm parameter "Run1" is not a realm name: use 1 to 64 lowercase letters, digits and underscores`,
		},
		{
			name: "the realm parameter is too long",
			raw:  "ydb://h/local?dev_realm=" + strings.Repeat("a", 65),
			wantErr: `the dev_realm parameter "` + strings.Repeat("a", 65) +
				`" is not a realm name: use 1 to 64 lowercase letters, digits and underscores`,
		},
		{
			name:    "the realm parameter is given twice",
			raw:     "ydb://h/local?dev_realm=a&dev_realm=a",
			wantErr: `the dev_realm parameter is given more than once`,
		},
		{
			name:    "a realm with no database to hold it",
			raw:     "ydb://h:2136?dev_realm=a",
			wantErr: `the URL names a dev realm and no database to hold it`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydburl.Parse(test.raw)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.DeepEquals, ydburl.URL{})
		})
	}
}

// A monitoring value can carry a password in its user part or a token in its
// query, and a refusal reaches logs and terminals no URL redactor reads. No
// refusal repeats the secret, whichever check refused the value; each row
// reaches a different one.
func TestParse_FailurePath_NeverRepeatsTheMonitoringSecret(t *testing.T) {
	const secret = "s3cret"
	for _, test := range []struct {
		name    string
		raw     string
		wantErr string
	}{
		// #nosec G101 -- fixtures with a made-up password, not credentials
		{
			name:    "not a URL",
			raw:     "ydb://h/local?monitoring=http://viewer:" + secret + "@h:87%2565",
			wantErr: `the monitoring parameter is not a URL: .*`,
		},
		{
			name:    "another scheme",
			raw:     "ydb://h/local?monitoring=ftp://viewer:" + secret + "@h:8765",
			wantErr: `the monitoring parameter names no http:// or https:// endpoint: .*`,
		},
		{
			name:    "no host",
			raw:     "ydb://h/local?monitoring=http://viewer:" + secret + "@/viewer",
			wantErr: `the monitoring parameter names no host: .*`,
		},
		{
			name:    "a user",
			raw:     "ydb://h/local?monitoring=http://viewer:" + secret + "@h:8765",
			wantErr: `the monitoring parameter for http://h:8765 carries a user; .*`,
		},
		{
			name:    "a page",
			raw:     "ydb://h/local?monitoring=http://h:8765/viewer%3Ftoken%3D" + secret,
			wantErr: `the monitoring parameter for http://h:8765 names a page; .*`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydburl.Parse(test.raw)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err.Error(), qt.Not(qt.Contains), secret)
			c.Assert(got, qt.DeepEquals, ydburl.URL{})
		})
	}
}
