//go:build integration

package providercli_test

import (
	"encoding/json"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
)

// mysqlProviderID is the provider the fixture program reports when it serves
// the built-in engine name mysql.
const mysqlProviderID = "example.com/ptah-mysql-fixture"

// caBundleRefusal is the reason a URL with ssl-ca is not handed to a provider.
const caBundleRefusal = "the database URL sets ssl-ca, which only Ptah's built-in MySQL connector applies, " +
	"and a provider serves this URL's engine; remove ssl-ca, or remove the provider's claim on the engine from the provider manifest"

// caBundleQueries spell ssl-ca two ways that name the same parameter.
var caBundleQueries = []string{"ssl-ca=/etc/mysql/ca.pem", "SSL_CA=/etc/mysql/ca.pem"}

// mysqlProviderCLI is the CLI over the common-objects program serving the
// built-in engine name mysql, which the manifest claims. The program opens
// the SQLite file a mysql:// URL's path names and reads none of the URL's
// parameters, as a provider that does not apply ssl-ca would.
func mysqlProviderCLI(c *qt.C) providerCLI {
	c.Helper()
	spec := fixtureReleaseSpec(c, "common-objects", testutils.FixtureRelease{})
	spec.ProviderID = mysqlProviderID
	spec.Environment = append(spec.Environment, "FIXTURE_DEV_ENGINE=mysql", "FIXTURE_DEV_PROVIDER_ID="+mysqlProviderID)
	f := newProviderCLI(c, spec)
	spec.Environment = append(slices.Clone(spec.Environment), "PTAH_FIXTURE_PID_FILE="+f.pidPath)
	manifest, err := json.Marshal(providerconfig.Configuration{FormatVersion: providerconfig.FormatVersion,
		Providers: []providerconfig.Installation{{Spec: spec, Engines: []providerconfig.Engine{{ID: "mysql"}}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(f.manifestPath, manifest, 0o600), qt.IsNil)
	f.address = (&url.URL{Scheme: "mysql", Path: "/" + strings.TrimPrefix(filepath.ToSlash(f.databasePath), "/")}).String()
	return f
}

// mysqlProviderSelections are the two ways a provider comes to serve
// mysql://: the manifest claims the engine, or the command names it.
var mysqlProviderSelections = []struct {
	name string
	args []string
}{
	{name: "a claimed engine"},
	{name: "an explicit engine", args: []string{"--provider-engine", "mysql"}},
}

// TestProviderOfMySQLReadsAURL_HappyPath is the control for the refusal
// below: the provider serves a mysql:// URL that names no bundle, so the
// refusal is about ssl-ca and not about the engine.
func TestProviderOfMySQLReadsAURL_HappyPath(t *testing.T) {
	for _, selection := range mysqlProviderSelections {
		t.Run(selection.name, func(t *testing.T) {
			c := qt.New(t)
			f := mysqlProviderCLI(c)

			_, stderr, code := executeProviderCLI(c.Context(),
				slices.Concat([]string{"db", "read", "--provider-config", f.manifestPath, "--db-url", f.address}, selection.args))

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
			_, err := os.Stat(f.databasePath)
			c.Assert(err, qt.IsNil)
		})
	}
}

// TestProviderOfMySQLRefusesACABundle_FailurePath refuses a URL with ssl-ca
// before the provider is handed it. Only Ptah's MySQL connector applies the
// bundle, and this provider would open the database without reading it. The
// database file does not exist afterwards, so the provider never opened it.
func TestProviderOfMySQLRefusesACABundle_FailurePath(t *testing.T) {
	for _, selection := range mysqlProviderSelections {
		for _, query := range caBundleQueries {
			t.Run(selection.name+"/"+query, func(t *testing.T) {
				c := qt.New(t)
				f := mysqlProviderCLI(c)

				_, stderr, code := executeProviderCLI(c.Context(),
					slices.Concat([]string{"db", "read", "--provider-config", f.manifestPath, "--db-url", f.address + "?" + query}, selection.args))

				c.Assert(code, qt.Equals, 2, qt.Commentf("%s", stderr))
				c.Assert(stderr, qt.Contains, caBundleRefusal)
				_, err := os.Stat(f.databasePath)
				c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
			})
		}
	}
}

// mysqlContextRuntime is a runtime over the value-context program serving the
// engine name mysql. Its provider interprets the database a mysql:// URL
// names and reads none of the URL's parameters, so it would bind a context
// to a URL whose ssl-ca it never applies.
func mysqlContextRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	_, runtime := yamlProviderEnvironment(c, "value-context", []string{"FIXTURE_DEV_ENGINE=mysql"})
	return runtime
}

// mysqlFileAddress is a mysql:// URL whose path names the file path, with the
// query appended when there is one.
func mysqlFileAddress(host, path, query string) string {
	return (&url.URL{Scheme: "mysql", Host: host, Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/"), RawQuery: query}).String()
}

// TestProviderOfMySQLResolvesADatabaseContext_HappyPath is the control for
// the refusal below: the provider binds a context to a mysql:// URL that names
// no bundle, so the refusal is about ssl-ca and not about the engine.
func TestProviderOfMySQLResolvesADatabaseContext_HappyPath(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "shop.db")

	bound, err := mysqlContextRuntime(c).ResolveDatabaseContext(c.Context(),
		schemaext.DatabaseContextRequest{Target: "mysql", Address: mysqlFileAddress("", path, "")})

	c.Assert(err, qt.IsNil)
	c.Assert(bound.Database, qt.Equals, path)
}

// TestProviderOfMySQLRefusesACABundleForADatabaseContext_FailurePath refuses
// a URL with ssl-ca before the provider is asked what database it names.
func TestProviderOfMySQLRefusesACABundleForADatabaseContext_FailurePath(t *testing.T) {
	for _, query := range caBundleQueries {
		t.Run(query, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "shop.db")

			bound, err := mysqlContextRuntime(c).ResolveDatabaseContext(c.Context(),
				schemaext.DatabaseContextRequest{Target: "mysql", Address: mysqlFileAddress("", path, query)})

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(caBundleRefusal))
			c.Assert(bound.IsZero(), qt.IsTrue)
		})
	}
}

// mysqlRealmOwner is the dev database service of the devdb program serving
// the engine name mysql. Its provider creates a realm beside the database a
// mysql:// URL with a host names and reads none of the URL's parameters.
func mysqlRealmOwner(c *qt.C) *providerhost.DevDatabaseOwner {
	c.Helper()
	host, _ := yamlProviderEnvironment(c, "devdb", []string{"FIXTURE_DEV_ENGINE=mysql", "FIXTURE_DEV_DIR=" + c.TempDir()})
	owner := host.DevDatabases()
	c.Assert(owner.RealmSchemes(), qt.DeepEquals, []string{"mysql"})
	return owner
}

// TestProviderOfMySQLCreatesADevRealm_HappyPath is the control for the
// refusal below: the provider creates a realm inside the database a mysql://
// URL that names no bundle names.
func TestProviderOfMySQLCreatesADevRealm_HappyPath(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "shop.db")

	realm, release, err := mysqlRealmOwner(c).EnterRealm(c.Context(), mysqlFileAddress("localhost", path, ""))

	c.Assert(err, qt.IsNil)
	c.Cleanup(release)
	c.Assert(realm, qt.Equals, mysqlFileAddress("localhost", path+".lease-1", ""))
}

// TestProviderOfMySQLRefusesACABundleForADevRealm_FailurePath refuses a URL
// with ssl-ca before the provider is asked for a realm inside its database.
func TestProviderOfMySQLRefusesACABundleForADevRealm_FailurePath(t *testing.T) {
	for _, query := range caBundleQueries {
		t.Run(query, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "shop.db")

			realm, release, err := mysqlRealmOwner(c).EnterRealm(c.Context(), mysqlFileAddress("localhost", path, query))

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(caBundleRefusal))
			c.Assert(realm, qt.Equals, "")
			c.Assert(release, qt.IsNil)
		})
	}
}
