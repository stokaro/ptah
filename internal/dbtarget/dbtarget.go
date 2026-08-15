// Package dbtarget resolves the address of a live database for an integration
// test, from one declaration of which environment variable names an engine.
//
// It exists because the alternative was measured and is worse. Before this
// package, 129 test files each decided for themselves where to look, and the
// names drifted: PostgreSQL answered to POSTGRES_TEST_DSN, POSTGRES_URL and
// TEST_DATABASE_URL, MySQL to four spellings, and one CI step set 34 variables
// so that whichever a test happened to read would be present. A test written
// against a name that step did not set would skip in silence, and a skip reads
// as a pass, so the run stayed green while covering less than it claimed.
//
// The rule this package makes enforceable is that a test asks for an engine
// and never for a variable. A new test cannot invent a spelling, because there
// is nowhere to invent one; adding an engine or a synonym is an edit here.
package dbtarget

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
)

// Engine names a database an integration test can ask for.
type Engine int

// The engines the repository has live coverage for.
//
// MySQLAdmin is not MySQL under another name: it is the same server reached
// with an account that may create and drop databases, which the tests that
// exercise provisioning need and the rest must not have.
const (
	PostgreSQL Engine = iota
	MySQL
	MySQLAdmin
	MariaDB
	MariaDBAdmin
	ClickHouse
	SQLServer
	CockroachDB
	YugabyteDB
)

// source is where one engine's address comes from.
type source struct {
	// canonical is the variable a reader should set, and the one every
	// diagnostic names.
	canonical string
	// synonyms are older spellings still honored, in the order they are tried.
	// They exist so that a checkout configured before this package keeps
	// working; nothing new should be added here.
	synonyms []string
	// scheme, when set, is the URL scheme an address must carry. It catches
	// the configuration mistake this package cannot otherwise see: one engine's
	// address left in another engine's variable, which without the check
	// surfaces as a connection error deep inside a test.
	scheme []string
}

var sources = map[Engine]source{
	PostgreSQL: {
		canonical: "POSTGRES_TEST_DSN",
		synonyms:  []string{"POSTGRES_URL", "POSTGRES_TEST_URL", "TEST_DATABASE_URL"},
		scheme:    []string{"postgres", "postgresql"},
	},
	// The MySQL family declares its schemes because the two engines speak one
	// wire protocol: a MariaDB address left in a MySQL variable connects, and
	// the run reports MySQL coverage it never had. MariaDB also accepts the
	// mysql spelling, which is how its address is routinely written and which
	// says nothing false -- only the reverse direction is a lie about which
	// engine was covered.
	MySQL: {
		canonical: "MYSQL_TEST_URL",
		synonyms:  []string{"MYSQL_URL", "MYSQL_TEST_DSN"},
		scheme:    []string{"mysql"},
	},
	MySQLAdmin: {
		canonical: "MYSQL_ADMIN_TEST_URL",
		synonyms:  []string{"MYSQL_ADMIN_TEST_DSN"},
		scheme:    []string{"mysql"},
	},
	MariaDB: {
		canonical: "MARIADB_TEST_URL",
		synonyms:  []string{"MARIADB_URL", "MARIADB_TEST_DSN"},
		scheme:    []string{"mariadb", "mysql"},
	},
	MariaDBAdmin: {
		canonical: "MARIADB_ADMIN_TEST_URL",
		synonyms:  []string{"MARIADB_ADMIN_TEST_DSN"},
		scheme:    []string{"mariadb", "mysql"},
	},
	ClickHouse: {
		canonical: "CLICKHOUSE_URL",
		scheme:    []string{"clickhouse"},
	},
	SQLServer: {
		canonical: "PTAH_SQLSERVER_TEST_URL",
		synonyms:  []string{"SQLSERVER_TEST_DSN"},
		scheme:    []string{"sqlserver"},
	},
	CockroachDB: {
		canonical: "COCKROACHDB_URL",
		synonyms:  []string{"COCKROACHDB_TEST_DSN"},
		scheme:    []string{"postgres", "postgresql", "cockroachdb"},
	},
	YugabyteDB: {
		canonical: "YUGABYTEDB_URL",
		synonyms:  []string{"YUGABYTEDB_TEST_DSN"},
		scheme:    []string{"postgres", "postgresql", "yugabytedb"},
	},
}

// String names the engine as its canonical variable does.
func (e Engine) String() string {
	if s, ok := sources[e]; ok {
		return s.canonical
	}
	return fmt.Sprintf("Engine(%d)", int(e))
}

// URL returns the address of a live engine for this run, and skips the test
// when none is configured.
//
// The skip names the canonical variable, so a reader who sees it knows what to
// set. It does not list the synonyms: they exist for checkouts configured
// before this package, and telling a new reader about them would spread the
// spellings this package exists to collapse.
func URL(tb testing.TB, engine Engine) string {
	tb.Helper()

	address, err := Lookup(engine)
	if err != nil {
		tb.Fatalf("dbtarget: %v", err)
	}
	if address == "" {
		tb.Skipf("dbtarget: set %s to run this test against a live %s", engine, engineName(engine))
	}
	return address
}

// Lookup returns the address configured for an engine, or the empty string
// when none is. It is for the callers that decide their own skip, and for the
// gate that checks CI configures what it claims to.
//
// A value carrying another engine's scheme is an error rather than an empty
// answer: the variable was set, so silently skipping would report the run as
// covering an engine nobody configured.
func Lookup(engine Engine) (string, error) {
	src, ok := sources[engine]
	if !ok {
		return "", fmt.Errorf("unknown engine %d", int(engine))
	}

	for _, name := range append([]string{src.canonical}, src.synonyms...) {
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			continue
		}
		if err := checkScheme(src, name, value); err != nil {
			return "", err
		}
		return withScheme(src, value), nil
	}
	return "", nil
}

// withScheme adds the engine's own scheme to an address that carries none.
//
// A synonym spelled _DSN holds the driver form, and this function's callers
// hand what they get to dbschema.ConnectToDatabase, which refuses a schemeless
// address. Returning the raw value made a supported legacy spelling fail the
// run instead of connecting. The first declared scheme is the engine's own;
// the rest are spellings it also answers to.
func withScheme(src source, value string) string {
	if len(src.scheme) == 0 || strings.Contains(value, "://") {
		return value
	}
	return src.scheme[0] + "://" + value
}

// checkScheme refuses an address whose scheme is not one the engine speaks.
func checkScheme(src source, name, value string) error {
	if len(src.scheme) == 0 {
		return nil
	}
	scheme, _, found := strings.Cut(value, "://")
	if !found {
		// A driver-specific DSN carries no scheme. MySQL's is the shape this
		// allows for, and an engine that wants schemes checked at all still
		// accepts one rather than guessing at the value's dialect.
		return nil
	}
	if slices.Contains(src.scheme, scheme) {
		return nil
	}
	return fmt.Errorf("%s carries scheme %q, which %s does not speak; it names %s",
		name, scheme, src.canonical, strings.Join(src.scheme, " or "))
}

// engineName renders an engine for a human-facing message.
func engineName(engine Engine) string {
	switch engine {
	case PostgreSQL:
		return "PostgreSQL"
	case MySQL:
		return "MySQL"
	case MySQLAdmin:
		return "MySQL with an administrative account"
	case MariaDB:
		return "MariaDB"
	case MariaDBAdmin:
		return "MariaDB with an administrative account"
	case ClickHouse:
		return "ClickHouse"
	case SQLServer:
		return "SQL Server"
	case CockroachDB:
		return "CockroachDB"
	case YugabyteDB:
		return "YugabyteDB"
	}
	return engine.String()
}

// DriverDSN returns the address in the form a raw database/sql driver parses,
// and skips the test when none is configured.
//
// It exists because two different consumers want two different strings for the
// same server. Ptah connects through a URL carrying the engine's scheme, and
// that is what URL answers with. go-sql-driver/mysql reads a mysql:// prefix
// as part of the username, and pgx does not parse cockroachdb:// at all, so a
// test opening the driver directly needs the scheme gone.
//
// The DSN spelling of a variable is preferred when one is set, because that is
// a value someone wrote in driver form deliberately. Otherwise the scheme is
// removed from the URL form, which is exactly the transformation the two
// spellings differ by.
func DriverDSN(tb testing.TB, engine Engine) string {
	tb.Helper()

	address, err := LookupDriverDSN(engine)
	if err != nil {
		tb.Fatalf("dbtarget: %v", err)
	}
	if address == "" {
		tb.Skipf("dbtarget: set %s to run this test against a live %s", engine, engineName(engine))
	}
	return address
}

// LookupDriverDSN returns the driver-form address, or the empty string when
// none is configured.
func LookupDriverDSN(engine Engine) (string, error) {
	src, ok := sources[engine]
	if !ok {
		return "", fmt.Errorf("unknown engine %d", int(engine))
	}

	// A variable whose name ends in _DSN holds a value someone wrote for the
	// driver. Prefer it over deriving one.
	for _, name := range append([]string{src.canonical}, src.synonyms...) {
		if !strings.HasSuffix(name, "_DSN") {
			continue
		}
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value, nil
		}
	}

	address, err := Lookup(engine)
	if err != nil || address == "" {
		return "", err
	}
	if mysqlFamily(engine) {
		return mysqlNetworkDSN(address), nil
	}
	return stripScheme(address), nil
}

// mysqlFamily reports whether an engine is served by go-sql-driver/mysql,
// which is the one driver here that wants a network form rather than a host.
func mysqlFamily(engine Engine) bool {
	switch engine {
	case MySQL, MySQLAdmin, MariaDB, MariaDBAdmin:
		return true
	}
	return false
}

// mysqlNetworkDSN renders a MySQL-family address in the form
// go-sql-driver/mysql parses: user:pass@tcp(host:port)/db.
//
// Removing the scheme is not enough. A conventional mysql://root:pass@host/db
// becomes root:pass@host/db, which that driver does not accept, so an address
// Ptah itself resolves failed to open unless the operator happened to have
// written the driver's own network form already. An address that carries one
// is returned as it stands, because it is already what the driver wants and
// re-rendering it would only be a chance to lose a parameter.
func mysqlNetworkDSN(address string) string {
	_, rest, found := strings.Cut(address, "://")
	if !found {
		rest = address
	}
	if strings.Contains(rest, "@tcp(") || strings.Contains(rest, "@unix(") {
		return rest
	}
	credentials, remainder, found := strings.Cut(rest, "@")
	if !found {
		return rest
	}
	host, path, hasPath := strings.Cut(remainder, "/")
	if host == "" {
		return rest
	}
	if !strings.Contains(host, ":") {
		// The driver's own default, which a URL omitting the port means.
		host += ":3306"
	}
	rendered := credentials + "@tcp(" + host + ")"
	if hasPath {
		rendered += "/" + path
	}
	return rendered
}

// stripScheme removes a URL scheme, leaving what a raw driver parses.
//
// A value with no scheme is already in driver form and is returned unchanged;
// PostgreSQL is left alone whatever its scheme, because pgx parses postgres://
// and postgresql:// and stripping either would break it.
func stripScheme(address string) string {
	scheme, rest, found := strings.Cut(address, "://")
	if !found {
		return address
	}
	switch scheme {
	case "postgres", "postgresql":
		return address
	}
	return rest
}

// Engines returns every engine this package knows, in declaration order, so a
// gate can enumerate them without repeating the list.
func Engines() []Engine {
	return []Engine{
		PostgreSQL, MySQL, MySQLAdmin, MariaDB, MariaDBAdmin,
		ClickHouse, SQLServer, CockroachDB, YugabyteDB,
	}
}

// Variables returns every variable name an engine answers to, canonical first.
// The gate that checks a workflow configures what it claims uses this.
func Variables(engine Engine) []string {
	src, ok := sources[engine]
	if !ok {
		return nil
	}
	return append([]string{src.canonical}, src.synonyms...)
}
