package mysqlconnect

// White-box testing required: the DSN go-sql-driver receives, the product a banner names and the session foreign-key policy are computed by unexported helpers, and no exported result reports them without a live server.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/connector"
)

func TestRefineMySQLForeignKeyCapabilities_RestrictionEnabled(t *testing.T) {
	c := qt.New(t)
	probeCalls := 0

	got, err := refineMySQLForeignKeyCapabilities(
		"mysql",
		capability.MySQL84(),
		capability.MySQL84(),
		func(destination ...any) error {
			probeCalls++
			value := destination[0].(*int64)
			*value = 1
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(probeCalls, qt.Equals, 1)
	c.Assert(got.Has(capability.ForeignKeysRequireUniqueReference), qt.IsTrue)
	c.Assert(got.Has(capability.ForeignKeysRequireIndexedReference), qt.IsFalse)
	c.Assert(got.Validate(), qt.IsNil)
}

func TestRefineMySQLForeignKeyCapabilities_RestrictionDisabled(t *testing.T) {
	c := qt.New(t)

	got, err := refineMySQLForeignKeyCapabilities(
		"mysql",
		capability.MySQL84(),
		capability.MySQL84(),
		func(destination ...any) error {
			value := destination[0].(*int64)
			*value = 0
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(got.Has(capability.ForeignKeysRequireUniqueReference), qt.IsFalse)
	c.Assert(got.Has(capability.ForeignKeysRequireIndexedReference), qt.IsTrue)
	c.Assert(got.Validate(), qt.IsNil)
}

func TestRefineMySQLForeignKeyCapabilities_LegacyPresetSkipsProbe(t *testing.T) {
	c := qt.New(t)
	probeCalls := 0

	got, err := refineMySQLForeignKeyCapabilities(
		"mysql",
		capability.MySQL8019(),
		capability.MySQL8019(),
		func(...any) error {
			probeCalls++
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(probeCalls, qt.Equals, 0)
	c.Assert(got, qt.DeepEquals, capability.MySQL8019())
}

func TestRefineMySQLForeignKeyCapabilities_RestrictionEnabledRestoresStrictPolicy(t *testing.T) {
	c := qt.New(t)

	got, err := refineMySQLForeignKeyCapabilities(
		"mysql",
		capability.MySQL84().
			With(capability.ForeignKeysRequireUniqueReference, false).
			With(capability.ForeignKeysRequireIndexedReference, true),
		capability.MySQL84(),
		func(destination ...any) error {
			value := destination[0].(*int64)
			*value = 1
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(got.Has(capability.ForeignKeysRequireUniqueReference), qt.IsTrue)
	c.Assert(got.Has(capability.ForeignKeysRequireIndexedReference), qt.IsFalse)
	c.Assert(got.Validate(), qt.IsNil)
}

func TestRefineMySQLForeignKeyCapabilities_ProbeFailure(t *testing.T) {
	c := qt.New(t)

	got, err := refineMySQLForeignKeyCapabilities(
		"mysql",
		capability.MySQL84(),
		capability.MySQL84(),
		func(...any) error { return driver.ErrBadConn },
	)

	c.Assert(err, qt.ErrorMatches, `query restrict_fk_on_non_standard_key: driver: bad connection`)
	c.Assert(got.Len(), qt.Equals, 0)
}

// TestDetectMySQLWireDialect reads the product from the banner alone. The
// banners are the ones MySQL 8.4.11, MySQL 9.7.0, MariaDB 10.11.15 and MariaDB
// 11.8.9 answer with; the generic one is a server announcing neither, which is
// MySQL whatever scheme reached it, as it is to the pinned community binary
// (stokaro/ptah#3756).
func TestDetectMySQLWireDialect(t *testing.T) {
	tests := []struct {
		name     string
		version  string
		expected string
	}{
		{name: "MySQL 9.7", version: "9.7.0", expected: "mysql"},
		{name: "MySQL 8.4", version: "8.4.11", expected: "mysql"},
		{name: "MariaDB 10.11", version: "10.11.15-MariaDB-ubu2204", expected: "mariadb"},
		{name: "MariaDB 11.8", version: "11.8.9-MariaDB-ubu2404", expected: "mariadb"},
		{name: "MariaDB behind the replication prefix", version: "5.5.5-10.11.15-MariaDB-ubu2204", expected: "mariadb"},
		{name: "a server that does not announce MariaDB", version: "MySQL-compatible server", expected: "mysql"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(detectMySQLWireDialect(tt.version), qt.Equals, tt.expected)
		})
	}
}

// TestConvertMySQLURL_KeepsAUnixSocketTarget pins the network form the TCP
// branch already had.
//
// go-sql-driver takes tcp(...) and unix(...) as two spellings of one thing: the
// network and the address to reach it on. Only the first was recognized here
// and in the scheme detection above it, so a valid socket address was parsed as
// a host called "unix(" with the socket path folded into the database name.
// Nothing about that failure named the socket.
func TestConvertMySQLURL_KeepsAUnixSocketTarget(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "a socket target keeps its network",
			url:  "mysql://user:pass@unix(/tmp/mysql.sock)/db",
			want: "user:pass@unix(/tmp/mysql.sock)/db",
		},
		{
			name: "a mariadb socket target too",
			url:  "mariadb://user:pass@unix(/var/run/mysqld/mysqld.sock)/db",
			want: "user:pass@unix(/var/run/mysqld/mysqld.sock)/db",
		},
		{
			name: "the TCP spelling is unchanged",
			url:  "mysql://user:pass@tcp(127.0.0.1:3306)/db",
			want: "user:pass@tcp(127.0.0.1:3306)/db",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(convertMySQLURL(test.url), qt.Equals, test.want)
		})
	}
}

// TestConvertMySQLURL_ConvertsANetworkDSNWithoutCredentials pins the other
// half of stokaro/ptah#1540: the parser above accepted these two targets while
// the converter did not recognize them, and ConnectToDatabase calls both in
// sequence.
//
// What the caller saw was not a refusal. The TCP target reached the driver
// with "mysql://" still attached, and the socket target reached it as
// ":@tcp(unix()/tmp/mysql.sock)/shop" -- an address assembled out of a host
// that was never a host. Both fail at the driver, naming a DSN the caller
// never wrote.
func TestConvertMySQLURL_ConvertsANetworkDSNWithoutCredentials(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "a TCP target with no credentials", url: "mysql://tcp(localhost:3306)/shop", want: "tcp(localhost:3306)/shop"},
		{name: "a socket target with no credentials", url: "mysql://unix(/tmp/mysql.sock)/shop", want: "unix(/tmp/mysql.sock)/shop"},
		{name: "the MariaDB scheme reads the same", url: "mariadb://tcp(localhost:3306)/shop", want: "tcp(localhost:3306)/shop"},
		{name: "the maria spelling reads the same", url: "maria://user:pass@tcp(localhost:3306)/shop", want: "user:pass@tcp(localhost:3306)/shop"},
		{name: "a scheme in upper case reads the same", url: "MARIA://unix(/tmp/mysql.sock)/shop", want: "unix(/tmp/mysql.sock)/shop"},
		{name: "a socket URL becomes the driver's socket form", url: "mysql+unix://user:pass@/tmp/mysql.sock?database=shop", want: "user:pass@unix(/tmp/mysql.sock)/shop"},
		{name: "a socket URL keeps its other parameters", url: "maria+unix://user@/tmp/mysql.sock?database=shop&parseTime=true", want: "user@unix(/tmp/mysql.sock)/shop?parseTime=true"},
		{name: "credentials are still carried", url: "mysql://user:pass@tcp(localhost:3306)/shop", want: "user:pass@tcp(localhost:3306)/shop"},
		{name: "a driver DSN with no scheme is left alone", url: "tcp(localhost:3306)/shop", want: "tcp(localhost:3306)/shop"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(convertMySQLURL(test.url), qt.Equals, test.want)
		})
	}
}

// TestMySQLDatabaseAgreesBetweenParserAndConverter is the control that keeps
// the two ends of [ConnectToDatabase] from drifting apart.
//
// parseDatabaseURL says which database getDatabaseInfo reports, and
// convertMySQLURL says which database the driver connects to. The expected
// side is the driver's own reading of the DSN, not the parser both functions
// share: a control that asked that parser twice would agree with itself and
// stay green while one end stopped calling it, which is the state
// stokaro/ptah#1540 reported.
func TestMySQLDatabaseAgreesBetweenParserAndConverter(t *testing.T) {
	addresses := []string{
		"mysql://tcp(localhost:3306)/shop",
		"mysql://unix(/tmp/mysql.sock)/shop",
		"mysql://user:pass@tcp(localhost:3306)/shop",
		"mysql://user:pass@unix(/tmp/mysql.sock)/shop",
		"mariadb://tcp(localhost:3306)/shop",
		"maria://user:pass@tcp(localhost:3306)/shop",
		"MARIA://unix(/tmp/mysql.sock)/shop",
		"mysql://user:pass@localhost:3306/shop",
		"maria://user:pass@localhost:3306/shop",
		"mysql+unix://user:pass@/tmp/mysql.sock?database=shop",
		"MARIA+UNIX://user@/run/mysqld/mysqld.sock?database=shop&parseTime=true",
		"mariadb+unix://user:pass@/tmp/mysql.sock",
		"mysql://user:pass@localhost:3306/my%2Fshop",
	}

	for _, address := range addresses {
		t.Run(address, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := connector.ParseURL(address)
			c.Assert(err, qt.IsNil)
			driverConfig, err := mysqldriver.ParseDSN(convertMySQLURL(address))
			c.Assert(err, qt.IsNil)

			c.Assert(strings.TrimPrefix(parsed.Path, "/"), qt.Equals, driverConfig.DBName)
		})
	}
}
