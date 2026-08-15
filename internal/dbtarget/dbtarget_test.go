package dbtarget_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbtarget"
)

func TestLookup_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		set    func(t *testing.T)
		want   string
	}{
		{
			name:   "canonical variable",
			engine: dbtarget.PostgreSQL,
			set:    func(t *testing.T) { t.Setenv("POSTGRES_TEST_DSN", "postgres://localhost/a") },
			want:   "postgres://localhost/a",
		},
		{
			name:   "synonym when the canonical one is unset",
			engine: dbtarget.PostgreSQL,
			set:    func(t *testing.T) { t.Setenv("TEST_DATABASE_URL", "postgres://localhost/b") },
			want:   "postgres://localhost/b",
		},
		{
			name:   "surrounding whitespace is not an address",
			engine: dbtarget.ClickHouse,
			set:    func(t *testing.T) { t.Setenv("CLICKHOUSE_URL", "  clickhouse://localhost/c  ") },
			want:   "clickhouse://localhost/c",
		},
		{
			// Accepted, and given the engine's scheme on the way out: this
			// accessor's consumers connect through dbschema.ConnectToDatabase,
			// which refuses an address that carries none.
			name:   "a driver DSN carrying no scheme gains one",
			engine: dbtarget.MySQL,
			set:    func(t *testing.T) { t.Setenv("MYSQL_TEST_URL", "user:pass@tcp(127.0.0.1:3306)/db") },
			want:   "mysql://user:pass@tcp(127.0.0.1:3306)/db",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clearAll(t)
			test.set(t)

			got, err := dbtarget.Lookup(test.engine)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// The canonical variable wins, so a checkout carrying both an old spelling and
// the new one reads the new one. Without this the migration would depend on
// which name a reader happened to unset.
func TestLookup_CanonicalWinsOverSynonym(t *testing.T) {
	c := qt.New(t)
	clearAll(t)
	t.Setenv("POSTGRES_TEST_DSN", "postgres://localhost/canonical")
	t.Setenv("TEST_DATABASE_URL", "postgres://localhost/synonym")

	got, err := dbtarget.Lookup(dbtarget.PostgreSQL)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "postgres://localhost/canonical")
}

func TestLookup_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		engine  dbtarget.Engine
		set     func(t *testing.T)
		wantErr string
	}{
		{
			name:    "another engine's address",
			engine:  dbtarget.PostgreSQL,
			set:     func(t *testing.T) { t.Setenv("POSTGRES_TEST_DSN", "clickhouse://localhost/x") },
			wantErr: `POSTGRES_TEST_DSN carries scheme "clickhouse", which POSTGRES_TEST_DSN does not speak; it names postgres or postgresql`,
		},
		{
			name:    "another engine's address in a synonym",
			engine:  dbtarget.ClickHouse,
			set:     func(t *testing.T) { t.Setenv("CLICKHOUSE_URL", "postgres://localhost/x") },
			wantErr: `CLICKHOUSE_URL carries scheme "postgres", which CLICKHOUSE_URL does not speak; it names clickhouse`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clearAll(t)
			test.set(t)

			got, err := dbtarget.Lookup(test.engine)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// Nothing configured is not an error: it is the ordinary state of a checkout
// without that engine, and the caller turns it into a skip.
func TestLookup_UnsetIsNotAnError(t *testing.T) {
	c := qt.New(t)
	clearAll(t)

	got, err := dbtarget.Lookup(dbtarget.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "")
}

// Every engine declares a canonical variable, and no two engines share one.
// A shared name would make one engine's address answer for another.
func TestEnginesDeclareDistinctCanonicalVariables(t *testing.T) {
	c := qt.New(t)

	seen := make(map[string]dbtarget.Engine)
	for _, engine := range dbtarget.Engines() {
		names := dbtarget.Variables(engine)
		c.Assert(names, qt.Not(qt.HasLen), 0, qt.Commentf("engine %d declares no variable", int(engine)))

		canonical := names[0]
		other, clash := seen[canonical]
		c.Assert(clash, qt.IsFalse,
			qt.Commentf("%s is canonical for two engines, %v and %v", canonical, other, engine))
		seen[canonical] = engine
	}
}

// A raw database/sql driver needs a different string from the one ptah
// connects with: go-sql-driver/mysql reads a mysql:// prefix as part of the
// username, and pgx does not parse cockroachdb:// at all.
func TestLookupDriverDSN_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		set    func(t *testing.T)
		want   string
	}{
		{
			name:   "the DSN spelling is preferred when one is set",
			engine: dbtarget.MySQL,
			set: func(t *testing.T) {
				t.Setenv("MYSQL_TEST_URL", "mysql://user:pass@tcp(127.0.0.1:3306)/db")
				t.Setenv("MYSQL_TEST_DSN", "user:pass@tcp(127.0.0.1:3306)/db")
			},
			want: "user:pass@tcp(127.0.0.1:3306)/db",
		},
		{
			name:   "the scheme is removed when only a URL is set",
			engine: dbtarget.MySQL,
			set:    func(t *testing.T) { t.Setenv("MYSQL_TEST_URL", "mysql://user:pass@tcp(127.0.0.1:3306)/db") },
			want:   "user:pass@tcp(127.0.0.1:3306)/db",
		},
		{
			name:   "a value already in driver form is unchanged",
			engine: dbtarget.MariaDB,
			set:    func(t *testing.T) { t.Setenv("MARIADB_TEST_URL", "user:pass@tcp(127.0.0.1:3307)/db") },
			want:   "user:pass@tcp(127.0.0.1:3307)/db",
		},
		{
			// Rewritten, not removed. pgx accepts a PostgreSQL URL or a
			// keyword/value DSN, and a bare root@host:26257/db is neither.
			name:   "a distributed SQL scheme is rewritten for pgx",
			engine: dbtarget.CockroachDB,
			set:    func(t *testing.T) { t.Setenv("COCKROACHDB_URL", "cockroachdb://root@localhost:26257/db") },
			want:   "postgres://root@localhost:26257/db",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clearAll(t)
			test.set(t)

			got, err := dbtarget.LookupDriverDSN(test.engine)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// PostgreSQL is left alone: pgx parses both spellings of its scheme, and
// stripping either would hand it a string it cannot open.
func TestLookupDriverDSN_KeepsThePostgresScheme(t *testing.T) {
	c := qt.New(t)
	clearAll(t)
	// POSTGRES_URL rather than POSTGRES_TEST_DSN: a name ending in _DSN is
	// returned verbatim before the scheme is ever looked at, so setting that
	// one would test the preference and not the stripping.
	t.Setenv("POSTGRES_URL", "postgres://user@localhost/db")

	got, err := dbtarget.LookupDriverDSN(dbtarget.PostgreSQL)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "postgres://user@localhost/db")
}

// clearAll unsets every variable the package reads, so one test's environment
// cannot answer another's lookup.
func clearAll(t *testing.T) {
	t.Helper()
	for _, engine := range dbtarget.Engines() {
		for _, name := range dbtarget.Variables(engine) {
			t.Setenv(name, "")
		}
	}
}

// TestURL_AlwaysCarriesAScheme pins the half of the contract the name states.
//
// A legacy synonym spelled _DSN holds a driver-form value with no scheme, and
// URL forwards what it finds. Its consumers hand the result to
// dbschema.ConnectToDatabase, which refuses a schemeless address, so a
// supported spelling made the run fail instead of connecting.
func TestURL_AlwaysCarriesAScheme(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		set    func(t *testing.T)
		want   string
	}{
		{
			name:   "a driver DSN in a synonym gains the engine's scheme",
			engine: dbtarget.MySQL,
			set:    func(t *testing.T) { t.Setenv("MYSQL_TEST_DSN", "user:pass@tcp(127.0.0.1:3306)/db") },
			want:   "mysql://user:pass@tcp(127.0.0.1:3306)/db",
		},
		{
			name:   "a value that already carries one is unchanged",
			engine: dbtarget.MySQL,
			set:    func(t *testing.T) { t.Setenv("MYSQL_TEST_URL", "mysql://user:pass@tcp(127.0.0.1:3306)/db") },
			want:   "mysql://user:pass@tcp(127.0.0.1:3306)/db",
		},
		{
			name:   "the admin engine names its own scheme",
			engine: dbtarget.MariaDBAdmin,
			set:    func(t *testing.T) { t.Setenv("MARIADB_ADMIN_TEST_DSN", "root:pass@tcp(127.0.0.1:3307)/mysql") },
			want:   "mariadb://root:pass@tcp(127.0.0.1:3307)/mysql",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clearAll(t)
			test.set(t)

			got, err := dbtarget.Lookup(test.engine)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestLookupDriverDSN_RendersTheNetworkFormTheDriverParses pins the other half.
//
// go-sql-driver/mysql wants user:pass@tcp(host:port)/db. Removing the scheme
// from a conventional mysql://user:pass@host:port/db leaves
// user:pass@host:port/db, which that driver does not accept, so a URL Ptah
// itself accepts failed to open unless the operator happened to spell the
// address in the driver's own network form already.
func TestLookupDriverDSN_RendersTheNetworkFormTheDriverParses(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		set    func(t *testing.T)
		want   string
	}{
		{
			name:   "a conventional URL becomes a network DSN",
			engine: dbtarget.MySQLAdmin,
			set:    func(t *testing.T) { t.Setenv("MYSQL_ADMIN_TEST_URL", "mysql://root:pass@localhost:3306/mysql") },
			want:   "root:pass@tcp(localhost:3306)/mysql",
		},
		{
			name:   "a URL already in network form is left alone",
			engine: dbtarget.MySQL,
			set:    func(t *testing.T) { t.Setenv("MYSQL_TEST_URL", "mysql://user:pass@tcp(127.0.0.1:3306)/db") },
			want:   "user:pass@tcp(127.0.0.1:3306)/db",
		},
		{
			name:   "a MariaDB URL renders the same way",
			engine: dbtarget.MariaDB,
			set:    func(t *testing.T) { t.Setenv("MARIADB_TEST_URL", "mariadb://root:pass@localhost:3307/db") },
			want:   "root:pass@tcp(localhost:3307)/db",
		},
		{
			name:   "a host with no port keeps the driver's default",
			engine: dbtarget.MySQL,
			set:    func(t *testing.T) { t.Setenv("MYSQL_TEST_URL", "mysql://root@localhost/db") },
			want:   "root@tcp(localhost:3306)/db",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clearAll(t)
			test.set(t)

			got, err := dbtarget.LookupDriverDSN(test.engine)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestLookup_RefusesASiblingEngineURL keeps a MySQL-family variable from
// answering for its sibling.
//
// The two speak one wire protocol, so a MariaDB address in MYSQL_TEST_URL
// connects and the suite reports MySQL coverage it never had. The scheme is the
// only thing that distinguishes them, and these entries declared none.
func TestLookup_RefusesASiblingEngineURL(t *testing.T) {
	c := qt.New(t)
	clearAll(t)
	t.Setenv("MYSQL_TEST_URL", "mariadb://root@localhost:3307/db")

	got, err := dbtarget.Lookup(dbtarget.MySQL)

	c.Assert(err, qt.ErrorMatches, `MYSQL_TEST_URL carries scheme "mariadb".*`)
	c.Assert(got, qt.Equals, "")
}

// TestLookup_AcceptsTheMySQLSpellingOfAMariaDBAddress is the control the rule
// above needs. MariaDB speaks the MySQL protocol and its address is routinely
// written mysql://, so refusing that spelling would break working setups; only
// the reverse direction is a lie about which engine was covered.
func TestLookup_AcceptsTheMySQLSpellingOfAMariaDBAddress(t *testing.T) {
	c := qt.New(t)
	clearAll(t)
	t.Setenv("MARIADB_TEST_URL", "mysql://root@localhost:3307/db")

	got, err := dbtarget.Lookup(dbtarget.MariaDB)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "mysql://root@localhost:3307/db")
}

// TestLookupDriverDSN_RewritesAPostgresFamilyAlias keeps pgx given something it
// parses.
//
// pgx accepts a PostgreSQL URL or a keyword/value DSN. Removing the scheme from
// cockroachdb://root@host:26257/db leaves root@host:26257/db, which is neither,
// so a probe that dbtarget.URL connects with fine failed on the driver path.
// The alias is rewritten rather than removed, which is what
// dbschema.convertPostgresWireURL already does for the same reason.
func TestLookupDriverDSN_RewritesAPostgresFamilyAlias(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		set    func(t *testing.T)
		want   string
	}{
		{
			name:   "a cockroachdb alias becomes a postgres URL",
			engine: dbtarget.CockroachDB,
			set:    func(t *testing.T) { t.Setenv("COCKROACHDB_URL", "cockroachdb://root@localhost:26257/db") },
			want:   "postgres://root@localhost:26257/db",
		},
		{
			name:   "a yugabytedb alias becomes a postgres URL",
			engine: dbtarget.YugabyteDB,
			set:    func(t *testing.T) { t.Setenv("YUGABYTEDB_URL", "yugabytedb://root@localhost:5433/db") },
			want:   "postgres://root@localhost:5433/db",
		},
		{
			name:   "a postgres URL is already what pgx wants",
			engine: dbtarget.CockroachDB,
			set:    func(t *testing.T) { t.Setenv("COCKROACHDB_URL", "postgres://root@localhost:26257/db") },
			want:   "postgres://root@localhost:26257/db",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clearAll(t)
			test.set(t)

			got, err := dbtarget.LookupDriverDSN(test.engine)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestLookupDriverDSN_DecodesEscapedCredentials keeps the two paths agreeing on
// what the password is.
//
// A URL carries credentials percent-escaped, and net/url decodes them for every
// consumer that connects through a URL. Splicing the string instead hands the
// driver the literal escapes, so the raw-driver tests authenticate with a
// different password from the URL consumers reading the same variable.
func TestLookupDriverDSN_DecodesEscapedCredentials(t *testing.T) {
	c := qt.New(t)
	clearAll(t)
	t.Setenv("MYSQL_TEST_URL", "mysql://app:p%40ss%2Fword@db:3306/shop")

	got, err := dbtarget.LookupDriverDSN(dbtarget.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "app:p@ss/word@tcp(db:3306)/shop")
}

// TestLookupDriverDSN_ValidatesThePreferredDSNSpelling closes the path that
// skipped every check.
//
// A variable whose name ends in _DSN is preferred because someone wrote it in
// driver form deliberately. That preference was reading the value and
// returning it, so a URL left in a _DSN variable reached the driver whole: a
// raw MySQL consumer read the scheme as part of the username, and pgx refused
// the wrong scheme. Both surface as a credential or connection failure naming
// nothing about the actual mistake, which is that the value is in the wrong
// variable.
func TestLookupDriverDSN_ValidatesThePreferredDSNSpelling(t *testing.T) {
	tests := []struct {
		name    string
		engine  dbtarget.Engine
		set     func(t *testing.T)
		wantErr string
	}{
		{
			name:    "a sibling engine's URL in a preferred DSN is refused",
			engine:  dbtarget.MySQL,
			set:     func(t *testing.T) { t.Setenv("MYSQL_TEST_DSN", "mariadb://user@host/db") },
			wantErr: `MYSQL_TEST_DSN carries scheme "mariadb".*`,
		},
		{
			name:    "another engine's URL in a preferred DSN is refused",
			engine:  dbtarget.PostgreSQL,
			set:     func(t *testing.T) { t.Setenv("POSTGRES_TEST_DSN", "mysql://user@host/db") },
			wantErr: `POSTGRES_TEST_DSN carries scheme "mysql".*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			clearAll(t)
			test.set(t)

			got, err := dbtarget.LookupDriverDSN(test.engine)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestLookupDriverDSN_DerivesFromAPreferredDSNCarryingItsOwnScheme keeps the
// preference useful. A value written in driver form with the engine's own
// scheme in front of it is still that engine's address, and it is converted
// rather than refused.
func TestLookupDriverDSN_DerivesFromAPreferredDSNCarryingItsOwnScheme(t *testing.T) {
	c := qt.New(t)
	clearAll(t)
	t.Setenv("MYSQL_TEST_DSN", "mysql://user:pass@tcp(127.0.0.1:3306)/db")

	got, err := dbtarget.LookupDriverDSN(dbtarget.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "user:pass@tcp(127.0.0.1:3306)/db")
}
