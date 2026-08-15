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
			name:   "a driver DSN carrying no scheme is accepted",
			engine: dbtarget.MySQL,
			set:    func(t *testing.T) { t.Setenv("MYSQL_TEST_URL", "user:pass@tcp(127.0.0.1:3306)/db") },
			want:   "user:pass@tcp(127.0.0.1:3306)/db",
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
			name:   "a distributed SQL scheme is removed for pgx",
			engine: dbtarget.CockroachDB,
			set:    func(t *testing.T) { t.Setenv("COCKROACHDB_URL", "cockroachdb://root@localhost:26257/db") },
			want:   "root@localhost:26257/db",
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
