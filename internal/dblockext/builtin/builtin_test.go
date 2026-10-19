package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/builtindialects"
	"ptah.run/internal/dblockext"
	"ptah.run/internal/dblockext/builtin"
	"ptah.run/internal/dblockext/mysql"
)

// lockAgreement is the lock protocol each built-in dialect's descriptor named
// before these services replaced it, written out as it stood; the protocol
// chose the arm of the switch in internal/dblock that took the lock. A dialect
// left out took a no-op lock. The unbounded GET_LOCK wait MariaDB's descriptor
// stated, 31,536,000 seconds, is pinned by the mysql package's tests.
var lockAgreement = map[string]string{
	platform.Postgres:   "pg-advisory",
	platform.YugabyteDB: "pg-advisory",
	platform.MySQL:      "get-lock",
	platform.MariaDB:    "get-lock",
	platform.SQLServer:  "app-lock",
	platform.YDB:        "coordination-semaphore",
}

// protocol names the shape of the services the way the descriptor named the
// protocol: the PostgreSQL advisory lock is the session lock with an
// exclusion check, and the coordination semaphore the one a separate session
// holds. Each other session lock is told apart by the services that offer it.
func protocol(services dblockext.Services) string {
	switch {
	case services.Separate != nil:
		return "coordination-semaphore"
	case services.Exclusion != nil:
		return "pg-advisory"
	case services.Session == mysql.MySQL().Session || services.Session == mysql.MariaDB().Session:
		return "get-lock"
	case services.Session != nil:
		return "app-lock"
	default:
		return ""
	}
}

func TestLookup_ServesTheProtocolsTheDescriptorsNamed(t *testing.T) {
	for _, spelling := range platform.DialectSpellings() {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			dialect := platform.NormalizeDialect(spelling)

			services := builtin.Lookup(spelling)

			c.Assert(protocol(services), qt.Equals, lockAgreement[dialect])
			c.Assert(services.Locks(), qt.Equals, lockAgreement[dialect] != "")
			c.Assert(services.Session != nil && services.Separate != nil, qt.IsFalse)
		})
	}
}

// MariaDB's services are MySQL's with the one difference the MariaDB
// descriptor stated, and the two are told apart only by it.
func TestLookup_MySQLAndMariaDBDifferByTheUnboundedWait(t *testing.T) {
	c := qt.New(t)
	c.Assert(builtin.Lookup(platform.MySQL).Session, qt.Equals, mysql.MySQL().Session)
	c.Assert(builtin.Lookup(platform.MariaDB).Session, qt.Equals, mysql.MariaDB().Session)
	c.Assert(mysql.MySQL().Session, qt.Not(qt.Equals), mysql.MariaDB().Session)
}

func TestLookup_FoldsSpellings(t *testing.T) {
	c := qt.New(t)
	c.Assert(builtin.Lookup(" PGX ").Dialects, qt.DeepEquals, builtin.Lookup(platform.Postgres).Dialects)
	c.Assert(builtin.Lookup("ydbs").Dialects, qt.DeepEquals, []string{platform.YDB})
}

func TestLookup_UnknownDialectTakesNoLock(t *testing.T) {
	for _, dialect := range []string{"", "nonesuch", platform.SQLite, platform.CockroachDB, platform.Spanner, platform.Oracle} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			services := builtin.Lookup(dialect)
			c.Assert(services.Dialects, qt.IsNil)
			c.Assert(services.Locks(), qt.IsFalse)
			c.Assert(services.Exclusion, qt.IsNil)
		})
	}
}

// Every dialect a service serves is the canonical name of a built-in dialect,
// so a misspelled one cannot leave its engine on a no-op lock.
func TestDialects_AreBuiltinNames(t *testing.T) {
	c := qt.New(t)
	var names []string
	for _, described := range builtindialects.Descriptors() {
		names = append(names, described.Name)
	}
	for _, dialect := range builtin.Dialects() {
		c.Assert(names, qt.Contains, dialect)
	}
	c.Assert(builtin.Dialects(), qt.HasLen, len(lockAgreement))
}
