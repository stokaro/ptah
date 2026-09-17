package identifier_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
)

// A default schema that names the database is not a rule about identifiers, and
// two databases on such an engine always differ in it.
//
// The shadow checks in migration/shadow compare a target's semantics with a
// shadow's, and a shadow database is a different database by construction. With
// the field in the comparison, every shadow MySQL could ever be given was
// refused, while the same check on PostgreSQL passed because both sides answer
// `public` there (stokaro/ptah#3375).

// mysqlSemantics and postgresSemantics are the rules each engine carries, with
// the database each connection selected filled in by the caller.
func semanticsIn(dialect, database string) identifier.Semantics {
	rules := identifier.ForDialect(dialect)
	rules.DefaultSchema = database
	return rules
}

// TestDefaultSchemaNamesTheDatabase_HappyPath pins which engines take the field
// from the connected database. The list is what dbschema.DatabaseConnection
// does for each, not a guess about SQL syntax.
func TestDefaultSchemaNamesTheDatabase_HappyPath(t *testing.T) {
	cases := []struct {
		dialect string
		want    bool
	}{
		{platform.MySQL, true},
		{platform.MariaDB, true},
		{platform.ClickHouse, true},
		{platform.Oracle, true},
		{platform.Postgres, false},
		{platform.CockroachDB, false},
		{platform.SQLServer, false},
		{platform.SQLite, false},
	}

	for _, tc := range cases {
		t.Run(tc.dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(identifier.DefaultSchemaNamesTheDatabase(tc.dialect), qt.Equals, tc.want)
		})
	}
}

// TestAgreeOn_DatabaseNamedDefaultSchema is the case that was refused. Same
// rules, two databases: they agree.
func TestAgreeOn_DatabaseNamedDefaultSchema(t *testing.T) {
	c := qt.New(t)
	target := semanticsIn(platform.MySQL, "adopt")
	shadow := semanticsIn(platform.MySQL, "adopt_shadow")

	c.Assert(target.AgreeOn(platform.MySQL, shadow), qt.IsTrue)
	// The control: the field is the only difference, and Equal still sees it.
	c.Assert(target.Equal(shadow), qt.IsFalse)
}

// TestAgreeOn_NamespaceDefaultSchema keeps the refusal where the field is a
// real scope. Two PostgreSQL connections in different schemas resolve the same
// unqualified name to different objects, which is a difference worth refusing.
func TestAgreeOn_NamespaceDefaultSchema(t *testing.T) {
	c := qt.New(t)
	target := semanticsIn(platform.Postgres, "public")
	other := semanticsIn(platform.Postgres, "app")

	c.Assert(target.AgreeOn(platform.Postgres, other), qt.IsFalse)
}

// TestAgreeOn_RulesStillDecide is the other control. Dropping the default
// schema from the comparison must not drop the rules with it: two MySQL
// semantics that fold table names differently still disagree.
func TestAgreeOn_RulesStillDecide(t *testing.T) {
	c := qt.New(t)
	target := semanticsIn(platform.MySQL, "adopt")
	shadow := semanticsIn(platform.MySQL, "adopt_shadow")
	shadow.TableNames = identifier.ComparisonExact
	target.TableNames = identifier.ComparisonASCIIInsensitive

	c.Assert(target.AgreeOn(platform.MySQL, shadow), qt.IsFalse)
}

// TestAgreeOn_LeavesItsOperandsAlone holds the method to a value receiver's
// promise. It blanks a field to compare, and a caller that reused either side
// afterwards would read a semantics value with no default schema at all.
func TestAgreeOn_LeavesItsOperandsAlone(t *testing.T) {
	c := qt.New(t)
	target := semanticsIn(platform.MySQL, "adopt")
	shadow := semanticsIn(platform.MySQL, "adopt_shadow")

	target.AgreeOn(platform.MySQL, shadow)

	c.Assert(target.DefaultSchema, qt.Equals, "adopt")
	c.Assert(shadow.DefaultSchema, qt.Equals, "adopt_shadow")
}
