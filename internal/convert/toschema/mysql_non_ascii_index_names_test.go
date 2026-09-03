package toschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/convert/toschema"
)

// The two engines fold non-ASCII index names differently, and not in a way any
// single rule covers. Measured 2026-09-03 on MySQL 8.4.11 and MariaDB 11.8.9,
// two named secondary indexes on one table, sent as UTF-8 over a utf8mb4
// connection:
//
//	I and dotless ı            accepted     ERROR 1061
//	dotted İ and i             ERROR 1061   accepted
//	Sigma and final sigma      accepted     ERROR 1061
//	Kelvin sign and ASCII K    ERROR 1061   accepted
//	a lone `prımary`           accepted     ERROR 1280
//
// The engines disagree on every row. Ptah folded with strings.ToLower, one
// rule, and it was wrong for MariaDB on all five: it missed the three
// collisions that engine reports and invented the two it does not. The lone
// `prımary` row is why a solitary non-ASCII name is not a safe exception --
// it is still an unresolved comparison against the reserved PRIMARY.
//
// So the name is refused. See stokaro/ptah#2768.

func TestToDatabase_NonASCIIIndexNameFailurePath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "ASCII I beside dotless i",
			sql:  "CREATE TABLE nz (a INT, b INT, KEY `I` (a), KEY `\u0131` (b));",
		},
		{
			name: "dotted I beside ASCII i",
			sql:  "CREATE TABLE nz (a INT, b INT, KEY `\u0130` (a), KEY `i` (b));",
		},
		{
			name: "sigma beside final sigma",
			sql:  "CREATE TABLE nz (a INT, b INT, KEY `\u03a3` (a), KEY `\u03c2` (b));",
		},
		{
			name: "Kelvin sign beside ASCII K",
			sql:  "CREATE TABLE nz (a INT, b INT, KEY `\u212a` (a), KEY `K` (b));",
		},
		{
			name: "a lone name that folds onto PRIMARY on one engine",
			sql:  "CREATE TABLE rn (a INT, KEY `pr\u0131mary` (a));",
		},
		{
			name: "a lone name that collides with nothing",
			sql:  "CREATE TABLE rn (a INT, KEY `\u0438\u043c\u044f` (a));",
		},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				_, err := dialectSchema(c, dialect, test.sql)

				c.Assert(err, qt.ErrorIs, toschema.ErrNonASCIIIndexName)
			})
		}
	}
}

// A name derived from a column reaches the same refusal, because the question
// is about the name that ends up in the namespace rather than where it came
// from.
func TestToDatabase_NonASCIIDerivedIndexNameFailurePath(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			_, err := dialectSchema(c, dialect, "CREATE TABLE nz (`\u0438\u043c\u044f` INT, KEY (`\u0438\u043c\u044f`));")

			c.Assert(err, qt.ErrorIs, toschema.ErrNonASCIIIndexName)
		})
	}
}

// ASCII folding is shared and deterministic, and stays. Without these rows a
// refusal that swallowed every index name would read as a fix.
func TestToDatabase_ASCIIIndexNamesAreUnaffected(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect+"/distinct names are kept", func(t *testing.T) {
			c := qt.New(t)

			database, err := dialectSchema(c, dialect,
				"CREATE TABLE nz (a INT, b INT, KEY `one` (a), KEY `two` (b));")

			c.Assert(err, qt.IsNil)
			c.Assert(database.Indexes, qt.HasLen, 2)
			c.Assert(database.Indexes[0].Name, qt.Equals, "one")
			c.Assert(database.Indexes[1].Name, qt.Equals, "two")
		})

		t.Run(dialect+"/case-folded duplicates are still duplicates", func(t *testing.T) {
			c := qt.New(t)

			_, err := dialectSchema(c, dialect,
				"CREATE TABLE nz (a INT, b INT, KEY `Foo` (a), KEY `foo` (b));")

			c.Assert(err, qt.ErrorIs, toschema.ErrDuplicateIndexName)
			c.Assert(err, qt.Not(qt.ErrorIs), toschema.ErrNonASCIIIndexName)
		})
	}
}

// PostgreSQL is the control that keeps the refusal on the family whose engines
// disagree. It folds identifiers by its own rule, which this says nothing
// about, so the same document still converts.
func TestToDatabase_NonASCIIIndexNameOutsideTheMySQLFamily(t *testing.T) {
	c := qt.New(t)

	database, err := dialectSchema(c, platform.Postgres,
		"CREATE TABLE nz (a INT, b INT); CREATE INDEX \"\u0438\u043c\u044f\" ON nz (a);")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Indexes, qt.HasLen, 1)
	c.Assert(database.Indexes[0].Name, qt.Equals, "\u0438\u043c\u044f")
}
