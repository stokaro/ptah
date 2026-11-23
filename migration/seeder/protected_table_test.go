package seeder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/seeder"
)

// TestApplyProtectedTables_HappyPath seeds a SQLite target whose one table,
// countries, no entry fences: an entry naming a table the target does not
// hold, bare or qualified by the connection's schema, main.
func TestApplyProtectedTables_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
	}{
		{name: "a table the target does not hold", entries: []string{"audit"}},
		{name: "the same table by the connection's schema", entries: []string{"main.audit"}},
		{name: "the connection's schema in another case", entries: []string{" Main.audit "}},
		{name: "the connection's schema, quoted", entries: []string{`"main".audit`}},
		{name: "the connection's schema, in backticks", entries: []string{"`main`.audit"}},
		{name: "the connection's schema, in brackets", entries: []string{"[main].audit"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openSeedTarget(c)

			_, err := seeder.Apply(c.Context(), conn, seedsWith(firstSeed),
				seeder.Options{Env: "dev", ProtectedTables: test.entries})

			c.Assert(err, qt.IsNil)
			c.Assert(countCountries(c, conn), qt.Equals, 1)
		})
	}
}

// TestApplyProtectedTables_FailurePath refuses before a seed is written. The
// fence lists the tables of the connection's schema, so an entry qualified by
// another schema can never fence one; accepted, `other.countries` would seed
// a target whatever its other schema holds.
func TestApplyProtectedTables_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
		wantErr string
	}{
		{
			name:    "a table the target holds",
			entries: []string{"countries"},
			wantErr: `refusing to seed target database because protected tables exist: countries; pass --allow-prod to override`,
		},
		{
			name:    "a table the target holds, quoted",
			entries: []string{`main."countries"`},
			wantErr: `refusing to seed target database because protected tables exist: main."countries"; pass --allow-prod to override`,
		},
		{
			name:    "a table the target holds, quoted throughout",
			entries: []string{`"main"."countries"`},
			wantErr: `refusing to seed target database because protected tables exist: "main"."countries"; pass --allow-prod to override`,
		},
		{
			name:    "a table the target holds, in backticks",
			entries: []string{"`countries`"},
			wantErr: "refusing to seed target database because protected tables exist: `countries`; pass --allow-prod to override",
		},
		{
			name:    "a table of another schema",
			entries: []string{"other.countries"},
			wantErr: `refusing to seed target database: the protected table entry "other.countries" names a schema ` +
				`other than "main", and the fence checks the tables of "main" alone; name the table without a schema, ` +
				`or qualified by "main"`,
		},
		{
			name:    "two tables of other schemas beside one of the connection's",
			entries: []string{"other.countries", "main.audit", "app2.extra"},
			wantErr: `refusing to seed target database: the protected table entries "other.countries", "app2.extra" name ` +
				`a schema other than "main", and the fence checks the tables of "main" alone; name the table without ` +
				`a schema, or qualified by "main"`,
		},
		{
			// Quoted, the schema is main.x, which is not the connection's.
			name:    "a quoted schema whose name begins with the connection's",
			entries: []string{`"main.x".countries`},
			wantErr: `refusing to seed target database: the protected table entry "\\"main.x\\".countries" names a schema ` +
				`other than "main", and the fence checks the tables of "main" alone; name the table without a schema, ` +
				`or qualified by "main"`,
		},
		{
			name:    "a name of three parts",
			entries: []string{"db.main.countries"},
			wantErr: `refusing to seed target database: the protected table entry "db.main.countries" is not a table ` +
				`name, bare or qualified by one schema; quote a name that holds a dot`,
		},
		{
			name:    "an unclosed quote",
			entries: []string{`"main.countries`},
			wantErr: `refusing to seed target database: the protected table entry "\\"main.countries" is not a table ` +
				`name, bare or qualified by one schema; quote a name that holds a dot`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openSeedTarget(c)

			result, err := seeder.Apply(c.Context(), conn, seedsWith(firstSeed),
				seeder.Options{Env: "dev", ProtectedTables: test.entries})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.IsNil)
			c.Assert(countCountries(c, conn), qt.Equals, 0)
		})
	}
}
