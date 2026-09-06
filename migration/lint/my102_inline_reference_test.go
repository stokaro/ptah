package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// MY102 is about a clause that means different things on the two engines.
// Measured 2026-09-03 against `CREATE TABLE parents (id INT PRIMARY KEY)`,
// both spellings of a column-level REFERENCES:
//
//	                                  MySQL 8.4.11              MariaDB 11.8.9
//	CREATE TABLE child (a INT REF..)  no foreign key, no index  enforced
//	ALTER TABLE .. ADD COLUMN b ..    no foreign key, no index  enforced, backing index `b`
//
// The rule reported only the ALTER TABLE spelling, so a migration creating a
// table with the clause passed with no finding while the same relationship
// added later warned. See stokaro/ptah#2831.

func TestMY102_ReportsBothColumnSpellings(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "create table",
			sql:  "CREATE TABLE child (a INT REFERENCES parents (id));",
		},
		{
			name: "create table beside other columns",
			sql:  "CREATE TABLE child (id INT PRIMARY KEY, a INT REFERENCES parents (id), b INT);",
		},
		{
			name: "alter table add column",
			sql:  "ALTER TABLE child ADD COLUMN a INT REFERENCES parents (id);",
		},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				codes := lintOneDialect(c, dialect, test.sql)

				c.Assert(codes, qt.Contains, "MY102")
			})
		}
	}
}

// The controls that keep the rule off a relationship both engines build. A
// table-level FOREIGN KEY is enforced everywhere, and without these rows a
// scanner that reported every REFERENCES would pass the table above.
func TestMY102_LeavesAnEnforcedForeignKeyAlone(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "table-level foreign key",
			sql:  "CREATE TABLE child (a INT, FOREIGN KEY (a) REFERENCES parents (id));",
		},
		{
			name: "named table-level foreign key",
			sql:  "CREATE TABLE child (a INT, CONSTRAINT f FOREIGN KEY (a) REFERENCES parents (id));",
		},
		{
			name: "no reference at all",
			sql:  "CREATE TABLE child (a INT, KEY k (a));",
		},
		{
			name: "alter table adding a constraint",
			sql:  "ALTER TABLE child ADD CONSTRAINT f FOREIGN KEY (a) REFERENCES parents (id);",
		},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				codes := lintOneDialect(c, dialect, test.sql)

				c.Assert(codes, qt.Not(qt.Contains), "MY102")
			})
		}
	}
}

// PostgreSQL enforces the clause, so the rule does not run there at all.
func TestMY102_DoesNotRunOutsideTheMySQLFamily(t *testing.T) {
	c := qt.New(t)

	codes := lintOneDialect(c, platform.Postgres,
		"CREATE TABLE child (a INT REFERENCES parents (id));")

	c.Assert(codes, qt.Not(qt.Contains), "MY102")
}
