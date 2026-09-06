package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// uniqueFS is the fixture the unique-index cases analyze: an existing table
// in file 1, then the migration under test in file 2, so a table or column
// the migration itself creates is one file 2 spells out.
func uniqueFS(migration string) map[string]string {
	return map[string]string{
		"1_base.sql":   "CREATE TABLE orders (id int, email varchar(100), code varchar(10));",
		"2_change.sql": migration,
	}
}

func analyzeUnique(c *qt.C, dialect, migration string) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(uniqueFS(migration)), lint.Options{
		Dialect:   dialect,
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// uniqueCodes keeps the codes of this family, so a quiet case asserts that
// the unique-index rules said nothing rather than that PG101 or PG105 fell
// silent on a statement they still describe.
func uniqueCodes(codes []string) []string {
	var kept []string
	for _, code := range codes {
		if code == "MF101" || code == "MF102" {
			kept = append(kept, code)
		}
	}
	return kept
}

// TestUniqueIndexAddedRule_ReportsTheBuildOverExistingRows pins MF101 to
// every spelling that builds a unique index over rows the table already
// holds, with the query that settles the risk.
func TestUniqueIndexAddedRule_ReportsTheBuildOverExistingRows(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		migration string
		want      []string
	}{
		{
			name:      "CREATE UNIQUE INDEX",
			dialect:   "postgres",
			migration: "CREATE UNIQUE INDEX orders_email ON orders (email);",
			want: []string{
				"CREATE UNIQUE INDEX orders_email builds over the rows orders already holds and fails on the first duplicate it meets",
				"SELECT email, COUNT(*) FROM orders GROUP BY email HAVING COUNT(*) > 1",
				"rows where every key column is NULL do not collide",
			},
		},
		{
			name:      "CONCURRENTLY names what a failure leaves behind",
			dialect:   "postgres",
			migration: "CREATE UNIQUE INDEX CONCURRENTLY orders_email ON orders (email);",
			want:      []string{"a CONCURRENTLY build that fails leaves an invalid index of that name behind, which must be dropped before the retry"},
		},
		{
			name:      "an unnamed index",
			dialect:   "postgres",
			migration: "CREATE UNIQUE INDEX ON orders (email);",
			want:      []string{"CREATE UNIQUE INDEX builds over the rows orders already holds"},
		},
		{
			name:      "a composite key lists every column",
			dialect:   "postgres",
			migration: "CREATE UNIQUE INDEX orders_key ON orders USING btree (email, code DESC);",
			want:      []string{"SELECT email, code, COUNT(*) FROM orders GROUP BY email, code HAVING COUNT(*) > 1"},
		},
		{
			name:      "ADD CONSTRAINT UNIQUE",
			dialect:   "postgres",
			migration: "ALTER TABLE orders ADD CONSTRAINT orders_email UNIQUE (email);",
			want:      []string{"ADD UNIQUE orders_email builds over the rows orders already holds"},
		},
		{
			name:      "MySQL ADD UNIQUE KEY",
			dialect:   "mysql",
			migration: "ALTER TABLE orders ADD UNIQUE KEY orders_email (email);",
			want:      []string{"ADD UNIQUE orders_email builds over the rows orders already holds", "MySQL and MariaDB: Duplicate entry for key"},
		},
		{
			name:      "MySQL ADD UNIQUE INDEX without a name",
			dialect:   "mariadb",
			migration: "ALTER TABLE orders ADD UNIQUE INDEX (email);",
			want:      []string{"ADD UNIQUE builds over the rows orders already holds"},
		},
		{
			name:      "SQLite",
			dialect:   "sqlite",
			migration: "CREATE UNIQUE INDEX orders_email ON orders (email);",
			want:      []string{"fails on the first duplicate it meets"},
		},
		{
			name:      "a column added in this file with a DEFAULT holds the same value in every row",
			dialect:   "postgres",
			migration: "ALTER TABLE orders ADD COLUMN slug varchar(20) DEFAULT 'x';\nCREATE UNIQUE INDEX orders_slug ON orders (slug);",
			want:      []string{"Every existing row holds the DEFAULT this migration gives the column, so the build fails as soon as two rows exist"},
		},
		{
			name:      "a key mixing an added column with an existing one is judged on the existing one",
			dialect:   "postgres",
			migration: "ALTER TABLE orders ADD COLUMN slug varchar(20);\nCREATE UNIQUE INDEX orders_slug ON orders (email, slug);",
			want:      []string{"Nothing in the migration says whether email, slug is unique today"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeUnique(c, test.dialect, test.migration)
			c.Assert(uniqueCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MF101"})
			message := messageOf(analysis.Findings(), "MF101")
			for _, want := range test.want {
				c.Assert(message, qt.Contains, want)
			}
		})
	}
}

// TestUniqueIndexAddedRule_NullsNotDistinctWithdrawsTheNullExemption: under
// NULLS NOT DISTINCT two NULLs collide, so the message stops saying they do
// not, and a column added without a DEFAULT is no longer exempt.
func TestUniqueIndexAddedRule_NullsNotDistinctWithdrawsTheNullExemption(t *testing.T) {
	c := qt.New(t)

	analysis := analyzeUnique(c, "postgres",
		"ALTER TABLE orders ADD COLUMN slug varchar(20);\nCREATE UNIQUE INDEX orders_slug ON orders (slug) NULLS NOT DISTINCT;")

	c.Assert(uniqueCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MF101"})
	c.Assert(messageOf(analysis.Findings(), "MF101"), qt.Not(qt.Contains), "do not collide")
}

// TestUniqueIndexAddedRule_StaysQuietWhereNoRowCanCollide holds the two
// facts the file knows without a database, and the shapes that build no
// index at all.
func TestUniqueIndexAddedRule_StaysQuietWhereNoRowCanCollide(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		migration string
	}{
		{
			name:      "a table this migration creates holds no rows",
			dialect:   "postgres",
			migration: "CREATE TABLE invoices (id int, number text);\nCREATE UNIQUE INDEX invoices_number ON invoices (number);",
		},
		{
			name:      "a schema-qualified table this migration creates",
			dialect:   "postgres",
			migration: "CREATE TABLE app.invoices (id int, number text);\nCREATE UNIQUE INDEX invoices_number ON app.invoices (number);",
		},
		{
			name:      "a column added without a DEFAULT holds NULL in every row",
			dialect:   "postgres",
			migration: "ALTER TABLE orders ADD COLUMN slug varchar(20);\nCREATE UNIQUE INDEX orders_slug ON orders (slug);",
		},
		{
			name:      "the same through ADD UNIQUE on MySQL",
			dialect:   "mysql",
			migration: "ALTER TABLE orders ADD COLUMN slug varchar(20);\nALTER TABLE orders ADD UNIQUE KEY orders_slug (slug);",
		},
		{
			name:      "a plain index",
			dialect:   "postgres",
			migration: "CREATE INDEX orders_email ON orders (email);",
		},
		{
			name:      "a unique constraint attached to an index that already exists",
			dialect:   "postgres",
			migration: "ALTER TABLE orders ADD CONSTRAINT orders_email UNIQUE USING INDEX orders_email_idx;",
		},
		{
			name:      "a unique constraint declared in CREATE TABLE meets no rows",
			dialect:   "postgres",
			migration: "CREATE TABLE invoices (id int, number text, UNIQUE (number));",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeUnique(c, test.dialect, test.migration)
			c.Assert(uniqueCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestIndexMadeUniqueRule_ReportsTheReplacement pins MF102 to an index the
// file drops and rebuilds as unique under the same name.
func TestIndexMadeUniqueRule_ReportsTheReplacement(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		migration string
		want      []string
	}{
		{
			name:      "PostgreSQL DROP INDEX then CREATE UNIQUE INDEX",
			dialect:   "postgres",
			migration: "DROP INDEX orders_email;\nCREATE UNIQUE INDEX orders_email ON orders (email);",
			want: []string{
				"CREATE UNIQUE INDEX orders_email replaces the index orders_email dropped earlier with a unique one over the rows orders already holds",
				"which also leaves the table without the index it had",
				"build the unique index CONCURRENTLY under a new name first",
			},
		},
		{
			name:      "a schema-qualified drop",
			dialect:   "postgres",
			migration: "DROP INDEX IF EXISTS public.orders_email;\nCREATE UNIQUE INDEX orders_email ON orders (email);",
			want:      []string{"replaces the index orders_email dropped earlier"},
		},
		{
			name:      "MySQL in one statement",
			dialect:   "mysql",
			migration: "ALTER TABLE orders DROP INDEX orders_email, ADD UNIQUE INDEX orders_email (email);",
			want:      []string{"ADD UNIQUE orders_email replaces the index orders_email dropped earlier"},
		},
		{
			name:      "MySQL across two statements",
			dialect:   "mysql",
			migration: "ALTER TABLE orders DROP KEY orders_email;\nALTER TABLE orders ADD UNIQUE KEY orders_email (email);",
			want:      []string{"replaces the index orders_email dropped earlier"},
		},
		{
			name:      "a constraint dropped and rebuilt as unique",
			dialect:   "postgres",
			migration: "ALTER TABLE orders DROP CONSTRAINT orders_email;\nALTER TABLE orders ADD CONSTRAINT orders_email UNIQUE (email);",
			want:      []string{"ADD UNIQUE orders_email replaces the index orders_email dropped earlier"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeUnique(c, test.dialect, test.migration)
			c.Assert(uniqueCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MF102"})
			message := messageOf(analysis.Findings(), "MF102")
			for _, want := range test.want {
				c.Assert(message, qt.Contains, want)
			}
		})
	}
}

func TestIndexMadeUniqueRule_LeavesOtherBuildsToMF101(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		migration string
	}{
		{
			name:      "a different index dropped",
			dialect:   "postgres",
			migration: "DROP INDEX orders_code;\nCREATE UNIQUE INDEX orders_email ON orders (email);",
		},
		{
			name:      "the drop comes after the build",
			dialect:   "postgres",
			migration: "CREATE UNIQUE INDEX orders_email ON orders (email);\nDROP INDEX orders_email_old;",
		},
		{
			name:      "an unnamed unique index cannot replace anything by name",
			dialect:   "postgres",
			migration: "DROP INDEX orders_email;\nCREATE UNIQUE INDEX ON orders (email);",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeUnique(c, test.dialect, test.migration)
			c.Assert(uniqueCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MF101"})
		})
	}
}

// TestUniqueRules_SitBesideTheLockRules: the failure MF101 names and the
// lock PG105 names are two hazards of one statement, and both are reported.
func TestUniqueRules_SitBesideTheLockRules(t *testing.T) {
	c := qt.New(t)

	analysis := analyzeUnique(c, "postgres", "ALTER TABLE orders ADD CONSTRAINT orders_email UNIQUE (email);")

	c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"MF101", "PG105"})
	c.Assert(analysis.Findings()[0].Context.Subjects, qt.DeepEquals, []lint.Subject{{Kind: lint.SubjectColumn, Name: "email", Parent: "orders"}})
}

// TestFileFormRules_CarryTheConventionSuffix pins the identifiers the
// missing-down and empty-file rules report under, which the Atlas checks
// MF101 and MF102 took over.
func TestFileFormRules_CarryTheConventionSuffix(t *testing.T) {
	c := qt.New(t)

	findings, err := lint.LintFS(fixture(map[string]string{
		"0000000001_orphan.up.sql":  "CREATE TABLE t (id INT);\n",
		"0000000002_empty.up.sql":   "-- nothing\n",
		"0000000002_empty.down.sql": "-- nothing\n",
	}), lint.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"MF101P", "MF102P"})
}
