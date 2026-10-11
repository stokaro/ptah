package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/migration/safety"
)

// dropColumnReason is the reason a dropped column is destructive.
const dropColumnReason = "DROP COLUMN removes existing column data"

// PostgreSQL and MySQL both drop a column when ALTER TABLE writes DROP with no
// COLUMN keyword, so the statement is as destructive as the one that writes
// it: read as safe, a hand-edited plan that drops a column would pass the
// gate. Each engine's own spelling is judged on its target and with none.
func TestAssessSQL_DropWithoutColumnDropsTheColumn(t *testing.T) {
	tests := []struct {
		target    string
		statement string
	}{
		{target: platform.Postgres, statement: `ALTER TABLE "users" DROP "legacy"`},
		{target: platform.Postgres, statement: `ALTER TABLE users DROP IF EXISTS legacy CASCADE`},
		{target: platform.Postgres, statement: `ALTER TABLE ONLY users DROP legacy, ADD COLUMN fresh int`},
		{target: platform.Postgres, statement: "ALTER TABLE users\n  DROP\n  legacy;"},
		{target: platform.MySQL, statement: "ALTER TABLE `users` DROP `legacy`"},
		{target: platform.MySQL, statement: "alter table users drop legacy"},
		{target: platform.MySQL, statement: "ALTER TABLE users ADD INDEX by_name (name), DROP legacy"},
		{target: platform.MariaDB, statement: "ALTER TABLE users DROP IF EXISTS legacy"},
		{target: "", statement: "ALTER TABLE users DROP legacy"},
	}
	for _, test := range tests {
		t.Run(test.target+"/"+test.statement, func(t *testing.T) {
			c := qt.New(t)
			got := assessText(c, test.target, test.statement)
			c.Assert(got.Severity, qt.Equals, safety.Destructive)
			c.Assert(got.Reason, qt.Equals, dropColumnReason)
		})
	}
}

// A DROP that names another kind of object keeps the verdict its words had
// before: a constraint's drop removes a data protection, and an index's or a
// key's drop inside ALTER TABLE, and an ALTER COLUMN that drops a property,
// are left as they were, which is safe by their words. A DROP inside a string or a comment is no DROP at all, and a name
// that only Unicode case mapping turns into a keyword is still a name.
func TestAssessSQL_DropOfAnotherObjectIsNoColumnDrop(t *testing.T) {
	tests := []struct {
		target    string
		statement string
		severity  safety.Severity
		reason    string
	}{
		{target: platform.Postgres, statement: "ALTER TABLE users DROP CONSTRAINT users_email_key",
			severity: safety.Destructive, reason: "DROP CONSTRAINT removes an existing data protection"},
		{target: platform.MySQL, statement: "ALTER TABLE users DROP CONSTRAINT chk_age",
			severity: safety.Destructive, reason: "DROP CONSTRAINT removes an existing data protection"},
		{target: platform.MySQL, statement: "ALTER TABLE users DROP INDEX by_name", severity: safety.Safe, reason: safeReason},
		{target: platform.MySQL, statement: "ALTER TABLE users DROP KEY by_name", severity: safety.Safe, reason: safeReason},
		{target: platform.MySQL, statement: "ALTER TABLE users DROP PRIMARY KEY", severity: safety.Safe, reason: safeReason},
		{target: platform.MySQL, statement: "ALTER TABLE users DROP FOREIGN KEY fk_team", severity: safety.Safe, reason: safeReason},
		{target: platform.MySQL, statement: "ALTER TABLE users DROP PARTITION p0", severity: safety.Safe, reason: safeReason},
		{target: platform.MySQL, statement: "ALTER TABLE users ALTER COLUMN age DROP DEFAULT", severity: safety.Safe, reason: safeReason},
		{target: platform.Postgres, statement: "ALTER TABLE users ALTER COLUMN age DROP EXPRESSION", severity: safety.Safe, reason: safeReason},
		{target: platform.Spanner, statement: `ALTER TABLE "sessions" DROP TTL`, severity: safety.Safe, reason: safeReason},
		{target: platform.Postgres, statement: "ALTER TABLE users ALTER COLUMN id DROP IDENTITY IF EXISTS", severity: safety.Safe, reason: safeReason},
		{target: platform.Postgres, statement: "ALTER TABLE users ADD CONSTRAINT c CHECK (note <> 'a drop b c')", severity: safety.Safe,
			reason: safeReason},
		{target: platform.Postgres, statement: "ALTER TABLE users -- drop legacy\n  ADD COLUMN fresh int", severity: safety.Safe, reason: safeReason},
		{target: platform.Postgres, statement: "ALTER TABLE users DROP ſtatistics", severity: safety.Destructive, reason: dropColumnReason},
	}
	for _, test := range tests {
		t.Run(test.target+"/"+test.statement, func(t *testing.T) {
			c := qt.New(t)
			got := assessText(c, test.target, test.statement)
			c.Assert(got.Severity, qt.Equals, test.severity)
			c.Assert(got.Reason, qt.Equals, test.reason)
		})
	}
}

// Every word that follows DROP when an ALTER TABLE drops something other
// than a column keeps the statement from reading as a column drop, in each
// dialect's spelling, and a DROP outside an ALTER TABLE drops no column
// either.
func TestAssessSQL_NoColumnDropWords(t *testing.T) {
	for _, statement := range []string{
		"ALTER TABLE t DROP CHANGEFEED feed",
		"ALTER TABLE t DROP CHECK chk",
		"ALTER TABLE t DROP DETACHED PARTITION p",
		"ALTER TABLE t DROP FAMILY f",
		"ALTER TABLE t ALTER COLUMN c DROP HIDDEN",
		"ALTER TABLE t ALTER COLUMN c DROP MASKED",
		"ALTER TABLE t ALTER COLUMN c DROP NOT NULL",
		"ALTER TABLE t DROP PART 'all_1_1_0'",
		"ALTER TABLE t DROP PERIOD FOR SYSTEM_TIME",
		"ALTER TABLE t ALTER COLUMN c DROP PERSISTED",
		"ALTER TABLE t DROP PROJECTION p",
		"ALTER TABLE t DROP ROW DELETION POLICY",
		"ALTER TABLE t ALTER COLUMN c DROP ROWGUIDCOL",
		"ALTER TABLE t ALTER COLUMN c DROP SPARSE",
		"ALTER TABLE t DROP STATISTICS c",
		"ALTER TABLE t DROP SUBPARTITION p",
		"ALTER TABLE t DROP SYSTEM VERSIONING",
		"ALTER TABLE t DROP UNIQUE (a, b)",
		"ALTER TABLE t DROP UNUSED COLUMNS",
		"GRANT SELECT, DROP ON shop.* TO admin",
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(assessText(c, "", statement).Reason, qt.Not(qt.Equals), dropColumnReason)
		})
	}
}
