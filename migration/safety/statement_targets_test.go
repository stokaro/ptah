package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/safety"
)

// TestStatementTargets reads each statement by the words before the object's
// name, the name's parts, and in an ALTER the head of the clause after the
// name, which stops before a literal so no value is read.
func TestStatementTargets(t *testing.T) {
	for _, test := range []struct {
		name      string
		target    string
		statement string
		want      []safety.StatementTarget
	}{
		{
			name: "a PostgreSQL table created", target: "postgres", statement: `CREATE TABLE "public"."notes" ("id" bigint)`,
			want: []safety.StatementTarget{{Action: "CREATE TABLE", Objects: [][]string{{"public", "notes"}}}},
		},
		{
			name: "a PostgreSQL column added", target: "postgres", statement: `ALTER TABLE "public"."notes" ADD COLUMN "body" text`,
			want: []safety.StatementTarget{{Action: "ALTER TABLE", Objects: [][]string{{"public", "notes"}}, Clause: "ADD COLUMN body TEXT"}},
		},
		{
			name: "a PostgreSQL default set to a literal", target: "postgres",
			statement: `ALTER TABLE "public"."notes" ALTER COLUMN "body" SET DEFAULT 'secret'`,
			want:      []safety.StatementTarget{{Action: "ALTER TABLE", Objects: [][]string{{"public", "notes"}}, Clause: "ALTER COLUMN body SET"}},
		},
		{
			name: "a PostgreSQL column whose type takes a parameter", target: "postgres",
			statement: `ALTER TABLE "public"."notes" ADD COLUMN "body" varchar(20)`,
			want:      []safety.StatementTarget{{Action: "ALTER TABLE", Objects: [][]string{{"public", "notes"}}, Clause: "ADD COLUMN body VARCHAR"}},
		},
		{
			name: "a PostgreSQL table enabling row security", target: "postgres", statement: `ALTER TABLE "public"."notes" ENABLE ROW LEVEL SECURITY`,
			want: []safety.StatementTarget{{Action: "ALTER TABLE", Objects: [][]string{{"public", "notes"}}, Clause: "ENABLE ROW LEVEL SECURITY"}},
		},
		{
			name: "a PostgreSQL sequence restarted at a number", target: "postgres", statement: `ALTER SEQUENCE "public"."ids" RESTART WITH 100`,
			want: []safety.StatementTarget{{Action: "ALTER SEQUENCE", Objects: [][]string{{"public", "ids"}}, Clause: "RESTART WITH"}},
		},
		{
			name: "a MySQL table altered by several clauses", target: "mysql",
			statement: "ALTER TABLE `app`.`notes` ADD COLUMN `price` decimal(10,2) NOT NULL, DROP COLUMN `body`, COMMENT 'secret'",
			want: []safety.StatementTarget{{
				Action: "ALTER TABLE", Objects: [][]string{{"app", "notes"}}, Clause: "ADD COLUMN price DECIMAL, DROP COLUMN body, COMMENT",
			}},
		},
		{
			name: "a MySQL comment in double quotes, which MySQL reads as a string", target: "mysql",
			statement: "ALTER TABLE `notes` COMMENT \"secret\"",
			want:      []safety.StatementTarget{{Action: "ALTER TABLE", Objects: [][]string{{"notes"}}, Clause: "COMMENT"}},
		},
		{
			name: "a PostgreSQL index built concurrently", target: "postgres",
			statement: `CREATE UNIQUE INDEX CONCURRENTLY "notes_body" ON "public"."notes" ("body")`,
			want:      []safety.StatementTarget{{Action: "CREATE UNIQUE INDEX CONCURRENTLY", Objects: [][]string{{"notes_body"}}}},
		},
		{
			name: "a MySQL row inserted", target: "mysql", statement: "INSERT IGNORE INTO `app`.`notes` (`id`) VALUES (1)",
			want: []safety.StatementTarget{{Action: "INSERT IGNORE INTO", Objects: [][]string{{"app", "notes"}}}},
		},
		{
			name: "a MySQL table whose one name holds a dot", target: "mysql", statement: "DROP TABLE IF EXISTS `app.notes`",
			want: []safety.StatementTarget{{Action: "DROP TABLE IF EXISTS", Objects: [][]string{{"app.notes"}}}},
		},
		{
			name: "a SQLite pragma", target: "sqlite", statement: "PRAGMA foreign_keys = ON",
			want: []safety.StatementTarget{{Action: "PRAGMA", Objects: [][]string{{"foreign_keys"}}}},
		},
		{
			name: "a SQL Server name with an escaped bracket and a literal dot", target: "sqlserver",
			statement: "DROP TABLE IF EXISTS [dbo].[order]]s.items]",
			want:      []safety.StatementTarget{{Action: "DROP TABLE IF EXISTS", Objects: [][]string{{"dbo", "order]s.items"}}}},
		},
		{
			name: "a SQL Server clause with a quoted column", target: "sqlserver",
			statement: "ALTER TABLE [Sales].[order items] ADD [unit]]Price] decimal(10,2) DEFAULT 'secret'",
			want: []safety.StatementTarget{{
				Action: "ALTER TABLE", Objects: [][]string{{"Sales", "order items"}}, Clause: "ADD unit]Price DECIMAL",
			}},
		},
		{
			name: "the hybrid scanner keeps separate bracket tokens", target: "", statement: "DROP TABLE [dbo].[notes]",
			want: []safety.StatementTarget{{Action: "DROP TABLE", Objects: [][]string{{"dbo", "notes"}}}},
		},
		{
			name: "an anonymous block names no object", target: "postgres", statement: "DO $$ BEGIN PERFORM 1; END $$",
			want: []safety.StatementTarget{{Action: "DO"}},
		},
		{
			name: "two statements", target: "sqlite", statement: "CREATE TABLE a (id INTEGER); DROP TABLE b",
			want: []safety.StatementTarget{
				{Action: "CREATE TABLE", Objects: [][]string{{"a"}}},
				{Action: "DROP TABLE", Objects: [][]string{{"b"}}},
			},
		},
		{
			name: "an engine with no descriptor is read with the default lexer", target: "no-such-engine", statement: "ALTER TABLE notes DROP COLUMN body",
			want: []safety.StatementTarget{{Action: "ALTER TABLE", Objects: [][]string{{"notes"}}, Clause: "DROP COLUMN BODY"}},
		},
		{name: "no statement", target: "sqlite", statement: "  ", want: make([]safety.StatementTarget, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(safety.StatementTargets(test.statement, test.target), qt.DeepEquals, test.want)
		})
	}
}

// TestAssessSQL_NamesWhatEachStatementActsOn reads the subject of a statement
// assessed as SQL text the way a rendered statement's is read.
func TestAssessSQL_NamesWhatEachStatementActsOn(t *testing.T) {
	for _, test := range []struct {
		name      string
		target    string
		statement string
		want      string
	}{
		{name: "a PostgreSQL table", target: "postgres", statement: `ALTER TABLE "public"."notes" ADD COLUMN "body" text`, want: "public.notes"},
		{name: "a MySQL table", target: "mysql", statement: "ALTER TABLE `app`.`notes` ADD COLUMN `body` text", want: "app.notes"},
		{name: "a SQLite table", target: "sqlite", statement: "CREATE TABLE notes (id INTEGER)", want: "notes"},
		{name: "an anonymous block", target: "postgres", statement: "DO $$ BEGIN PERFORM 1; END $$", want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(assessText(c, test.target, test.statement).Subject, qt.Equals, test.want)
		})
	}
}
