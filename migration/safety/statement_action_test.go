package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/safety"
)

// TestStatementAction names each statement by the words before the object's
// name, which differ between a creation, an alteration and a drop of one
// object, and by its first word where the words give no name.
func TestStatementAction(t *testing.T) {
	for _, test := range []struct {
		name      string
		target    string
		statement string
		want      string
	}{
		{name: "a PostgreSQL table created", target: "postgres", statement: `CREATE TABLE "public"."notes" ("id" bigint)`, want: "CREATE TABLE"},
		{name: "a PostgreSQL column added", target: "postgres", statement: `ALTER TABLE "public"."notes" ADD COLUMN "body" text`, want: "ALTER TABLE"},
		{
			name: "a PostgreSQL index built concurrently", target: "postgres",
			statement: `CREATE UNIQUE INDEX CONCURRENTLY "notes_body" ON "public"."notes" ("body")`, want: "CREATE UNIQUE INDEX CONCURRENTLY",
		},
		{name: "a MySQL row inserted", target: "mysql", statement: "INSERT IGNORE INTO `app`.`notes` (`id`) VALUES (1)", want: "INSERT IGNORE INTO"},
		{name: "a MySQL table dropped", target: "mysql", statement: "DROP TABLE IF EXISTS `app`.`notes`", want: "DROP TABLE IF EXISTS"},
		{name: "a SQLite pragma", target: "sqlite", statement: "PRAGMA foreign_keys = ON", want: "PRAGMA"},
		{name: "an anonymous block names no object", target: "postgres", statement: "DO $$ BEGIN PERFORM 1; END $$", want: "DO"},
		{name: "two statements", target: "sqlite", statement: "CREATE TABLE a (id INTEGER); DROP TABLE b", want: "CREATE TABLE; DROP TABLE"},
		{name: "no statement", target: "sqlite", statement: "  ", want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(safety.StatementAction(test.statement, test.target), qt.Equals, test.want)
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
