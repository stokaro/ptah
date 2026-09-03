//go:build integration

package integration_test

// The regression stokaro/ptah#2713 reported: an inline `KEY (email)` written
// without a name.
//
// MySQL and MariaDB name such an index themselves -- after its first key part's
// column -- and Ptah's desired model held the empty string instead. Measured
// through `ptah schema compare` against the very database the declaration had
// just created, on MySQL 8.4.11:
//
//	KEY (email)         exit 2, comparing nothing at all: "target index
//	                    reference at position 0 requires a name and owning table"
//	UNIQUE KEY (email)  exit 0 and a difference that never converges -- drop the
//	                    constraint the server called `email`, add a nameless one,
//	                    on every run forever
//	KEY (a), KEY (a, b) schemamodel.Finalize deduplicates indexes on
//	                    {table, name}, so both shared the key {table, ""} and the
//	                    second was discarded in silence
//
// Only a live server can answer these. The name under test is the one the
// SERVER would have assigned, so a fixture written by hand is a restatement of
// the rule rather than a measurement of it; and the index Ptah renders has to
// be read back out of information_schema rather than through Ptah, because a
// model that misunderstands an index misunderstands it in both directions and
// the two sides agree while both are wrong.
//
// Where each shape reddens here is not where the issue reported it, and the
// difference is worth stating so a future failure is read correctly. `schema
// apply` computes the same diff before it writes anything, so with the naming
// pass removed the two non-unique forms never reach their comparison at all:
// measured on both engines, they fail at `applyDesiredSchema` with the issue's
// own sentence, and the catalog and comparison assertions below them do not
// run. Only the unique form applies cleanly and is caught by the comparison.
//
// That also says which assertion measures what. Removing the naming pass is
// answered by the CLI assertions alone -- both servers auto-name an index Ptah
// leaves nameless, so the catalog sees `email` either way. What the catalog
// read pins is the other direction: an implementation that derived some other
// unique name would satisfy every comparison here, because Ptah would render
// its invention and read its invention back. Measured with `derive` returning
// `idx_<column>`: all three comparisons stay green and all three catalog
// assertions redden.
//
// The commands are driven in process, through the cobra tree `cmd/schema`
// builds, rather than through a binary this test compiles. That is the shape
// `integration/sqlitecmd` and `schema_lineage_live_e2e_test.go` already
// establish for the native schema verbs: it is the same user-facing surface
// `ptah schema apply` and `ptah schema compare` present -- flags, exit status
// and all -- without a `go build` in the middle of a database test. The public
// Go API was the alternative and would have measured less: the exit-2 refusal
// above is raised by the comparator both verbs run, so it is a status a caller
// of the command sees, and `--exit-code` is where a non-empty diff becomes a
// failure.

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the MySQL driver, which both engines here speak, for database/sql

	cmdschema "go.5x5.cz/ptah/cmd/schema"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// inlineIndexEngines are the two servers that name an unnamed index this way.
//
// The administrative account rather than the ordinary one, because each case
// needs a database of its own: `schema apply` reconciles the WHOLE target, so
// running it against a shared database would plan a drop for every table the
// declaration does not mention. Both variables are set by
// .github/workflows/go-integration-tests.yml, so neither row skips.
var inlineIndexEngines = []struct {
	name  string
	admin dbtarget.Engine
}{
	{name: "mysql", admin: dbtarget.MySQLAdmin},
	{name: "mariadb", admin: dbtarget.MariaDBAdmin},
}

// unnamedKeySchema is the declaration the issue was filed about.
const unnamedKeySchema = `CREATE TABLE users (
  id BIGINT NOT NULL PRIMARY KEY,
  email VARCHAR(255) NOT NULL,
  KEY (email)
);
`

// unnamedUniqueKeySchema is the same declaration made unique.
//
// It is the half that failed differently: the comparator accepted it and
// planned a difference that could not converge, so a run reported success while
// promising to do the same work again next time.
const unnamedUniqueKeySchema = `CREATE TABLE users (
  id BIGINT NOT NULL PRIMARY KEY,
  email VARCHAR(255) NOT NULL,
  UNIQUE KEY (email)
);
`

// twoUnnamedKeysSchema declares two indexes that derive their name from the
// same column, which is what makes the _2 suffix observable.
//
// The second index covers (a, b) rather than (a) alone so that the assertion
// can tell the two apart: a test whose indexes had identical key parts would
// pass against an implementation that emitted the first one twice.
const twoUnnamedKeysSchema = `CREATE TABLE users (
  id BIGINT NOT NULL PRIMARY KEY,
  a VARCHAR(64) NOT NULL,
  b VARCHAR(64) NOT NULL,
  KEY (a),
  KEY (a, b)
);
`

// foreignKeyBackingSchema is stokaro/ptah#2769: a foreign key with nothing to
// reuse, and an unnamed index that would take the name the engine is about to
// give its backing index.
const foreignKeyBackingSchema = `CREATE TABLE parents (
  id BIGINT NOT NULL PRIMARY KEY
);

CREATE TABLE users (
  id BIGINT NOT NULL PRIMARY KEY,
  parent_id BIGINT NOT NULL,
  email VARCHAR(255) NOT NULL,
  CONSTRAINT email FOREIGN KEY (parent_id) REFERENCES parents (id),
  KEY (email)
);
`

// descendingCandidateSchema is the same contention with a descending key that
// could back the foreign key -- which is where the two engines part.
const descendingCandidateSchema = `CREATE TABLE parents (
  id BIGINT NOT NULL PRIMARY KEY
);

CREATE TABLE users (
  id BIGINT NOT NULL PRIMARY KEY,
  parent_id BIGINT NOT NULL,
  email VARCHAR(255) NOT NULL,
  KEY by_parent (parent_id DESC),
  CONSTRAINT email FOREIGN KEY (parent_id) REFERENCES parents (id),
  KEY (email)
);
`

// TestAForeignKeysBackingIndexTakesItsNameBeforeAnUnnamedIndex is
// stokaro/ptah#2769 through the verb that failed.
//
// A foreign key with no covering index makes the engine build one named after
// the constraint. Ptah derived that same name for the later unnamed index and
// emits it as its own CREATE INDEX first, so the ALTER TABLE that follows
// failed with `ERROR 1061 (42000): Duplicate key name`.
//
// Only apply can show it. The name Ptah derives is a model fact and the
// collision is a server fact, so a fixture asserting `email_2` restates the
// rule while an apply demonstrates that the two agree -- and the catalog read
// is what separates "Ptah picked the same name as the server" from "Ptah picked
// a name and read its own invention back".
func TestAForeignKeysBackingIndexTakesItsNameBeforeAnUnnamedIndex(t *testing.T) {
	for _, test := range inlineIndexEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			target := newInlineIndexDatabase(c, ctx, test.admin, "fkbacking")
			schemaPath := writeInlineIndexSchema(c, foreignKeyBackingSchema)

			applyDesiredSchema(c, target.url, schemaPath)

			c.Assert(inlineIndexShape(c, ctx, target), qt.DeepEquals, map[string]inlineIndex{
				"PRIMARY": {NonUnique: 0, Columns: []string{"id"}},
				"email":   {NonUnique: 1, Columns: []string{"parent_id"}},
				"email_2": {NonUnique: 1, Columns: []string{"email"}},
			})

			assertComparesInSync(c, target.url, schemaPath)
		})
	}
}

// TestADescendingCandidateBacksTheForeignKeyOnMariaDBOnly is the rule the two
// engines answer differently, applied.
//
// Measured on MySQL 8.4.11 and MariaDB 11.8.9 before it was written here: with
// `KEY by_parent (parent_id DESC)` ahead of the constraint, MySQL keeps that
// index descending and adds an ascending `email` to back the key, while MariaDB
// reuses it and adds nothing. So the same declaration is three indexes on one
// engine and two on the other, and the unnamed `KEY (email)` takes a different
// name on each.
//
// A shared answer is wrong for exactly one engine whichever way it is chosen,
// which is what makes this worth an apply on both rather than one.
func TestADescendingCandidateBacksTheForeignKeyOnMariaDBOnly(t *testing.T) {
	shapes := map[string]map[string]inlineIndex{
		"mysql": {
			"PRIMARY":   {NonUnique: 0, Columns: []string{"id"}},
			"by_parent": {NonUnique: 1, Columns: []string{"parent_id"}},
			"email":     {NonUnique: 1, Columns: []string{"parent_id"}},
			"email_2":   {NonUnique: 1, Columns: []string{"email"}},
		},
		"mariadb": {
			"PRIMARY":   {NonUnique: 0, Columns: []string{"id"}},
			"by_parent": {NonUnique: 1, Columns: []string{"parent_id"}},
			"email":     {NonUnique: 1, Columns: []string{"email"}},
		},
	}

	for _, test := range inlineIndexEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			target := newInlineIndexDatabase(c, ctx, test.admin, "fkdesc")
			schemaPath := writeInlineIndexSchema(c, descendingCandidateSchema)

			applyDesiredSchema(c, target.url, schemaPath)

			c.Assert(inlineIndexShape(c, ctx, target), qt.DeepEquals, shapes[test.name])

			assertComparesInSync(c, target.url, schemaPath)
		})
	}
}

// TestUnnamedInlineKeyTakesTheNameItsServerWouldGiveIt establishes that a plain
// `KEY (email)` reaches the database as the non-unique index `email`, that the
// column it indexes still accepts a repeated value, and that the declaration
// then compares in sync against the database it created.
//
// All three matter separately. The name is what the comparator needs to compare
// at all; the non-uniqueness is what the author asked for; and the duplicate
// row is the harm the issue names, which NON_UNIQUE = 1 argues for without
// demonstrating.
func TestUnnamedInlineKeyTakesTheNameItsServerWouldGiveIt(t *testing.T) {
	for _, test := range inlineIndexEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			target := newInlineIndexDatabase(c, ctx, test.admin, "key")
			schemaPath := writeInlineIndexSchema(c, unnamedKeySchema)

			applyDesiredSchema(c, target.url, schemaPath)

			c.Assert(inlineIndexShape(c, ctx, target), qt.DeepEquals, map[string]inlineIndex{
				"PRIMARY": {NonUnique: 0, Columns: []string{"id"}},
				"email":   {NonUnique: 1, Columns: []string{"email"}},
			})
			c.Assert(insertUser(ctx, target, 1, "duplicate@example.test"), qt.IsNil)
			c.Assert(insertUser(ctx, target, 2, "duplicate@example.test"), qt.IsNil)
			c.Assert(userCount(c, ctx, target), qt.Equals, 2)

			assertComparesInSync(c, target.url, schemaPath)
		})
	}
}

// TestUnnamedInlineUniqueKeyTakesTheNameItsServerWouldGiveIt establishes the
// same three facts for `UNIQUE KEY (email)`: the index arrives named `email`,
// it is unique, and the declaration compares in sync.
//
// The rejected duplicate is not a failure-path case of the test above; it is
// how uniqueness is measured rather than assumed. NON_UNIQUE = 0 is the
// catalog's word for it, and the refused INSERT is the server's.
func TestUnnamedInlineUniqueKeyTakesTheNameItsServerWouldGiveIt(t *testing.T) {
	for _, test := range inlineIndexEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			target := newInlineIndexDatabase(c, ctx, test.admin, "unique")
			schemaPath := writeInlineIndexSchema(c, unnamedUniqueKeySchema)

			applyDesiredSchema(c, target.url, schemaPath)

			c.Assert(inlineIndexShape(c, ctx, target), qt.DeepEquals, map[string]inlineIndex{
				"PRIMARY": {NonUnique: 0, Columns: []string{"id"}},
				"email":   {NonUnique: 0, Columns: []string{"email"}},
			})
			c.Assert(insertUser(ctx, target, 1, "duplicate@example.test"), qt.IsNil)
			refused := insertUser(ctx, target, 2, "duplicate@example.test")
			c.Assert(refused, qt.IsNotNil)
			// The message and nothing around it: MySQL 26.7 answers
			// `Duplicate entry 'duplicate@example.test' for key 'users.email'`
			// and MariaDB 12.3 the same sentence with `for key 'email'`, so the
			// qualified name is one engine's spelling rather than the fact.
			c.Assert(refused.Error(), qt.Contains, "Duplicate entry")
			c.Assert(userCount(c, ctx, target), qt.Equals, 1)

			assertComparesInSync(c, target.url, schemaPath)
		})
	}
}

// TestTwoUnnamedInlineKeysTakeDistinctServerNames establishes that two indexes
// deriving from the same column arrive as two indexes, named `a` and `a_2`.
//
// It needs its own test because the earlier failure was not a wrong name but a
// missing index: with both names empty, schemamodel.Finalize deduplicated them
// onto the single key {users, ""} and the second declaration was dropped
// without a word. A test that only read `a` would have passed throughout.
func TestTwoUnnamedInlineKeysTakeDistinctServerNames(t *testing.T) {
	for _, test := range inlineIndexEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			target := newInlineIndexDatabase(c, ctx, test.admin, "two")
			schemaPath := writeInlineIndexSchema(c, twoUnnamedKeysSchema)

			applyDesiredSchema(c, target.url, schemaPath)

			c.Assert(inlineIndexShape(c, ctx, target), qt.DeepEquals, map[string]inlineIndex{
				"PRIMARY": {NonUnique: 0, Columns: []string{"id"}},
				"a":       {NonUnique: 1, Columns: []string{"a"}},
				"a_2":     {NonUnique: 1, Columns: []string{"a", "b"}},
			})

			assertComparesInSync(c, target.url, schemaPath)
		})
	}
}

// inlineIndexTarget is one throwaway database on a live MySQL-family server,
// carrying both spellings of its address.
//
// The two are not interchangeable and the fields say which is which: `admin` is
// opened from dbtarget.DriverDSN, because go-sql-driver reads a `mysql://`
// prefix as part of the username, while `url` is what Ptah connects with and
// carries the scheme its dialect is resolved from.
type inlineIndexTarget struct {
	admin    *sql.DB
	database string
	url      string
}

// newInlineIndexDatabase creates an empty database of its own for one case and
// removes it afterwards.
func newInlineIndexDatabase(
	c *qt.C, ctx context.Context, engine dbtarget.Engine, purpose string,
) inlineIndexTarget {
	c.Helper()

	adminDB, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	database := fmt.Sprintf("ptah_2713_%s_%d", purpose, time.Now().UnixNano())
	createMySQLDatabase(c, ctx, adminDB, database)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), adminDB, database) })

	// replaceMySQLDatabaseName rather than replaceDatabaseName: the latter goes
	// through url.Parse, and `mysql://root:***@tcp(127.0.0.1:3306)/mysql` is not
	// a URL -- the driver-style host makes it answer `invalid port ":3306)"`.
	return inlineIndexTarget{
		admin:    adminDB,
		database: database,
		url:      replaceMySQLDatabaseName(c, dbtarget.URL(c, engine), database),
	}
}

// writeInlineIndexSchema puts one declaration where --schema-file can read it.
func writeInlineIndexSchema(c *qt.C, body string) string {
	c.Helper()

	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// applyDesiredSchema drives `ptah schema apply` against a database.
func applyDesiredSchema(c *qt.C, dbURL, schemaPath string) {
	c.Helper()

	cmd := cmdschema.NewSchemaCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"apply",
		"--db-url", dbURL,
		"--schema-file", schemaPath,
		"--auto-approve",
	})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("apply said:\n%s", out.String()))
}

// assertComparesInSync drives `ptah schema compare` and requires it to find
// nothing, which is the assertion the issue turns on.
//
// --exit-code is what makes a reported difference a failure here, and that is
// the shape the unique form took: a plan dropping the constraint the server
// named and adding a nameless one, reported as success on every run forever.
// The other shape -- the refusal that compares nothing at all -- is raised by
// the same comparator inside `schema apply`, so a declaration carrying it never
// reaches this helper; see the file header. The printed line is asserted beside
// the error so that a future change making compare exit 0 without comparing
// cannot pass silently.
func assertComparesInSync(c *qt.C, dbURL, schemaPath string) {
	c.Helper()

	cmd := cmdschema.NewSchemaCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"compare",
		"--db-url", dbURL,
		"--schema-file", schemaPath,
		"--exit-code",
	})
	err := cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("compare said:\n%s", out.String()))
	c.Assert(out.String(), qt.Contains, "No schema differences detected.")
}

// inlineIndex is one index as information_schema.STATISTICS describes it.
//
// NonUnique is kept as the catalog's own 0-or-1 rather than folded to a bool,
// so a row asserting 1 is naming the value the server reported.
type inlineIndex struct {
	NonUnique int
	Columns   []string
}

// inlineIndexShape reads back every index on the applied table, from the
// catalog rather than through Ptah.
//
// Reading it through Ptah would compare the model with itself: a reader that
// gave an index the wrong name would report that same wrong name, and the
// assertion would agree with the defect. The whole map is returned, and the
// callers compare the whole map, so an index that should not exist fails the
// same assertion as one that is missing.
func inlineIndexShape(c *qt.C, ctx context.Context, target inlineIndexTarget) map[string]inlineIndex {
	c.Helper()

	rows, err := target.admin.QueryContext(ctx,
		"SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME "+
			"FROM information_schema.STATISTICS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'users' "+
			"ORDER BY INDEX_NAME, SEQ_IN_INDEX",
		target.database)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	shape := make(map[string]inlineIndex)
	for rows.Next() {
		var name, column string
		var nonUnique int
		c.Assert(rows.Scan(&name, &nonUnique, &column), qt.IsNil)
		entry := shape[name]
		entry.NonUnique = nonUnique
		entry.Columns = append(entry.Columns, column)
		shape[name] = entry
	}
	c.Assert(rows.Err(), qt.IsNil)
	return shape
}

// insertUser writes one row and hands its outcome back for the caller to
// assert, because whether the server accepts it is the thing under test.
func insertUser(ctx context.Context, target inlineIndexTarget, id int, email string) error {
	_, err := target.admin.ExecContext(ctx,
		fmt.Sprintf("INSERT INTO `%s`.`users` (id, email) VALUES (?, ?)", target.database),
		id, email)
	return err
}

// userCount is how many rows actually landed, which is what separates a write
// the server accepted from one it reported and discarded.
func userCount(c *qt.C, ctx context.Context, target inlineIndexTarget) int {
	c.Helper()

	var count int
	c.Assert(target.admin.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`users`", target.database)).Scan(&count), qt.IsNil)
	return count
}

// foreignKeyOwnershipSchema is the desired state of stokaro/ptah#2782 AFTER the
// unrelated index is removed from it: `cover` backs the foreign key and the
// same-named `f` is gone.
const foreignKeyOwnershipSchema = `CREATE TABLE parents (
  id BIGINT NOT NULL PRIMARY KEY
);

CREATE TABLE users (
  id BIGINT NOT NULL PRIMARY KEY,
  parent_id BIGINT NOT NULL,
  email VARCHAR(255) NOT NULL,
  KEY cover (parent_id),
  CONSTRAINT f FOREIGN KEY (parent_id) REFERENCES parents (id)
);
`

// TestAnIndexSharingAForeignKeysNameIsPlannedForRemoval is stokaro/ptah#2782
// applied: the live database holds an unrelated `f(email)` beside the covering
// `cover(parent_id)`, and the desired schema no longer declares it.
//
// Ownership was inferred from the name, so `f(email)` was suppressed from the
// comparison entirely and the run reported the target synchronized. The index
// then stayed in the database forever, managed by nothing.
//
// The live half is what shows the drop is legal. Both engines refuse to drop
// an index a foreign key needs -- `ERROR 1553` -- so a plan that names the
// wrong index does not merely disagree with the model, it fails to apply. Here
// it applies and the catalog converges on both engines.
func TestAnIndexSharingAForeignKeysNameIsPlannedForRemoval(t *testing.T) {
	for _, test := range inlineIndexEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			target := newInlineIndexDatabase(c, ctx, test.admin, "fkowned")
			seedForeignKeyOwnership(c, ctx, target)

			// The unrelated index is there, sharing the constraint's name.
			c.Assert(inlineIndexShape(c, ctx, target), qt.DeepEquals, map[string]inlineIndex{
				"PRIMARY": {NonUnique: 0, Columns: []string{"id"}},
				"cover":   {NonUnique: 1, Columns: []string{"parent_id"}},
				"f":       {NonUnique: 1, Columns: []string{"email"}},
			})

			schemaPath := writeInlineIndexSchema(c, foreignKeyOwnershipSchema)
			applyDesiredSchema(c, target.url, schemaPath)

			c.Assert(inlineIndexShape(c, ctx, target), qt.DeepEquals, map[string]inlineIndex{
				"PRIMARY": {NonUnique: 0, Columns: []string{"id"}},
				"cover":   {NonUnique: 1, Columns: []string{"parent_id"}},
			})

			assertComparesInSync(c, target.url, schemaPath)
		})
	}
}

// seedForeignKeyOwnership builds the live shape by hand, because it is the one
// Ptah must READ correctly rather than one it would write.
func seedForeignKeyOwnership(c *qt.C, ctx context.Context, target inlineIndexTarget) {
	c.Helper()
	for _, statement := range []string{
		"CREATE TABLE parents (id BIGINT NOT NULL PRIMARY KEY)",
		"CREATE TABLE users (" +
			"id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT NOT NULL, " +
			"email VARCHAR(255) NOT NULL, KEY cover (parent_id), KEY f (email), " +
			"CONSTRAINT f FOREIGN KEY (parent_id) REFERENCES parents (id))",
	} {
		_, err := target.admin.ExecContext(ctx, "USE "+target.database)
		c.Assert(err, qt.IsNil)
		_, err = target.admin.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("seeding %s", statement))
	}
}
