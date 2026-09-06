//go:build integration

package integration_test

// The regression stokaro/ptah#2770 reported: `UNIQUE KEY uq (slug(7))`.
//
// A UNIQUE key was read into a schemamodel constraint, which carries column
// NAMES and nothing else, so the prefix length was parsed and then discarded.
// What Ptah applied was `CONSTRAINT uq UNIQUE (slug)`, and the difference
// between the two is not a representation. Measured on MySQL 26.7, both
// spellings side by side in one database:
//
//	authored  UNIQUE KEY uq (slug(7))      rejects the second row, ERROR 1062
//	rendered  CONSTRAINT uq UNIQUE (slug)  accepts it
//	rows      authored = 1, rendered = 2
//
// Ptah weakened a uniqueness guarantee the author had written down, and
// reported the database in sync while doing it.
//
// Only a server can answer that, and only with a write. NON_UNIQUE = 0 is true
// of both spellings, so a catalog assertion alone would have passed throughout
// the defect; SUB_PART separates them, and the refused INSERT is the harm
// itself rather than an argument for it. Both are asserted here, and they fail
// for different reasons: SUB_PART says Ptah built a different index, the
// refused row says that difference lets data in.
//
// The catalog is read directly rather than through Ptah, because a model that
// misunderstands an index misunderstands it in both directions -- Ptah would
// render its misunderstanding and read the same misunderstanding back, and the
// two sides would agree while both were wrong. The comparison then closes the
// loop from the other end: paired with a catalog read that does not go through
// Ptah at all, an in-sync `schema compare` says the desired model and the
// server describe one index.
//
// The fixture carries stokaro/ptah#2776 as well, in `UNIQUE uq_accounts_email
// (email)`: a name with no KEY or INDEX keyword before it, which this reader
// refused outright before the same change. It costs no second database and it
// fails early and loudly -- a refused declaration never reaches an apply -- so
// the spelling is exercised against both servers rather than at the parser
// alone. Which rows it covers, and which it deliberately does not, is in
// TestParse_MySQLUniqueKeyNameSpellings_HappyPath.
//
// The commands are driven in process through the cobra tree `cmd/schema`
// builds, which is the shape mysql_inline_index_naming_e2e_test.go and
// mysql_typed_index_roundtrip_e2e_test.go already establish for the native
// schema verbs: the same user-facing surface `ptah schema apply` and
// `ptah schema compare` present, flags and exit status included, without a
// `go build` in the middle of a database test.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the MySQL driver, which both engines here speak, for database/sql

	"ptah.run/internal/dbtarget"
)

// uniqueKeyPrefixSchema is the declaration the issue was filed about, beside
// the bare-name spelling of stokaro/ptah#2776.
//
// VARCHAR(64) with a prefix of 7 rather than a prefix covering the whole
// column: a prefix as long as the column is one both engines normalize away,
// reporting SUB_PART NULL, so the fixture would assert the very absence a
// dropped prefix produces.
//
// `uq_accounts_name (name DESC)` is the OTHER attribute stokaro/ptah#2770
// names, and it is here because the prefix alone cannot speak for it: the
// promotion predicate reads the two separately, so a reader that kept the
// prefix and dropped the direction leaves every assertion about `slug`
// standing. Measured on MySQL 26.7 and MariaDB 12.3, both report `COLLATION D`
// for it and `A` for every other part, and both apply and read the direction
// back -- the constraint form Ptah used to render reports `A`.
const uniqueKeyPrefixSchema = `CREATE TABLE accounts (
  id BIGINT NOT NULL PRIMARY KEY,
  slug VARCHAR(64) NOT NULL,
  email VARCHAR(64) NOT NULL,
  name VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_accounts_slug (slug(7)),
  UNIQUE uq_accounts_email (email),
  UNIQUE KEY uq_accounts_name (name DESC)
);
`

// TestAUniqueKeyPrefixRejectsTheDuplicateItsConstraintFormWouldAccept
// establishes that `UNIQUE KEY uq_accounts_slug (slug(7))` reaches the server
// as a unique index over the first seven characters of the column, that the
// server then refuses a second row sharing those seven characters, and that the
// same file compares in sync against the database it created.
//
// The three assertions answer different defects, and where each one reddens was
// measured rather than argued by reverting the promotion predicate to the one
// it replaced:
//
//	the prefix is dropped between the reader and the renderer -- the catalog
//	map, on both engines, which reports SUB_PART NULL for an index that is
//	still named `uq_accounts_slug` and still NON_UNIQUE 0;
//
//	that dropped prefix admits a row the author forbade -- the refused INSERT,
//	on both engines, which is the same run's second write succeeding;
//
//	the index is built correctly and then not read back -- assertComparesInSync
//	alone, with the catalog and the write both passing.
//
// The direction is the fourth, and it fails alone: with the promotion widened
// for a prefix but not for a DESC part, every assertion about `slug` above
// still passes and `uq_accounts_name` comes back `COLLATION A` on both engines,
// which is the ascending index the constraint form renders.
//
// The declaration is refused at the reader instead -- `UNIQUE uq_accounts_email
// (email)`, before stokaro/ptah#2776 -- and then nothing below runs: the apply
// fails first, with the parse error, on both engines.
func TestAUniqueKeyPrefixRejectsTheDuplicateItsConstraintFormWouldAccept(t *testing.T) {
	for _, test := range inlineIndexEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			target := newUniqueKeyPrefixDatabase(c, ctx, test.admin)
			schemaPath := writeInlineIndexSchema(c, uniqueKeyPrefixSchema)

			applyDesiredSchema(c, target.url, schemaPath)

			c.Assert(uniqueKeyPrefixShape(c, ctx, target), qt.DeepEquals, map[string]uniqueKeyIndex{
				"PRIMARY": {NonUnique: 0, Parts: []uniqueKeyPart{
					{Column: "id", Collation: "A"},
				}},
				"uq_accounts_slug": {NonUnique: 0, Parts: []uniqueKeyPart{
					{Column: "slug", SubPart: "7", Collation: "A"},
				}},
				"uq_accounts_email": {NonUnique: 0, Parts: []uniqueKeyPart{
					{Column: "email", Collation: "A"},
				}},
				"uq_accounts_name": {NonUnique: 0, Parts: []uniqueKeyPart{
					{Column: "name", Collation: "D"},
				}},
			})

			// The two slugs differ, and share their first seven characters.
			// That is what makes the second write a question about the PREFIX:
			// a unique index over the whole column accepts both, which is
			// exactly what the constraint form Ptah used to render is.
			c.Assert(insertAccount(ctx, target, 1, "abcdefg-one", "one@example.test", "one"), qt.IsNil)
			refused := insertAccount(ctx, target, 2, "abcdefg-two", "two@example.test", "two")
			c.Assert(refused, qt.IsNotNil)
			// The message and nothing around it. MySQL 26.7 answers `Duplicate
			// entry 'abcdefg' for key 'accounts.uq_accounts_slug'` and MariaDB
			// 12.3 the same sentence with `for key 'uq_accounts_slug'`, so the
			// qualified name is one engine's spelling rather than the fact. The
			// truncated value in it is the server saying which key refused the
			// write.
			c.Assert(refused.Error(), qt.Contains, "Duplicate entry 'abcdefg'")
			c.Assert(accountCount(c, ctx, target), qt.Equals, 1)

			assertComparesInSync(c, target.url, schemaPath)
		})
	}
}

// newUniqueKeyPrefixDatabase creates an empty database of its own for one
// engine and removes it afterwards.
//
// A database per case rather than the shared one, because `schema apply`
// reconciles the WHOLE target: run against a database holding anything else, it
// would plan a drop for every table this declaration does not mention. The
// administrative account is what can create one, and both engines' admin
// variables are set by .github/workflows/go-integration-tests.yml, so neither
// row skips.
func newUniqueKeyPrefixDatabase(c *qt.C, ctx context.Context, engine dbtarget.Engine) inlineIndexTarget {
	c.Helper()

	adminDB, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	database := fmt.Sprintf("ptah_2770_prefix_%d", time.Now().UnixNano())
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

// uniqueKeyPart is one key part as information_schema.STATISTICS describes it.
//
// SubPart is the catalog's own SUB_PART rendered as text, empty where the
// catalog reported NULL, and the emptiness is produced by the query rather than
// by a decision here. Collation is the catalog's own `A` or `D`, likewise
// rendered rather than folded to a direction of this file's choosing.
//
// The two are the fields this test exists for, and they are two rather than one
// because the reader decides them separately: they are what separates the index
// the author declared from the whole-column, ascending unique constraint Ptah
// used to render in its place, and a reader that kept one attribute and dropped
// the other satisfies an assertion carrying only the one it kept.
type uniqueKeyPart struct {
	Column    string
	SubPart   string
	Collation string
}

// uniqueKeyIndex is one index, as its parts in key order.
//
// NonUnique is kept as the catalog's 0-or-1 rather than folded to a bool, so a
// row asserting 0 is naming the value the server reported. It cannot fail alone
// here, and that is the point of keeping it: the defect this file is about left
// NON_UNIQUE at 0 throughout, so an assertion that read only this field would
// have passed against every version of the code.
type uniqueKeyIndex struct {
	NonUnique int
	Parts     []uniqueKeyPart
}

// uniqueKeyPrefixShape reads back every index on the applied table, from the
// catalog rather than through Ptah.
//
// Reading it through Ptah would compare the model with itself: a reader that
// dropped a prefix renders an index without one and reads that same index back,
// and the assertion would agree with the defect. The whole map is returned, and
// the caller compares the whole map, so an index that should not exist fails
// the same assertion as one that is missing.
//
// SUB_PART and COLLATION are both cast in the query because both are nullable,
// and the empty string either becomes is a value the catalog produced rather
// than a sentinel chosen here.
func uniqueKeyPrefixShape(c *qt.C, ctx context.Context, target inlineIndexTarget) map[string]uniqueKeyIndex {
	c.Helper()

	rows, err := target.admin.QueryContext(ctx,
		"SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME, COALESCE(CAST(SUB_PART AS CHAR), ''), "+
			"COALESCE(CAST(COLLATION AS CHAR), '') "+
			"FROM information_schema.STATISTICS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'accounts' "+
			"ORDER BY INDEX_NAME, SEQ_IN_INDEX",
		target.database)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	shape := make(map[string]uniqueKeyIndex)
	for rows.Next() {
		var name, column, subPart, collation string
		var nonUnique int
		c.Assert(rows.Scan(&name, &nonUnique, &column, &subPart, &collation), qt.IsNil)
		entry := shape[name]
		entry.NonUnique = nonUnique
		entry.Parts = append(entry.Parts, uniqueKeyPart{Column: column, SubPart: subPart, Collation: collation})
		shape[name] = entry
	}
	c.Assert(rows.Err(), qt.IsNil)
	return shape
}

// insertAccount writes one row and hands its outcome back for the caller to
// assert, because whether the server accepts it is the thing under test.
func insertAccount(ctx context.Context, target inlineIndexTarget, id int, slug, email, name string) error {
	_, err := target.admin.ExecContext(ctx,
		fmt.Sprintf("INSERT INTO `%s`.`accounts` (id, slug, email, name) VALUES (?, ?, ?, ?)", target.database),
		id, slug, email, name)
	return err
}

// accountCount is how many rows actually landed, which is what separates a
// write the server accepted from one it reported and discarded.
func accountCount(c *qt.C, ctx context.Context, target inlineIndexTarget) int {
	c.Helper()

	var count int
	c.Assert(target.admin.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`accounts`", target.database)).Scan(&count), qt.IsNil)
	return count
}
