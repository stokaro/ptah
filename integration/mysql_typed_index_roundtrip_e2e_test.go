//go:build integration

package integration_test

// The regression stokaro/ptah#2747 reported: a table body declaring
// `SPATIAL KEY` or `FULLTEXT KEY`.
//
// The grammar is `{SPATIAL|FULLTEXT} [INDEX|KEY] [name] (key_part,...)`. Of the
// four spellings that name a keyword, three were refused by the reader:
// `SPATIAL KEY` demanded INDEX; FULLTEXT was in no keyword list at all, so the
// element fell through to column parsing and the diagnostic named the index
// name as an unsupported column attribute. Measured through
// `ptah schema render --dialect mysql` before the fix:
//
//	SPATIAL INDEX sp_g (geom)  accepted
//	SPATIAL KEY   sp_g (geom)  expected INDEX after SPATIAL: got 'KEY'
//	FULLTEXT INDEX ft_b (bio)  unsupported column attribute: FT_B
//	FULLTEXT KEY   ft_b (bio)  unsupported column attribute: FT_B
//
// The refused spellings are the ones both dumpers WRITE: a table created with
// `SPATIAL INDEX` comes back out of mysqldump 26.7 and mariadb-dump 12.3 as
// `SPATIAL KEY`, so no dump of any MySQL or MariaDB database holding either
// index type could be read at all.
//
// The two spellings that omit the keyword are outside this file, and read at
// the parser instead by TestParse_MySQLTypedIndexOmitsTheKeyword. No dumper
// writes them, so a live round trip would exercise the same server path this
// one does while costing a second database.
//
// A parser test answers whether the declaration is understood. It cannot answer
// whether what Ptah then builds is the index the author asked for, and that is
// the half worth a server: the access method is not observable in the DDL Ptah
// emits alone -- `CREATE SPATIAL INDEX` is accepted by both engines and could
// still have produced a BTREE had the method been dropped between the reader
// and the renderer. So the index is read back out of information_schema, and
// INDEX_TYPE is what is asserted rather than the index's existence.
//
// Asserting the TYPE rather than the name is the discrimination stokaro/ptah#2711
// established. A spatial index parsed as an ordinary index applies cleanly,
// arrives under the name the author wrote, and indexes the right column; only
// INDEX_TYPE separates it from the one that was asked for. NON_UNIQUE travels
// with it as part of one whole-map comparison, but it is the parser test that
// measures the pre-#2711 promise: see the note on typedIndex below for why no
// defect can reach this map with NON_UNIQUE wrong.
//
// The comparison closes the loop in the other direction. A model that
// misunderstood the index would render its misunderstanding and read the same
// misunderstanding back, so `schema compare` agreeing with itself proves
// nothing on its own -- but paired with the catalog read above, which does not
// go through Ptah at all, an in-sync comparison says the desired model and the
// server now describe one index.
//
// The commands are driven in process, through the cobra tree `cmd/schema`
// builds, which is the shape mysql_inline_index_naming_e2e_test.go and
// schema_lineage_live_e2e_test.go already establish for the native schema
// verbs: the same user-facing surface `ptah schema apply` and
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

// typedIndexSchema is the declaration the issue was filed about, written the
// way both dumpers write it.
//
// One fixture serves both engines, which was measured rather than assumed.
// MySQL 26.7 wants a spatial column NOT NULL and takes `SRID 4326` after the
// type; MariaDB 12.3 refuses that spelling outright with a syntax error, and
// neither server needs it to CREATE the index -- `GEOMETRY NOT NULL` alone
// builds a SPATIAL index on both. Declaring the SRID would therefore have cost
// a per-engine fixture and bought nothing this test asserts.
//
// KEY rather than INDEX on both indexes, deliberately: INDEX was the one
// spelling that already worked, so a fixture using it would pass before the
// fix.
const typedIndexSchema = `CREATE TABLE places (
  id BIGINT NOT NULL PRIMARY KEY,
  geom GEOMETRY NOT NULL,
  bio TEXT,
  SPATIAL KEY sp_places_geom (geom),
  FULLTEXT KEY ft_places_bio (bio)
);
`

// TestTypedIndexKeyDeclarationsReachTheServerWithTheirAccessMethod establishes
// that a table body declaring `SPATIAL KEY` and `FULLTEXT KEY` applies, that
// the two indexes arrive as SPATIAL and FULLTEXT rather than as ordinary
// non-unique indexes, and that the same file then compares in sync against the
// database it created.
//
// The three assertions fail in different places, and which defect each one
// answers was measured rather than argued:
//
//	the declaration is refused as it is read -- applyDesiredSchema, on both
//	engines, before the catalog is ever read;
//
//	the access method never reaches the renderer -- INDEX_TYPE, on the MariaDB
//	row, which applies a plain index and reports BTREE where SPATIAL and
//	FULLTEXT were asked for. MySQL refuses that same DDL first, with `Error 1170
//	BLOB/TEXT column 'bio' used in key specification without a key length`;
//
//	the method is written but not read back -- assertComparesInSync alone, on
//	both engines, with the catalog assertion passing.
//
// The middle row is why running both engines matters here rather than being
// thoroughness. On MySQL a dropped access method is caught by the server, and a
// test that ran there alone would be reading an engine's refusal where it meant
// to read the index Ptah built.
func TestTypedIndexKeyDeclarationsReachTheServerWithTheirAccessMethod(t *testing.T) {
	for _, test := range inlineIndexEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			target := newTypedIndexDatabase(c, ctx, test.admin)
			schemaPath := writeInlineIndexSchema(c, typedIndexSchema)

			applyDesiredSchema(c, target.url, schemaPath)

			c.Assert(typedIndexShape(c, ctx, target), qt.DeepEquals, map[string]typedIndex{
				"PRIMARY":        {IndexType: "BTREE", NonUnique: 0, Columns: []string{"id"}},
				"sp_places_geom": {IndexType: "SPATIAL", NonUnique: 1, Columns: []string{"geom"}},
				"ft_places_bio":  {IndexType: "FULLTEXT", NonUnique: 1, Columns: []string{"bio"}},
			})

			assertComparesInSync(c, target.url, schemaPath)
		})
	}
}

// newTypedIndexDatabase creates an empty database of its own for one engine and
// removes it afterwards.
//
// A database per case rather than the shared one, because `schema apply`
// reconciles the WHOLE target: run against a database holding anything else, it
// would plan a drop for every table this declaration does not mention. The
// administrative account is what can create one, and both engines' admin
// variables are set by .github/workflows/go-integration-tests.yml, so neither
// row skips.
func newTypedIndexDatabase(c *qt.C, ctx context.Context, engine dbtarget.Engine) inlineIndexTarget {
	c.Helper()

	adminDB, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	database := fmt.Sprintf("ptah_2747_typed_%d", time.Now().UnixNano())
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

// typedIndex is one index as information_schema.STATISTICS describes it.
//
// IndexType is the field this test exists for: it is the server's own word for
// the access method, and it is the only one of the three that separates a
// spatial index from an ordinary index over the same column under the same
// name.
//
// NonUnique is kept as the catalog's 0-or-1 rather than folded to a bool, so a
// row asserting 1 is naming the value the server reported. It cannot be
// falsified here, and that is a fact about the engines rather than a gap:
// measured, a reader that made these indexes unique renders `CREATE UNIQUE
// FULLTEXT INDEX`, which MySQL 26.7 and MariaDB 12.3 both refuse with `Error
// 1064`, so the apply fails before this map is read. The parser test's
// `Unique, qt.IsFalse` is what catches that shape; the field stays here because
// the caller compares the whole map.
type typedIndex struct {
	IndexType string
	NonUnique int
	Columns   []string
}

// typedIndexShape reads back every index on the applied table, from the catalog
// rather than through Ptah.
//
// Reading it through Ptah would compare the model with itself: a reader that
// gave an index the wrong access method would report that same wrong method,
// and the assertion would agree with the defect. The whole map is returned, and
// the caller compares the whole map, so an index that should not exist fails
// the same assertion as one that is missing.
func typedIndexShape(c *qt.C, ctx context.Context, target inlineIndexTarget) map[string]typedIndex {
	c.Helper()

	rows, err := target.admin.QueryContext(ctx,
		"SELECT INDEX_NAME, INDEX_TYPE, NON_UNIQUE, COLUMN_NAME "+
			"FROM information_schema.STATISTICS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'places' "+
			"ORDER BY INDEX_NAME, SEQ_IN_INDEX",
		target.database)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	shape := make(map[string]typedIndex)
	for rows.Next() {
		var name, indexType, column string
		var nonUnique int
		c.Assert(rows.Scan(&name, &indexType, &nonUnique, &column), qt.IsNil)
		entry := shape[name]
		entry.IndexType = indexType
		entry.NonUnique = nonUnique
		entry.Columns = append(entry.Columns, column)
		shape[name] = entry
	}
	c.Assert(rows.Err(), qt.IsNil)
	return shape
}
