//go:build integration

package integration_test

// The regression stokaro/ptah#2773 reported: two documents that differ in
// nothing but the order of a table body's two elements.
//
// MySQL and MariaDB allocate an index name at the moment the element is read,
// so the order decides both which names exist and whether the document can be
// created at all. Measured on MySQL 26.7.0 and MariaDB 12.3.3, identically on
// both:
//
//	CONSTRAINT b FOREIGN KEY (a) ..., KEY (b)   b(a) and b_2(b)
//	KEY (b), CONSTRAINT b FOREIGN KEY (a) ...   ERROR 1061 Duplicate key name 'b'
//
// Ptah held the constraint and the index in two slices with no interleaving
// between them, so both documents converted to one model and emitted
// `CREATE INDEX b` beside `ADD CONSTRAINT b`, which both servers answer with
// ERROR 1061. A document both engines accept became DDL neither can run.
//
// Only a live server can answer this. The catalog under test is the one the
// SERVER builds from the document, so the assertion is not a name written down
// here -- it is the same document handed to the server directly, in a database
// of its own, and the two catalogs compared. A fixture spelling `b` and `b_2`
// would restate the rule; this measures it, and it keeps measuring it if some
// future release of either engine allocates differently.
//
// The literal shape is asserted too, of the SERVER's half alone. Comparing two
// catalogs proves they agree; it cannot notice that both are empty, which is
// what a `users` table that never got created looks like from here.
//
// The commands are driven in process, through the cobra tree `cmd/schema`
// builds, for the reason the sibling file
// mysql_inline_index_naming_e2e_test.go states: it is the surface a user has,
// including the exit status, without a `go build` in the middle of a database
// test.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the MySQL driver, which both engines here speak, for database/sql

	cmdschema "ptah.run/cmd/schema"
	"ptah.run/internal/dbtarget"
)

// elementOrderParent is the table the foreign key points at.
//
// It carries the primary key the reference needs and nothing else: every
// assertion below reads the child table, so anything more here would be a
// column nothing measures.
const elementOrderParent = `CREATE TABLE parents (
  id BIGINT NOT NULL PRIMARY KEY
);
`

// elementOrderForeignKeyFirst is the document both engines accept.
//
// The constraint is named `b` and the column indexed after it is called `b`
// too, which is what makes the collision reachable: the foreign key needs a
// backing index and takes the bare name, so the unnamed `KEY (b)` declared
// after it gets `b_2`.
const elementOrderForeignKeyFirst = `CREATE TABLE users (
  a BIGINT NOT NULL,
  b BIGINT NOT NULL,
  CONSTRAINT b FOREIGN KEY (a) REFERENCES parents(id),
  KEY (b)
);
`

// elementOrderIndexFirst is the same two elements the other way round, which
// both engines refuse.
const elementOrderIndexFirst = `CREATE TABLE users (
  a BIGINT NOT NULL,
  b BIGINT NOT NULL,
  KEY (b),
  CONSTRAINT b FOREIGN KEY (a) REFERENCES parents(id)
);
`

// TestADeclarationOrderReachesTheServersCatalog establishes that applying the
// accepted document through Ptah builds the catalog the server builds from the
// same document.
//
// Both halves are needed and neither replaces the other. The server's own
// catalog is what says which names the engine allocates -- nothing in this
// repository decides that -- and the comparison is what says Ptah reproduces
// it. Read through Ptah instead, both sides would come from the same model and
// agree while both were wrong.
//
// The comparison at the end is the third fact: a database Ptah built and then
// reports as differing from the declaration that built it is a database that
// never converges, which is the shape the same naming defect took in
// stokaro/ptah#2713.
func TestADeclarationOrderReachesTheServersCatalog(t *testing.T) {
	for _, test := range inlineIndexEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			byServer := newElementOrderDatabase(c, ctx, test.admin, "server")
			handle := openElementOrderDatabase(c, ctx, test.admin, byServer.database)
			createElementOrderParent(c, ctx, handle)
			c.Assert(createElementOrderChild(ctx, handle, elementOrderForeignKeyFirst),
				qt.IsNil)
			serverShape := inlineIndexShape(c, ctx, byServer)

			c.Assert(serverShape, qt.DeepEquals, map[string]inlineIndex{
				"b":   {NonUnique: 1, Columns: []string{"a"}},
				"b_2": {NonUnique: 1, Columns: []string{"b"}},
			})

			byPtah := newElementOrderDatabase(c, ctx, test.admin, "ptah")
			schemaPath := writeInlineIndexSchema(c,
				elementOrderParent+"\n"+elementOrderForeignKeyFirst)
			applyDesiredSchema(c, byPtah.url, schemaPath)

			c.Assert(inlineIndexShape(c, ctx, byPtah), qt.DeepEquals, serverShape)
			assertComparesInSync(c, byPtah.url, schemaPath)
		})
	}
}

// TestADocumentBothEnginesRefuseIsRefusedBeforeAnyDDL establishes that Ptah's
// refusal of the reordered document is not Ptah being stricter than the
// engines.
//
// The server's own answer is measured first, in a database of its own, so the
// refusal below is held against what the engine does rather than against a
// sentence written here. Ptah then declines the same document at conversion,
// before it opens a connection, which is where an author can still act on it.
//
// A test that asserted only Ptah's error would pass just as well if the
// document were valid and Ptah had simply become unable to convert it.
func TestADocumentBothEnginesRefuseIsRefusedBeforeAnyDDL(t *testing.T) {
	for _, test := range inlineIndexEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			byServer := newElementOrderDatabase(c, ctx, test.admin, "refused")
			handle := openElementOrderDatabase(c, ctx, test.admin, byServer.database)
			createElementOrderParent(c, ctx, handle)

			refused := createElementOrderChild(ctx, handle, elementOrderIndexFirst)

			c.Assert(refused, qt.IsNotNil)
			// The engines answer `Error 1061 (42000): Duplicate key name 'b'`
			// word for word, measured on both, so the name is asserted with the
			// sentence rather than the error code alone.
			c.Assert(refused.Error(), qt.Contains, "Duplicate key name 'b'")
			c.Assert(inlineIndexShape(c, ctx, byServer), qt.HasLen, 0)

			byPtah := newElementOrderDatabase(c, ctx, test.admin, "ptahrefused")
			schemaPath := writeInlineIndexSchema(c,
				elementOrderParent+"\n"+elementOrderIndexFirst)

			err := runSchemaApply(byPtah.url, schemaPath)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains,
				"two indexes on one table claim the same name: b on users")
			// Nothing was written: the refusal happens while the declaration is
			// being read, so the database Ptah was pointed at is still empty --
			// including of the parent table, which is valid and would have been
			// created first.
			c.Assert(inlineIndexShape(c, ctx, byPtah), qt.HasLen, 0)
			c.Assert(elementOrderTableCount(c, ctx, byPtah), qt.Equals, 0)
		})
	}
}

// newElementOrderDatabase creates an empty database of its own for one case and
// removes it afterwards.
//
// One per case rather than one per test, because `schema apply` reconciles the
// whole target: a shared database would have the server's half planned away as
// tables the declaration does not mention. It returns the sibling file's
// inlineIndexTarget so that inlineIndexShape reads this catalog too -- the type
// carries a database name, an administrative handle and the URL Ptah connects
// with, which is exactly what both files need.
func newElementOrderDatabase(
	c *qt.C, ctx context.Context, engine dbtarget.Engine, purpose string,
) inlineIndexTarget {
	c.Helper()

	adminDB, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	database := fmt.Sprintf("ptah_2773_%s_%d", purpose, time.Now().UnixNano())
	createMySQLDatabase(c, ctx, adminDB, database)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), adminDB, database) })

	return inlineIndexTarget{
		admin:    adminDB,
		database: database,
		url:      replaceMySQLDatabaseName(c, dbtarget.URL(c, engine), database),
	}
}

// openElementOrderDatabase opens a handle whose default database is the one
// just created, so the document's statements reach the server exactly as
// written.
//
// Qualifying the table names instead would hand the server a different document
// from the one Ptah is given, and the document is the thing under test.
// dbtarget.DriverDSN rather than dbtarget.URL, because go-sql-driver reads a
// `mysql://` prefix as part of the username.
func openElementOrderDatabase(
	c *qt.C, ctx context.Context, engine dbtarget.Engine, database string,
) *sql.DB {
	c.Helper()

	handle, err := sql.Open("mysql",
		replaceMySQLDatabaseName(c, dbtarget.DriverDSN(c, engine), database))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = handle.Close() })
	c.Assert(handle.PingContext(ctx), qt.IsNil)
	return handle
}

// createElementOrderParent creates the referenced table, which every case needs
// and no case is about.
func createElementOrderParent(c *qt.C, ctx context.Context, handle *sql.DB) {
	c.Helper()

	_, err := handle.ExecContext(ctx, elementOrderStatement(elementOrderParent))
	c.Assert(err, qt.IsNil)
}

// createElementOrderChild gives the server one of the two documents and hands
// its outcome back, because whether the server accepts it is the measurement.
func createElementOrderChild(ctx context.Context, handle *sql.DB, document string) error {
	_, err := handle.ExecContext(ctx, elementOrderStatement(document))
	return err
}

// elementOrderStatement is one document as a driver will take it: the trailing
// semicolon removed, since these handles send a single statement per call.
func elementOrderStatement(document string) string {
	return strings.TrimSuffix(strings.TrimSpace(document), ";")
}

// runSchemaApply drives `ptah schema apply` and returns what it answered,
// unlike applyDesiredSchema, which requires success.
//
// The error rather than the printed text: the refusal is what the command exits
// on, so it is what a script running Ptah sees.
func runSchemaApply(dbURL, schemaPath string) error {
	cmd := cmdschema.NewSchemaCommand()
	cmd.SetOut(&strings.Builder{})
	cmd.SetErr(&strings.Builder{})
	cmd.SetArgs([]string{
		"apply",
		"--db-url", dbURL,
		"--schema-file", schemaPath,
		"--auto-approve",
	})
	return cmd.Execute()
}

// elementOrderTableCount is how many tables the database holds, which is how a
// refusal that wrote nothing is told from one that wrote the valid half.
func elementOrderTableCount(c *qt.C, ctx context.Context, target inlineIndexTarget) int {
	c.Helper()

	var count int
	c.Assert(target.admin.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
		target.database).Scan(&count), qt.IsNil)
	return count
}

// elementOrderKeyThenUnique and elementOrderUniqueThenKey are the pair both
// engines accept, whose catalogs differ in which element got the bare name.
//
// The document the issue was filed about cannot measure the order on this path:
// constraints before indexes is the order it was written in, so a conversion
// that ignores the recorded order builds the same catalog for it and the live
// round trip above stays green. Measured with `declaredOrder` returning the
// unordered fallback, which is what preceded the fix: the test above passes on
// both engines and this one fails on both.
//
// Measured on MySQL 26.7.0 and MariaDB 12.3.3, identically: `KEY (a),
// UNIQUE (a)` builds a non-unique `a` beside a unique `a_2`, and the same two
// elements the other way round build a unique `a` beside a non-unique `a_2`.
// Nothing is refused either way -- the two documents simply describe different
// databases, and a Ptah that read them alike would build the one its author did
// not write while reporting success.
const elementOrderKeyThenUnique = `CREATE TABLE users (
  a BIGINT NOT NULL,
  b BIGINT NOT NULL,
  KEY (a),
  UNIQUE (a)
);
`

const elementOrderUniqueThenKey = `CREATE TABLE users (
  a BIGINT NOT NULL,
  b BIGINT NOT NULL,
  UNIQUE (a),
  KEY (a)
);
`

// TestReorderingATableBodyReachesTheServersCatalogAsTheOtherDatabase drives
// both orders through the server and through Ptah, in four databases, and
// requires each Ptah catalog to be its own server's.
//
// The difference between the two is the assertion, and it is made twice over.
// Each half is held against the catalog the SERVER built from the same
// document, so what the names should be is the engine's answer rather than a
// sentence written here; and the two Ptah halves are required to differ from
// one another, which is what a conversion reading both documents alike loses.
// Either assertion alone would be weaker: two catalogs that agree with each
// other prove nothing if both are empty, and two that differ prove nothing
// about which of them the engine would have built.
//
// The uniqueness flag is what carries the swap. Both documents produce an `a`
// and an `a_2` over the same column, so a comparison on names alone is
// satisfied by either answer.
func TestReorderingATableBodyReachesTheServersCatalogAsTheOtherDatabase(t *testing.T) {
	for _, test := range inlineIndexEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			keyFirstByServer := newElementOrderDatabase(c, ctx, test.admin, "serverkey")
			keyFirstHandle := openElementOrderDatabase(
				c, ctx, test.admin, keyFirstByServer.database)
			c.Assert(
				createElementOrderChild(ctx, keyFirstHandle, elementOrderKeyThenUnique),
				qt.IsNil,
			)
			uniqueFirstByServer := newElementOrderDatabase(c, ctx, test.admin, "serverunique")
			uniqueFirstHandle := openElementOrderDatabase(
				c, ctx, test.admin, uniqueFirstByServer.database)
			c.Assert(
				createElementOrderChild(ctx, uniqueFirstHandle, elementOrderUniqueThenKey),
				qt.IsNil,
			)

			keyFirstShape := inlineIndexShape(c, ctx, keyFirstByServer)
			uniqueFirstShape := inlineIndexShape(c, ctx, uniqueFirstByServer)

			c.Assert(keyFirstShape, qt.DeepEquals, map[string]inlineIndex{
				"a":   {NonUnique: 1, Columns: []string{"a"}},
				"a_2": {NonUnique: 0, Columns: []string{"a"}},
			})
			c.Assert(uniqueFirstShape, qt.DeepEquals, map[string]inlineIndex{
				"a":   {NonUnique: 0, Columns: []string{"a"}},
				"a_2": {NonUnique: 1, Columns: []string{"a"}},
			})

			keyFirstByPtah := newElementOrderDatabase(c, ctx, test.admin, "ptahkey")
			keyFirstPath := writeInlineIndexSchema(c, elementOrderKeyThenUnique)
			applyDesiredSchema(c, keyFirstByPtah.url, keyFirstPath)
			uniqueFirstByPtah := newElementOrderDatabase(c, ctx, test.admin, "ptahunique")
			uniqueFirstPath := writeInlineIndexSchema(c, elementOrderUniqueThenKey)
			applyDesiredSchema(c, uniqueFirstByPtah.url, uniqueFirstPath)

			c.Assert(inlineIndexShape(c, ctx, keyFirstByPtah), qt.DeepEquals, keyFirstShape)
			c.Assert(inlineIndexShape(c, ctx, uniqueFirstByPtah), qt.DeepEquals, uniqueFirstShape)
			c.Assert(keyFirstShape, qt.Not(qt.DeepEquals), uniqueFirstShape)
			assertComparesInSync(c, keyFirstByPtah.url, keyFirstPath)
			assertComparesInSync(c, uniqueFirstByPtah.url, uniqueFirstPath)
		})
	}
}

// elementOrderCoveredByItsOwnColumn and elementOrderCoveredByALeadingPrefix are
// the two documents where a key declared before the foreign key serves as its
// backing index.
//
// Both engines reuse such a key rather than building one of their own, so the
// constraint allocates no index name and the `KEY fk1 (b)` written after it is
// free to take the one the constraint carries. A Ptah that had the foreign key
// claim regardless would refuse both documents as a duplicate name -- stricter
// than the engines, on a document neither of them objects to, which is the half
// of the compatibility policy that costs a user something they wrote.
//
// The trailing `KEY fk1 (b)` is what makes the rule observable at all. A claim
// nothing renders is invisible in the catalog until a second key wants the name.
const elementOrderCoveredByItsOwnColumn = `CREATE TABLE users (
  a BIGINT NOT NULL,
  b BIGINT NOT NULL,
  KEY (a),
  CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES parents(id),
  KEY fk1 (b)
);
`

const elementOrderCoveredByALeadingPrefix = `CREATE TABLE users (
  a BIGINT NOT NULL,
  b BIGINT NOT NULL,
  KEY (a, b),
  CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES parents(id),
  KEY fk1 (b)
);
`

// TestAForeignKeyAKeyAlreadyCoversIsAppliedAsTheServerBuildsIt establishes that
// Ptah accepts what both engines accept here, and builds what they build.
//
// The refusal test above proves Ptah declines a document the engines decline.
// This one is its control: a rule that refused every foreign key sharing a name
// with a later index would satisfy that test and cost an author two valid
// documents. Both halves are needed, and the second is the one nothing else in
// this file would notice.
//
// The wider key is a separate row rather than a variation of the first, because
// coverage is about the key's LEADING columns: a rule comparing whole key
// column lists serves the first document and refuses the second, and both
// engines accept both.
func TestAForeignKeyAKeyAlreadyCoversIsAppliedAsTheServerBuildsIt(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     map[string]inlineIndex
	}{
		{
			name:     "a key on the foreign key's own column",
			document: elementOrderCoveredByItsOwnColumn,
			want: map[string]inlineIndex{
				"a":   {NonUnique: 1, Columns: []string{"a"}},
				"fk1": {NonUnique: 1, Columns: []string{"b"}},
			},
		},
		{
			name:     "a wider key whose leading column is the foreign key's",
			document: elementOrderCoveredByALeadingPrefix,
			want: map[string]inlineIndex{
				"a":   {NonUnique: 1, Columns: []string{"a", "b"}},
				"fk1": {NonUnique: 1, Columns: []string{"b"}},
			},
		},
	}

	for _, engine := range inlineIndexEngines {
		for _, test := range tests {
			t.Run(engine.name+": "+test.name, func(t *testing.T) {
				c := qt.New(t)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()

				byServer := newElementOrderDatabase(c, ctx, engine.admin, "servercovered")
				handle := openElementOrderDatabase(c, ctx, engine.admin, byServer.database)
				createElementOrderParent(c, ctx, handle)
				c.Assert(createElementOrderChild(ctx, handle, test.document), qt.IsNil)
				serverShape := inlineIndexShape(c, ctx, byServer)

				c.Assert(serverShape, qt.DeepEquals, test.want)

				byPtah := newElementOrderDatabase(c, ctx, engine.admin, "ptahcovered")
				schemaPath := writeInlineIndexSchema(c,
					elementOrderParent+"\n"+test.document)
				applyDesiredSchema(c, byPtah.url, schemaPath)

				c.Assert(inlineIndexShape(c, ctx, byPtah), qt.DeepEquals, serverShape)
				assertComparesInSync(c, byPtah.url, schemaPath)
			})
		}
	}
}
