//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// atlasCompatExtensionRoundTripCase is one extension shape, its seed DDL, and
// whether the compatibility surface is expected to keep the extension block.
type atlasCompatExtensionRoundTripCase struct {
	name string
	// extension is the CREATE EXTENSION name.
	extension string
	// seed runs after the extension is installed and creates whatever depends
	// on it (or, for the control, whatever does not).
	seed string
	// wantBlock is whether `extension "<extension>"` must survive into the
	// rendered document.
	wantBlock bool
	// why records the measurement behind the expectation.
	why string
	// unreplayable, when set, is the reason this row's fixture cannot be
	// materialized on a fresh dev database no matter what the renderer emits,
	// so the round trip below is not an assertion about Ptah for this row. An
	// empty string means the round trip is asserted, which is the default and
	// what every row that builds its extension from an install script gets.
	//
	// Only a fixture whose membership comes from `ALTER EXTENSION ... ADD` can
	// need this, and only when the added member is a TYPE. `ALTER EXTENSION`
	// writes the pg_depend membership row without adding anything to the
	// extension's install script, so it builds a catalog state no
	// `CREATE EXTENSION` can reproduce. Measured on PostgreSQL 17.10 against a
	// clean database:
	//
	//	CREATE EXTENSION pgcrypto;
	//	SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
	//	 WHERE n.nspname = 'public' AND t.typname = 'box';   -> 0
	//
	// against `CREATE EXTENSION lo` answering 1 for its own domain `lo`. A
	// function member is not affected, because the bodies that call one are
	// plpgsql and PostgreSQL does not resolve them at creation time; a type
	// member is resolved when the column is created, so the replay stops there.
	unreplayable string
}

// TestAtlasCompatInspectExtensionRoundTripE2E pins the property that
// stokaro/ptah#1266 broke: `ptah-compat schema inspect` must emit a document
// Ptah can read back.
//
// The assertion is a real round trip and not a re-run of the renderer's own
// reference scan -- the rendered document is fed to the same binary surface
// against a FRESH dev database, where the extension is not installed, so a
// document that dropped a still-needed extension fails to materialize. That is
// exactly how the regression showed up in the field:
//
//	CREATE EXTENSION isn;
//	CREATE TABLE books (id integer PRIMARY KEY, code isbn NOT NULL);
//
// rendered at exit 0 with `extensions.isn: omitted ...` on stderr, and the
// result was then unreadable -- `type "isbn" does not exist` -- by Ptah AND by
// the pinned Atlas community binary, measured on PostgreSQL 17.10. The omission
// was not a compatibility win, it was a document nobody could read.
//
// The rows expecting no block are the control that keeps the fix from becoming
// "never omit an extension", which would throw away what #1266 bought. Three of
// them are the shapes stokaro/ptah#1280 measured going the other way: a
// document naming `strpos`, `max` or `gen_random_uuid` kept the extension that
// overloads them although pg_catalog supplies all three, and the pinned Atlas
// community binary refuses ANY document declaring an extension block -- so a
// spurious keep is not a smaller compatibility win, it is none at all.
//
// The last row is the control in the other direction. Excluding the shadowed
// names must not exclude a name only the extension supplies, or the fix would
// have traded #1280's shapes back for #1266's.
//
// The five rows after it are stokaro/ptah#1281: the same over-match arriving
// through SQL keywords instead of through pg_catalog. `hstore` supplies three
// functions named `delete` and `ltree` supplies one named `index`, and
// pg_catalog supplies neither, so the shadowed-name filter keeps both -- while
// the scan that consumes the member list reads words rather than positions, so
// a plpgsql body saying `DELETE FROM audit` or `CREATE INDEX` looks exactly
// like a call. Both of those pinned an extension the database does not use, and
// the pinned Atlas community binary refused the result: `postgres: extensions
// are not supported by this version`, exit 1, PostgreSQL 18.4.
//
// The three that follow expect a block, and they are what makes dropping
// keyword-named members a fix rather than a deletion. Two of them are the
// redundancy the fix relies on: a genuine call to `delete` needs an hstore
// value, so the document spells that type either on a column or in the calling
// function's own signature, and the extension survives on it. The third is
// `cube`, where the keyword is the type's name and the extension's label at
// once; it is the row that fails if the exclusion is moved off the member list
// and onto the words the keep decision matches.
//
// The last two rows are why that redundancy is a PRECONDITION of the exclusion
// rather than a description of contrib. Over the 46 extensions this build ships
// every keyword-named function member does take or return a type its own
// extension supplies -- but nothing in the catalog requires that, and a filter
// resting on it drops the only evidence there is the moment an extension
// supplies `merge(text, text)` and no type at all. Measured on PostgreSQL 18.4
// against a fixture extension of exactly that shape: the block went away, the
// CHECK calling `merge` stayed, and the pinned Atlas community binary v1.3.0
// answered `create "names" table: pq: function merge(text, text) does not
// exist` where before it had refused the extension block instead.
//
// Both rows build that shape through `ALTER EXTENSION ... ADD`, which writes
// the same pg_depend membership rows an extension's install script writes, so
// no fixture control file has to be installed on the server for CI to run them.
// The assertion that carries these two rows is therefore the block, not the
// round trip: `ALTER EXTENSION` adds nothing to the extension's install script,
// so `CREATE EXTENSION` on a fresh dev database does not recreate the added
// member and the fixture is one no replay can rebuild.
//
// Whether that costs the round trip depends on WHAT was added, and the two rows
// differ on exactly this. A function member does not: the dependent call lives
// in a plpgsql body, which PostgreSQL does not resolve at creation time, so the
// first row still materializes. A TYPE member does, because a column's type is
// resolved when the table is created -- so the second row records the reason in
// `unreplayable` and asserts the block alone. Before stokaro/ptah#1294 that row
// materialized only because the reader described the extension's own domain as
// a user domain, which is the defect that issue is about; the round trip was
// passing because of the bug, not despite it. The word in
// that body is also indistinguishable from the false positives above, which is
// the whole point: the decision cannot be made from the word, only from what
// the extension is the only supplier of.
//
// The second of the two pins the answering type against its own arm. An
// extension supplying a type named like a pg_catalog type has that name
// filtered out as shadowed, so it can no longer answer for anything, and a
// keyword filter that consults the raw membership instead of the reported
// member list drops the function name too -- the refuted shape again, one level
// down.
//
// The btree_gin and btree_gist rows are stokaro/ptah#1286, where the member list
// cannot help because no identifier in the document names the dependency.
// PostgreSQL prints an operator class only when it is not the default for its
// key's type, so `CREATE INDEX t_gin ON t USING gin (n int4_ops)` over an integer
// column comes back as `USING gin (n)` and every word of btree_gin is gone. The
// block was dropped at exit 0 and the pinned Atlas community binary v1.3.0 then
// refused the result: `create index "t_gin" to table: "t": pq: data type
// integer has no default operator class for access method "gin"`, exit 1, with
// Ptah's own apply failing identically. The btree_gist row is the same dependency
// arriving through an exclusion constraint, whose elements print operators and
// print a class under the same not-the-default rule. The jsonb/tsvector row is
// the control that keeps the fix from becoming "pin the extension to every GIN
// index": those have core GIN classes, and that document must still come out
// with no extension block.
//
// The two pg_trgm rows are the same rule read the other way, and they are here
// because the reason reported for them was wrong. gin_trgm_ops is not the
// default GIN class for text, so it IS printed: the index arrives as
// `USING gin (txt gin_trgm_ops)` and renders `ops = "gin_trgm_ops"`, and the
// exclusion form renders `elements = "txt gist_trgm_ops WITH ="`. The block is
// needed either way -- without it the round trip below fails with `operator class
// "gin_trgm_ops" does not exist for access method "gin"` -- but the document does
// name what pg_trgm supplies, so the keep is reported as a dependency the reader
// can find rather than as one the DDL does not spell.
//
// The materialized-view row is the fix's other control, and the shape it first
// got wrong. Carrying the edge is not the same as being in the document:
// renderTables drops an index whose table this render does not write and reports
// it as an orphan, and a materialized view's index is exactly that -- pg_index
// resolves its operator classes like any other index, and a `materialized` block
// carries no index, so it can never be rendered. Keeping btree_gin for it
// produced a document with no index block anywhere that the pinned Atlas
// community binary v1.3.0 refused, `postgres: extensions are not supported by
// this version`, exit 1, where the same document without the block is read at
// exit 0 -- and both apply at exit 0, so the block bought nothing at all.
func TestAtlasCompatInspectExtensionRoundTripE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	tests := []atlasCompatExtensionRoundTripCase{
		{
			name:      "extension supplying a column type",
			extension: "isn",
			seed:      "CREATE TABLE books (id integer PRIMARY KEY, code isbn NOT NULL)",
			wantBlock: true,
			why:       "isn supplies the type isbn; the word isn appears nowhere in the document",
		},
		{
			name:      "extension supplying only a function",
			extension: "pgcrypto",
			seed:      "CREATE TABLE tokens (id integer PRIMARY KEY, salt text NOT NULL DEFAULT gen_salt('bf'))",
			wantBlock: true,
			// gen_salt is the deliberate choice over gen_random_uuid: measured
			// on PostgreSQL 17.10, gen_random_uuid also exists in pg_catalog, so
			// a document using it round-trips even with pgcrypto dropped and
			// would pass this test without testing anything. gen_salt has no
			// core equivalent.
			why: "pgcrypto supplies gen_salt, which has no core equivalent",
		},
		{
			name:      "extension nothing depends on",
			extension: "isn",
			seed:      "CREATE TABLE plain (id integer PRIMARY KEY, label text NOT NULL)",
			wantBlock: false,
			why:       "nothing uses anything isn supplies, so #1266's omission still applies",
		},
		{
			name:      "core function an extension also overloads, in a check",
			extension: "citext",
			seed: "CREATE TABLE t (id integer PRIMARY KEY, label text," +
				" CONSTRAINT ck CHECK (strpos(label, 'x') = 0))",
			wantBlock: false,
			why: "citext overloads strpos, but pg_catalog supplies it too and is always" +
				" on the search path, so this document resolves with citext dropped",
		},
		{
			name:      "core function an extension also overloads, in a view",
			extension: "citext",
			seed: "CREATE TABLE t (id integer PRIMARY KEY, n integer);" +
				" CREATE VIEW v AS SELECT max(n) AS m FROM t",
			wantBlock: false,
			why:       "max is one of the fifteen names citext overloads that pg_catalog also supplies",
		},
		{
			name:      "extension function core has since absorbed",
			extension: "pgcrypto",
			seed:      "CREATE TABLE u (id uuid PRIMARY KEY DEFAULT gen_random_uuid())",
			wantBlock: false,
			why: "pgcrypto supplies gen_random_uuid, but so has pg_catalog since" +
				" PostgreSQL 13; the default evaluates without the extension",
		},
		{
			name:      "extension supplying a column type it also overloads functions for",
			extension: "citext",
			seed:      "CREATE TABLE t (id integer PRIMARY KEY, label citext NOT NULL)",
			wantBlock: true,
			why: "the control against over-narrowing: dropping the shadowed names must not" +
				" drop citext's own type, which nothing but citext supplies",
		},
		{
			name:      "keyword-named extension function in statement position",
			extension: "hstore",
			seed: "CREATE TABLE audit (id integer PRIMARY KEY);" +
				" CREATE FUNCTION purge() RETURNS void LANGUAGE plpgsql" +
				" AS $$ BEGIN DELETE FROM audit; END $$",
			wantBlock: false,
			why: "hstore supplies three functions named delete and pg_catalog supplies none," +
				" so the shadowed-name filter keeps every one of them; DELETE FROM audit is" +
				" a statement and nothing in this database uses hstore",
		},
		{
			name:      "keyword-named extension function in nested DDL",
			extension: "ltree",
			seed: "CREATE TABLE docs (id integer PRIMARY KEY, title text);" +
				" CREATE FUNCTION reindex_docs() RETURNS void LANGUAGE plpgsql AS $$ BEGIN" +
				" CREATE INDEX IF NOT EXISTS docs_title_idx ON docs (title); END $$",
			wantBlock: false,
			// A second keyword and a second extension, because a fix that only
			// knows the word `delete` would pass the row above and nothing else.
			why: "ltree supplies a function named index; CREATE INDEX in a body is not a call to it",
		},
		{
			name:      "keyword-named extension function called on a column of the extension's type",
			extension: "hstore",
			seed: "CREATE TABLE attrs (id integer PRIMARY KEY, props hstore NOT NULL," +
				" CONSTRAINT attrs_no_secret CHECK (delete(props, 'secret') = props))",
			wantBlock: true,
			why: "the call is genuine, and it is the column type hstore rather than the word" +
				" delete that keeps the extension -- which is what makes dropping the" +
				" keyword-named member safe",
		},
		{
			name:      "keyword-named extension function called with no column of that type",
			extension: "hstore",
			seed: "CREATE TABLE audit (id integer PRIMARY KEY);" +
				" CREATE FUNCTION strip_secret(h hstore) RETURNS hstore LANGUAGE sql" +
				" AS $$ SELECT delete(h, 'secret') $$",
			wantBlock: true,
			why: "the only hstore here is the function's own signature; parameter and return" +
				" types are scanned as well, so the redundancy holds without a column",
		},
		{
			name:      "extension whose TYPE name is a keyword",
			extension: "cube",
			seed:      "CREATE TABLE points (id integer PRIMARY KEY, loc cube NOT NULL)",
			wantBlock: true,
			// This row is why the exclusion is applied to the member list, on the
			// function arm, instead of to the words the keep decision matches.
			// `cube` is an unreserved keyword, and it is equally cube's type and
			// cube's own label: a keyword filter sitting where the names are
			// matched drops all three at once and leaves this column with nothing
			// declaring its type. Measured -- with the filter moved there, this is
			// the only one of the twelve rows that fails.
			why: "cube is a keyword, cube's type and cube's label at once; the column has no other evidence",
		},
		{
			name:      "keyword-named extension function no type of that extension answers for",
			extension: "pgcrypto",
			seed: "CREATE FUNCTION merge(a text, b text) RETURNS text LANGUAGE sql IMMUTABLE" +
				" AS $fn$ SELECT a || b $fn$;" +
				" ALTER EXTENSION pgcrypto ADD FUNCTION merge(text, text);" +
				" CREATE TABLE names (id integer PRIMARY KEY, a text NOT NULL, b text NOT NULL);" +
				" CREATE FUNCTION combined(row_id integer) RETURNS text LANGUAGE plpgsql AS $fn$" +
				" DECLARE result text;" +
				" BEGIN SELECT merge(names.a, names.b) INTO result FROM names" +
				" WHERE names.id = row_id; RETURN result; END $fn$",
			wantBlock: true,
			why: "merge is an unreserved keyword and pgcrypto supplies no type at all, so the" +
				" name is the only evidence the document carries; dropping it because it is a" +
				" keyword leaves a body calling a function nothing declares",
		},
		{
			name:      "keyword-named extension function whose answering type pg_catalog shadows",
			extension: "pgcrypto",
			seed: "CREATE DOMAIN public.box AS text;" +
				" ALTER EXTENSION pgcrypto ADD DOMAIN public.box;" +
				" CREATE FUNCTION public.merge(a public.box, b public.box) RETURNS public.box" +
				" LANGUAGE sql IMMUTABLE AS $fn$ SELECT (a::text || b::text)::public.box $fn$;" +
				" ALTER EXTENSION pgcrypto ADD FUNCTION public.merge(public.box, public.box);" +
				" CREATE TABLE labels (id integer PRIMARY KEY, a public.box NOT NULL," +
				" b public.box NOT NULL);" +
				" CREATE FUNCTION labelled(row_id integer) RETURNS text LANGUAGE plpgsql AS $fn$" +
				" DECLARE result text;" +
				" BEGIN SELECT merge(labels.a, labels.b)::text INTO result FROM labels" +
				" WHERE labels.id = row_id; RETURN result; END $fn$",
			wantBlock: true,
			why: "the type in merge's signature is named box, which pg_catalog also supplies," +
				" so the type arm already dropped it and it cannot answer for anything;" +
				" excluding merge as well leaves this document with no evidence either",
			unreplayable: "pgcrypto owns public.box through ALTER EXTENSION ... ADD, so no" +
				" CREATE EXTENSION recreates it and the labels table can never be built on a" +
				" fresh dev database (stokaro/ptah#1294)",
		},
		{
			name:      "index resting on an operator class the extension supplies as the default",
			extension: "btree_gin",
			seed: "CREATE TABLE t (id integer PRIMARY KEY, n integer NOT NULL);" +
				" CREATE INDEX t_gin ON t USING gin (n int4_ops)",
			wantBlock: true,
			why: "PostgreSQL prints an operator class only when it is not the default, so the" +
				" catalog stores this as USING gin (n) and the document carries no token of" +
				" btree_gin -- not its label, and none of the support functions its member list holds",
		},
		{
			name:      "exclusion constraint resting on an operator class the extension supplies",
			extension: "btree_gist",
			seed: "CREATE TABLE booking (id integer PRIMARY KEY, room integer NOT NULL," +
				" during tsrange NOT NULL, EXCLUDE USING gist (room WITH =, during WITH &&))",
			wantBlock: true,
			why: "an EXCLUDE element prints its operator and not its operator class, and the index" +
				" backing the constraint is not in the entity model, so the requirement has to" +
				" travel on the constraint",
		},
		{
			name:      "index printing an operator class the extension supplies",
			extension: "pg_trgm",
			seed: "CREATE TABLE w (id integer PRIMARY KEY, txt text NOT NULL);" +
				" CREATE INDEX w_trgm ON w USING gin (txt gin_trgm_ops)",
			wantBlock: true,
			// The other side of the not-the-default rule, and the row the
			// diagnostic was false on: this class is printed, so the document
			// names something pg_trgm supplies, and the keep must be reported as
			// a dependency the reader can look up rather than as one the DDL does
			// not spell. The round trip is what proves the block is needed at
			// all: on a dev database without pg_trgm the index cannot be built.
			why: "gin_trgm_ops is not the default GIN class for text, so PostgreSQL prints it and" +
				" the document does name what pg_trgm supplies -- and the index still cannot be" +
				" built without the extension",
		},
		{
			name:      "exclusion constraint printing an operator class the extension supplies",
			extension: "pg_trgm",
			seed: "CREATE TABLE y (id integer PRIMARY KEY, txt text NOT NULL," +
				" EXCLUDE USING gist (txt gist_trgm_ops WITH =))",
			wantBlock: true,
			// The constraint arm of the same shape. pg_get_constraintdef prints
			// the class here for the same reason, and it lands in the rendered
			// `elements` string, which the reference scan has always read -- so
			// this row was already answerable by name before #1286 and was
			// nonetheless reported as a catalog-only edge.
			why: "gist_trgm_ops is not the default gist class for text either, so the constraint" +
				" definition prints it and the rendered elements string carries it",
		},
		{
			name:      "gin indexes whose operator classes core supplies",
			extension: "btree_gin",
			seed: "CREATE TABLE doc (id integer PRIMARY KEY, body jsonb NOT NULL, tsv tsvector NOT NULL);" +
				" CREATE INDEX doc_body_gin ON doc USING gin (body);" +
				" CREATE INDEX doc_tsv_gin ON doc USING gin (tsv)",
			wantBlock: false,
			// The control the two rows above are worthless without. Same
			// extension, same access method, same rendered `type = "gin"`: only
			// the resolved operator class differs. A rule that read `USING gin`
			// as a reference to btree_gin passes both rows above and fails here,
			// and so does keeping every extension with a non-empty member list.
			why: "jsonb and tsvector have core GIN operator classes, so these indexes resolve to" +
				" nothing btree_gin supplies and the document materializes without it",
		},
		{
			name:      "index resolving to the extension on a relation this render cannot write",
			extension: "btree_gin",
			seed: "CREATE TABLE src (id integer PRIMARY KEY, n integer NOT NULL);" +
				" CREATE MATERIALIZED VIEW mv AS SELECT id, n FROM src;" +
				" CREATE INDEX mv_gin ON mv USING gin (n int4_ops)",
			wantBlock: false,
			// The control in the other direction: the edge is real and the
			// object carrying it reaches no document. A materialized view's
			// index is read out of pg_index with its resolved operator classes
			// like any other, and a `materialized` block carries no index, so
			// the renderer reports `index mv_gin: index cannot be rendered
			// because the target table is absent from the exported schema` and
			// writes nothing for it. Keeping btree_gin here cost the file the
			// whole compatibility this suppression exists to produce: the
			// pinned Atlas community binary v1.3.0 answered `postgres:
			// extensions are not supported by this version`, exit 1, on a
			// document that applies at exit 0 with or without the block.
			why: "the only object resolving to btree_gin is one this render reports it wrote nowhere," +
				" so the document neither names the extension nor needs it",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			adminDB, err := sql.Open("pgx", adminURL)
			c.Assert(err, qt.IsNil)
			defer adminDB.Close()

			stamp := time.Now().UnixNano()
			sourceName := fmt.Sprintf("ptah_ext_rt_src_%d", stamp)
			devName := fmt.Sprintf("ptah_ext_rt_dev_%d", stamp)
			createE2EDatabase(c, ctx, adminDB, sourceName)
			defer dropE2EDatabase(c, context.Background(), adminDB, sourceName)
			createE2EDatabase(c, ctx, adminDB, devName)
			defer dropE2EDatabase(c, context.Background(), adminDB, devName)

			sourceURL := replaceDatabaseName(c, adminURL, sourceName)
			devURL := replaceDatabaseName(c, adminURL, devName)

			seedAtlasCompatExtensionDB(c, ctx, sourceURL, test.extension, test.seed)

			rendered := runAtlasCompatInspect(c, sourceURL, "")
			c.Assert(
				strings.Contains(rendered, fmt.Sprintf("extension %q", test.extension)),
				qt.Equals, test.wantBlock,
				qt.Commentf("%s", test.why),
			)

			// The round trip. A fresh dev database has none of these
			// extensions, so materializing the document is what proves it
			// carries everything it depends on.
			assertAtlasCompatRoundTrip(c, test, rendered, devURL)
		})
	}
}

// assertAtlasCompatRoundTrip materializes the rendered document on a fresh dev
// database, unless the row has recorded a reason its fixture can never be
// materialized at all.
//
// The skip is per row and carries its reason with it rather than being a flag,
// because "this document is unreadable" and "this fixture is unbuildable" are
// different findings and only the first is about Ptah. Every row that builds
// its extension from an install script is asserted, which is all of them but
// one.
func assertAtlasCompatRoundTrip(c *qt.C, test atlasCompatExtensionRoundTripCase, rendered, devURL string) {
	c.Helper()

	if test.unreplayable != "" {
		c.Logf("round trip not asserted: %s", test.unreplayable)
		return
	}

	documentPath := filepath.Join(c.TempDir(), "inspected.hcl")
	c.Assert(os.WriteFile(documentPath, []byte(rendered), 0o600), qt.IsNil)
	readBack := runAtlasCompatInspect(c, "file://"+documentPath, devURL)
	c.Assert(readBack, qt.Not(qt.Equals), "",
		qt.Commentf("the round trip produced an empty document"))
}

// seedAtlasCompatExtensionDB installs the extension and runs the shape's DDL,
// then verifies through the catalog that both actually landed rather than
// trusting that the statements returned without error.
func seedAtlasCompatExtensionDB(c *qt.C, ctx context.Context, dbURL, extension, seed string) {
	c.Helper()

	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, err = db.ExecContext(ctx, "CREATE EXTENSION "+quoteE2EIdent(extension))
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, seed)
	c.Assert(err, qt.IsNil)

	var installed int
	c.Assert(db.QueryRowContext(ctx,
		"SELECT count(*) FROM pg_extension WHERE extname = $1", extension,
	).Scan(&installed), qt.IsNil)
	c.Assert(installed, qt.Equals, 1,
		qt.Commentf("the catalog does not report %q installed", extension))

	// pg_depend is what the reader consults to answer "what does this extension
	// supply". An extension whose membership rows are missing would make every
	// expectation here vacuous.
	var members int
	c.Assert(db.QueryRowContext(ctx, `
		SELECT count(*)
		  FROM pg_depend d
		  JOIN pg_extension e ON e.oid = d.refobjid
		 WHERE d.deptype = 'e' AND e.extname = $1`, extension,
	).Scan(&members), qt.IsNil)
	c.Assert(members > 0, qt.IsTrue,
		qt.Commentf("the catalog reports %q supplying nothing", extension))
}

// runAtlasCompatInspect runs `schema inspect` on the compatibility surface and
// returns the rendered document, failing the test on a non-nil error.
//
// Roles are excluded because PostgreSQL roles are CLUSTER-scoped: every role
// any other database on the same server has ever created is visible here, and
// replaying a document that declares them dies on "role already exists" before
// reaching the extension question this test is about.
func runAtlasCompatInspect(c *qt.C, sourceURL, devURL string) string {
	c.Helper()

	args := []string{
		"schema", "inspect",
		"--url", sourceURL,
		"--format", "{{ hcl . }}",
		"--exclude", "*[type=role]",
	}
	args = append(args, devURLArgs(devURL)...)

	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("stderr: %s", errOut.String()))

	return out.String()
}

// devURLArgs appends --dev-url only when one was given, keeping the caller's
// test body free of branching.
func devURLArgs(devURL string) []string {
	if devURL == "" {
		return nil
	}
	return []string{"--dev-url", devURL}
}
