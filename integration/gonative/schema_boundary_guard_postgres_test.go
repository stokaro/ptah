//go:build integration

// Boundary guard for stokaro/ptah#1276.
//
// Ptah has a READ path that describes a database and a COMPARE path that
// decides what to change. Both make scope decisions -- which schemas, which
// objects, which attributes -- and they make them independently. Five defects
// in one day shared that root, and every one of them had a green test at the
// level that changed. None had a test that crossed the boundary.
//
// This file is that crossing. It asserts two properties per fixture:
//
//	property 1  inspect -> parse back -> the document describes the same
//	            objects the reader reported;
//	property 2  inspect -> apply back -> the plan is empty.
//
// Property 2 runs on the COMPATIBILITY surface as well as the native one. The
// suppression behind one of the failures below exists only there, so a
// native-only guard cannot see it at all.
//
// It is a CHARACTERIZATION test, not a green one. Master does not hold either
// property on most of these fixtures. Every expectation that records a WRONG
// answer says so in a comment naming #1276 and what the value must become, so
// the file can be read as a list of what is broken. Nothing here is skipped:
// a skipped row is invisible in a green run, which is the exact failure mode
// #1276 is about.
//
// Every value below was measured against live PostgreSQL 17 and PostgreSQL 18,
// which agreed on all of them. First measured on f2e7fa0c (#1274) and
// re-measured unchanged on 4c4a8a1d, after #1277 and #1278 landed -- #1277 in
// particular renames a qualified HCL table reference, which is the same defect
// class as the grant churn pinned below, and it does not move any row here.

package gonative_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/schemafile"
)

// boundaryCase is one throwaway PostgreSQL database and the answer master
// gives for it today.
type boundaryCase struct {
	// name also names the database, so a leaked one says which case leaked it.
	name string
	// seed puts the fresh database into the state this case is about. Each
	// case owns its database: the server is shared, and a case that creates
	// schemas or installs extensions must not be able to disturb another.
	seed []string
	// query is appended to the database URL. "" is the plain URL form and is a
	// fixture in its own right rather than a default -- it is the form that hid
	// two of the five defects, and a suite that always pins a schema cannot
	// fail on them.
	query string

	// wantDescribedSchemas is the set of schemas the INSPECTED DOCUMENT
	// declares, read back out of the document itself.
	wantDescribedSchemas []string
	// wantReadSchemas is the set of schemas the READER reported for the same
	// database. The pair is the whole issue in two lines: when the two
	// disagree, one side is deciding scope without the other.
	wantReadSchemas []string

	// wantDocumentOnly and wantDatabaseOnly are property 1: the symmetric
	// difference between the objects the document describes and the objects
	// the reader reported. Both empty means the property holds.
	wantDocumentOnly []string
	wantDatabaseOnly []string

	// wantNativePlan and wantCompatPlan are property 2, per surface. They are
	// functions because the statements name the role that owns the fixture
	// tables, which is whatever role POSTGRES_TEST_DSN connects as. Returning
	// nil means the plan is empty and the property holds.
	wantNativePlan func(role string) []string
	wantCompatPlan func(role string) []string
}

// boundaryCases is the fixture set. Each entry exists because a real defect
// lived in it; none is here for coverage.
func boundaryCases() []boundaryCase {
	return []boundaryCase{
		{
			// Two schemas reached by a plain URL with no search_path. This URL
			// form hid #1257 and #1275.
			name:  "two_schemas_plain_url",
			seed:  []string{"CREATE SCHEMA extra", "CREATE TABLE public.a (id integer)", "CREATE TABLE extra.b (id integer)"},
			query: "",

			// WRONG (#1276): the database has two schemas and the document
			// describes one. `extra` and `extra.b` are absent from the
			// document because the reader was never asked about them, and
			// nothing downstream can tell that from "the database has no
			// schema `extra`". Must become []string{"extra", "public"} -- the
			// same answer the scoped URL below gives for `public`, extended to
			// every schema a plain URL reaches.
			wantDescribedSchemas: []string{"public"},
			// WRONG (#1276): the reader reports NO schemas at all, for every
			// fixture in this file, while the renderer synthesizes a schema
			// block from the tables it was given. Two sides, two independent
			// answers. Must become []string{"extra", "public"}.
			wantReadSchemas: nil,

			// WRONG (#1276): the document declares a schema the reader never
			// reported. Must become empty once both sides read the same list.
			//
			// Note what this row canNOT see: `extra.b` is missing from BOTH
			// sides, so it does not appear in the difference. Two sides that
			// agree on a lie look exactly like two sides that agree. That is
			// why wantDescribedSchemas above is asserted separately against
			// the database's real shape rather than against the reader.
			wantDocumentOnly: []string{"schema:public"},
			wantDatabaseOnly: nil,

			// FIXED (#1283): applying a database's own description back to it
			// plans nothing. The fourteen statements this row used to expect
			// were one grant revoked under the unqualified spelling and
			// re-granted under the qualified one; keying the comparison by
			// table identity collapsed them.
			wantNativePlan: boundaryNoPlan,
			wantCompatPlan: boundaryNoPlan,
		},
		{
			// The control. The same content reached with an explicit
			// search_path must keep reporting exactly one schema, so a fix
			// that widens the plain URL above cannot widen this one too.
			name:  "two_schemas_search_path_public",
			seed:  []string{"CREATE SCHEMA extra", "CREATE TABLE public.a (id integer)", "CREATE TABLE extra.b (id integer)"},
			query: "search_path=public",

			// CORRECT and must stay correct.
			wantDescribedSchemas: []string{"public"},
			// WRONG (#1276): must become []string{"public"} -- one schema,
			// because that is what this URL asked for.
			wantReadSchemas: nil,

			// WRONG (#1276): must become empty.
			wantDocumentOnly: []string{"schema:public"},
			wantDatabaseOnly: nil,

			// FIXED (#1283): the grant churn is gone here too, on the URL form
			// that pins a single schema.
			wantNativePlan: boundaryNoPlan,
			wantCompatPlan: boundaryNoPlan,
		},
		{
			// An extension supplying a type a column uses. #1266: inspect
			// omitted the block because nothing NAMED `isn`, but a column's
			// type `isbn` still needed it, and Ptah could not read its own
			// output. #1274 fixed it by asking what the extension PROVIDES.
			name: "extension_used_by_a_column",
			seed: []string{"CREATE EXTENSION isn", "CREATE TABLE books (code isbn)"},

			// CORRECT and must stay correct.
			wantDescribedSchemas: []string{"public"},
			// WRONG (#1276): must become []string{"public"}.
			wantReadSchemas: nil,

			// WRONG (#1276): must become empty. The extension itself is on
			// neither side of this difference, which is #1274 holding.
			wantDocumentOnly: []string{"schema:public"},
			wantDatabaseOnly: nil,

			// FIXED (#1283): no extension statement appeared here even when
			// this row was red -- the plan was the grant churn every table got,
			// and it is gone. An extension a column uses stays described.
			wantNativePlan: boundaryNoPlan,
			wantCompatPlan: boundaryNoPlan,
		},
		{
			// An extension nothing references. This is the fifth defect, and
			// the reason property 2 has to run on the compatibility surface:
			// the omission that causes it exists only there.
			name: "extension_nothing_references",
			seed: []string{"CREATE EXTENSION pgcrypto"},

			// CORRECT: a database with no tables declares no schema.
			wantDescribedSchemas: nil,
			// WRONG (#1276): must become []string{"public"} -- the reader did
			// read `public`, it just does not say so.
			wantReadSchemas: nil,

			// CORRECT: property 1 holds here. The native surface omits
			// nothing, so its document names pgcrypto and the round trip is
			// clean. The damage is only visible in the plan below.
			wantDocumentOnly: nil,
			wantDatabaseOnly: nil,

			// CORRECT: the native document declares the extension, so applying
			// it back changes nothing.
			wantNativePlan: boundaryNoPlan,
			// WRONG (#1276), and DESTRUCTIVE. The compatibility surface omits
			// an extension nothing else in the document names -- a presentation
			// decision, made so the tool this binary stands in for can read the
			// document at all -- and the comparator reads that omission as "the
			// desired state does not have this extension" and plans to remove
			// it. Inspecting a database and applying its own output back plans
			// to DROP something the database has.
			//
			// Must become nil. The comparator needs three states, not two:
			// present, absent, and deliberately not described.
			wantCompatPlan: boundaryDropExtension("pgcrypto"),
		},
		{
			// A table with a primary key, so the constraint-keying path is
			// exercised rather than assumed.
			name: "table_with_primary_key",
			seed: []string{"CREATE TABLE pk_t (id integer PRIMARY KEY, name text)"},

			// CORRECT and must stay correct.
			wantDescribedSchemas: []string{"public"},
			// WRONG (#1276): must become []string{"public"}.
			wantReadSchemas: nil,

			// WRONG (#1276): must become empty. The primary key itself round
			// trips: it is on neither side of this difference.
			wantDocumentOnly: []string{"schema:public"},
			wantDatabaseOnly: nil,

			// FIXED (#1283): nil on both surfaces.
			wantNativePlan: boundaryNoPlan,
			wantCompatPlan: boundaryNoPlan,
		},
		{
			// An empty database. The floor: whatever else is broken, nothing
			// may be planned against a database with nothing in it.
			name: "empty_database",

			// CORRECT.
			wantDescribedSchemas: nil,
			// WRONG (#1276): must become []string{"public"}.
			wantReadSchemas: nil,

			// CORRECT: property 1 holds.
			wantDocumentOnly: nil,
			wantDatabaseOnly: nil,

			// CORRECT: property 2 holds on both surfaces.
			wantNativePlan: boundaryNoPlan,
			wantCompatPlan: boundaryNoPlan,
		},
	}
}

func TestPostgreSQLSchemaBoundaryGuardIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)

	for _, tc := range boundaryCases() {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			observed := observeBoundaryCase(c, dsn, tc)

			c.Run("scope: reader and document agree on which schemas exist", func(c *qt.C) {
				c.Assert(boundarySchemaNames(observed.document), qt.DeepEquals, tc.wantDescribedSchemas)
				c.Assert(boundarySchemaNames(observed.live), qt.DeepEquals, tc.wantReadSchemas)
			})

			c.Run("property 1: inspect then parse back describes the same objects", func(c *qt.C) {
				documentOnly, databaseOnly := boundaryObjectDifference(observed.document, observed.live, observed.defaultSchema)
				c.Assert(documentOnly, qt.DeepEquals, tc.wantDocumentOnly)
				c.Assert(databaseOnly, qt.DeepEquals, tc.wantDatabaseOnly)
			})

			c.Run("property 2: inspect then apply back is a no-op on the native surface", func(c *qt.C) {
				c.Assert(observed.nativePlan, qt.DeepEquals, tc.wantNativePlan(observed.role))
			})

			c.Run("property 2: inspect then apply back is a no-op on the compatibility surface", func(c *qt.C) {
				c.Assert(observed.compatPlan, qt.DeepEquals, tc.wantCompatPlan(observed.role))
			})
		})
	}
}

// boundaryObservation is everything one case's database has to say, gathered
// before any assertion runs so the test bodies are assertions only.
type boundaryObservation struct {
	// live is the schema the reader reported, in the same IR the renderer is
	// handed.
	live *goschema.Database
	// document is the NATIVE surface's inspected output, parsed back.
	//
	// Property 1 is asked of the native surface alone, deliberately. The
	// compatibility surface omits blocks the tool it stands in for refuses to
	// read, so a difference there is a presentation decision rather than a
	// defect -- and property 2 below is where that decision turns into a plan
	// that removes things, which is the part worth guarding.
	document      *goschema.Database
	defaultSchema string
	// role owns the fixture tables: whatever role POSTGRES_TEST_DSN connects
	// as, since that role created them.
	role string
	// nativePlan and compatPlan are each surface's own document applied back to
	// the database it was inspected from.
	nativePlan []string
	compatPlan []string
}

// observeBoundaryCase provisions the case's throwaway database, then runs both
// round trips against it.
func observeBoundaryCase(c *qt.C, dsn string, tc boundaryCase) boundaryObservation {
	c.Helper()

	dbURL := newBoundaryDatabase(c, dsn, tc)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	live, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	c.Assert(err, qt.IsNil)

	// The two surfaces differ in exactly the two options cmd/atlas sets and
	// cmd/schema does not: the compatibility binary omits the block types the
	// tool it stands in for refuses, and tolerates names Ptah does not model.
	// Everything else is the same library call, so a difference between the
	// two rows is a difference those options caused.
	nativeDocument := boundaryInspect(c, dbURL, false)
	compatDocument := boundaryInspect(c, dbURL, true)

	return boundaryObservation{
		live:          dbschematogo.ConvertDBSchemaToGoSchema(live),
		document:      boundaryParseBack(c, nativeDocument, false),
		defaultSchema: conn.Info().Schema,
		role:          boundaryConnectedRole(c, dbURL),
		nativePlan:    boundaryApplyBack(c, conn, nativeDocument, false),
		compatPlan:    boundaryApplyBack(c, conn, compatDocument, true),
	}
}

// newBoundaryDatabase creates and seeds a database of this case's own and
// returns the URL under test. The server is shared with other suites and other
// agents, so nothing here touches the database POSTGRES_TEST_DSN names.
//
// The seed runs through a separate connection that is closed again before the
// URL under test is used: that is the real order, since a database is already
// in its state when a run reaches it.
func newBoundaryDatabase(c *qt.C, dsn string, tc boundaryCase) string {
	c.Helper()

	admin, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(c.Context()), qt.IsNil)

	name := fmt.Sprintf("ptah_boundary_%s_%d", tc.name, time.Now().UnixNano())
	ident := pgx.Identifier{name}.Sanitize()
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+ident)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		// WITH (FORCE) because a connection this test opened may outlive the
		// assertion that failed, and a database left behind on a shared server
		// is the next run's unexplained failure.
		_, dropErr := admin.ExecContext(
			context.WithoutCancel(c.Context()),
			"DROP DATABASE IF EXISTS "+ident+" WITH (FORCE)",
		)
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})

	parsed, err := url.Parse(dsn)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	parsed.RawPath = ""
	boundarySeed(c, parsed.String(), tc.seed)

	// RawQuery is assigned rather than rebuilt through url.Values so a case can
	// spell its parameter exactly as an operator would on a command line.
	parsed.RawQuery = boundaryQuery(parsed.RawQuery, tc.query)
	return parsed.String()
}

func boundarySeed(c *qt.C, dbURL string, statements []string) {
	c.Helper()

	seed, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(seed.Close(), qt.IsNil) }()
	for _, statement := range statements {
		_, execErr := seed.ExecContext(c.Context(), statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("seed statement: %s", statement))
	}
}

// boundaryQuery joins the DSN's own parameters, such as sslmode, with the
// case's.
func boundaryQuery(base, extra string) string {
	joined := []string{}
	for _, part := range []string{base, extra} {
		if strings.TrimSpace(part) != "" {
			joined = append(joined, part)
		}
	}
	return strings.Join(joined, "&")
}

// boundaryConnectedRole reports the role the fixture tables belong to. It is
// asked of the database rather than parsed out of the URL because the URL may
// carry no user at all and leave it to the environment.
func boundaryConnectedRole(c *qt.C, dbURL string) string {
	c.Helper()

	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()

	var role string
	c.Assert(db.QueryRowContext(c.Context(), "SELECT current_user").Scan(&role), qt.IsNil)
	return role
}

// boundaryInspect renders the live database, through the same entry point
// cmd/atlas/schema_inspect.go and cmd/schema/inspect.go both call.
func boundaryInspect(c *qt.C, dbURL string, compatibility bool) string {
	c.Helper()

	rendered, err := atlasschema.InspectSource(c.Context(), atlasschema.InspectSourceOptions{
		URL:    dbURL,
		Format: "hcl",
		// The diagnostics stream carries the compatibility surface's report of
		// what it left out. It is discarded here on purpose: this guard asks
		// what the DOCUMENT says and what the plan DOES, and a guard that
		// accepted a removal because it was announced would be the same
		// mistake in a new place.
		Diagnostics:            io.Discard,
		OmitAtlasRefusedBlocks: compatibility,
		IgnoreUnknownHCLNames:  compatibility,
	})
	c.Assert(err, qt.IsNil)
	return rendered
}

// boundaryParseBack reads an inspected document back into Ptah's IR. A parse
// failure is property 1 failing in its loudest form -- Ptah unable to read its
// own output, which is what #1266 was.
func boundaryParseBack(c *qt.C, document string, compatibility bool) *goschema.Database {
	c.Helper()

	parsed, err := schemafile.LoadAll([]string{"file://" + boundaryDocumentFile(c, document)}, schemafile.Options{
		Dialect:               "postgres",
		IgnoreUnknownHCLNames: compatibility,
	})
	c.Assert(err, qt.IsNil)
	return parsed
}

// boundaryApplyBack plans the document against the database it came from and
// returns the statements, through the same entry point
// cmd/atlas/schema_apply.go and cmd/schema/apply.go both call. An empty result
// is property 2 holding.
func boundaryApplyBack(c *qt.C, conn *dbschema.DatabaseConnection, document string, compatibility bool) []string {
	c.Helper()

	plan, err := atlasschema.PrepareApply(c.Context(), conn, atlasschema.ApplyRuntimeOptions{
		ToURLs: []string{"file://" + boundaryDocumentFile(c, document)},
		// Nothing is executed. The plan is the observation, and one of the
		// plans below would drop an extension.
		DryRun:                true,
		IgnoreUnknownHCLNames: compatibility,
	})
	c.Assert(err, qt.IsNil)
	return boundaryStripComments(plan.Statements())
}

func boundaryDocumentFile(c *qt.C, document string) string {
	c.Helper()

	path := filepath.Join(c.TempDir(), "inspected.hcl")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// boundaryStripComments drops the leading `--` advisory lines the planner
// attaches to some statements, so an expectation records what a statement DOES
// rather than how it is narrated. The wording of a warning is not this guard's
// business; whether the statement exists at all is.
func boundaryStripComments(statements []string) []string {
	var out []string
	for _, statement := range statements {
		body := []string{}
		for line := range strings.SplitSeq(statement, "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), "--") {
				body = append(body, line)
			}
		}
		trimmed := strings.TrimSpace(strings.Join(body, "\n"))
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// boundaryNoPlan is property 2 holding: applying a database's own description
// back to it changes nothing.
func boundaryNoPlan(string) []string { return nil }

// boundaryDropExtension is the destructive plan: the description a database
// gave of itself, applied back to that database, removes an object it has.
//
// Must become nil.
func boundaryDropExtension(name string) func(role string) []string {
	return func(string) []string {
		return []string{fmt.Sprintf("DROP EXTENSION IF EXISTS %q", name)}
	}
}

// boundarySchemaNames is the set of schemas an IR says the database has.
func boundarySchemaNames(db *goschema.Database) []string {
	var names []string
	for _, schema := range db.Schemas {
		names = append(names, schema.Name)
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// boundaryObjectDifference is property 1's measurement: what the document
// describes that the reader did not report, and the reverse.
//
// It compares object IDENTITY only -- which schemas, which tables, which
// columns, which primary keys, which extensions -- because #1276 is about
// scope. Attribute fidelity is a different axis with its own open issue
// (#1272, where a comparator that ignores an index's access method makes
// `USING gin` and `USING gist` equal), and folding it in here would make every
// row fail for a reason this guard is not asking about.
//
// Roles and grants are left out for a different reason: PostgreSQL roles are
// cluster-scoped, so a sibling suite's leftover role would appear in one of
// these reads and turn this guard into a flake. Grants are covered by property
// 2 instead, where they are the whole finding.
func boundaryObjectDifference(document, live *goschema.Database, defaultSchema string) (documentOnly, databaseOnly []string) {
	inDocument := boundaryObjectIDs(document, defaultSchema)
	inDatabase := boundaryObjectIDs(live, defaultSchema)
	return boundaryMissingFrom(inDocument, inDatabase), boundaryMissingFrom(inDatabase, inDocument)
}

func boundaryMissingFrom(have, other []string) []string {
	var missing []string
	for _, id := range have {
		if !slices.Contains(other, id) {
			missing = append(missing, id)
		}
	}
	return missing
}

// boundaryObjectIDs names every object an IR describes, in a spelling both
// sides can produce.
//
// Tables are keyed by qualified name rather than by StructName: StructName is
// an internal handle and the two sides spell it differently for the same table
// (`PkT` from the catalog, `pk_t` from the document), which is a difference in
// bookkeeping rather than in what exists.
func boundaryObjectIDs(db *goschema.Database, defaultSchema string) []string {
	var ids []string
	for _, schema := range db.Schemas {
		ids = append(ids, "schema:"+schema.Name)
	}
	for _, extension := range db.Extensions {
		ids = append(ids, "extension:"+extension.Name)
	}
	tableByStruct := map[string]string{}
	for _, table := range db.Tables {
		qualified := boundaryQualify(table.Schema, table.Name, defaultSchema)
		tableByStruct[table.StructName] = qualified
		ids = append(ids, "table:"+qualified)
		ids = append(ids, boundaryPrimaryKeyID(qualified, table.PrimaryKey)...)
	}
	primaryKeyColumns := map[string][]string{}
	for _, field := range db.Fields {
		qualified, known := tableByStruct[field.StructName]
		if !known {
			continue
		}
		ids = append(ids, "column:"+qualified+"."+field.Name)
		if field.Primary {
			primaryKeyColumns[qualified] = append(primaryKeyColumns[qualified], field.Name)
		}
	}
	for qualified, columns := range primaryKeyColumns {
		ids = append(ids, boundaryPrimaryKeyID(qualified, columns)...)
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// boundaryPrimaryKeyID keys a primary key on its table and columns. The two
// sides carry it in different places -- the catalog read marks the columns,
// the parsed document fills the table's key list -- and both spellings must
// land on the same identity or every keyed table would look like a difference.
func boundaryPrimaryKeyID(qualifiedTable string, columns []string) []string {
	if len(columns) == 0 {
		return nil
	}
	return []string{"primary_key:" + qualifiedTable + ":" + strings.Join(columns, ",")}
}

func boundaryQualify(schema, name, defaultSchema string) string {
	if strings.TrimSpace(schema) == "" {
		return defaultSchema + "." + name
	}
	return schema + "." + name
}
