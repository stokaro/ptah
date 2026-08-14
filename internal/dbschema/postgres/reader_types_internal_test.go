package postgres

// White-box testing required: the whole of stokaro/ptah#1294 is the SQL that
// readDomainsForSchema, readCompositesForSchema and readRangesForSchema send.
// Whether a type is owned by an extension is a pg_depend fact that no exported
// reader API exposes -- a described domain looks the same either way -- so the
// difference is only observable against a server that answers the ownership
// question. All three methods are unexported and the query text has no other
// source.
//
// The fake server below EVALUATES the ownership predicate against its own
// pg_depend rows rather than recognizing a token in the query. It holds its own
// literal copy of the conjuncts it knows how to evaluate, and a predicate it
// does not know is an error, never a guess. That is what stops the guard being
// true by construction of the fake: the two mutants that matter here are
// dropping `extdep.objid = t.oid` -- which makes the NOT EXISTS drop EVERY type
// as soon as the database holds any extension member -- and changing
// `extdep.deptype` or `extdep.classid` to some other edge, which silently stops
// excluding anything. Both are refused by name instead of being answered, so
// every test in this file turns red rather than one of them passing.
//
// A query carrying no pg_depend clause at all is a different fact, and a real
// one: it is the read as it stood before this change, and the fake answers it
// with every type the server has, which is exactly what master did.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// catalogType is one pg_type row on the simulated server together with the one
// fact this file is about: whether pg_depend records an extension owning it.
type catalogType struct {
	// name is the type name pg_type reports.
	name string
	// kind is pg_type.typtype: 'd' domain, 'c' composite, 'r' range.
	kind string
	// ownedByExtension is whether pg_depend holds a deptype='e' row naming
	// this type. That row is written by CREATE EXTENSION and by
	// ALTER EXTENSION ... ADD, and by nothing else.
	ownedByExtension bool
}

// The conjuncts of the ownership exclusion this fake knows how to evaluate,
// spelled out here rather than read from the reader's own constant: a guard
// that imports the value it is checking agrees with every mutation of it.
//
// Each one is load-bearing and each is separately fatal to leave out:
//
//	ownershipCatalog      the edge lives in pg_depend and nowhere else
//	ownershipClass        a pg_depend row names its object by (classid, objid),
//	                      and OIDs come from one counter shared by every
//	                      catalog, so objid alone can match another class's row
//	ownershipCorrelation  without it the subquery is uncorrelated and the
//	                      NOT EXISTS is false for every row the moment the
//	                      database holds one extension member -- the read then
//	                      describes nothing at all
//	ownershipEdge         'e' is extension membership; 'n', 'a' and 'i' are
//	                      ordinary, automatic and internal dependencies, which
//	                      every user type has
const (
	ownershipNotExists   = "NOT EXISTS"
	ownershipCatalog     = "FROM pg_depend extdep"
	ownershipClass       = "extdep.classid = 'pg_type'::regclass"
	ownershipCorrelation = "extdep.objid = t.oid"
	ownershipEdge        = "extdep.deptype = 'e'"
)

// ownershipConjuncts is the exclusion taken apart, so a refusal can name the
// piece that is missing instead of reporting "not the predicate I know".
var ownershipConjuncts = []string{
	ownershipNotExists,
	ownershipCatalog,
	ownershipClass,
	ownershipCorrelation,
	ownershipEdge,
}

// stripTypeSQLComments removes `-- ...` comments so the reader's prose cannot
// stand in for the catalog expression it describes. The exclusion is documented
// at length right above itself, and every conjunct's name appears in that
// prose.
func stripTypeSQLComments(query string) string {
	lines := strings.Split(query, "\n")
	for i, line := range lines {
		lines[i], _, _ = strings.Cut(line, "--")
	}
	return strings.Join(lines, "\n")
}

// typeFilter reports whether the server hands a type back for the statement it
// was resolved from. The statement is turned into a predicate rather than into
// a boolean so that "which types does this query select" stays one idea.
type typeFilter func(catalogType) bool

// everyType is what a statement carrying no ownership clause selects: all of
// them. That is the read as it stood before stokaro/ptah#1294.
func everyType(catalogType) bool { return true }

// onlyUserDeclared is what the exclusion selects.
func onlyUserDeclared(entry catalogType) bool { return !entry.ownedByExtension }

// describedTypes resolves the statement's ownership clause into the predicate
// the server evaluates, and refuses any pg_depend clause this fake cannot
// evaluate.
func describedTypes(stripped string) (typeFilter, error) {
	missing := missingConjuncts(stripped)
	if len(missing) == len(ownershipConjuncts) {
		return everyType, nil
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf(
			"this fake evaluates the extension-ownership exclusion only in full, and the query is "+
				"missing %s. A reader that changes the exclusion on purpose updates ownershipConjuncts "+
				"and the tests in this file",
			strings.Join(missing, ", "),
		)
	}
	return onlyUserDeclared, nil
}

// missingConjuncts lists the pieces of the exclusion the statement does not
// carry.
func missingConjuncts(stripped string) []string {
	var missing []string
	for _, conjunct := range ownershipConjuncts {
		missing = appendWhenAbsent(missing, stripped, conjunct)
	}
	return missing
}

func appendWhenAbsent(missing []string, stripped, conjunct string) []string {
	if strings.Contains(stripped, conjunct) {
		return missing
	}
	return append(missing, conjunct)
}

// typeKindRead returns the pg_type.typtype the statement selects, so one fake
// can answer all three reads and a read asking for the wrong kind is visible as
// a wrong answer rather than being waved through.
func typeKindRead(stripped string) (string, error) {
	for _, kind := range []string{"d", "c", "r"} {
		if strings.Contains(stripped, fmt.Sprintf("t.typtype = '%s'", kind)) {
			return kind, nil
		}
	}
	return "", fmt.Errorf("the query restricts no pg_type.typtype, so this fake cannot tell which read it is")
}

// answerTypes plays PostgreSQL for the three type reads.
func answerTypes(query string, args []driver.NamedValue, catalog []catalogType) (dbtest.QueryResult, error) {
	stripped := stripTypeSQLComments(query)

	kind, err := typeKindRead(stripped)
	if err != nil {
		return dbtest.QueryResult{}, err
	}
	described, err := describedTypes(stripped)
	if err != nil {
		return dbtest.QueryResult{}, err
	}
	schema, err := boundSchema(stripped, args)
	if err != nil {
		return dbtest.QueryResult{}, err
	}

	return typeRows(kind, schema, described, catalog), nil
}

// boundSchema returns the schema name the statement binds, refusing a read that
// binds a placeholder it never spells.
func boundSchema(stripped string, args []driver.NamedValue) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("the query binds no schema")
	}
	arg := args[0]
	placeholder := fmt.Sprintf("$%d", arg.Ordinal)
	if !strings.Contains(stripped, placeholder) {
		return "", fmt.Errorf("argument %v is bound but %s never appears in the query", arg.Value, placeholder)
	}
	name, ok := arg.Value.(string)
	if !ok {
		return "", fmt.Errorf("argument %s is not a schema name", placeholder)
	}
	return name, nil
}

// typeRows builds the answer for one read. Composites carry a field so the
// reader has something to group, and the column sets differ per kind because
// the three statements select different things.
func typeRows(kind, schema string, described typeFilter, catalog []catalogType) dbtest.QueryResult {
	result := dbtest.QueryResult{Columns: typeColumns(kind)}
	for _, entry := range catalog {
		result.Rows = appendTypeRow(result.Rows, entry, kind, schema, described)
	}
	return result
}

func typeColumns(kind string) []string {
	return map[string][]string{
		"d": {"schema_name", "domain_name", "base_type", "not_null", "default_value", "check_expr"},
		"c": {"schema_name", "type_name", "field_name", "field_type", "attnum"},
		"r": {"schema_name", "range_name", "subtype", "subtype_opclass", "collation_name", "canonical", "subtype_diff"},
	}[kind]
}

func appendTypeRow(
	rows [][]driver.Value,
	entry catalogType,
	kind, schema string,
	described typeFilter,
) [][]driver.Value {
	if entry.kind != kind || !described(entry) {
		return rows
	}
	return append(rows, map[string][]driver.Value{
		"d": {schema, entry.name, "text", false, "", ""},
		"c": {schema, entry.name, "lo", "integer", int64(1)},
		"r": {schema, entry.name, "integer", "int4_ops", "", "", ""},
	}[kind])
}

// newTypesServer returns a Reader backed by a server holding catalog.
func newTypesServer(tb interface{ Cleanup(func()) }, catalog []catalogType) *Reader {
	db := dbtest.Open(tb, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		return answerTypes(query, args, catalog)
	})
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())
	reader.SetSchemas([]string{"public"})
	return reader
}

// mixedCatalog is a server holding, for each of the three kinds, one type an
// extension owns and one the user declared -- named as closely as PostgreSQL
// allows, so a filter matching on the NAME rather than on the pg_depend edge
// catches the user's type too.
//
// The underscore is the point of the pairing. A `NOT LIKE 'lo_%'` written
// without an ESCAPE clause reads that underscore as a single-character
// wildcard, which is how this reader lost ordinary pg-prefixed roles in
// stokaro/ptah#1291. Ownership is a catalog edge and has no such failure mode,
// and these rows are what says so.
func mixedCatalog() []catalogType {
	return []catalogType{
		{name: "lo", kind: "d", ownedByExtension: true},
		{name: "lo_own", kind: "d", ownedByExtension: false},
		{name: "dblink_pkey_results", kind: "c", ownedByExtension: true},
		{name: "dblink_pkey_results_own", kind: "c", ownedByExtension: false},
		{name: "ptahspan", kind: "r", ownedByExtension: true},
		{name: "ptahspan_own", kind: "r", ownedByExtension: false},
	}
}

// readTypeNames is the three reads behind one signature, so a table row can name
// the read it exercises instead of the test body choosing between them.
var (
	readDomainNames = func(r *Reader) ([]string, error) {
		domains, err := r.readDomains()
		names := make([]string, 0, len(domains))
		for _, domain := range domains {
			names = append(names, domain.Name)
		}
		return names, err
	}
	readCompositeNames = func(r *Reader) ([]string, error) {
		composites, err := r.readComposites()
		names := make([]string, 0, len(composites))
		for _, composite := range composites {
			names = append(names, composite.Name)
		}
		return names, err
	}
	readRangeNames = func(r *Reader) ([]string, error) {
		ranges, err := r.readRanges()
		names := make([]string, 0, len(ranges))
		for _, rangeType := range ranges {
			names = append(names, rangeType.Name)
		}
		return names, err
	}
)

func TestReadTypesLeavesExtensionOwnedTypesToTheExtension(t *testing.T) {
	// The headline of stokaro/ptah#1294. CREATE EXTENSION creates these types,
	// so a description that declares both the extension and the type cannot be
	// replayed -- the second declaration collides with what the first already
	// made. Measured on PostgreSQL 17.10 against a database holding
	// `CREATE EXTENSION lo` and a `lo`-typed column:
	//
	//	Error: materialize schema on dev database: ... ERROR: type "lo"
	//	already exists (SQLSTATE 42710)  SQL: CREATE DOMAIN "lo" AS oid;
	//
	// The reader already excluded extension-owned FUNCTIONS for exactly this
	// reason and the three type reads were never given that filter.
	tests := []struct {
		name string
		read func(*Reader) ([]string, error)
		want []string
	}{
		{name: "domains", read: readDomainNames, want: []string{"lo_own"}},
		{name: "composites", read: readCompositeNames, want: []string{"dblink_pkey_results_own"}},
		{name: "ranges", read: readRangeNames, want: []string{"ptahspan_own"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := newTypesServer(c.TB, mixedCatalog())

			names, err := test.read(reader)

			c.Assert(err, qt.IsNil)
			c.Assert(names, qt.DeepEquals, test.want)
		})
	}
}

func TestReadTypesStillDescribesWhatTheUserDeclared(t *testing.T) {
	// The control in the other direction, and the reason the exclusion is a fix
	// rather than a deletion. A server whose types are all the user's own must
	// be described in full, whatever else is installed alongside -- including
	// types named as closely as PostgreSQL allows to an extension's.
	//
	// This row stays green when the exclusion is reverted, which is what a
	// non-interference control is for. What stops it being vacuous is the
	// inverse mutant: dropping the correlation `extdep.objid = t.oid` makes the
	// exclusion swallow every type on the server, and the fake refuses that
	// predicate by name, so this test reddens for it.
	tests := []struct {
		name string
		read func(*Reader) ([]string, error)
		want []string
	}{
		{name: "domains", read: readDomainNames, want: []string{"lo", "lo_own"}},
		{
			name: "composites",
			read: readCompositeNames,
			want: []string{"dblink_pkey_results", "dblink_pkey_results_own"},
		},
		{name: "ranges", read: readRangeNames, want: []string{"ptahspan", "ptahspan_own"}},
	}

	userDeclared := make([]catalogType, 0, len(mixedCatalog()))
	for _, entry := range mixedCatalog() {
		entry.ownedByExtension = false
		userDeclared = append(userDeclared, entry)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := newTypesServer(c.TB, userDeclared)

			names, err := test.read(reader)

			c.Assert(err, qt.IsNil)
			c.Assert(names, qt.DeepEquals, test.want)
		})
	}
}

func TestReadTypesAsksPgDependRatherThanTheName(t *testing.T) {
	// The property of the three STATEMENTS, which is where the defect would
	// live, stated separately from the property of the answers. A read that
	// excluded by name -- `NOT LIKE 'lo\_%'`, an allow-list of contrib type
	// names -- could satisfy every assertion above on this fixture and be wrong
	// on the next database. The correlation is asserted explicitly because
	// without it the subquery is uncorrelated: the exclusion then withholds
	// every type as soon as the server holds one extension member, and a
	// database with nothing installed would still pass the tests above.
	tests := []struct {
		name string
		read func(*Reader) ([]string, error)
	}{
		{name: "domains", read: readDomainNames},
		{name: "composites", read: readCompositeNames},
		{name: "ranges", read: readRangeNames},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var sent []string
			db := dbtest.Open(c.TB, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
				sent = append(sent, query)
				return answerTypes(query, args, mixedCatalog())
			})
			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())
			reader.SetSchemas([]string{"public"})

			_, err := test.read(reader)
			c.Assert(err, qt.IsNil)
			c.Assert(sent, qt.HasLen, 1)

			stripped := stripTypeSQLComments(sent[0])
			c.Assert(missingConjuncts(stripped), qt.HasLen, 0,
				qt.Commentf("the ownership exclusion is not carried in full"))
			c.Assert(stripped, qt.Not(qt.Contains), "LIKE",
				qt.Commentf("ownership is a pg_depend edge, not a name pattern"))
		})
	}
}
