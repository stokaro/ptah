package clickhouse

// White-box testing required: what stokaro/ptah#1025 asks of this reader is
// mostly a property of the SQL it sends, and the exported surface cannot show
// it. ReadSchema answers a described set; whether a row is missing from that set
// because the statement excluded it or because the server happened not to have
// it is invisible from outside, and the rows that matter here -- a USER's
// privileges, another database's grants, a column-scoped grant -- are exactly
// the ones a fake could quietly decline to invent. The queries are unexported
// constants with no other source, and readGrants and readRoles are unexported
// too.
//
// The simulated server below therefore evaluates the statement instead of
// recognizing it. It applies each restriction only when the statement carries
// it, so a filter that is dropped shows up as a row in the description rather
// than being answered away, and it refuses a statement naming a system.grants
// column that 24.10.4.191 does not have -- which makes every behavioral test in
// this file a test against the oldest release line this repository declares, not
// only against the newest one. It refuses the credential-bearing surfaces on the
// same principle. TestTheSimulatedServerRefusesWhatItMustRefuse is the control
// that both refusals can fire at all; without it a guard that recognizes nothing
// would report a clean run.

import (
	"cmp"
	"database/sql/driver"
	"fmt"
	"slices"
	"strings"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// rbacDatabase is the database the reader under test is connected to. Every
// fixture below holds rows for it and rows for somewhere else, because "scoped
// to the connected database" is not observable on a server that has only one.
const rbacDatabase = "app"

// grantRow is one row of the simulated system.grants.
//
// The empty string stands for NULL in the three columns where ClickHouse uses
// one, and each NULL means something different: a NULL user_name marks a row
// owned by a role, a NULL database marks the global `*.*` scope, a NULL table
// marks a database-wide grant, and a NULL column marks a grant that is not
// column-scoped.
type grantRow struct {
	userName      string
	roleName      string
	privilege     string
	database      string
	table         string
	column        string
	partialRevoke bool
	grantOption   bool
}

// roleRow is one row of the simulated system.roles. storage is
// 'local_directory' for a role SQL created and 'users_xml' for one the server's
// configuration declares.
type roleRow struct {
	name    string
	storage string
}

// rbacServer is the state one simulated ClickHouse holds.
type rbacServer struct {
	roles  []roleRow
	grants []grantRow
}

// grantsColumnsAddedAfter2410 are the two system.grants columns 26.7.3.19 has
// and 24.10.4.191 does not.
//
// Measured on both: 24.10 answers user_name, role_name, access_type, database,
// table, column, is_partial_revoke and grant_option -- eight -- and 26.7 answers
// those plus access_object and is_wildcard. A statement naming either of the two
// passes on 26.7 and fails on 24.10, a line internal/capabilityprobe/cells.go
// declares and CI starts, so the simulated server is the older one.
var grantsColumnsAddedAfter2410 = []string{"access_object", "is_wildcard"}

// secretBearingSurfaces are the catalogs and statements a ClickHouse credential
// can come out of.
//
// A role carries none: system.roles is (name, id, storage). Only a USER does,
// through system.users.auth_params and through SHOW CREATE USER, which renders
// `IDENTIFIED WITH ...`. SHOW GRANTS is here for its user form, which names a
// principal this reader has no business reading. The rule is that the reader
// never asks, so nothing downstream has to be careful with the answer.
var secretBearingSurfaces = []string{"SYSTEM.USERS", "AUTH_PARAMS", "SHOW CREATE", "SHOW GRANTS"}

// refuseColumnsAddedAfter2410 plays 24.10's catalog.
//
// A `SELECT *` over system.grants is refused for the same reason as a named
// column: it is ten columns on one supported line and eight on the other, so a
// reader built on it scans a different shape per server.
func refuseColumnsAddedAfter2410(query string) error {
	for _, column := range grantsColumnsAddedAfter2410 {
		if strings.Contains(query, column) {
			return fmt.Errorf(
				"simulated 24.10.4.191: system.grants has no column %q (UNKNOWN_IDENTIFIER)", column)
		}
	}
	if strings.Contains(query, "system.grants") && strings.Contains(query, "SELECT *") {
		return fmt.Errorf(
			"simulated 24.10.4.191: SELECT * over system.grants answers 8 columns here and 10 on 26.7")
	}
	return nil
}

// refuseSecretBearingSurface refuses any statement that reaches for a
// credential-bearing catalog or command.
func refuseSecretBearingSurface(query string) error {
	upper := strings.ToUpper(query)
	for _, surface := range secretBearingSurfaces {
		if strings.Contains(upper, surface) {
			return fmt.Errorf("the reader must never name %s", surface)
		}
	}
	return nil
}

// The restrictions the grants statement is expected to carry, spelled out here
// rather than read from the reader's own constant: a guard that imports the
// value it checks agrees with every mutation of it.
const (
	excludesUserRows    = "user_name IS NULL"
	requiresRoleRows    = "role_name IS NOT NULL"
	scopesToOneDatabase = "database = ?"
	excludesColumnScope = "ifNull(`column`, '') = ''"
)

// knownGrantRestrictions is the whole of what this fake can evaluate.
//
// A restriction outside it is an error rather than a guess, for the reason the
// PostgreSQL role fake refuses an unknown membership predicate: a statement that
// narrows the read in a way the fake ignores is answered as though it did not
// narrow it, and the test then passes on a description the server would never
// have produced. `AND is_partial_revoke = 0` is the concrete one -- it drops the
// rows that record a grant minus exceptions, which types.DBGrant.IsPartialRevoke
// exists to report -- and it walked past every test in this file until the fake
// started reading the clauses instead of searching for them.
var knownGrantRestrictions = []string{
	excludesUserRows,
	requiresRoleRows,
	scopesToOneDatabase,
	excludesColumnScope,
}

// grantFilters is which of those restrictions one statement actually carries.
type grantFilters struct {
	excludesUsers    bool
	requiresRoles    bool
	scopedToDatabase bool
	excludesColumns  bool
}

// filtersOf reads the statement's WHERE clause, one restriction at a time.
func filtersOf(query string) (grantFilters, error) {
	_, conditions, found := strings.Cut(query, "WHERE ")
	if !found {
		return grantFilters{}, nil
	}
	conditions, _, _ = strings.Cut(conditions, "ORDER BY")

	var filters grantFilters
	for clause := range strings.SplitSeq(conditions, " AND ") {
		restriction := strings.Join(strings.Fields(clause), " ")
		if !slices.Contains(knownGrantRestrictions, restriction) {
			return grantFilters{}, fmt.Errorf(
				"this fake cannot evaluate the restriction %q; a reader that adds one updates knownGrantRestrictions",
				restriction)
		}
		filters.excludesUsers = filters.excludesUsers || restriction == excludesUserRows
		filters.requiresRoles = filters.requiresRoles || restriction == requiresRoleRows
		filters.scopedToDatabase = filters.scopedToDatabase || restriction == scopesToOneDatabase
		filters.excludesColumns = filters.excludesColumns || restriction == excludesColumnScope
	}
	return filters, nil
}

// keeps reports whether a server evaluating this statement would answer this
// row. A restriction the statement does not carry restricts nothing, which is
// the whole point: dropping one shows up as an extra described grant.
func (f grantFilters) keeps(row grantRow, database string) bool {
	if f.excludesUsers && row.userName != "" {
		return false
	}
	if f.requiresRoles && row.roleName == "" {
		return false
	}
	if f.excludesColumns && row.column != "" {
		return false
	}
	if f.scopedToDatabase && row.database != database {
		return false
	}
	return true
}

// boundDatabase returns the database the statement is scoped to, and refuses a
// statement whose placeholders and arguments disagree.
func boundDatabase(query string, args []driver.NamedValue) (string, error) {
	placeholders := strings.Count(query, "?")
	if placeholders != len(args) {
		return "", fmt.Errorf("the statement carries %d placeholders and %d arguments", placeholders, len(args))
	}
	if len(args) == 0 {
		return "", nil
	}
	if len(args) != 1 {
		return "", fmt.Errorf("the grants read binds one database, got %d arguments", len(args))
	}
	database, ok := args[0].Value.(string)
	if !ok {
		return "", fmt.Errorf("argument 1 is %v, which is not a database name", args[0].Value)
	}
	return database, nil
}

func answerGrants(query string, args []driver.NamedValue, rows []grantRow) (dbtest.QueryResult, error) {
	filters, err := filtersOf(query)
	if err != nil {
		return dbtest.QueryResult{}, err
	}
	database, err := boundDatabase(query, args)
	if err != nil {
		return dbtest.QueryResult{}, err
	}
	result := dbtest.QueryResult{
		Columns: []string{
			"grantee", "privilege", "database_name", "table_name", "is_partial_revoke", "grant_option",
		},
	}
	for _, row := range orderedGrantRows(query, rows) {
		if !filters.keeps(row, database) {
			continue
		}
		result.Rows = append(result.Rows, []driver.Value{
			row.roleName, row.privilege, row.database, row.table,
			uint8OfBool[row.partialRevoke], uint8OfBool[row.grantOption],
		})
	}
	return result, nil
}

// grantsOrder is the ordering the reader asks for. A statement that asks for a
// different one is answered in the order the fixture holds, which is how a
// description that stopped being deterministic shows up as a failing comparison
// rather than as an ordering nobody asserted.
const grantsOrder = "ORDER BY grantee, table_name, privilege"

func orderedGrantRows(query string, rows []grantRow) []grantRow {
	if !strings.Contains(query, grantsOrder) {
		return rows
	}
	ordered := slices.Clone(rows)
	slices.SortStableFunc(ordered, func(a, b grantRow) int {
		return cmp.Or(
			cmp.Compare(a.roleName, b.roleName),
			cmp.Compare(a.table, b.table),
			cmp.Compare(a.privilege, b.privilege),
		)
	})
	return ordered
}

// answerRoles hands back the whole catalog, and refuses a statement that tried
// to narrow it.
//
// Narrowing in SQL is not wrong in itself; answering it here without evaluating
// it would be. A read that excluded the configuration-defined roles server-side
// would keep passing while the description silently lost the rows that keep a
// comparator from planning CREATE ROLE for a name the server already has.
func answerRoles(query string, rows []roleRow) (dbtest.QueryResult, error) {
	if strings.Contains(query, "WHERE") {
		return dbtest.QueryResult{}, fmt.Errorf(
			"this fake answers the whole of system.roles and cannot evaluate a WHERE clause; " +
				"a reader that narrows the read in SQL updates answerRoles")
	}
	ordered := slices.Clone(rows)
	if strings.Contains(query, "ORDER BY name") {
		slices.SortStableFunc(ordered, func(a, b roleRow) int { return cmp.Compare(a.name, b.name) })
	}
	result := dbtest.QueryResult{Columns: []string{"name", "storage"}}
	for _, row := range ordered {
		result.Rows = append(result.Rows, []driver.Value{row.name, row.storage})
	}
	return result, nil
}

// uint8OfBool renders a Go bool the way the catalog stores one: system.grants
// carries is_partial_revoke and grant_option as UInt8, which is what the reader
// scans them into.
var uint8OfBool = map[bool]uint8{false: 0, true: 1}

// answerClickHouse plays the whole server: the RBAC catalogs with their rows,
// and the object catalogs ReadSchema visits on its way there with nothing at
// all.
func answerClickHouse(query string, args []driver.NamedValue, server rbacServer) (dbtest.QueryResult, error) {
	if err := refuseColumnsAddedAfter2410(query); err != nil {
		return dbtest.QueryResult{}, err
	}
	if err := refuseSecretBearingSurface(query); err != nil {
		return dbtest.QueryResult{}, err
	}
	switch {
	case strings.Contains(query, "FROM system.roles"):
		return answerRoles(query, server.roles)
	case strings.Contains(query, "FROM system.grants"):
		return answerGrants(query, args, server.grants)
	// The reader describes row policies now that ClickHouse carries
	// capability.RowLevelSecurity. These tests are about roles and grants, so
	// the catalog answers empty rather than falling to the unexpected-statement
	// arm, which would report an RBAC failure for a policy statement
	// (stokaro/ptah#1736).
	case strings.Contains(query, "FROM system.row_policies"):
		return dbtest.QueryResult{Columns: []string{
			"short_name", "table", "select_filter", "apply_to_all", "apply_to_list", "apply_to_except",
		}}, nil
	case strings.Contains(query, "SELECT count()"):
		return dbtest.QueryResult{Columns: []string{"count()"}, Rows: [][]driver.Value{{uint64(0)}}}, nil
	case strings.Contains(query, "FROM system.tables"), strings.Contains(query, "FROM system.columns"):
		return dbtest.QueryResult{}, nil
	default:
		return dbtest.QueryResult{}, fmt.Errorf("unexpected statement: %s", query)
	}
}

// newRBACServer returns a reader connected to rbacDatabase on a server holding
// the given catalog, plus every statement it sends, in order.
func newRBACServer(c *qt.C, server rbacServer, caps capability.Capabilities) (*Reader, *[]string) {
	var sent []string
	db := dbtest.Open(c, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		sent = append(sent, query)
		return answerClickHouse(query, args, server)
	})
	return NewClickHouseReaderWithCapabilities(db.SQL, rbacDatabase, "26.7.3.19", caps), &sent
}

// withRoleManagement and withoutRoleManagement state the gate rather than
// inheriting whatever the ClickHouse preset currently says, so these tests keep
// meaning the same thing when the preset moves.
func withRoleManagement() capability.Capabilities {
	return capability.ClickHouse24().With(capability.RoleManagement, true)
}

func withoutRoleManagement() capability.Capabilities {
	return capability.ClickHouse24().With(capability.RoleManagement, false)
}

// mixedServer is one server holding a row for every reason a row is or is not
// described: two roles with grants in the connected database, a role whose only
// grant is elsewhere, a role with no grant at all, a configuration-defined role,
// a USER whose name collides with a role, a global grant, and a column-scoped
// one.
func mixedServer() rbacServer {
	return rbacServer{
		roles: []roleRow{
			{name: "analyst", storage: "local_directory"},
			{name: "configured", storage: "users_xml"},
			{name: "elsewhere", storage: "local_directory"},
			{name: "reader", storage: "local_directory"},
			{name: "ungranted", storage: "local_directory"},
		},
		grants: []grantRow{
			{roleName: "analyst", privilege: "SELECT", database: rbacDatabase, table: "events"},
			{roleName: "configured", privilege: "SELECT", database: rbacDatabase},
			{roleName: "elsewhere", privilege: "SELECT", database: "other"},
			{roleName: "reader", privilege: "SELECT", database: rbacDatabase},
			{roleName: "reader", privilege: "INSERT", database: rbacDatabase, table: "events"},
			{roleName: "someone", privilege: "SELECT", database: ""},
			{roleName: "analyst", privilege: "SELECT", database: rbacDatabase, table: "events", column: "id"},
			{userName: "reader", privilege: "DROP TABLE", database: rbacDatabase},
		},
	}
}

func TestReadGrantsDescribesTheConnectedDatabase(t *testing.T) {
	// One server per shape of row, so a mapping that loses a field loses its own
	// row rather than being covered by a neighbour.
	tests := []struct {
		name   string
		grants []grantRow
		want   []types.DBGrant
	}{
		{
			name: "a database-wide grant",
			grants: []grantRow{
				{roleName: "reader", privilege: "SELECT", database: rbacDatabase},
			},
			want: []types.DBGrant{
				{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: rbacDatabase},
			},
		},
		{
			name: "a table grant",
			grants: []grantRow{
				{roleName: "reader", privilege: "SELECT", database: rbacDatabase, table: "events"},
			},
			want: []types.DBGrant{
				{
					Role: "reader", Privilege: "SELECT", ObjectType: "TABLE",
					Schema: rbacDatabase, ObjectName: "events",
				},
			},
		},
		{
			name: "a grant carrying the grant option",
			grants: []grantRow{
				{roleName: "reader", privilege: "SELECT", database: rbacDatabase, table: "events", grantOption: true},
			},
			want: []types.DBGrant{
				{
					Role: "reader", Privilege: "SELECT", ObjectType: "TABLE",
					Schema: rbacDatabase, ObjectName: "events", WithOption: true,
				},
			},
		},
		{
			// `GRANT SELECT ON app.*` then `REVOKE SELECT ON app.events` leaves
			// two rows, the second subtracting from the first. Dropping it would
			// describe a role whose effective privileges are narrower than the
			// description says.
			name: "a partial revoke",
			grants: []grantRow{
				{roleName: "reader", privilege: "SELECT", database: rbacDatabase},
				{roleName: "reader", privilege: "SELECT", database: rbacDatabase, table: "events", partialRevoke: true},
			},
			want: []types.DBGrant{
				{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: rbacDatabase},
				{
					Role: "reader", Privilege: "SELECT", ObjectType: "TABLE",
					Schema: rbacDatabase, ObjectName: "events", IsPartialRevoke: true,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader, _ := newRBACServer(c, rbacServer{grants: test.grants}, withRoleManagement())

			grants, err := reader.readGrants(rbacDatabase)

			c.Assert(err, qt.IsNil)
			c.Assert(grants, qt.DeepEquals, test.want)
		})
	}
}

func TestReadGrantsLeavesOutEveryRowAPlanMustNeverRevoke(t *testing.T) {
	// A described grant is a revocable one, so each row here is a row that must
	// not reach the description. The simulated server applies a restriction only
	// when the statement carries it, so removing one from the query surfaces its
	// row instead of being answered away.
	tests := []struct {
		name string
		row  grantRow
	}{
		{
			// system.grants mixes users and roles. A user and a role may share a
			// name, and revoking from the wrong one takes access from a principal
			// Ptah does not manage.
			name: "a privilege held by a USER of the same name as a role",
			row:  grantRow{userName: "reader", privilege: "DROP TABLE", database: rbacDatabase},
		},
		{
			name: "a grant on another database",
			row:  grantRow{roleName: "reader", privilege: "SELECT", database: "other"},
		},
		{
			// A NULL database is the global `*.*` scope, which reaches objects no
			// declared schema describes.
			name: "a global grant",
			row:  grantRow{roleName: "reader", privilege: "SELECT"},
		},
		{
			// `GRANT SELECT(id) ON app.events` is a scope Ptah does not model;
			// reported without its column it would claim a privilege the role
			// does not hold.
			name: "a column-scoped grant",
			row: grantRow{
				roleName: "reader", privilege: "SELECT",
				database: rbacDatabase, table: "events", column: "id",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader, _ := newRBACServer(c, rbacServer{grants: []grantRow{test.row}}, withRoleManagement())

			grants, err := reader.readGrants(rbacDatabase)

			c.Assert(err, qt.IsNil)
			c.Assert(grants, qt.HasLen, 0)
		})
	}
}

func TestReadRolesPartitionsTheCatalog(t *testing.T) {
	// Roles the description defines, and roles it deliberately does not. The two
	// lists partition system.roles: a role in neither would read as absent to a
	// comparator, which then plans CREATE ROLE for a name the server has, and
	// granting to a role that does not exist is the other half of the same
	// failure (Code 511, UNKNOWN_ROLE).
	tests := []struct {
		name           string
		server         rbacServer
		wantDescribed  []string
		wantOutOfScope []string
	}{
		{
			name:           "a role holding a described grant",
			server:         mixedServer(),
			wantDescribed:  []string{"analyst", "reader"},
			wantOutOfScope: []string{"configured", "elsewhere", "ungranted"},
		},
		{
			name: "a server whose roles are all defined in configuration",
			server: rbacServer{
				roles: []roleRow{
					{name: "configured", storage: "users_xml"},
					{name: "also_configured", storage: "users_xml"},
				},
				grants: []grantRow{
					{roleName: "configured", privilege: "SELECT", database: rbacDatabase},
				},
			},
			wantDescribed:  nil,
			wantOutOfScope: []string{"also_configured", "configured"},
		},
		{
			name: "a server with no roles at all",
			server: rbacServer{
				grants: []grantRow{
					{roleName: "reader", privilege: "SELECT", database: rbacDatabase},
				},
			},
			wantDescribed:  nil,
			wantOutOfScope: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader, _ := newRBACServer(c, test.server, withRoleManagement())

			grants, err := reader.readGrants(rbacDatabase)
			c.Assert(err, qt.IsNil)
			described, outOfScope, err := reader.readRoles(grants)

			c.Assert(err, qt.IsNil)
			c.Assert(roleNames(described), qt.DeepEquals, test.wantDescribed)
			c.Assert(roleNames(outOfScope), qt.DeepEquals, test.wantOutOfScope)
			c.Assert(len(described)+len(outOfScope), qt.Equals, len(test.server.roles),
				qt.Commentf("the two lists must partition system.roles"))
		})
	}
}

func TestReadRolesReportsTheOnlyAttributeClickHouseHas(t *testing.T) {
	c := qt.New(t)

	// A ClickHouse role carries no attributes: system.roles is (name, id,
	// storage), and `CREATE ROLE ... COMMENT 'x'` is a syntax error. Inherit is
	// reported true because it is true -- membership always inherits, there is no
	// NOINHERIT to read, and the annotation parser defaults a declared role to
	// inherit=true, so a live false would make every role differ from its own
	// declaration.
	server := rbacServer{
		roles:  []roleRow{{name: "reader", storage: "local_directory"}},
		grants: []grantRow{{roleName: "reader", privilege: "SELECT", database: rbacDatabase}},
	}
	reader, _ := newRBACServer(c, server, withRoleManagement())

	grants, err := reader.readGrants(rbacDatabase)
	c.Assert(err, qt.IsNil)
	described, _, err := reader.readRoles(grants)

	c.Assert(err, qt.IsNil)
	c.Assert(described, qt.DeepEquals, []types.DBRole{{Name: "reader", Inherit: true}})
}

func TestReadSchemaFillsTheDescriptionUnderTheCapability(t *testing.T) {
	c := qt.New(t)

	reader, _ := newRBACServer(c, mixedServer(), withRoleManagement())

	schema, err := reader.ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(roleNames(schema.Roles), qt.DeepEquals, []string{"analyst", "reader"})
	c.Assert(roleNames(schema.RolesOutOfScope), qt.DeepEquals, []string{"configured", "elsewhere", "ungranted"})
	c.Assert(schema.Grants, qt.DeepEquals, []types.DBGrant{
		{Role: "analyst", Privilege: "SELECT", ObjectType: "TABLE", Schema: rbacDatabase, ObjectName: "events"},
		{Role: "configured", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: rbacDatabase},
		{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: rbacDatabase},
		{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", Schema: rbacDatabase, ObjectName: "events"},
	})
}

// accessDenied is the exception a server raises when the connected account may
// not read a system table, built as the driver builds it so that the reader's
// own predicate is what decides rather than a string this file invented.
//
// Measured on 26.7.3.19 with an account holding only SELECT, SHOW TABLES and
// SHOW COLUMNS on one database: both RBAC catalogs answer Code 497
// ACCESS_DENIED while system.tables answers normally.
func accessDenied(catalog string) error {
	return &clickhousedriver.Exception{
		Code: 497,
		Name: "ACCESS_DENIED",
		Message: "lowpriv: Not enough privileges. To execute this query, it's necessary to have " +
			"the grant SELECT for at least one column on " + catalog + ".",
	}
}

// newRBACServerFailing is [newRBACServer] on a server that answers one catalog
// with the given error and everything else normally.
func newRBACServerFailing(
	c *qt.C, server rbacServer, caps capability.Capabilities, catalog string, failure error,
) (*Reader, *[]string) {
	var sent []string
	db := dbtest.Open(c, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		sent = append(sent, query)
		if strings.Contains(query, catalog) {
			return dbtest.QueryResult{}, failure
		}
		return answerClickHouse(query, args, server)
	})
	return NewClickHouseReaderWithCapabilities(db.SQL, rbacDatabase, "26.7.3.19", caps), &sent
}

// unknownIdentifier is a catalog error that is NOT a privilege refusal: the
// shape a system table changing under Ptah would produce.
func unknownIdentifier() error {
	return &clickhousedriver.Exception{
		Code: 47, Name: "UNKNOWN_IDENTIFIER", Message: "Missing columns: 'is_partial_revoke'",
	}
}

// TestReadSchemaDegradesWhenTheAccountMayNotReadTheAccessCatalog pins the one
// failure this reader must not turn into a failed command.
//
// The capability preset is a statement about the SERVER; it cannot know what
// the connected account was granted. An account that may read tables and not
// system.grants is ordinary, and before this branch such an account read a
// ClickHouse schema fine — including a schema declaring no role at all, which
// has nothing to do with RBAC. Failing the whole read for it would be a
// regression caused by adding a capability.
//
// The description must also SAY it did not look, which is the half that keeps
// the degradation from being a silent one: a reader returning the tables and an
// empty role list would tell the comparator this server has no roles, and the
// comparator would plan CREATE ROLE for every declared one.
func TestReadSchemaDegradesWhenTheAccountMayNotReadTheAccessCatalog(t *testing.T) {
	tests := []struct {
		name    string
		catalog string
	}{
		{name: "system.grants is refused", catalog: "system.grants"},
		{name: "system.roles is refused", catalog: "system.roles"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reader, _ := newRBACServerFailing(
				c, mixedServer(), withRoleManagement(), test.catalog, accessDenied(test.catalog),
			)

			schema, err := reader.ReadSchema()

			c.Assert(err, qt.IsNil)
			c.Assert(schema.Roles, qt.HasLen, 0)
			c.Assert(schema.RolesOutOfScope, qt.HasLen, 0)
			c.Assert(schema.Grants, qt.HasLen, 0)
			c.Assert(schema.NotDescribed.Describes(coverage.Role, "reader"), qt.IsFalse,
				qt.Commentf("a read that could not look must not claim the role is absent"))
		})
	}
}

// TestReadSchemaFailsOnEveryOtherRBACError is the control on the degradation
// above. Without it a reader that swallowed every RBAC failure would satisfy
// that test completely, and a catalog that changed shape under Ptah would read
// as a server with no roles.
func TestReadSchemaFailsOnEveryOtherRBACError(t *testing.T) {
	c := qt.New(t)

	reader, _ := newRBACServerFailing(
		c, mixedServer(), withRoleManagement(), "system.grants", unknownIdentifier(),
	)

	_, err := reader.ReadSchema()

	c.Assert(err, qt.ErrorMatches, `(?s).*Missing columns.*`)
}

// TestReadSchemaDescribesRolesWhenTheAccountMayReadThem is the other half of
// the same control: the coverage record must appear only when the read was
// actually refused. A reader that recorded it unconditionally would pass the
// degradation test and would then withhold every role addition on a server that
// answered perfectly.
func TestReadSchemaDescribesRolesWhenTheAccountMayReadThem(t *testing.T) {
	c := qt.New(t)

	reader, _ := newRBACServer(c, mixedServer(), withRoleManagement())

	schema, err := reader.ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(schema.NotDescribed.IsZero(), qt.IsTrue)
	c.Assert(schema.NotDescribed.Describes(coverage.Role, "reader"), qt.IsTrue)
}

func TestReadSchemaAsksNothingAboutRBACWithoutTheCapability(t *testing.T) {
	c := qt.New(t)

	// A caller without the capability reads exactly what this reader read before
	// RBAC existed. Asserting the empty fields alone would pass on a reader that
	// queried both catalogs and threw the answer away, which is the read an
	// operator without role privileges cannot afford, so the statements are
	// asserted too.
	reader, sent := newRBACServer(c, mixedServer(), withoutRoleManagement())

	schema, err := reader.ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(schema.Roles, qt.HasLen, 0)
	c.Assert(schema.RolesOutOfScope, qt.HasLen, 0)
	c.Assert(schema.Grants, qt.HasLen, 0)
	c.Assert(*sent, qt.Not(qt.HasLen), 0, qt.Commentf("the object reads must still have run"))
	for _, statement := range *sent {
		c.Assert(statement, qt.Not(qt.Contains), "system.roles")
		c.Assert(statement, qt.Not(qt.Contains), "system.grants")
	}
}

func TestGrantsStatementCarriesEveryRestrictionTheDescriptionDependsOn(t *testing.T) {
	// The behavioral tests above prove the restrictions have an effect on a
	// server that evaluates them. These prove the text the reader sends is the
	// text they were written against, so a restriction rewritten into a spelling
	// this file's fake does not evaluate turns red here instead of silently
	// making those tests vacuous.
	tests := []struct {
		name   string
		clause string
	}{
		{name: "the row belongs to no user", clause: excludesUserRows},
		{name: "the row belongs to a role", clause: requiresRoleRows},
		{name: "the row is in the connected database", clause: scopesToOneDatabase},
		{name: "the row is not column-scoped", clause: excludesColumnScope},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader, sent := newRBACServer(c, mixedServer(), withRoleManagement())

			_, err := reader.readGrants(rbacDatabase)

			c.Assert(err, qt.IsNil)
			c.Assert(*sent, qt.HasLen, 1)
			c.Assert((*sent)[0], qt.Contains, test.clause)
		})
	}
}

func TestGrantsProjectionNamesOnlyColumnsTheOldestDeclaredLineHas(t *testing.T) {
	// 24.10.4.191 answers eight columns and 26.7.3.19 answers ten. Naming one of
	// the two newer ones costs nothing on the server this was developed against
	// and breaks the read outright on the older line, which this repository
	// declares and starts in CI.
	tests := []struct {
		name   string
		absent string
	}{
		{name: "access_object", absent: "access_object"},
		{name: "is_wildcard", absent: "is_wildcard"},
		{name: "the whole row", absent: "SELECT *"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader, sent := newRBACServer(c, mixedServer(), withRoleManagement())

			_, err := reader.readGrants(rbacDatabase)

			c.Assert(err, qt.IsNil)
			c.Assert(*sent, qt.HasLen, 1)
			c.Assert((*sent)[0], qt.Not(qt.Contains), test.absent)
		})
	}
}

func TestRBACReadsTouchNoCredentialBearingSurface(t *testing.T) {
	// A ClickHouse role has no credentials; a USER does. The reader never asks
	// for one, so no plan, log or diff downstream has to be careful with the
	// answer. Asserted over a whole ReadSchema rather than over the two RBAC
	// statements, because the promise is about the reader and not about one
	// method.
	tests := []struct {
		name    string
		surface string
	}{
		{name: "the users catalog", surface: "system.users"},
		{name: "the authentication parameters", surface: "auth_params"},
		{name: "SHOW CREATE, which renders IDENTIFIED WITH", surface: "SHOW CREATE"},
		{name: "SHOW GRANTS, whose user form names a user", surface: "SHOW GRANTS"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader, sent := newRBACServer(c, mixedServer(), withRoleManagement())

			_, err := reader.ReadSchema()

			c.Assert(err, qt.IsNil)
			c.Assert(*sent, qt.Not(qt.HasLen), 0)
			for _, statement := range *sent {
				c.Assert(strings.ToUpper(statement), qt.Not(qt.Contains), strings.ToUpper(test.surface))
			}
		})
	}
}

func TestTheSimulatedServerRefusesWhatItMustRefuse(t *testing.T) {
	// The control for the two tests above. Both assert an absence, and an absence
	// is also what a guard that recognizes nothing reports, so the guards are
	// driven with the statements they exist to catch. Without this, deleting the
	// bodies of refuseColumnsAddedAfter2410 and refuseSecretBearingSurface would
	// leave this file green.
	tests := []struct {
		name    string
		query   string
		wantErr string
	}{
		{
			name:    "a column 24.10 does not have",
			query:   "SELECT access_object FROM system.grants",
			wantErr: `simulated 24.10.4.191: system.grants has no column "access_object" \(UNKNOWN_IDENTIFIER\)`,
		},
		{
			name:    "the wildcard flag 24.10 does not have",
			query:   "SELECT role_name, is_wildcard FROM system.grants",
			wantErr: `simulated 24.10.4.191: system.grants has no column "is_wildcard" \(UNKNOWN_IDENTIFIER\)`,
		},
		{
			name:    "the whole row",
			query:   "SELECT * FROM system.grants",
			wantErr: "simulated 24.10.4.191: SELECT \\* over system.grants answers 8 columns here and 10 on 26.7",
		},
		{
			name:    "the users catalog",
			query:   "SELECT name, auth_params FROM system.users",
			wantErr: "the reader must never name SYSTEM.USERS",
		},
		{
			name:    "a rendered credential",
			query:   "SHOW CREATE USER default",
			wantErr: "the reader must never name SHOW CREATE",
		},
		{
			name:    "the grant listing",
			query:   "SHOW GRANTS FOR reader",
			wantErr: "the reader must never name SHOW GRANTS",
		},
		{
			// A restriction the fake would otherwise ignore is a restriction the
			// tests above would answer as though it were absent. This one drops
			// the partial-revoke rows.
			name:    "a restriction this fake cannot evaluate",
			query:   "SELECT grantee FROM system.grants WHERE user_name IS NULL AND is_partial_revoke = 0",
			wantErr: `this fake cannot evaluate the restriction "is_partial_revoke = 0"; .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := answerClickHouse(test.query, nil, mixedServer())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestOnServerNamesTheVersionOnlyWhenThereIsOne(t *testing.T) {
	// The version is carried for the diagnostic and for nothing else: the
	// projection names the columns the oldest declared line has, so no read
	// branches on it. What it buys is that a catalog failure says which server
	// refused, since a failure here is far more likely to be about the version
	// than about the schema.
	tests := []struct {
		name    string
		version string
		want    string
	}{
		{name: "a server that reported a version", version: "24.10.4.191", want: ` on server "24.10.4.191"`},
		{name: "a reader built without one", version: "", want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := NewClickHouseReaderWithCapabilities(nil, rbacDatabase, test.version, withRoleManagement())

			c.Assert(reader.onServer(), qt.Equals, test.want)
		})
	}
}

func roleNames(roles []types.DBRole) []string {
	if len(roles) == 0 {
		return nil
	}
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	return names
}
