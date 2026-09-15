//go:build integration

package generator_test

import (
	"cmp"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// TestReverseCatalogObjects_DownRoundTrip_Integration executes the generated
// down bodies for the four catalog families a server alone can judge: RLS
// policies, sequences, roles and standalone functions.
//
// A reverse-generator unit test asserts a substring of the down text, and a
// substring is not an execution. The order these drops arrive in is the
// server's rule rather than the renderer's: PostgreSQL records a dependency
// from a policy to the function its predicate calls and a shared dependency
// from that policy to the role it names, so a rollback that reaches
// DROP FUNCTION or DROP ROLE while the policy stands is refused with "cannot
// drop ... because other objects depend on it". Reading the rendered text
// reports none of that.
//
// Each case seeds the pre-up state through Ptah itself, applies the generated
// up, applies the generated down, and asserts the catalog it left behind is the
// pre-up catalog. The table the policy hangs off outlives every case and no
// down carries a DROP TABLE, so a cascade can never be what removed the
// objects.
func TestReverseCatalogObjects_DownRoundTrip_Integration(t *testing.T) {
	cases := []struct {
		name   string
		prior  func() *schemamodel.Database
		target func() *schemamodel.Database
		// downHas and downLacks pin the shape of the rollback. The catalog
		// compare says the database arrived; these say it arrived by undoing
		// the up rather than by taking the table and its dependents with it.
		downHas   []string
		downLacks []string
		// downOrder holds pairs that must appear in the rollback in the order
		// written, each pair one dependency the server enforces. Executing the
		// script proves the order held; these say which order, so a CASCADE
		// bolted onto a drop cannot make the script pass by taking the
		// dependent with it.
		downOrder [][]string
		// roleAfterDown is whether the cluster still holds the role once the
		// rollback has run. The catalog compare cannot answer it: PostgreSQL
		// scopes a role out of a description as soon as nothing in the read
		// schema refers to it, so a DROP ROLE that quietly did nothing and one
		// that removed the role read the same there.
		roleAfterDown bool
	}{
		{
			name:   "up creates the role, function, sequence and policy",
			prior:  func() *schemamodel.Database { return revCatSchemaFor(revCatOptions{}) },
			target: func() *schemamodel.Database { return revCatSchemaFor(revCatOptions{objects: true}) },
			downHas: []string{
				"DROP POLICY IF EXISTS " + revCatPolicy + " ON " + revCatTable,
				"DROP SEQUENCE IF EXISTS " + revCatSequence,
				"DROP FUNCTION IF EXISTS " + revCatFunction,
				"DROP ROLE IF EXISTS " + revCatRole,
			},
			downLacks: []string{"DROP TABLE", "CASCADE"},
			downOrder: [][]string{
				{"DROP POLICY IF EXISTS " + revCatPolicy, "DROP FUNCTION IF EXISTS " + revCatFunction},
				{"DROP POLICY IF EXISTS " + revCatPolicy, "DROP ROLE IF EXISTS " + revCatRole},
			},
			roleAfterDown: false,
		},
		{
			// The other direction of the same family: a removal reversed into
			// an addition has to carry the definition the pre-change database
			// held, because a name alone renders a CREATE SEQUENCE with no
			// bounds and lands on a sequence that is not the one that was
			// dropped.
			name:   "up drops the sequence",
			prior:  func() *schemamodel.Database { return revCatSchemaFor(revCatOptions{objects: true}) },
			target: func() *schemamodel.Database { return revCatSchemaFor(revCatOptions{objects: true, noSequence: true}) },
			downHas: []string{
				"CREATE SEQUENCE " + revCatSequence,
				"INCREMENT BY 2",
			},
			downLacks:     []string{"DROP TABLE"},
			roleAfterDown: true,
		},
		{
			name:  "up rewrites all four definitions",
			prior: func() *schemamodel.Database { return revCatSchemaFor(revCatOptions{objects: true}) },
			target: func() *schemamodel.Database {
				return revCatSchemaFor(revCatOptions{
					objects:           true,
					functionBody:      "SELECT current_setting('app.revcat_other', true)",
					policyUsing:       "tenant_id <> " + revCatFunction + "()",
					sequenceIncrement: 5,
					roleLogin:         true,
				})
			},
			downHas: []string{
				"FUNCTION " + revCatFunction,
				"ALTER SEQUENCE " + revCatSequence + " INCREMENT BY 2",
				"ALTER ROLE " + revCatRole + " NOLOGIN",
				"CREATE POLICY " + revCatPolicy + " ON " + revCatTable,
			},
			downLacks:     []string{"DROP TABLE"},
			roleAfterDown: true,
		},
	}

	address := dbtarget.URL(t, dbtarget.PostgreSQL)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			// The dedicated schema is what keeps this test off everything else
			// on a shared server. The generator plans against the whole
			// connection scope, so seeding a pre-up state over the default
			// search_path would answer every other object in the database with
			// a DROP.
			admin := revCatConnect(c, address)
			revCatResetSchema(c, admin)
			t.Cleanup(func() { revCatDropAll(admin) })

			conn := revCatConnect(c, revCatScopedURL(c, address))

			// 1. Install the pre-up state with Ptah's own planner, so the
			//    bodies in the catalog are the ones Ptah writes rather than a
			//    hand-typed approximation of them.
			seedSQL, _ := generateLiveMigrationSQL(c, conn, tc.prior())
			execScript(c, conn, seedSQL, "SEED")

			dbPrior, err := conn.Reader().ReadSchemaContext(t.Context())
			c.Assert(err, qt.IsNil)
			priorCatalog := revCatCatalog(dbPrior)
			c.Assert(revCatMissingObjects(tc.prior(), dbPrior), qt.HasLen, 0,
				qt.Commentf("seed SQL:\n%s", seedSQL))

			// 2. The up migration under test.
			target := tc.target()
			upSQL, downSQL := generateLiveMigrationSQL(c, conn, target)
			execScript(c, conn, upSQL, "UP")

			dbAfterUp, err := conn.Reader().ReadSchemaContext(t.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(revCatCatalog(dbAfterUp), qt.Not(qt.DeepEquals), priorCatalog,
				qt.Commentf("the up must change the catalog, or the down proves nothing:\n%s", upSQL))
			c.Assert(revCatMissingObjects(target, dbAfterUp), qt.HasLen, 0,
				qt.Commentf("up SQL:\n%s", upSQL))

			// 3. THE GATE. The down is the generator's own, and applying it
			//    must not error. A DROP FUNCTION the policy still depends on, a
			//    DROP ROLE the policy still names, or a CREATE POLICY ahead of
			//    either fails right here.
			unquotedDown := legacyRenderedSQL(downSQL)
			for _, fragment := range tc.downHas {
				c.Assert(unquotedDown, qt.Contains, fragment, qt.Commentf("down SQL:\n%s", downSQL))
			}
			for _, fragment := range tc.downLacks {
				c.Assert(unquotedDown, qt.Not(qt.Contains), fragment, qt.Commentf("down SQL:\n%s", downSQL))
			}
			for _, pair := range tc.downOrder {
				c.Assert(slices.IsSorted(revCatPositions(c, unquotedDown, pair)), qt.IsTrue,
					qt.Commentf("%v must appear in this order in the down SQL:\n%s", pair, downSQL))
			}
			execScript(c, conn, downSQL, "DOWN")

			// 4. The rollback has to land on the pre-up catalog, not merely
			//    apply. A DROP POLICY that ran and a DROP ROLE that quietly did
			//    nothing both exit 0.
			dbAfterDown, err := conn.Reader().ReadSchemaContext(t.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(revCatCatalog(dbAfterDown), qt.DeepEquals, priorCatalog,
				qt.Commentf("down SQL:\n%s", downSQL))

			// 5. The role, asked of the cluster rather than of the
			//    description, because a DROP ROLE is invisible to the compare
			//    above once the policy naming the role is gone.
			c.Assert(revCatRoleExists(c, admin), qt.Equals, tc.roleAfterDown,
				qt.Commentf("down SQL:\n%s", downSQL))

			// 6. And the table the policy hangs off must have survived.
			c.Assert(revCatHasTable(dbAfterDown), qt.IsTrue,
				qt.Commentf("the rollback must not have taken the pre-existing table with it"))
		})
	}
}

const (
	// revCatSchema is this test's own schema. A role belongs to the cluster and
	// cannot be scoped that way, which is why the role name is as distinctive
	// as it is.
	revCatSchema   = "ptah_revcat"
	revCatTable    = "ptah_revcat_orders"
	revCatFunction = "ptah_revcat_current_tenant"
	revCatSequence = "ptah_revcat_order_seq"
	revCatRole     = "ptah_revcat_app"
	revCatPolicy   = "ptah_revcat_tenant_isolation"

	revCatFunctionBody = "SELECT current_setting('app.revcat_tenant', true)"
	revCatPolicyUsing  = "tenant_id = " + revCatFunction + "()"
	// revCatIncrement is stated rather than defaulted so a rollback that
	// recreates the sequence from its name alone is visibly the wrong
	// sequence: PostgreSQL's own default is 1.
	revCatIncrement = 2
)

type revCatOptions struct {
	// objects declares the four families this test governs. The table is
	// always declared: it is what the policy hangs off, and what proves no
	// rollback reached these objects through a cascade.
	objects bool
	// noSequence withdraws the sequence alone, so an up can remove one object
	// while the policy keeps the role and the function in scope.
	noSequence bool
	// The rest move a definition without changing which objects exist.
	functionBody      string
	policyUsing       string
	sequenceIncrement int64
	roleLogin         bool
}

func revCatSchemaFor(opts revCatOptions) *schemamodel.Database {
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "RevCatOrder", Name: revCatTable}},
		Fields: []schemamodel.Field{
			{StructName: "RevCatOrder", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "RevCatOrder", Name: "tenant_id", Type: "TEXT", Nullable: true},
		},
	}
	if opts.objects {
		increment := cmp.Or(opts.sequenceIncrement, int64(revCatIncrement))
		schema.Roles = []schemamodel.Role{{
			StructName: "RevCatRoles",
			Name:       revCatRole,
			Inherit:    true,
			Login:      opts.roleLogin,
		}}
		// The policy's predicate calls this function, which is what makes the
		// order of the two drops the server's decision rather than the
		// renderer's.
		schema.Functions = []schemamodel.Function{{
			StructName: "RevCatTenant",
			Name:       revCatFunction,
			Returns:    "TEXT",
			Language:   "sql",
			Volatility: "STABLE",
			Body:       cmp.Or(opts.functionBody, revCatFunctionBody),
		}}
		if !opts.noSequence {
			schema.Sequences = []schemamodel.Sequence{{
				StructName: "RevCatOrderSeq",
				Name:       revCatSequence,
				AsType:     "bigint",
				Increment:  &increment,
			}}
		}
		schema.RLSEnabledTables = []schemamodel.RLSEnabledTable{{
			StructName: "RevCatOrder",
			Table:      revCatTable,
		}}
		schema.RLSPolicies = []schemamodel.RLSPolicy{{
			StructName:      "RevCatOrder",
			Name:            revCatPolicy,
			Table:           revCatTable,
			PolicyFor:       "ALL",
			ToRoles:         revCatRole,
			UsingExpression: cmp.Or(opts.policyUsing, revCatPolicyUsing),
		}}
	}
	schemamodel.Finalize(schema)
	return schema
}

// revCatScopedURL is the same server reached with this test's own schema as the
// default one. It derives the address from the engine's rather than reading a
// second variable, so there is still one declaration of where PostgreSQL is.
func revCatScopedURL(c *qt.C, address string) string {
	c.Helper()
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", revCatSchema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func revCatConnect(c *qt.C, address string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return conn
}

// revCatResetSchema clears whatever a previous run left and asserts the empty
// schema exists, because a CREATE TABLE with no schema to create in is the
// failure this would otherwise produce three steps later.
func revCatResetSchema(c *qt.C, admin *dbschema.DatabaseConnection) {
	c.Helper()
	revCatDropAll(admin)
	_, err := admin.Exec("CREATE SCHEMA " + revCatSchema)
	c.Assert(err, qt.IsNil)
}

// revCatDropAll removes this test's objects and nothing else. The schema goes
// first: the policy inside it holds the dependency that would otherwise refuse
// the role.
func revCatDropAll(admin *dbschema.DatabaseConnection) {
	statements := []string{
		"DROP SCHEMA IF EXISTS " + revCatSchema + " CASCADE",
		"DROP ROLE IF EXISTS " + revCatRole,
	}
	for _, statement := range statements {
		_, _ = admin.Exec(statement)
	}
}

// revCatCatalog reduces the introspected schema to the families this test
// governs, so a mismatch names the object rather than dumping a whole schema.
func revCatCatalog(db *catalog.Database) []string {
	var lines []string
	for _, table := range db.Tables {
		lines = append(lines, fmt.Sprintf("table %s rls=%t forced=%t", table.Name, table.RLSEnabled, table.RLSForced))
	}
	for _, function := range db.Functions {
		lines = append(lines, fmt.Sprintf("function %s(%s) returns %s language %s %s = %s",
			function.Name, function.Parameters, function.Returns, function.Language,
			function.Volatility, revCatNormalize(function.Body)))
	}
	for _, sequence := range db.Sequences {
		lines = append(lines, fmt.Sprintf("sequence %s as %s start %s increment %s min %s max %s cache %s cycle=%t",
			sequence.Name, sequence.DataType,
			revCatNumber(sequence.Start), revCatNumber(sequence.Increment),
			revCatNumber(sequence.MinValue), revCatNumber(sequence.MaxValue),
			revCatNumber(sequence.Cache), sequence.Cycle))
	}
	for _, role := range db.Roles {
		lines = append(lines, fmt.Sprintf("role %s login=%t superuser=%t createdb=%t createrole=%t inherit=%t replication=%t",
			role.Name, role.Login, role.Superuser, role.CreateDB, role.CreateRole, role.Inherit, role.Replication))
	}
	for _, policy := range db.RLSPolicies {
		lines = append(lines, fmt.Sprintf("policy %s on %s for %s to %s using %s check %s restrictive=%t",
			policy.Name, policy.Table, policy.PolicyFor, policy.ToRoles,
			revCatNormalize(policy.UsingExpression), revCatNormalize(policy.WithCheckExpression),
			policy.Restrictive))
	}
	sort.Strings(lines)
	return lines
}

// revCatMissingObjects names what a schema declares that the catalog does not
// hold. An empty result is what "the migration arrived" means; a non-empty one
// is a plan that applied and still left work undone.
func revCatMissingObjects(schema *schemamodel.Database, db *catalog.Database) []string {
	var missing []string
	for _, function := range schema.Functions {
		if !slices.ContainsFunc(db.Functions, func(candidate catalog.Function) bool {
			return candidate.Name == function.Name
		}) {
			missing = append(missing, "function "+function.Name)
		}
	}
	for _, sequence := range schema.Sequences {
		if !slices.ContainsFunc(db.Sequences, func(candidate catalog.Sequence) bool {
			return candidate.Name == sequence.Name
		}) {
			missing = append(missing, "sequence "+sequence.Name)
		}
	}
	for _, role := range schema.Roles {
		if !slices.ContainsFunc(db.Roles, func(candidate catalog.Role) bool {
			return candidate.Name == role.Name
		}) {
			missing = append(missing, "role "+role.Name)
		}
	}
	for _, policy := range schema.RLSPolicies {
		if !slices.ContainsFunc(db.RLSPolicies, func(candidate catalog.RLSPolicy) bool {
			return candidate.Name == policy.Name
		}) {
			missing = append(missing, "policy "+policy.Name)
		}
	}
	return missing
}

// revCatRoleExists asks the cluster whether the role is there. A role belongs
// to the cluster rather than to one database, and a description leaves out the
// roles nothing in its scope refers to, so this is the only place the answer
// exists.
func revCatRoleExists(c *qt.C, admin *dbschema.DatabaseConnection) bool {
	c.Helper()
	var exists bool
	err := admin.QueryRowContext(c.Context(),
		"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", revCatRole).Scan(&exists)
	c.Assert(err, qt.IsNil)
	return exists
}

func revCatHasTable(db *catalog.Database) bool {
	return slices.ContainsFunc(db.Tables, func(table catalog.Table) bool {
		return table.Name == revCatTable
	})
}

// revCatPositions is where each fragment sits in the script, asserting on the
// way that every one of them is there: strings.Index answers -1 for an absent
// fragment, and a -1 leading the list would read as correct ordering.
func revCatPositions(c *qt.C, script string, fragments []string) []int {
	c.Helper()
	positions := make([]int, 0, len(fragments))
	for _, fragment := range fragments {
		at := strings.Index(script, fragment)
		c.Assert(at >= 0, qt.IsTrue, qt.Commentf("%q is not in the down SQL:\n%s", fragment, script))
		positions = append(positions, at)
	}
	return positions
}

func revCatNormalize(body string) string {
	return strings.Join(strings.Fields(body), " ")
}

// revCatNumber prints a sequence bound, including the absence of one: a reader
// that stopped reporting MINVALUE and one that reports it unchanged must not
// compare equal.
func revCatNumber(value *int64) string {
	if value == nil {
		return "unset"
	}
	return fmt.Sprintf("%d", *value)
}
