//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/core/sqlutil"
)

// renderPlanCatalogCase is one PostgreSQL-family target this measurement can
// reach: the dialect `schema render` is asked for, and the environment variable
// holding a URL for a live server of that engine.
//
// Spanner is absent because it has no live server here. Issue stokaro/ptah#942
// records that it has no live coverage at all, and the pinned community binary
// has no Spanner driver either. It is measured offline instead, by
// TestRenderAndPlanAgreeOnEveryPostgresFamilyTarget in
// ./internal/convert/fromschema.
type renderPlanCatalogCase struct {
	name    string
	dialect string
	urlEnv  string
}

// catalogProbe asks the live catalog whether one declared object exists. The
// query is a count so a missing object is a row rather than a scan failure, and
// so an engine that refuses the query at all reports that refusal identically
// for both surfaces.
type catalogProbe struct {
	kind  string
	query func(suffix string) string
}

// renderPlanCatalogProbes covers every object kind the fixture declares.
var renderPlanCatalogProbes = []catalogProbe{
	{kind: "extension", query: func(string) string {
		return `SELECT count(*) FROM pg_extension WHERE extname = 'pgcrypto'`
	}},
	{kind: "sequence", query: func(s string) string {
		return `SELECT count(*) FROM pg_class WHERE relkind = 'S' AND relname = 'p929_seq_` + s + `'`
	}},
	{kind: "domain", query: func(s string) string {
		return `SELECT count(*) FROM pg_type WHERE typtype = 'd' AND typname = 'p929_dom_` + s + `'`
	}},
	{kind: "composite", query: func(s string) string {
		return `SELECT count(*) FROM pg_type WHERE typtype = 'c' AND typname = 'p929_comp_` + s + `'`
	}},
	{kind: "range", query: func(s string) string {
		return `SELECT count(*) FROM pg_type WHERE typtype = 'r' AND typname = 'p929_rng_` + s + `'`
	}},
	{kind: "role", query: func(s string) string {
		return `SELECT count(*) FROM pg_roles WHERE rolname = 'p929_role_` + s + `'`
	}},
	{kind: "table", query: func(s string) string {
		return `SELECT count(*) FROM pg_class WHERE relkind = 'r' AND relname = 'p929_tbl_` + s + `'`
	}},
	{kind: "view", query: func(s string) string {
		return `SELECT count(*) FROM pg_class WHERE relkind = 'v' AND relname = 'p929_view_` + s + `'`
	}},
	{kind: "matview", query: func(s string) string {
		return `SELECT count(*) FROM pg_class WHERE relkind = 'm' AND relname = 'p929_mview_` + s + `'`
	}},
	{kind: "function", query: func(s string) string {
		return `SELECT count(*) FROM pg_proc WHERE proname = 'p929_func_` + s + `'`
	}},
	{kind: "trigger", query: func(s string) string {
		return `SELECT count(*) FROM pg_trigger WHERE tgname = 'p929_trg_` + s + `'`
	}},
	{kind: "rls", query: func(s string) string {
		return `SELECT count(*) FROM pg_class WHERE relname = 'p929_tbl_` + s + `' AND relrowsecurity`
	}},
	{kind: "policy", query: func(s string) string {
		return `SELECT count(*) FROM pg_policy WHERE polname = 'p929_pol_` + s + `'`
	}},
	{kind: "grant", query: func(s string) string {
		return `SELECT count(*) FROM information_schema.role_table_grants` +
			` WHERE grantee = 'p929_role_` + s + `' AND table_name = 'p929_tbl_` + s + `'` +
			` AND privilege_type = 'SELECT'`
	}},
}

// renderPlanFixtureTemplate declares one object of every PostgreSQL-family kind
// the issue named. SFX is replaced with a per-run suffix so two runs against one
// shared server cannot read each other's objects — roles are cluster-global, not
// per-database, so a fixed role name would make a second run find the first
// run's role and call that agreement.
const renderPlanFixtureTemplate = `package models

//ptah:schema:extension name="pgcrypto" if_not_exists="true"
//ptah:schema:sequence name="p929_seq_SFX" as="bigint" start="1000" increment="1"
//ptah:schema:domain name="p929_dom_SFX" type="TEXT"
//ptah:schema:composite name="p929_comp_SFX" fields="street:TEXT,city:TEXT"
//ptah:schema:range name="p929_rng_SFX" subtype="float8"
//ptah:schema:role name="p929_role_SFX" login="true" inherit="true"
type _objects struct{}

//ptah:schema:table name="p929_tbl_SFX"
type Probe struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="touched" type="TIMESTAMP"
	Touched string
}

//ptah:schema:function name="p929_func_SFX" returns="integer" language="sql" body="SELECT 1;"
type _function struct{}

//ptah:schema:view name="p929_view_SFX" body="SELECT id FROM p929_tbl_SFX"
type _view struct{}

//ptah:schema:matview name="p929_mview_SFX" body="SELECT id FROM p929_tbl_SFX"
type _matview struct{}

//ptah:schema:trigger name="p929_trg_SFX" table="p929_tbl_SFX" timing="AFTER" event="INSERT" for="ROW" body="SELECT 1;"
type _trigger struct{}

//ptah:schema:rls:enable table="p929_tbl_SFX"
//ptah:schema:rls:policy name="p929_pol_SFX" table="p929_tbl_SFX" for="SELECT" to="p929_role_SFX" using="true"
type _rls struct{}

//ptah:schema:grant role="p929_role_SFX" privilege="SELECT" on_table="p929_tbl_SFX"
type _grant struct{}
`

// TestSchemaRenderAndPlanCatalogAgreementE2E is the live half of
// stokaro/ptah#929 item 4: for one desired schema on one empty PostgreSQL-family
// database, offline `schema render` and the live `schema apply` plan must leave
// the same objects in the catalog.
//
// The issue reported the two surfaces disagreeing about the same file --
// `schema render --dialect yugabytedb` emitting one statement where
// `schema apply --dry-run` against a live YugabyteDB planned six -- with no
// diagnostic on the omitting side. The converter has since been repaired, but
// agreement had never been observed against a server.
//
// Both surfaces are executed, not compared as text. Each one's SQL is applied
// statement by statement to its own freshly created database and the catalog is
// then read back, so what is compared is what the engine ended up holding. That
// matters here beyond the usual reason: these targets reject some of the SQL
// (CockroachDB has no CREATE DOMAIN), and a text comparison would call the two
// surfaces equal while the engine kept different things.
//
// Statements are applied individually in autocommit and their errors recorded
// rather than raised. A fail-fast apply stops at the first rejected statement,
// and the two surfaces order their statements differently, so a fail-fast run
// would report an ordering difference as a disagreement about objects. Both
// surfaces get exactly the same treatment, so a difference in the census is a
// difference in what the surfaces asked for.
func TestSchemaRenderAndPlanCatalogAgreementE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, binaryPath)

	tests := []renderPlanCatalogCase{
		{name: "postgres", dialect: "postgres", urlEnv: "POSTGRES_URL"},
		{name: "cockroachdb", dialect: "cockroachdb", urlEnv: "COCKROACHDB_URL"},
		{name: "yugabytedb", dialect: "yugabytedb", urlEnv: "YUGABYTEDB_URL"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			runRenderPlanCatalogCase(c, ctx, binaryPath, test)
		})
	}
}

func runRenderPlanCatalogCase(
	c *qt.C,
	ctx context.Context,
	binaryPath string,
	test renderPlanCatalogCase,
) {
	c.Helper()

	adminURL := requireIntegrationEnvironment(c, test.urlEnv)
	adminDB, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	// Registered before the drops below so it runs after them: cleanups are
	// last-in-first-out, and a deferred Close would shut the pool while the
	// drops still needed it.
	c.Cleanup(func() { adminDB.Close() })
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	roleName := "p929_role_" + suffix
	fixtureDir := writeRenderPlanFixture(c, suffix)

	renderName := "p929_render_" + suffix
	planName := "p929_plan_" + suffix

	// The role is registered ahead of both databases so it is dropped after
	// them. A role that still holds a grant inside a live database cannot be
	// dropped -- PostgreSQL refuses with "role cannot be dropped because some
	// objects depend on it" -- and roles are cluster-global, so getting this
	// order wrong leaks one role per run onto a shared server rather than into a
	// database somebody later removes.
	c.Cleanup(func() { dropRenderPlanRole(adminDB, roleName) })
	createE2EDatabase(c, ctx, adminDB, renderName)
	c.Cleanup(func() { dropRenderPlanDatabase(c, adminDB, renderName) })
	createE2EDatabase(c, ctx, adminDB, planName)
	c.Cleanup(func() { dropRenderPlanDatabase(c, adminDB, planName) })

	renderURL := replaceDatabaseName(c, adminURL, renderName)
	planURL := replaceDatabaseName(c, adminURL, planName)

	// The plan is read first, while both databases are empty and no role of this
	// name exists anywhere on the server. Reading it after the render surface ran
	// would let the planner see the role the render surface created and omit
	// CREATE ROLE, which is the one way this comparison could agree for the wrong
	// reason.
	planSQL := readPlanSurfaceSQL(c, ctx, binaryPath, fixtureDir, planURL)
	renderSQL := readRenderSurfaceSQL(c, ctx, binaryPath, fixtureDir, test.dialect)

	renderCensus := applyAndCensus(c, ctx, test.dialect, renderURL, renderSQL, suffix)

	// The render surface's database and role are removed before the plan surface
	// runs, so the plan surface starts from the same empty server the plan was
	// read against.
	dropRenderPlanDatabase(c, adminDB, renderName)
	dropRenderPlanRole(adminDB, roleName)

	planCensus := applyAndCensus(c, ctx, test.dialect, planURL, planSQL, suffix)

	c.Logf("%s catalog after the render surface:\n%s", test.name, formatRenderPlanCensus(renderCensus))
	c.Logf("%s catalog after the plan surface:\n%s", test.name, formatRenderPlanCensus(planCensus))

	// Non-vacuity: a census whose every row said count=0 would compare equal to
	// another such census and prove nothing. The table is the one object every
	// member of this family accepts, so it must be present on both sides.
	//
	// Check rather than Assert so a surface that applied nothing still reaches
	// the comparison below, which is the assertion that names which object kind
	// the two surfaces left differently.
	c.Check(renderCensus["table"], qt.Equals, "count=1 err=<nil>",
		qt.Commentf("render surface census:\n%s", formatRenderPlanCensus(renderCensus)))
	c.Check(planCensus["table"], qt.Equals, "count=1 err=<nil>",
		qt.Commentf("plan surface census:\n%s", formatRenderPlanCensus(planCensus)))

	c.Assert(planCensus, qt.DeepEquals, renderCensus,
		qt.Commentf("render and plan left different catalogs on %s\nrender:\n%s\nplan:\n%s",
			test.name, formatRenderPlanCensus(renderCensus), formatRenderPlanCensus(planCensus)))
}

// writeRenderPlanFixture materializes the annotated fixture with this run's
// suffix and returns the directory holding it.
func writeRenderPlanFixture(c *qt.C, suffix string) string {
	c.Helper()

	dir := filepath.Join(c.TempDir(), "models")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	source := strings.ReplaceAll(renderPlanFixtureTemplate, "SFX", suffix)
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(source), 0o600), qt.IsNil)
	return dir
}

// readRenderSurfaceSQL returns what `schema render` emits for the dialect.
func readRenderSurfaceSQL(
	c *qt.C,
	ctx context.Context,
	binaryPath, fixtureDir, dialect string,
) string {
	c.Helper()

	stdout, stderr, err := runPtahCapturingStreams(ctx, binaryPath,
		"schema", "render", "--root-dir", fixtureDir, "--dialect", dialect)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	return stdout
}

// readPlanSurfaceSQL returns the plan `schema apply` would execute against the
// still-empty target, with its one-line heading removed.
//
// The heading is asserted rather than trimmed blindly: if the command's first
// line ever became SQL, silently dropping it would delete a statement from the
// surface under measurement.
func readPlanSurfaceSQL(
	c *qt.C,
	ctx context.Context,
	binaryPath, fixtureDir, dbURL string,
) string {
	c.Helper()

	stdout, stderr, err := runPtahCapturingStreams(ctx, binaryPath,
		"schema", "apply", "--root-dir", fixtureDir, "--db-url", dbURL, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))

	heading, plan, found := strings.Cut(stdout, "\n")
	c.Assert(found, qt.IsTrue, qt.Commentf("dry-run output:\n%s", stdout))
	c.Assert(strings.TrimSpace(heading), qt.Equals, "Planned schema changes:")
	return plan
}

// runPtahCapturingStreams runs the built binary with stdout and stderr kept
// apart. The merged-stream helper in this package would fold the converter's
// progress reporting into the SQL being measured.
func runPtahCapturingStreams(
	ctx context.Context,
	binaryPath string,
	args ...string,
) (stdout, stderr string, err error) {
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	var outBuf, errBuf strings.Builder
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err = cmd.Run()
	return outBuf.String(), errBuf.String(), err
}

// applyAndCensus executes one surface's SQL against dbURL and reads the catalog
// back.
func applyAndCensus(
	c *qt.C,
	ctx context.Context,
	dialect, dbURL, surfaceSQL, suffix string,
) map[string]string {
	c.Helper()

	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	statements := sqlutil.SplitSQLStatementsForDialect(surfaceSQL, dialect)

	// Control on the split: a surface reduced to nothing would apply nothing and
	// still census cleanly.
	c.Assert(len(statements) > 1, qt.IsTrue,
		qt.Commentf("%s surface split into %d statements:\n%s",
			dialect, len(statements), surfaceSQL))

	for _, statement := range statements {
		_, execErr := db.ExecContext(ctx, statement)
		c.Logf("%s: %v <- %s", dialect, execErr, strings.TrimSpace(statement))
	}

	census := map[string]string{}
	for _, probe := range renderPlanCatalogProbes {
		var count int
		scanErr := db.QueryRowContext(ctx, probe.query(suffix)).Scan(&count)
		census[probe.kind] = fmt.Sprintf("count=%d err=%v", count, scanErr)
	}
	return census
}

// dropRenderPlanDatabase removes one of this measurement's throwaway databases.
//
// It evicts other sessions first, and carries its own context rather than the
// test's. A database still holding a backend refuses to drop, and the drop also
// has to survive being called from a cleanup after the test's context is
// canceled. The budget is deliberately generous: a loaded machine has taken
// over thirty seconds to drop a database of this size, which is a fact about
// the disk and not about the surfaces under measurement.
func dropRenderPlanDatabase(c *qt.C, adminDB *sql.DB, name string) {
	c.Helper()

	cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Best-effort: not every member of this family implements
	// pg_terminate_backend, and a target that does not still drops fine.
	_, _ = adminDB.ExecContext(cleanupCtx,
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity"+
			" WHERE datname = $1 AND pid <> pg_backend_pid()", name)

	_, err := adminDB.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+quoteE2EIdent(name))
	c.Assert(err, qt.IsNil)
}

// dropRenderPlanRole removes the cluster-global role between the two surfaces.
//
// The error is deliberately dropped: the role is absent whenever the surface
// that would have created it was refused by the engine, and that is a result
// this measurement records rather than a failure of the measurement.
func dropRenderPlanRole(adminDB *sql.DB, roleName string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = adminDB.ExecContext(cleanupCtx, "DROP ROLE IF EXISTS "+quoteE2EIdent(roleName))
}

// formatRenderPlanCensus renders a census as one sorted line per object kind.
func formatRenderPlanCensus(census map[string]string) string {
	lines := make([]string, 0, len(census))
	for kind, verdict := range census {
		lines = append(lines, fmt.Sprintf("  %-10s %s", kind, verdict))
	}
	slices.Sort(lines)
	return strings.Join(lines, "\n")
}
