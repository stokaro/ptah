package dbtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/internal/shadowdb"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/seeder"
)

// Options configures a single [RunMigrationTest] invocation.
type Options struct {
	// Cases are the test cases to run, in order.
	Cases []Case
	// MigrationsDir is the directory containing migration files. It is required
	// only when a case has a migrate_to step.
	MigrationsDir string
	// RootDir is a directory of Go entity annotations describing the desired
	// schema. It is required only when a case has an apply_schema step.
	RootDir string
	// SeedDir is the default directory of seed files for seed steps that omit
	// their own [SeedStep.Dir].
	SeedDir string
	// DBURL is an optional database URL to run the tests against. It must point
	// at a throwaway database, because tests mutate schema and data. When empty,
	// an ephemeral SQLite database is provisioned in a temporary directory and
	// removed afterwards.
	DBURL string
	// DirFormat selects how MigrationsDir is parsed. The zero value defaults to
	// the Ptah directory format (not the migrator's own "auto" default), so a
	// direct caller that wants format auto-detection must set it explicitly.
	DirFormat migrator.MigrationDirFormat
}

// Report is the outcome of a [RunMigrationTest] or [RunSchemaTest] run.
type Report struct {
	Cases []CaseResult

	// kind labels the report header ("MIGRATION" or "SCHEMA"). It defaults to
	// "MIGRATION" so a zero-value Report still renders sensibly.
	kind string
}

// CaseResult is the outcome of a single [Case]. Passed is true only when every
// executed step passed.
type CaseResult struct {
	Name   string       `json:"name"`
	Steps  []StepResult `json:"steps"`
	Passed bool         `json:"passed"`
}

// StepResult is the outcome of a single [Step]. Detail carries a short
// human-readable explanation of the result, especially on failure.
type StepResult struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail,omitempty"`
}

// Failed reports whether any case failed.
func (r *Report) Failed() bool {
	for i := range r.Cases {
		if !r.Cases[i].Passed {
			return true
		}
	}
	return false
}

// Text renders a human-readable summary of the report.
func (r *Report) Text() string {
	var b strings.Builder
	kind := r.kind
	if kind == "" {
		kind = "MIGRATION"
	}
	fmt.Fprintf(&b, "=== %s TEST ===\n", kind)

	passed := 0
	for i := range r.Cases {
		c := r.Cases[i]
		caseStatus := "FAIL"
		if c.Passed {
			caseStatus = "PASS"
			passed++
		}
		fmt.Fprintf(&b, "%s  case %q\n", caseStatus, c.Name)
		for j := range c.Steps {
			s := c.Steps[j]
			stepStatus := "FAIL"
			if s.Passed {
				stepStatus = "PASS"
			}
			label := s.Name
			if label == "" {
				label = "(unnamed step)"
			}
			if s.Detail != "" {
				fmt.Fprintf(&b, "    %s  step %q — %s\n", stepStatus, label, s.Detail)
				continue
			}
			fmt.Fprintf(&b, "    %s  step %q\n", stepStatus, label)
		}
	}

	fmt.Fprintf(&b, "\n%d cases, %d passed, %d failed\n", len(r.Cases), passed, len(r.Cases)-passed)
	return b.String()
}

// Render renders the report in the named format: "" or "text" for the text
// summary, "json" for indented JSON, or "html" for a self-contained HTML page.
// An unknown format is an error.
func (r *Report) Render(format string) (string, error) {
	switch format {
	case "", "text":
		return r.Text(), nil
	case "json":
		return r.JSON()
	case "html":
		return r.HTML()
	default:
		return "", fmt.Errorf("unsupported report format %q: want text, json, or html", format)
	}
}

// reportKind returns the report's kind label, defaulting to "MIGRATION" so a
// zero-value Report still renders sensibly.
func (r *Report) reportKind() string {
	if r.kind == "" {
		return "MIGRATION"
	}
	return r.kind
}

// counts returns the total, passed, and failed case counts.
func (r *Report) counts() (total, passed, failed int) {
	total = len(r.Cases)
	for i := range r.Cases {
		if r.Cases[i].Passed {
			passed++
		}
	}
	return total, passed, total - passed
}

// JSON renders the report as indented JSON: the kind, summary counts, and every
// case and step. It is stable enough to consume from CI tooling.
func (r *Report) JSON() (string, error) {
	total, passed, failed := r.counts()
	// Normalize a nil case slice to an empty one so the JSON is always
	// "cases": [] rather than "cases": null, even for a zero-value Report built
	// directly by a library caller.
	cases := r.Cases
	if cases == nil {
		cases = []CaseResult{}
	}
	doc := struct {
		Kind   string       `json:"kind"`
		Total  int          `json:"total"`
		Passed int          `json:"passed"`
		Failed int          `json:"failed"`
		Cases  []CaseResult `json:"cases"`
	}{
		Kind:   r.reportKind(),
		Total:  total,
		Passed: passed,
		Failed: failed,
		Cases:  cases,
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", fmt.Errorf("render JSON report: %w", err)
	}
	return string(out) + "\n", nil
}

// HTML renders the report as a self-contained HTML document.
func (r *Report) HTML() (string, error) {
	total, passed, failed := r.counts()
	data := struct {
		Kind   string
		Total  int
		Passed int
		Failed int
		Cases  []CaseResult
	}{r.reportKind(), total, passed, failed, r.Cases}

	var b strings.Builder
	if err := reportHTMLTemplate.Execute(&b, data); err != nil {
		return "", fmt.Errorf("render HTML report: %w", err)
	}
	return b.String(), nil
}

var reportHTMLTemplate = template.Must(template.New("dbtest-report").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{{.Kind}} test report</title>
<style>
body { font-family: system-ui, sans-serif; margin: 2rem; }
.pass { color: #157f3b; }
.fail { color: #b3261e; }
.case { margin: 0.75rem 0; }
.steps { margin: 0.25rem 0 0 1.5rem; padding: 0; list-style: none; }
.detail { color: #555; }
</style>
</head>
<body>
<h1>{{.Kind}} test report</h1>
<p>{{.Total}} cases, {{.Passed}} passed, {{.Failed}} failed</p>
{{range .Cases}}
<div class="case">
  <strong class="{{if .Passed}}pass{{else}}fail{{end}}">{{if .Passed}}PASS{{else}}FAIL{{end}}</strong>
  case &ldquo;{{.Name}}&rdquo;
  <ul class="steps">
    {{range .Steps}}
    <li>
      <span class="{{if .Passed}}pass{{else}}fail{{end}}">{{if .Passed}}PASS{{else}}FAIL{{end}}</span>
      step &ldquo;{{.Name}}&rdquo;{{if .Detail}} <span class="detail">&mdash; {{.Detail}}</span>{{end}}
    </li>
    {{end}}
  </ul>
</div>
{{end}}
</body>
</html>
`))

// RunMigrationTest runs the test cases in opts against a database and returns a
// report describing every case and step. A returned error indicates the run
// itself could not be set up (for example, the test cases are invalid or the
// database is unreachable), or the context is interrupted; ordinary assertion
// failures are captured in the report, not returned as an error, so callers
// should inspect [Report.Failed].
func RunMigrationTest(ctx context.Context, opts Options) (*Report, error) {
	if err := validateCasesForRun(opts.Cases, opts.SeedDir); err != nil {
		return nil, fmt.Errorf("invalid test cases: %w", err)
	}
	if casesUseStepKind(opts.Cases, stepKindMigrateTo) && strings.TrimSpace(opts.MigrationsDir) == "" {
		return nil, fmt.Errorf("migrate_to requires a migrations directory")
	}

	desiredSchema, err := desiredSchemaForMigrationCases(opts.RootDir, opts.Cases)
	if err != nil {
		return nil, err
	}

	dirFormat := opts.DirFormat
	if dirFormat == "" {
		dirFormat = migrator.MigrationDirFormatPtah
	}

	run := func(ctx context.Context, conn *dbschema.DatabaseConnection, c Case) (CaseResult, error) {
		r := &runner{
			conn:          conn,
			migrationsDir: opts.MigrationsDir,
			dirFormat:     dirFormat,
			desiredSchema: desiredSchema,
			seedDir:       opts.SeedDir,
		}
		r.migrateTo = r.runMigrateTo
		r.applySchema = r.runApplySchema
		return r.runCase(ctx, c)
	}
	// Migration tests need no per-database provisioning: each case applies its
	// own migrations via migrate_to.
	return runCases(ctx, opts.DBURL, "MIGRATION", opts.Cases, nil, run)
}

// caseRunner runs a single case against a freshly provisioned connection.
// Returning an error aborts the whole run: it signals the test bed itself could
// not be set up (for example, a desired schema that fails to render or apply),
// which is distinct from an ordinary assertion failure captured in the
// [CaseResult].
type caseRunner func(ctx context.Context, conn *dbschema.DatabaseConnection, c Case) (CaseResult, error)

// provisionFunc sets up a freshly connected database once, before any case runs
// against it. It is called once per shared connection or once per ephemeral
// database.
type provisionFunc func(ctx context.Context, conn *dbschema.DatabaseConnection) error

// runCases drives every case with the database isolation both public entry
// points ([RunMigrationTest] and [RunSchemaTest]) share. An explicit database
// URL is a single throwaway the caller owns, so all cases share one connection
// and the caller is responsible for isolating them. The default (ephemeral) mode
// instead gives each case its own fresh SQLite database so cases cannot
// contaminate one another.
//
// provision, when non-nil, sets up a database once after it is connected and
// before any case runs against it: once per shared connection, or once per fresh
// ephemeral database. Schema tests use it to create the desired schema exactly
// once per database; migration tests pass nil because each case applies its own
// migrations.
func runCases(ctx context.Context, dbURL, kind string, cases []Case, provision provisionFunc, run caseRunner) (*Report, error) {
	if dbURL != "" {
		return runExplicitCases(ctx, dbURL, kind, cases, provision, run)
	}

	report := &Report{Cases: make([]CaseResult, 0, len(cases)), kind: kind}
	for i := range cases {
		result, err := runEphemeralCase(ctx, cases[i], provision, run)
		if err != nil {
			return nil, err
		}
		report.Cases = append(report.Cases, result)
	}
	return report, nil
}

func runExplicitCases(
	ctx context.Context,
	dbURL,
	kind string,
	cases []Case,
	provision provisionFunc,
	run caseRunner,
) (*Report, error) {
	database, err := shadowdb.Open(ctx, dbURL, "")
	if err != nil {
		return nil, fmt.Errorf("connect to test database: %w", err)
	}

	report, runErr := runConnectedCases(ctx, database.Connection(), kind, cases, provision, run)
	closeErr := database.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("close test database: %w", closeErr)
	}
	if err := errors.Join(runErr, closeErr); err != nil {
		return nil, err
	}
	return report, nil
}

func runConnectedCases(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	kind string,
	cases []Case,
	provision provisionFunc,
	run caseRunner,
) (*Report, error) {
	if provision != nil {
		if err := provision(ctx, conn); err != nil {
			return nil, err
		}
	}

	report := &Report{Cases: make([]CaseResult, 0, len(cases)), kind: kind}
	for i := range cases {
		result, err := run(ctx, conn, cases[i])
		if err != nil {
			return nil, err
		}
		report.Cases = append(report.Cases, result)
	}
	return report, nil
}

// runEphemeralCase runs one case against a fresh SQLite database that exists
// only for that case, so state created by one case is never visible to another.
func runEphemeralCase(ctx context.Context, c Case, provision provisionFunc, run caseRunner) (CaseResult, error) {
	database, err := shadowdb.Open(ctx, "", "ptah-dbtest-*")
	if err != nil {
		return CaseResult{}, fmt.Errorf("connect to ephemeral test database: %w", err)
	}
	conn := database.Connection()

	var result CaseResult
	var runErr error
	if provision != nil {
		runErr = provision(ctx, conn)
	}
	if runErr == nil {
		result, runErr = run(ctx, conn, c)
	}
	closeErr := database.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("close ephemeral test database: %w", closeErr)
	}
	return result, errors.Join(runErr, closeErr)
}

// runner executes steps against a single shared database connection.
type runner struct {
	conn          *dbschema.DatabaseConnection
	migrationsDir string
	dirFormat     migrator.MigrationDirFormat
	desiredSchema *goschema.Database
	seedDir       string
	// migrateTo handles a migrate_to step. Migration tests wire it to
	// [runner.runMigrateTo]; schema tests wire it to a rejection because a schema
	// test applies a desired schema directly and has no migrations to move
	// between. It must be set before [runner.execStep] is called.
	migrateTo func(ctx context.Context, target string) (passed bool, detail string)
	// applySchema handles an apply_schema step.
	applySchema func(ctx context.Context) (passed bool, detail string)
}

func (r *runner) runCase(ctx context.Context, c Case) (CaseResult, error) {
	result := CaseResult{Name: c.Name, Passed: true, Steps: make([]StepResult, 0, len(c.Steps))}
	for i := range c.Steps {
		step := c.Steps[i]
		passed, detail := r.execStep(ctx, step)
		result.Steps = append(result.Steps, StepResult{Name: step.Name, Passed: passed, Detail: detail})
		if err := ctx.Err(); err != nil {
			return result, fmt.Errorf("run test case %q: %w", c.Name, err)
		}
		if !passed {
			// Stop at the first failure: later steps almost always depend on the
			// state the failed step was supposed to establish, so running them
			// would produce noise rather than signal.
			result.Passed = false
			break
		}
	}
	return result, nil
}

func (r *runner) execStep(ctx context.Context, step Step) (passed bool, detail string) {
	kind, _ := step.kind()
	switch kind {
	case stepKindMigrateTo:
		return r.migrateTo(ctx, step.MigrateTo)
	case stepKindApplySchema:
		return r.applySchema(ctx)
	case stepKindExec:
		return r.runExec(ctx, step.Exec)
	case stepKindSeed:
		return r.runSeed(ctx, step.Seed)
	case stepKindAssert:
		return r.runAssert(ctx, step.Assert)
	default:
		return false, "step performs no action"
	}
}

func (r *runner) runApplySchema(ctx context.Context) (passed bool, detail string) {
	if r.desiredSchema == nil {
		return false, "apply_schema requires a desired schema root directory"
	}
	applied, err := applyDesiredSchema(ctx, r.conn, r.desiredSchema)
	if err != nil {
		return false, fmt.Sprintf("apply_schema failed: %v", err)
	}
	if !applied {
		return true, "desired schema already applied"
	}
	return true, "desired schema applied"
}

func (r *runner) runMigrateTo(ctx context.Context, target string) (passed bool, detail string) {
	if r.migrationsDir == "" {
		return false, "migrate_to requires a migrations directory"
	}
	m, err := r.newMigrator()
	if err != nil {
		return false, fmt.Sprintf("build migrator: %v", err)
	}

	normalized := strings.ToLower(strings.TrimSpace(target))
	switch normalized {
	case "latest":
		if err := m.MigrateUp(ctx); err != nil {
			return false, fmt.Sprintf("migrate to latest failed: %v", err)
		}
		return true, "migrated to latest"
	case "0":
		if err := m.MigrateDownTo(ctx, 0); err != nil {
			return false, fmt.Sprintf("migrate to 0 failed: %v", err)
		}
		return true, "migrated to 0"
	default:
		version, err := strconv.ParseInt(normalized, 10, 64)
		if err != nil || version < 0 {
			return false, fmt.Sprintf(
				"invalid migrate_to target %q: expected a non-negative integer, \"latest\", or \"0\"",
				target,
			)
		}
		if err := m.MigrateTo(ctx, version); err != nil {
			return false, fmt.Sprintf("migrate to %d failed: %v", version, err)
		}
		return true, fmt.Sprintf("migrated to %d", version)
	}
}

func (r *runner) newMigrator() (*migrator.Migrator, error) {
	provider, err := migrator.NewFSMigrationProvider(os.DirFS(r.migrationsDir), migrator.WithMigrationDirFormat(r.dirFormat))
	if err != nil {
		return nil, err
	}
	return migrator.NewMigrator(r.conn, provider).WithLogger(slog.New(slog.DiscardHandler)), nil
}

func (r *runner) runExec(ctx context.Context, sql string) (passed bool, detail string) {
	if _, err := r.conn.ExecContext(ctx, sql); err != nil {
		return false, fmt.Sprintf("exec failed: %v", err)
	}
	return true, "exec ok"
}

func (r *runner) runSeed(ctx context.Context, seed *SeedStep) (passed bool, detail string) {
	dir := seed.Dir
	if strings.TrimSpace(dir) == "" {
		dir = r.seedDir
	}
	// The test database is a throwaway, so AllowProd bypasses the seeder's
	// protected-environment and protected-table guards, which exist to stop
	// accidental seeding of real environments.
	result, err := seeder.Apply(ctx, r.conn, os.DirFS(dir), seeder.Options{
		Env:       seed.Env,
		AllowProd: true,
	})
	if err != nil {
		return false, fmt.Sprintf("seed failed: %v", err)
	}
	return true, fmt.Sprintf("seeded %d file(s)", len(result.Applied))
}

func (r *runner) runAssert(ctx context.Context, a *Assertion) (passed bool, detail string) {
	switch {
	case a.RowCount != nil:
		return r.assertRowCount(ctx, a.Query, *a.RowCount)
	case a.Scalar != nil:
		return r.assertScalar(ctx, a.Query, *a.Scalar)
	default:
		return r.assertErrorContains(ctx, a.Query, a.ErrorContains)
	}
}

func (r *runner) assertRowCount(ctx context.Context, query string, want int) (passed bool, detail string) {
	rows, err := r.conn.QueryContext(ctx, query)
	if err != nil {
		return false, fmt.Sprintf("row_count query failed: %v", err)
	}
	defer func() { _ = rows.Close() }()

	got := 0
	for rows.Next() {
		got++
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Sprintf("row_count query failed while reading rows: %v", err)
	}
	if got != want {
		return false, fmt.Sprintf("expected row_count %d, got %d", want, got)
	}
	return true, fmt.Sprintf("row_count %d", got)
}

func (r *runner) assertScalar(ctx context.Context, query, want string) (passed bool, detail string) {
	rows, err := r.conn.QueryContext(ctx, query)
	if err != nil {
		return false, fmt.Sprintf("scalar query failed: %v", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return false, fmt.Sprintf("scalar query failed to report columns: %v", err)
	}
	if len(cols) == 0 {
		return false, "scalar query returned no columns"
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return false, fmt.Sprintf("scalar query failed while reading rows: %v", err)
		}
		return false, "scalar query returned no rows"
	}

	dests := make([]any, len(cols))
	for i := range dests {
		dests[i] = new(any)
	}
	if err := rows.Scan(dests...); err != nil {
		return false, fmt.Sprintf("scalar query failed to scan row: %v", err)
	}

	got := normalizeScalar(*(dests[0].(*any)))
	if got != want {
		return false, fmt.Sprintf("expected scalar %q, got %q", want, got)
	}
	return true, fmt.Sprintf("scalar %q", got)
}

func (r *runner) assertErrorContains(ctx context.Context, query, want string) (passed bool, detail string) {
	err := r.runExpectingError(ctx, query)
	if err == nil {
		return false, fmt.Sprintf("expected an error containing %q, but the query succeeded", want)
	}
	if !strings.Contains(err.Error(), want) {
		return false, fmt.Sprintf("expected error to contain %q, got %q", want, err.Error())
	}
	return true, fmt.Sprintf("error contained %q", want)
}

// runExpectingError runs query and returns any error it produces, draining the
// result set so errors that only surface during row iteration are observed.
func (r *runner) runExpectingError(ctx context.Context, query string) error {
	rows, err := r.conn.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		// Drain the result set so errors that only surface during row iteration
		// are observed.
	}
	return rows.Err()
}

// normalizeScalar renders a scanned value as the string an assertion compares
// against. time.Time is formatted with a fixed layout so date/time columns —
// which some drivers surface as time.Time rather than the stored text — compare
// deterministically instead of via time.Time's default String form (which
// appends " +0000 UTC"). To compare the raw stored text of a date/time column,
// select it as text (for example `CAST(col AS TEXT)`).
func normalizeScalar(v any) string {
	switch val := v.(type) {
	case []byte:
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", v)
	}
}
