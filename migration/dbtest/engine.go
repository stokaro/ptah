package dbtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/htmlstyle"
	"ptah.run/migration/internal/scratchdb"
	"ptah.run/migration/internal/shadowdb"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
	"ptah.run/migration/seeder"
)

// Options configures a single [RunMigrationTest] invocation.
type Options struct {
	// Cases are the test cases to run, in order.
	Cases []Case
	// MigrationsDir names the directory holding the migration files. Name it
	// whenever a case has a migrate_to step, including when MigrationsFS
	// supplies the bytes: [RunMigrationTest] refuses a run whose cases migrate
	// and that names no directory, before it reaches a database.
	MigrationsDir string
	// MigrationsFS is the immutable migration history used by every migrate_to
	// step. When nil, MigrationsDir is opened for compatibility with existing
	// embedders. Command callers should capture once, authorize that snapshot,
	// and pass it here so test steps cannot reopen different bytes.
	//
	// A non-nil MigrationsFS is the only source of migration bytes for the run.
	// MigrationsDir is not read from disk, which is what makes the authorized
	// snapshot authoritative.
	MigrationsFS fs.FS
	// RootDir is a directory of Go entity annotations describing the desired
	// schema. It is required only when a case has an apply_schema step.
	RootDir string
	// SeedDir is the default directory of seed files for seed steps that omit
	// their own [SeedStep.Dir].
	SeedDir string
	// AllowExternalCommands authorizes [ExternalStep]. It is false by default:
	// an external step names a program on the machine running the suite, which
	// is a larger authority than the rest of a test file has, so the run is
	// refused with [ErrExternalNotAuthorized] before any database is
	// provisioned unless the caller grants it.
	AllowExternalCommands bool
	// ExternalTimeout bounds each external step. Zero selects
	// [DefaultExternalTimeout]; a test file cannot change it.
	ExternalTimeout time.Duration
	// Parallelism bounds how many cases marked [Case.Parallel] run at once.
	// Zero reads the machine's GOMAXPROCS, which is the right default for a
	// suite and the wrong basis for anything that has to know how much
	// concurrency it got.
	Parallelism int
	// DBURL is an optional database URL to run the tests against. It must point
	// at a throwaway database, because tests mutate schema and data. When empty,
	// an ephemeral SQLite database is provisioned in a temporary directory and
	// removed afterwards.
	DBURL string
	// DirFormat selects how MigrationsDir is parsed. The zero value defaults to
	// the Ptah directory format (not the migrator's own "auto" default), so a
	// direct caller that wants format auto-detection must set it explicitly.
	DirFormat migrationfile.DirFormat
	// RevisionsSchema places the revision table a migrate_to step writes in a
	// named schema instead of the connection's default one. It matters when the
	// throwaway database already holds a revision table the run must not touch,
	// or when the tests exist to prove that a deployment's revision schema is
	// the one the migrations expect. Empty keeps the connection default.
	RevisionsSchema string
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
	// CleanupFailed marks a case whose cleanup did not complete. It is
	// independent of Passed: a case can fail its body, its cleanup, or both,
	// and a reader deciding whether the database was left dirty needs the
	// second answer rather than the conjunction.
	CleanupFailed bool `json:"cleanup_failed,omitempty"`
	// Skipped marks a case the run did not execute. Passed is false for such a
	// case and [Report.Failed] ignores it, so a skip neither claims a proof that
	// was never attempted nor reddens a suite for declining to attempt it.
	Skipped bool `json:"skipped,omitempty"`
}

// StepResult is the outcome of a single [Step]. Detail carries a short
// human-readable explanation of the result, especially on failure.
type StepResult struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail,omitempty"`
	// Kind names an outcome a reader cannot infer from Passed and Detail.
	//
	// It is empty for an ordinary step, so a report of nothing but ordinary
	// steps is byte-identical to one from before the field existed. Two
	// outcomes are not ordinary and were indistinguishable without it: a
	// logged message, which reached no database and decided nothing, and a
	// failure a case expected, which passed BECAUSE something went wrong
	// (stokaro/ptah#2866).
	Kind StepKind `json:"kind,omitempty"`
}

// StepKind labels a step outcome that Passed alone describes wrongly.
type StepKind string

const (
	// StepKindLog marks a message recorded among the steps. It touched no
	// database and could not have failed, so counting it as a passing check
	// overstates what ran.
	StepKindLog StepKind = "log"
	// StepKindCaught marks an expected failure that occurred. The step passed
	// and the statement did not, and a reader scanning for what a case proved
	// needs to see which of the two happened.
	StepKindCaught StepKind = "caught"
	// StepKindCleanup marks a step that ran as part of the case's cleanup
	// rather than its body. The distinction is what lets a reader tell a case
	// that failed its check from one whose check passed and whose teardown did
	// not, which are different problems with different owners.
	StepKindCleanup StepKind = "cleanup"
)

// Failed reports whether any case failed.
func (r *Report) Failed() bool {
	for i := range r.Cases {
		if !r.Cases[i].Passed && !r.Cases[i].Skipped {
			return true
		}
	}
	return false
}

// Text renders a human-readable summary of the report: a header naming the
// report kind, a PASS/FAIL row per case with a row per executed step (carrying
// the step's detail when it has one), and a trailing totals line. Cases and
// steps render in execution order, and the rendering is deterministic for a
// given report. Consume [Report.JSON] where a machine reads the result.
func (r *Report) Text() string {
	var b strings.Builder
	kind := r.kind
	if kind == "" {
		kind = "MIGRATION"
	}
	fmt.Fprintf(&b, "=== %s TEST ===\n", kind)

	for i := range r.Cases {
		c := r.Cases[i]
		fmt.Fprintf(&b, "%s  case %q\n", c.StatusLabel(), c.Name)
		for j := range c.Steps {
			s := c.Steps[j]
			stepStatus := stepStatusLabel(s)
			label := s.Name
			if label == "" {
				label = "(unnamed step)"
			}
			if s.Detail != "" {
				fmt.Fprintf(&b, "    %s  %s %q — %s\n", stepStatus, s.noun(), label, s.Detail)
				continue
			}
			fmt.Fprintf(&b, "    %s  %s %q\n", stepStatus, s.noun(), label)
		}
	}

	tally := r.counts()
	fmt.Fprintf(&b, "\n%d cases, %d passed, %d failed, %d skipped\n",
		tally.total, tally.passed, tally.failed, tally.skipped)
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
// reportCounts is the case tally a summary line and both structured formats
// render. It is a struct rather than four results so the four cannot be
// swapped at a call site, which is the one mistake a reader of a summary line
// has no way to notice.
type reportCounts struct {
	total   int
	passed  int
	failed  int
	skipped int
}

func (r *Report) counts() reportCounts {
	tally := reportCounts{total: len(r.Cases)}
	for i := range r.Cases {
		if r.Cases[i].Skipped {
			tally.skipped++
			continue
		}
		if r.Cases[i].Passed {
			tally.passed++
		}
	}
	tally.failed = tally.total - tally.passed - tally.skipped
	return tally
}

// JSON renders the report as indented JSON: the kind, summary counts, and every
// case and step. It is stable enough to consume from CI tooling.
// StatusLabel is the word a report puts in front of this case.
//
// SKIP is its own answer rather than a decorated PASS: a skipped case proved
// nothing, and calling it passed is the report telling a reader that a check
// they are relying on ran.
func (c CaseResult) StatusLabel() string {
	if c.Skipped {
		return "SKIP"
	}
	if c.Passed {
		return "PASS"
	}
	return "FAIL"
}

// StatusClass is the CSS class the HTML report gives this case.
func (c CaseResult) StatusClass() string {
	return strings.ToLower(c.StatusLabel())
}

// StatusLabel is the word a report puts in front of this step. It is a method
// so the text renderer and the HTML template answer from one place rather than
// each deciding what a logged or caught step is called.
func (s StepResult) StatusLabel() string {
	return strings.TrimSpace(stepStatusLabel(s))
}

// StatusClass is the CSS class the HTML report gives this step, matching the
// label so a reader scanning colors and a reader scanning words agree.
func (s StepResult) StatusClass() string {
	switch {
	case s.Kind == StepKindLog:
		return "log"
	case s.Kind == StepKindCaught:
		return "caught"
	case s.Passed:
		return "pass"
	default:
		return "fail"
	}
}

// stepStatusLabel is the word a text report puts in front of a step.
//
// A logged message is not a check that passed and a caught failure is not an
// ordinary success, so neither wears PASS: a reader counting PASS lines to see
// what a case proved would be counting a message among them.
// noun is what a report calls this step.
//
// A cleanup step keeps the ordinary PASS/FAIL word in the status column,
// because whether it worked is the same question as for any other step, and
// says which half of the case it belongs to here instead. Spending the status
// column on the distinction would cost the outcome, which is the fact a reader
// scans for.
func (s StepResult) noun() string {
	return s.Noun()
}

// Noun is what a report calls this step: a cleanup step or an ordinary one.
//
// It is exported because the HTML template needs it. Without it that report
// classified a step by outcome alone, so a failed cleanup and a failed check
// rendered identically -- and the word "cleanup" appeared there only when the
// step happened to be NAMED that, which is an accident of the `.test.hcl`
// translation rather than something the report says.
func (s StepResult) Noun() string {
	if s.Kind == StepKindCleanup {
		return "cleanup step"
	}
	return "step"
}

func stepStatusLabel(step StepResult) string {
	switch {
	case step.Kind == StepKindLog:
		return "LOG   "
	case step.Kind == StepKindCaught:
		return "CAUGHT"
	case step.Passed:
		return "PASS  "
	default:
		return "FAIL  "
	}
}

func (r *Report) JSON() (string, error) {
	tally := r.counts()
	// Normalize a nil case slice to an empty one so the JSON is always
	// "cases": [] rather than "cases": null, even for a zero-value Report built
	// directly by a library caller.
	cases := r.Cases
	if cases == nil {
		cases = make([]CaseResult, 0)
	}
	doc := struct {
		Kind    string       `json:"kind"`
		Total   int          `json:"total"`
		Passed  int          `json:"passed"`
		Failed  int          `json:"failed"`
		Skipped int          `json:"skipped"`
		Cases   []CaseResult `json:"cases"`
	}{
		Kind:    r.reportKind(),
		Total:   tally.total,
		Passed:  tally.passed,
		Failed:  tally.failed,
		Skipped: tally.skipped,
		Cases:   cases,
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", fmt.Errorf("render JSON report: %w", err)
	}
	return string(out) + "\n", nil
}

// HTML renders the report as a self-contained HTML document.
//
// The document fetches nothing: its appearance is inlined from
// internal/htmlstyle, the same declaration the exported schema document and
// the migration safety report read, so a pass is the same green on all three.
//
// The shell is written directly and only the cases go through a template, so
// nothing trusted has to be handed to html/template as pre-escaped markup.
func (r *Report) HTML() (string, error) {
	tally := r.counts()
	body := struct {
		Kind    string
		Total   int
		Passed  int
		Failed  int
		Skipped int
		Cases   []CaseResult
	}{r.reportKind(), tally.total, tally.passed, tally.failed, tally.skipped, r.Cases}

	var b strings.Builder
	b.WriteString(htmlstyle.Head(body.Kind+" test report", reportCSS))
	if err := reportHTMLTemplate.Execute(&b, body); err != nil {
		return "", fmt.Errorf("render HTML report: %w", err)
	}
	b.WriteString(htmlstyle.Footer("Rendered by Ptah from the test run. " +
		"This file is self-contained: opening it fetches nothing."))
	b.WriteString("</div></body>\n</html>\n")
	return b.String(), nil
}

// reportCSS is what this report adds to the shared appearance: the case and
// step list, which nothing else here has.
const reportCSS = `
.tag.pass { background: var(--ok-soft); border-color: transparent; color: var(--ok); }
.tag.fail { background: var(--danger-soft); border-color: transparent; color: var(--danger); }
.tag.caught { background: var(--warn-soft); border-color: transparent; color: var(--warn); }
.tag.skip, .tag.log { color: var(--text-mute); font-weight: 400; }
.case { border-top: 1px solid var(--border); padding: 12px 18px; }
.case:first-child { border-top: 0; }
.case-head { display: flex; align-items: baseline; gap: 10px; }
.case-name { font: 500 14px var(--mono); }
.steps { list-style: none; margin: 8px 0 0; padding: 0 0 0 2px; display: grid; gap: 5px; }
.steps li { display: flex; align-items: baseline; gap: 8px; font-size: 13.5px; }
.step-name { font-family: var(--mono); font-size: 13px; }
.noun { font: 500 10.5px var(--mono); letter-spacing: .08em; text-transform: uppercase; color: var(--text-mute); }
.detail { color: var(--text-mute); font-family: var(--mono); font-size: 12.5px; }
`

var reportHTMLTemplate = template.Must(template.New("dbtest-report").Parse(`<body><div class="page">
<h1>{{.Kind}} test report</h1>
<div class="lede">Declarative cases, run against a database</div>
<div class="stats">
<div class="stat"><div class="stat-n">{{.Total}}</div><div class="stat-l">cases</div></div>
<div class="stat"><div class="stat-n">{{.Passed}}</div><div class="stat-l">passed</div></div>
<div class="stat"><div class="stat-n">{{.Failed}}</div><div class="stat-l">failed</div></div>
<div class="stat"><div class="stat-n">{{.Skipped}}</div><div class="stat-l">skipped</div></div>
</div>
<h2>Cases</h2>
<div class="card">
{{range .Cases}}
<div class="case">
  <div class="case-head">
    <strong class="tag {{.StatusClass}}">{{.StatusLabel}}</strong>
    <span class="case-name">{{.Name}}</span>
  </div>
  <ul class="steps">
    {{range .Steps}}
    <li>
      <span class="tag {{.StatusClass}}">{{.StatusLabel}}</span>
      <span class="noun">{{.Noun}}</span>
      <span class="step-name">{{.Name}}</span>
      {{if .Detail}}<span class="detail">{{.Detail}}</span>{{end}}
    </li>
    {{end}}
  </ul>
</div>
{{end}}
</div>
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
	if !opts.AllowExternalCommands {
		if err := refuseExternalSteps(opts.Cases); err != nil {
			return nil, err
		}
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
		dirFormat = migrationfile.DirFormatPtah
	}

	run := func(ctx context.Context, conn *dbschema.DatabaseConnection, c Case) (CaseResult, error) {
		r := &runner{
			conn:            conn,
			migrationsDir:   opts.MigrationsDir,
			migrationsFS:    opts.MigrationsFS,
			dirFormat:       dirFormat,
			desiredSchema:   desiredSchema,
			seedDir:         opts.SeedDir,
			revisionsSchema: opts.RevisionsSchema,
			externalTimeout: opts.ExternalTimeout,
		}
		r.migrateTo = r.runMigrateTo
		r.applySchema = r.runApplySchema
		return r.runCase(ctx, c)
	}
	// Migration tests need no per-database provisioning: each case applies its
	// own migrations via migrate_to.
	return runCases(ctx, opts.DBURL, "MIGRATION", opts.Cases, opts.Parallelism, nil, run)
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
func runCases(
	ctx context.Context,
	dbURL, kind string,
	cases []Case,
	parallelism int,
	provision provisionFunc,
	run caseRunner,
) (*Report, error) {
	// A file that asks for parallel opts into per-case isolation, and that is
	// the whole of what `parallel` buys: each case gets a database of its own
	// on the server the URL names, created now and removed afterwards. Without
	// it an explicit URL keeps its documented behavior of one shared database,
	// a contract this does not change -- a file that never says `parallel`
	// never reaches that branch.
	if err := refuseParallelWithoutIsolation(cases, dbURL); err != nil {
		return nil, err
	}
	if anyParallel(cases) {
		return runParallelCases(ctx, kind, cases, dbURL, parallelism, provision, run)
	}
	if dbURL != "" {
		return runExplicitCases(ctx, dbURL, kind, cases, provision, run)
	}

	report := &Report{Cases: make([]CaseResult, 0, len(cases)), kind: kind}
	for i := range cases {
		if cases[i].Skip {
			report.Cases = append(report.Cases, skippedCaseResult(cases[i]))
			continue
		}
		result, err := runEphemeralCase(ctx, cases[i], dbURL, provision, run)
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
		if cases[i].Skip {
			report.Cases = append(report.Cases, skippedCaseResult(cases[i]))
			continue
		}
		result, err := run(ctx, conn, cases[i])
		if err != nil {
			return nil, err
		}
		report.Cases = append(report.Cases, result)
	}
	return report, nil
}

// skippedCaseResult is what a skipped case contributes to a report.
//
// It carries no steps, because none ran. A result that listed them as passed
// would put a proof in the report for statements the database never saw, which
// is worse than the case being absent.
func skippedCaseResult(c Case) CaseResult {
	return CaseResult{Name: c.Name, Skipped: true}
}

// runEphemeralCase runs one case against a fresh SQLite database that exists
// only for that case, so state created by one case is never visible to another.
func runEphemeralCase(
	ctx context.Context,
	c Case,
	baseURL string,
	provision provisionFunc,
	run caseRunner,
) (CaseResult, error) {
	// A base URL names a server rather than the database this case runs
	// against: the case gets one of its own there, created now and removed
	// afterwards.
	scratch, err := openScratch(ctx, baseURL)
	if err != nil {
		return CaseResult{}, err
	}
	defer func() { _ = scratch.Close(ctx) }()

	database, err := shadowdb.Open(ctx, scratch.URL(), "ptah-dbtest-*")
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
	migrationsFS  fs.FS
	dirFormat     migrationfile.DirFormat
	desiredSchema *schemamodel.Database
	seedDir       string
	// externalTimeout bounds one external step. Zero selects
	// [DefaultExternalTimeout].
	externalTimeout time.Duration
	// revisionsSchema is the schema the migrate_to migrator records revisions
	// in. Empty keeps the connection default.
	revisionsSchema string
	// migrateTo handles a migrate_to step. Migration tests wire it to
	// [runner.runMigrateTo]; schema tests wire it to a rejection because a schema
	// test applies a desired schema directly and has no migrations to move
	// between. It must be set before [runner.execStep] is called.
	migrateTo func(ctx context.Context, target string) (passed bool, detail string)
	// applySchema handles an apply_schema step.
	applySchema func(ctx context.Context) (passed bool, detail string)
	// resolveSchema and applyPlan handle the two steps a plan case adds. They
	// are nil for a run that never sees one; a case using the step then fails
	// with the reason rather than skipping the state it meant to establish
	// (stokaro/ptah#1211).
	resolveSchema func(url string) (*schemamodel.Database, error)
	applyPlan     func(ctx context.Context, conn *dbschema.DatabaseConnection, url string) error
}

func (r *runner) runCase(ctx context.Context, c Case) (CaseResult, error) {
	result := CaseResult{Name: c.Name, Passed: true, Steps: make([]StepResult, 0, len(c.Steps))}
	bodyErr := r.runCaseBody(ctx, c, &result)
	r.runCaseCleanup(ctx, c, &result)
	if bodyErr != nil {
		return result, bodyErr
	}
	return result, nil
}

// runCaseBody runs the steps the author wrote, stopping at the first failure.
//
// A returned error is the run aborting rather than the case failing, and the
// caller still runs cleanup before propagating it -- which is the whole reason
// this is a separate function rather than an early return.
func (r *runner) runCaseBody(ctx context.Context, c Case, result *CaseResult) error {
	for i := range c.Steps {
		step := c.Steps[i]
		passed, detail := r.execStep(ctx, step)
		outcome := StepResult{Name: step.Name, Passed: passed, Detail: detail}
		outcome.Kind = stepOutcomeKind(step, outcome)
		result.Steps = append(result.Steps, outcome)
		if err := ctx.Err(); err != nil {
			result.Passed = false
			return fmt.Errorf("run test case %q: %w", c.Name, err)
		}
		if !passed {
			// Stop at the first failure: later steps almost always depend on the
			// state the failed step was supposed to establish, so running them
			// would produce noise rather than signal.
			result.Passed = false
			return nil
		}
	}
	return nil
}

// cleanupGrace bounds a cleanup that runs after the caller has already given
// up. It is short because the work is a handful of statements against a
// database the run is holding open, and unbounded teardown on a canceled run is
// how a test suite stops being interruptible.
const cleanupGrace = 30 * time.Second

// runCaseCleanup runs the case's cleanup steps in reverse written order.
//
// Reverse order is what makes a cleanup written beside the setup it undoes
// correct without the author thinking about it: the last thing created is the
// first thing removed.
//
// Every step runs even when an earlier one failed, which is the opposite of the
// body's rule and deliberately so. A cleanup exists to release something, and
// stopping at the first failure would leave everything after it held -- a
// dropped table is not a reason to skip dropping the next one.
//
// A canceled run still cleans up, against a context detached from the
// cancellation and bounded by [cleanupGrace]. Running teardown on the canceled
// context instead would make every statement fail instantly, which reports as a
// cleanup that ran and failed rather than as one that never had a chance.
func (r *runner) runCaseCleanup(ctx context.Context, c Case, result *CaseResult) {
	if len(c.Cleanup) == 0 {
		return
	}
	if ctx.Err() != nil {
		detached, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupGrace)
		defer cancel()
		ctx = detached
	}

	for _, step := range slices.Backward(c.Cleanup) {
		passed, detail := r.execStep(ctx, step)
		result.Steps = append(result.Steps, StepResult{
			Name:   step.Name,
			Passed: passed,
			Detail: detail,
			Kind:   StepKindCleanup,
		})
		if !passed {
			result.Passed = false
			result.CleanupFailed = true
		}
	}
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
	case stepKindExternal:
		return r.runExternal(ctx, step.External)
	case stepKindEstablishSchema:
		return r.runEstablishSchema(ctx, step.EstablishSchema)
	case stepKindApplyPlan:
		return r.runApplyPlan(ctx, step.ApplyPlan)
	case stepKindLog:
		// Reaches no database and has no way to fail, so it never decides
		// whether a case passed. It is executed rather than skipped so its
		// position among the steps is what the report shows.
		return true, step.Log
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
	fsys := r.migrationsFS
	if fsys == nil {
		fsys = os.DirFS(r.migrationsDir)
	}
	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(r.dirFormat))
	if err != nil {
		return nil, err
	}
	m := migrator.NewMigrator(r.conn, provider).WithLogger(slog.New(slog.DiscardHandler))
	if r.revisionsSchema != "" {
		// The table name stays the revision format's own default; only the
		// schema moves, which is the single axis Atlas's --revisions-schema
		// names.
		m = m.WithMigrationsTable(r.revisionsSchema, "")
	}
	return m, nil
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

// stepOutcomeKind labels an outcome the pass flag describes wrongly.
//
// It reads the step rather than the detail string, because a detail is prose a
// renderer may reword and a reader may not parse. A failed expected-failure is
// deliberately unlabeled: it did not catch anything, and calling it caught
// would put the label on the case where it is least true.
func stepOutcomeKind(step Step, outcome StepResult) StepKind {
	kind, _ := step.kind()
	if kind == stepKindLog {
		return StepKindLog
	}
	if !outcome.Passed || kind != stepKindAssert {
		return ""
	}
	if step.Assert.ExpectAnyError || step.Assert.ErrorMatches != "" || step.Assert.ErrorContains != "" {
		return StepKindCaught
	}
	return ""
}

func (r *runner) runAssert(ctx context.Context, a *Assertion) (passed bool, detail string) {
	switch {
	case a.RowCount != nil:
		return r.assertRowCount(ctx, a.Query, *a.RowCount)
	case a.Scalar != nil:
		return r.assertScalar(ctx, a.Query, *a.Scalar)
	case a.ResultSet != nil:
		return r.assertResultSet(ctx, a.Query, *a.ResultSet, a.ResultLayout)
	case a.Match != "":
		return r.assertMatch(ctx, a.Query, a.Match)
	case a.True:
		return r.assertTrue(ctx, a.Query, a.Message)
	case a.ErrorMatches != "":
		return r.assertErrorMatches(ctx, a.Query, a.ErrorMatches)
	case a.ExpectAnyError:
		return r.assertAnyError(ctx, a.Query)
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
	if interruptedRatherThanRefused(err) {
		return false, fmt.Sprintf("the run was interrupted rather than the statement refused: %v", err)
	}
	if !strings.Contains(err.Error(), want) {
		return false, fmt.Sprintf("expected error to contain %q, got %q", want, err.Error())
	}
	return true, fmt.Sprintf("error contained %q", want)
}

// assertResultSet compares a whole result against the rendering the layout
// names.
//
// It reads every row rather than the first, so a query returning more than the
// author expected fails rather than passing on its first row -- which is the
// difference between this and Scalar, and the reason both exist.
func (r *runner) assertResultSet(
	ctx context.Context,
	query, want string,
	layout ResultLayout,
) (passed bool, detail string) {
	columns, rows, err := r.scanResultSet(ctx, query)
	if err != nil {
		return false, fmt.Sprintf("result_set query failed: %v", err)
	}
	got := renderResultSet(columns, rows, layout)
	if got != want {
		return false, fmt.Sprintf("expected result set %q, got %q", want, got)
	}
	return true, fmt.Sprintf("result set matched (%d row(s))", len(rows))
}

// scanResultSet reads the whole result, formatting each value the way every
// other comparison in this package formats one.
func (r *runner) scanResultSet(ctx context.Context, query string) ([]string, [][]string, error) {
	queried, err := r.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = queried.Close() }()

	columns, err := queried.Columns()
	if err != nil {
		return nil, nil, err
	}
	var rows [][]string
	for queried.Next() {
		dests := make([]any, len(columns))
		for i := range dests {
			dests[i] = new(any)
		}
		if err := queried.Scan(dests...); err != nil {
			return nil, nil, err
		}
		row := make([]string, len(columns))
		for i := range dests {
			row[i] = normalizeScalar(*(dests[i].(*any)))
		}
		rows = append(rows, row)
	}
	if err := queried.Err(); err != nil {
		return nil, nil, err
	}
	return columns, rows, nil
}

// renderResultSet lays a result out for comparison.
func renderResultSet(columns []string, rows [][]string, layout ResultLayout) string {
	if layout == ResultLayoutTable {
		return renderResultTable(columns, rows)
	}
	lines := make([]string, 0, len(rows))
	for _, row := range rows {
		lines = append(lines, strings.Join(row, ","))
	}
	return strings.Join(lines, "\n")
}

// renderResultTable lays a result out as a header, a rule, and padded cells.
//
// Every column is as wide as its widest cell including the header, so the rule
// lines up with the separators above and below it and a diff of two results
// stays readable rather than shifting every column when one value grows.
func renderResultTable(columns []string, rows [][]string) string {
	widths := make([]int, len(columns))
	for i, column := range columns {
		widths[i] = len(column)
	}
	for _, row := range rows {
		for i, cell := range row {
			widths[i] = max(widths[i], len(cell))
		}
	}

	var b strings.Builder
	for i, column := range columns {
		if i > 0 {
			b.WriteString("|")
		}
		fmt.Fprintf(&b, " %-*s ", widths[i], column)
	}
	b.WriteString("\n")
	for i := range columns {
		if i > 0 {
			b.WriteString("+")
		}
		b.WriteString(strings.Repeat("-", widths[i]+2))
	}
	for _, row := range rows {
		b.WriteString("\n")
		for i, cell := range row {
			if i > 0 {
				b.WriteString("|")
			}
			fmt.Fprintf(&b, " %-*s ", widths[i], cell)
		}
	}
	return b.String()
}

// assertMatch is the regexp-shaped sibling of assertScalar: the same scanned
// first value, formatted the same way, tested against a pattern rather than
// compared for equality.
//
// The pattern is unanchored, so it is a search rather than a full match. It is
// compiled here rather than carried on the assertion because a [Case] is a
// plain value an embedder may build directly, and a compiled regexp on it would
// be a field nothing could set meaningfully.
func (r *runner) assertMatch(ctx context.Context, query, pattern string) (passed bool, detail string) {
	expression, err := regexp.Compile(pattern)
	if err != nil {
		return false, fmt.Sprintf("match is not a valid regular expression: %v", err)
	}
	got, err := r.scanFirstValue(ctx, query)
	if err != nil {
		return false, fmt.Sprintf("match query failed: %v", err)
	}
	if !expression.MatchString(got) {
		return false, fmt.Sprintf("expected %q to match %q", got, pattern)
	}
	return true, fmt.Sprintf("matched %q", pattern)
}

// assertTrue requires the query to answer one value the driver reports as true.
//
// A false value is a test failure rather than an invalid case: the author asked
// a question and the database answered no. A query that returns no row at all
// is a different thing — nothing answered — and is reported as such.
func (r *runner) assertTrue(ctx context.Context, query, message string) (passed bool, detail string) {
	got, err := r.scanFirstValue(ctx, query)
	if err != nil {
		return false, fmt.Sprintf("true query failed: %v", err)
	}
	if isTruthyValue(got) {
		return true, "true"
	}
	if message == "" {
		return false, fmt.Sprintf("expected a true value, got %q", got)
	}
	return false, fmt.Sprintf("expected a true value, got %q: %s", got, message)
}

// assertErrorMatches is assertErrorContains with a pattern instead of a
// substring.
func (r *runner) assertErrorMatches(ctx context.Context, query, pattern string) (passed bool, detail string) {
	expression, err := regexp.Compile(pattern)
	if err != nil {
		return false, fmt.Sprintf("error_matches is not a valid regular expression: %v", err)
	}
	runErr := r.runExpectingError(ctx, query)
	if runErr == nil {
		return false, fmt.Sprintf("expected an error matching %q, but the query succeeded", pattern)
	}
	if interruptedRatherThanRefused(runErr) {
		return false, fmt.Sprintf("the run was interrupted rather than the statement refused: %v", runErr)
	}
	if !expression.MatchString(runErr.Error()) {
		return false, fmt.Sprintf("expected error %q to match %q", runErr.Error(), pattern)
	}
	return true, fmt.Sprintf("error matched %q", pattern)
}

// assertAnyError requires the query to fail without constraining the message.
//
// It is the weakest expectation of the three, and it is deliberately still an
// expectation: a statement that succeeds fails the case, because the author
// said it should not.
func (r *runner) assertAnyError(ctx context.Context, query string) (passed bool, detail string) {
	err := r.runExpectingError(ctx, query)
	if err == nil {
		return false, "expected an error, but the query succeeded"
	}
	if interruptedRatherThanRefused(err) {
		return false, fmt.Sprintf("the run was interrupted rather than the statement refused: %v", err)
	}
	return true, fmt.Sprintf("error occurred: %v", err)
}

// scanFirstValue runs query and returns its first column of its first row,
// formatted the way every value comparison in this package formats one.
func (r *runner) scanFirstValue(ctx context.Context, query string) (string, error) {
	rows, err := r.conn.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if len(columns) == 0 {
		return "", fmt.Errorf("query returned no columns")
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", err
		}
		return "", fmt.Errorf("query returned no rows")
	}
	dests := make([]any, len(columns))
	for i := range dests {
		dests[i] = new(any)
	}
	if err := rows.Scan(dests...); err != nil {
		return "", err
	}
	if err := rows.Close(); err != nil {
		return "", err
	}
	return normalizeScalar(*(dests[0].(*any))), nil
}

// isTruthyValue reads a formatted scalar as a boolean.
//
// Engines answer a boolean expression differently -- PostgreSQL prints `true`,
// SQLite and MySQL answer 1 -- so the set is spelled out rather than derived
// from a single driver's behavior.
func isTruthyValue(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "t", "1":
		return true
	default:
		return false
	}
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

// runEstablishSchema brings the database to the state a schema source
// describes, which is what a plan case does before applying anything: a plan is
// only meaningful against the state it was computed for.
func (r *runner) runEstablishSchema(
	ctx context.Context,
	step *SchemaSourceStep,
) (passed bool, detail string) {
	if r.resolveSchema == nil {
		return false, "schema steps are only available in a plan test run"
	}
	desired, err := r.resolveSchema(step.URL)
	if err != nil {
		return false, fmt.Sprintf("schema %s failed: %v", step.URL, err)
	}
	// Applied through the same convergence path apply_schema uses, so a plan
	// case starts from a state built exactly the way every other test builds
	// one.
	if _, err := applyDesiredSchema(ctx, r.conn, desired); err != nil {
		return false, fmt.Sprintf("schema %s failed: %v", step.URL, err)
	}
	return true, ""
}

// runApplyPlan executes a saved plan file, which is the action a plan case
// exists to check.
func (r *runner) runApplyPlan(ctx context.Context, step *ApplyPlanStep) (passed bool, detail string) {
	if r.applyPlan == nil {
		return false, "apply steps are only available in a plan test run"
	}
	if err := r.applyPlan(ctx, r.conn, step.URL); err != nil {
		return false, fmt.Sprintf("apply %s failed: %v", step.URL, err)
	}
	return true, ""
}

// openScratch provisions the database one case runs against.
//
// An empty base URL keeps the historical behavior: shadowdb creates a SQLite
// file in a temporary directory. A base URL names a server, and the case gets a
// database of its own on it, because that is what a caller asking for a
// throwaway target means and what keeps two cases from deciding each other's
// results.
func openScratch(ctx context.Context, baseURL string) (*scratchdb.Scratch, error) {
	if baseURL == "" {
		return nil, nil
	}
	scratch, err := scratchdb.Provision(ctx, baseURL)
	if err != nil {
		return nil, fmt.Errorf("provision a database for this case: %w", err)
	}
	return scratch, nil
}

// interruptedRatherThanRefused reports whether err is the run being stopped
// rather than the statement being refused.
//
// A caught step passes because the DATABASE rejected what it was asked to do.
// A canceled context and an expired deadline are neither: the statement may
// never have reached the server, and counting them as the expected failure
// makes an interrupted suite report that its refusals all occurred. That is the
// one misclassification #2866 names, and it is the one the runtime can identify
// exactly -- a dropped connection surfaces as a driver-specific error with no
// portable sentinel, so it is not claimed here.
func interruptedRatherThanRefused(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
