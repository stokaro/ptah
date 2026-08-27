// Package agentgate runs the deterministic checks that stand between an
// agent-written artifact and a report that the write succeeded.
//
// # Why the gates are here and not in the tool description
//
// A tool description asking a model to validate its own work is a suggestion.
// The model may comply, may forget, may be told not to by a file it read, and
// may report success either way -- and the person reading the transcript cannot
// tell those apart. Ariga's stated reason for not shipping an Atlas MCP server
// is exactly this: instructions to an agent are not guardrails.
//
// So the gates are not offered to the model. [Runner.Run] is called by the
// applier, on the bytes that reached the disk, and its report is part of the
// operation's result whether the model asked for it or not. A model cannot skip
// a gate, and cannot claim a gate passed, because it is not the one running it
// or reporting it.
//
// # What a gate is allowed to need
//
// Every gate here is static: it reads files and returns diagnostics. None of
// them needs a database, because a gate that needed one would be a gate that is
// skipped on the machines that do not have one -- and a skipped check reads as
// a pass to everything downstream. The checks that genuinely need a dev
// database stay outside this package until a phase can supply one out of band.
package agentgate

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agentworkspace"
	"go.5x5.cz/ptah/internal/migrationvalidate"
	"go.5x5.cz/ptah/internal/schemaload"
	"go.5x5.cz/ptah/internal/schemavalidate"
	"go.5x5.cz/ptah/internal/sqllint"
	"go.5x5.cz/ptah/migration/dbtest"
	"go.5x5.cz/ptah/migration/migrationfile"
)

// Gate names. They are stable identifiers a report is keyed by, so a client can
// tell "the schema stopped loading" from "the schema loads and is invalid"
// without reading prose.
const (
	GateMigrationIntegrity = "migration-integrity"
	GateMigrationSQL       = "migration-sql"
	GateSchemaLoad         = "schema-load"
	GateSchemaValidate     = "schema-validate"
	GateSchemaRender       = "schema-render"
	GateTestParse          = "test-parse"
)

// Severity is how much a diagnostic matters.
//
// Only [SeverityError] fails a gate. A warning is reported and does not block,
// because a lint opinion about a migration somebody is about to review is
// information rather than a veto.
const (
	SeverityError   = "error"
	SeverityWarning = "warning"
)

// Diagnostic is one thing a gate found.
type Diagnostic struct {
	Gate     string `json:"gate"`
	Severity string `json:"severity"`
	// Path is relative to the artifact scope, empty for a diagnostic about the
	// scope as a whole.
	Path    string `json:"path,omitempty"`
	Line    int    `json:"line,omitempty"`
	Rule    string `json:"rule,omitempty"`
	Message string `json:"message"`
}

// Key identifies a diagnostic across two runs of the same gate.
//
// The line number is deliberately absent. An inserted line moves every
// diagnostic below it, and a comparison that treated the moved copy as new
// would report a patch as introducing problems it only shifted.
func (d Diagnostic) Key() string {
	return strings.Join([]string{d.Gate, d.Path, d.Rule, d.Message}, "\x00")
}

// Result is one gate's outcome.
type Result struct {
	Gate string `json:"gate"`
	OK   bool   `json:"ok"`
	// Skipped reports a gate that did not run. It is a first-class field rather
	// than an absence, because a check that quietly does not run reads as a
	// check that passed.
	Skipped     bool         `json:"skipped,omitempty"`
	SkipReason  string       `json:"skip_reason,omitempty"`
	Diagnostics []Diagnostic `json:"diagnostics"`
}

// Report is every gate's outcome for one run.
type Report struct {
	Results []Result `json:"results"`
	// OK reports that every gate that ran found no error.
	OK bool `json:"ok"`
	// Skipped counts the gates that did not run, so a caller cannot read OK
	// without noticing that some of the checking did not happen.
	Skipped int `json:"skipped"`
}

// Errors returns every error-severity diagnostic in the report.
func (r Report) Errors() []Diagnostic {
	found := make([]Diagnostic, 0)
	for _, result := range r.Results {
		for _, diagnostic := range result.Diagnostics {
			if diagnostic.Severity == SeverityError {
				found = append(found, diagnostic)
			}
		}
	}
	return found
}

// Introduced returns the diagnostics present in this report and absent from the
// baseline.
//
// This is what an applier decides on. A directory that already fails to load
// must not make every patch unappliable, and a patch that repairs one must not
// be refused because the repair is incomplete -- the question a gate answers
// for a write is whether the write made things worse.
func (r Report) Introduced(baseline Report) []Diagnostic {
	before := make(map[string]struct{})
	for _, result := range baseline.Results {
		for _, diagnostic := range result.Diagnostics {
			before[diagnostic.Key()] = struct{}{}
		}
	}
	introduced := make([]Diagnostic, 0)
	for _, result := range r.Results {
		for _, diagnostic := range result.Diagnostics {
			if _, seen := before[diagnostic.Key()]; !seen {
				introduced = append(introduced, diagnostic)
			}
		}
	}
	return introduced
}

// Resolved returns the diagnostics the baseline carried and this report does
// not, so a repair workflow can report what it fixed rather than asserting it.
func (r Report) Resolved(baseline Report) []Diagnostic {
	after := make(map[string]struct{})
	for _, result := range r.Results {
		for _, diagnostic := range result.Diagnostics {
			after[diagnostic.Key()] = struct{}{}
		}
	}
	resolved := make([]Diagnostic, 0)
	for _, result := range baseline.Results {
		for _, diagnostic := range result.Diagnostics {
			if _, still := after[diagnostic.Key()]; !still {
				resolved = append(resolved, diagnostic)
			}
		}
	}
	return resolved
}

// Options configures the runner.
type Options struct {
	// Dialect is the target every SQL and schema check is run for. It is
	// required: a lint run without one either guesses or checks nothing, and
	// both answers are worse than refusing.
	Dialect string
	// Version is the server release the operator named, empty for the dialect
	// default. It reaches the linter unchanged, so a rule gated on a capability
	// that varies within a dialect answers for the server the project actually
	// runs rather than for the family's floor.
	Version string
	// Capabilities is the preset resolved from Dialect and Version. A nil
	// preset lets each check fall back to the dialect default, which is what an
	// unpinned run has always done.
	Capabilities capability.Capabilities
	// DirFormat selects the migration directory layout. The zero value lets the
	// integrity check detect it.
	DirFormat migrationfile.DirFormat
}

// Runner runs the gates for one artifact class.
type Runner struct {
	opts Options
}

// New builds a runner, refusing options that would make a gate a formality.
func New(opts Options) (*Runner, error) {
	if opts.Dialect == "" {
		return nil, errors.New("gate dialect is required: a check without a target checks nothing")
	}
	return &Runner{opts: opts}, nil
}

// Run executes every gate that governs the scope's class.
//
// It returns a report rather than an error for anything a gate found; an error
// means a gate could not run at all, which is different from a gate that ran
// and disliked what it saw.
func (r *Runner) Run(ctx context.Context, scope *agentworkspace.Scope) (Report, error) {
	switch scope.Class() {
	case agentpolicy.ClassMigrations:
		return r.runMigrations(ctx, scope)
	case agentpolicy.ClassSchema:
		return r.runSchema(ctx, scope)
	case agentpolicy.ClassTests:
		return r.runTests(scope)
	}
	return Report{}, fmt.Errorf("no gates are defined for artifact class %q", scope.Class())
}

// finish computes the report's summary fields from its results.
func finish(results []Result) Report {
	report := Report{Results: results, OK: true}
	for _, result := range results {
		if result.Skipped {
			report.Skipped++
			continue
		}
		if !result.OK {
			report.OK = false
		}
	}
	return report
}

// pass builds a gate result with no diagnostics.
func pass(gate string) Result {
	return Result{Gate: gate, OK: true, Diagnostics: make([]Diagnostic, 0)}
}

// failure builds a gate result from its diagnostics, failing when any is an
// error.
func failure(gate string, diagnostics []Diagnostic) Result {
	result := Result{Gate: gate, OK: true, Diagnostics: diagnostics}
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == SeverityError {
			result.OK = false
		}
	}
	return result
}

// runMigrations checks the integrity file and lints every SQL file.
func (r *Runner) runMigrations(ctx context.Context, scope *agentworkspace.Scope) (Report, error) {
	integrity, err := r.migrationIntegrity(ctx, scope)
	if err != nil {
		return Report{}, err
	}
	sql, err := r.migrationSQL(scope)
	if err != nil {
		return Report{}, err
	}
	return finish([]Result{integrity, sql}), nil
}

// migrationIntegrity verifies the directory against its own checksum file.
func (r *Runner) migrationIntegrity(
	ctx context.Context,
	scope *agentworkspace.Scope,
) (Result, error) {
	result, err := migrationvalidate.Validate(ctx, migrationvalidate.Options{
		Dir:       scope.Path(),
		DirFormat: r.opts.DirFormat,
	})
	if err != nil {
		return failure(GateMigrationIntegrity, []Diagnostic{{
			Gate:     GateMigrationIntegrity,
			Severity: SeverityError,
			Message:  err.Error(),
		}}), nil
	}
	if result.Integrity == nil || result.Integrity.OK() {
		return pass(GateMigrationIntegrity), nil
	}
	return failure(GateMigrationIntegrity, integrityDiagnostics(result)), nil
}

// integrityDiagnostics turns a checksum comparison into one diagnostic per
// disagreeing file.
func integrityDiagnostics(result migrationvalidate.Result) []Diagnostic {
	diagnostics := make([]Diagnostic, 0)
	for _, name := range result.Integrity.Added {
		diagnostics = append(diagnostics, Diagnostic{
			Gate: GateMigrationIntegrity, Severity: SeverityError, Path: name,
			Message: "file is present in the directory and absent from the integrity file",
		})
	}
	for _, name := range result.Integrity.Removed {
		diagnostics = append(diagnostics, Diagnostic{
			Gate: GateMigrationIntegrity, Severity: SeverityError, Path: name,
			Message: "file is recorded in the integrity file and absent from the directory",
		})
	}
	for _, name := range result.Integrity.Changed {
		diagnostics = append(diagnostics, Diagnostic{
			Gate: GateMigrationIntegrity, Severity: SeverityError, Path: name,
			Message: "file content does not match the integrity file",
		})
	}
	if len(diagnostics) == 0 {
		diagnostics = append(diagnostics, Diagnostic{
			Gate: GateMigrationIntegrity, Severity: SeverityError,
			Message: "the integrity file does not describe this directory",
		})
	}
	return diagnostics
}

// migrationSQL parses and lints every SQL file in the directory.
func (r *Runner) migrationSQL(scope *agentworkspace.Scope) (Result, error) {
	entries, err := scope.List()
	if err != nil {
		return Result{}, err
	}
	diagnostics := make([]Diagnostic, 0)
	for _, entry := range entries {
		if path.Ext(entry.Path) != ".sql" {
			continue
		}
		content, readErr := scope.ReadFile(entry.Path)
		if readErr != nil {
			return Result{}, readErr
		}
		findings, lintErr := sqllint.LintSource(
			sqllint.Source{Name: entry.Path, SQL: string(content)},
			sqllint.Options{
				Dialect:      r.opts.Dialect,
				Version:      r.opts.Version,
				Capabilities: r.opts.Capabilities,
			},
		)
		if lintErr != nil {
			diagnostics = append(diagnostics, Diagnostic{
				Gate: GateMigrationSQL, Severity: SeverityError, Path: entry.Path,
				Message: lintErr.Error(),
			})
			continue
		}
		diagnostics = append(diagnostics, lintDiagnostics(entry.Path, findings)...)
	}
	return failure(GateMigrationSQL, diagnostics), nil
}

// lintDiagnostics converts linter findings, preserving the linter's own
// severity rather than deciding a second time what matters.
func lintDiagnostics(name string, findings []sqllint.Finding) []Diagnostic {
	diagnostics := make([]Diagnostic, 0, len(findings))
	for _, finding := range findings {
		// SQL004 says which statement kinds no rule examined. That is a fact
		// about this linter's coverage rather than about the change under
		// review, and this package has two severities, so it would reach an
		// agent as a warning on almost every migration. `ptah sql lint` is
		// where a person asks how complete the analysis was
		// (stokaro/ptah#1270).
		if finding.Rule == sqllint.RuleStatementsNotAnalyzed {
			continue
		}
		diagnostics = append(diagnostics, Diagnostic{
			Gate:     GateMigrationSQL,
			Severity: severityOf(finding.Severity),
			Path:     name,
			Line:     finding.Line,
			Rule:     finding.Rule,
			Message:  finding.Message,
		})
	}
	return diagnostics
}

// severityOf maps a linter severity onto the two this package distinguishes.
func severityOf(severity sqllint.Severity) string {
	if string(severity) == SeverityError {
		return SeverityError
	}
	return SeverityWarning
}

// runSchema loads, validates and renders the declared schema.
func (r *Runner) runSchema(ctx context.Context, scope *agentworkspace.Scope) (Report, error) {
	database, err := schemaload.LoadContext(ctx, schemaload.Options{
		RootDirs: []string{scope.Path()},
		Dialect:  r.opts.Dialect,
	})
	if err != nil {
		// A schema that will not load answers the other two gates as well: they
		// have nothing to run against. They are reported as skipped with the
		// reason rather than passed, because a gate that did not run is not a
		// gate that agreed.
		return finish([]Result{
			failure(GateSchemaLoad, []Diagnostic{{
				Gate: GateSchemaLoad, Severity: SeverityError, Message: err.Error(),
			}}),
			{Gate: GateSchemaValidate, Skipped: true, SkipReason: "the schema did not load"},
			{Gate: GateSchemaRender, Skipped: true, SkipReason: "the schema did not load"},
		}), nil
	}

	problems := schemavalidate.CollectWithCapabilities(database, r.opts.Dialect, r.capabilities())
	validate := make([]Diagnostic, 0, len(problems))
	for _, problem := range problems {
		validate = append(validate, Diagnostic{
			Gate: GateSchemaValidate, Severity: SeverityError, Rule: problem.Kind,
			Message: schemaProblemMessage(problem),
		})
	}

	render := pass(GateSchemaRender)
	if _, renderErr := renderer.GetOrderedCreateStatements(database, r.opts.Dialect); renderErr != nil {
		render = failure(GateSchemaRender, []Diagnostic{{
			Gate: GateSchemaRender, Severity: SeverityError, Message: renderErr.Error(),
		}})
	}
	return finish([]Result{pass(GateSchemaLoad), failure(GateSchemaValidate, validate), render}), nil
}

// capabilities is the preset the checks run against, resolved to the dialect
// default when the operator named no server.
func (r *Runner) capabilities() capability.Capabilities {
	if r.opts.Capabilities == nil {
		return capability.ForDialect(r.opts.Dialect)
	}
	return r.opts.Capabilities
}

// schemaProblemMessage names the object a structural problem is about, because
// the message alone frequently does not.
func schemaProblemMessage(problem schemavalidate.Problem) string {
	if problem.Object == "" {
		return problem.Message
	}
	return problem.Object + ": " + problem.Message
}

// runTests parses every declarative test file.
func (r *Runner) runTests(scope *agentworkspace.Scope) (Report, error) {
	entries, err := scope.List()
	if err != nil {
		return Report{}, err
	}
	diagnostics := make([]Diagnostic, 0)
	for _, entry := range entries {
		if !isTestFile(entry.Path) {
			continue
		}
		content, readErr := scope.ReadFile(entry.Path)
		if readErr != nil {
			return Report{}, readErr
		}
		cases, parseErr := dbtest.ParseCases(content)
		if parseErr != nil {
			diagnostics = append(diagnostics, Diagnostic{
				Gate: GateTestParse, Severity: SeverityError, Path: entry.Path,
				Message: parseErr.Error(),
			})
			continue
		}
		if len(cases) == 0 {
			diagnostics = append(diagnostics, Diagnostic{
				Gate: GateTestParse, Severity: SeverityWarning, Path: entry.Path,
				Message: "the file parses and declares no test case",
			})
		}
	}
	return finish([]Result{failure(GateTestParse, diagnostics)}), nil
}

// isTestFile reports the extensions the declarative test loader reads.
func isTestFile(name string) bool {
	switch path.Ext(name) {
	case ".yaml", ".yml":
		return true
	}
	return false
}
