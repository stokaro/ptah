package dbtest

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

// Options configures a single [RunMigrationTest] invocation.
type Options struct {
	// Cases are the test cases to run, in order.
	Cases []Case
	// MigrationsDir is the directory containing migration files. It is required
	// only when a case has a migrate_to step.
	MigrationsDir string
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

// Report is the outcome of a [RunMigrationTest] run.
type Report struct {
	Cases []CaseResult
}

// CaseResult is the outcome of a single [Case]. Passed is true only when every
// executed step passed.
type CaseResult struct {
	Name   string
	Steps  []StepResult
	Passed bool
}

// StepResult is the outcome of a single [Step]. Detail carries a short
// human-readable explanation of the result, especially on failure.
type StepResult struct {
	Name   string
	Passed bool
	Detail string
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
	b.WriteString("=== MIGRATION TEST ===\n")

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

// RunMigrationTest runs the test cases in opts against a database and returns a
// report describing every case and step. A returned error indicates the run
// itself could not be set up (for example, the test cases are invalid or the
// database is unreachable); ordinary assertion failures are captured in the
// report, not returned as an error, so callers should inspect [Report.Failed].
func RunMigrationTest(ctx context.Context, opts Options) (*Report, error) {
	if err := validateCases(opts.Cases); err != nil {
		return nil, fmt.Errorf("invalid test cases: %w", err)
	}

	dirFormat := opts.DirFormat
	if dirFormat == "" {
		dirFormat = migrator.MigrationDirFormatPtah
	}

	report := &Report{Cases: make([]CaseResult, 0, len(opts.Cases))}

	// An explicit database URL is a single throwaway the caller owns, so all
	// cases share one connection and the caller is responsible for isolating
	// them. The default (ephemeral) mode instead gives each case its own fresh
	// SQLite database so cases cannot contaminate one another.
	if opts.DBURL != "" {
		conn, err := dbschema.ConnectToDatabase(ctx, opts.DBURL)
		if err != nil {
			return nil, fmt.Errorf("connect to test database: %w", err)
		}
		defer dbschema.CloseAndWarn(conn)

		r := &runner{conn: conn, migrationsDir: opts.MigrationsDir, dirFormat: dirFormat}
		for i := range opts.Cases {
			report.Cases = append(report.Cases, r.runCase(ctx, opts.Cases[i]))
		}
		return report, nil
	}

	for i := range opts.Cases {
		result, err := runEphemeralCase(ctx, opts.Cases[i], opts.MigrationsDir, dirFormat)
		if err != nil {
			return nil, err
		}
		report.Cases = append(report.Cases, result)
	}
	return report, nil
}

// runEphemeralCase runs one case against a fresh SQLite database that exists
// only for that case, so state created by one case is never visible to another.
func runEphemeralCase(ctx context.Context, c Case, migrationsDir string, dirFormat migrator.MigrationDirFormat) (CaseResult, error) {
	tmpDir, err := os.MkdirTemp("", "ptah-dbtest-*")
	if err != nil {
		return CaseResult{}, fmt.Errorf("create ephemeral database directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbURL := "sqlite://" + filepath.Join(tmpDir, "dbtest.db")
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	if err != nil {
		return CaseResult{}, fmt.Errorf("connect to ephemeral test database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	r := &runner{conn: conn, migrationsDir: migrationsDir, dirFormat: dirFormat}
	return r.runCase(ctx, c), nil
}

// runner executes steps against a single shared database connection.
type runner struct {
	conn          *dbschema.DatabaseConnection
	migrationsDir string
	dirFormat     migrator.MigrationDirFormat
}

func (r *runner) runCase(ctx context.Context, c Case) CaseResult {
	result := CaseResult{Name: c.Name, Passed: true, Steps: make([]StepResult, 0, len(c.Steps))}
	for i := range c.Steps {
		step := c.Steps[i]
		passed, detail := r.execStep(ctx, step)
		result.Steps = append(result.Steps, StepResult{Name: step.Name, Passed: passed, Detail: detail})
		if !passed {
			// Stop at the first failure: later steps almost always depend on the
			// state the failed step was supposed to establish, so running them
			// would produce noise rather than signal.
			result.Passed = false
			break
		}
	}
	return result
}

func (r *runner) execStep(ctx context.Context, step Step) (passed bool, detail string) {
	kind, _ := step.kind()
	switch kind {
	case stepKindMigrateTo:
		return r.runMigrateTo(ctx, step.MigrateTo)
	case stepKindExec:
		return r.runExec(ctx, step.Exec)
	case stepKindAssert:
		return r.runAssert(ctx, step.Assert)
	default:
		return false, "step performs no action"
	}
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
		if err != nil {
			return false, fmt.Sprintf("invalid migrate_to target %q: expected an integer, \"latest\", or \"0\"", target)
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
