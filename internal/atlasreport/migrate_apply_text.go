package atlasreport

import (
	"fmt"
	"io"
	"strings"
)

// migrateApplyTextRule separates the migrations of the default report from
// its summary.
var migrateApplyTextRule = strings.Repeat("-", 25)

// migrateApplyTextContinuation indents the second and later lines of a
// statement so they line up under the statement's first character, which
// follows the `    -> ` arrow.
const migrateApplyTextContinuation = "       "

// WriteMigrateApplyText writes the report `migrate apply` prints on the compat
// surface without --format: a header, each migration with the statements it
// ran, the database's answer under a statement it refused, and a summary of
// what passed and failed.
//
// It reproduces the report the pinned community binary v1.3.0 prints. Measured
// on SQLite for a run that succeeds, a run that fails at a statement, a dry
// run, a `-- atlas:txmode none` file, a declared delimiter, a baseline,
// `--allow-dirty`, each `--exec-order` and a Flyway directory, the two reports
// are the same byte for byte apart from durations and the SQLite driver's
// wording of an error. It renders from the data a --format template reads, so
// the two outputs cannot drift apart.
//
// Pre-migration checks are Ptah's own lines: the community binary runs none,
// so there is nothing of its to reproduce. A check prints no duration, because
// the run does not time checks one by one.
//
// Native `ptah migrations up` keeps its own output; only the compat surface is
// a contract with an existing pipeline.
func WriteMigrateApplyText(w io.Writer, opts MigrateApplyResultOptions) error {
	if err := validateMigrateApplyResultOptions(opts); err != nil {
		return err
	}
	result, err := buildAtlasMigrateApplyResult(opts)
	if err != nil {
		return err
	}
	_, err = io.WriteString(w, migrateApplyText(result))
	return err
}

// migrateApplyText is the whole default report for one run.
func migrateApplyText(result atlasMigrateApplyResult) string {
	if len(result.Pending) == 0 {
		return "No migration files to execute\n"
	}
	var report strings.Builder
	report.WriteString(result.Header())
	report.WriteString("\n")
	for _, file := range result.Applied {
		report.WriteString("\n")
		writeMigrateApplyFileText(&report, file)
	}
	fmt.Fprintf(&report, "\n  %s\n  %s\n", migrateApplyTextRule, result.Summary("  "))
	return report.String()
}

// writeMigrateApplyFileText writes one migration: its checks, then the
// statements it ran and how it ended. A check that refused stops the file
// before its first statement, so nothing after the refusal is written.
func writeMigrateApplyFileText(report *strings.Builder, file *atlasMigrateApplyAppliedFile) {
	for _, group := range file.Checks {
		if !writeMigrateApplyChecksText(report, file.Version, group) {
			return
		}
	}
	fmt.Fprintf(report, "  -- migrating version %s\n", file.Version)
	for _, statement := range file.Applied {
		writeMigrateApplyStatementText(report, statement)
	}
	if file.Error != nil {
		fmt.Fprintf(report, "    %s\n", file.Error.Text)
		return
	}
	fmt.Fprintf(report, "  -- ok (%s)\n", file.End.Sub(file.Start))
}

// writeMigrateApplyChecksText writes one group of pre-migration checks and
// reports whether every check in it held. A group from a txtar file carries
// that file's name, and a group of `-- +ptah check` directives has none. Only
// the checks that ran are listed.
func writeMigrateApplyChecksText(report *strings.Builder, version string, group *atlasMigrateApplyFileChecks) bool {
	report.WriteString("  -- pre-migration checks of version " + version)
	if group.Name != "" {
		report.WriteString(" (" + group.Name + ")")
	}
	report.WriteString("\n")
	outcome := atlasMigrateApplyChecksOutcomeOf(group)
	for _, check := range outcome.ran {
		writeMigrateApplyStatementText(report, check.Stmt)
	}
	if outcome.refused {
		fmt.Fprintf(report, "    %s\n", group.Error.Text)
		return false
	}
	report.WriteString("  -- ok\n")
	return true
}

// atlasMigrateApplyChecksOutcome is how far one group of checks got: the
// checks that ran, in order, how many held and how many refused, and whether
// the group refused the migration.
type atlasMigrateApplyChecksOutcome struct {
	ran            []*atlasMigrateApplyCheck
	passed, failed int
	refused        bool
}

// atlasMigrateApplyChecksOutcomeOf reads a group's outcome from the marks the
// report put on it. The checks of a group run in order and the first one that
// refuses stops it, so a marked check is the last one that ran. A group
// refusal marks no check: every check ran and none of them held.
func atlasMigrateApplyChecksOutcomeOf(group *atlasMigrateApplyFileChecks) atlasMigrateApplyChecksOutcome {
	if group.Error == nil {
		return atlasMigrateApplyChecksOutcome{ran: group.Stmts, passed: len(group.Stmts)}
	}
	for index, check := range group.Stmts {
		if check.Error != nil {
			return atlasMigrateApplyChecksOutcome{ran: group.Stmts[:index+1], passed: index, failed: 1, refused: true}
		}
	}
	return atlasMigrateApplyChecksOutcome{ran: group.Stmts, failed: max(len(group.Stmts), 1), refused: true}
}

// writeMigrateApplyStatementText writes one statement behind its arrow, with
// every line after the first indented to line up under it.
func writeMigrateApplyStatementText(report *strings.Builder, statement string) {
	report.WriteString("    -> ")
	report.WriteString(strings.ReplaceAll(statement, "\n", "\n"+migrateApplyTextContinuation))
	report.WriteString("\n")
}

// Header is the line the default report opens with. It names the version the
// run moves from only when the database had one. A --format template reads it
// as `.Header`.
//
// The count is the migrations the run selected, and it reads "migrations" even
// at one, as the community binary prints it.
func (r atlasMigrateApplyResult) Header() string {
	if r.Current == "" {
		return fmt.Sprintf("Migrating to version %s (%d migrations in total):", r.Target, len(r.Pending))
	}
	return fmt.Sprintf("Migrating to version %s from %s (%d migrations in total):", r.Target, r.Current, len(r.Pending))
}

// Summary is the footer of the default report: the run's duration, then what
// the migrations, their checks and their statements came to, one line each,
// every line after the first starting with indent. A --format template reads
// it as `.Summary "  "`.
//
// A line with nothing to count is left out, and a count with no failure says
// so by naming no failure, as the community binary does for migrations and
// statements.
func (r atlasMigrateApplyResult) Summary(indent string) string {
	var tally atlasMigrateApplyTally
	for _, file := range r.Applied {
		tally.add(file)
	}
	lines := append([]string{r.End.Sub(r.Start).String()}, tally.lines()...)
	return "-- " + strings.Join(lines, "\n"+indent+"-- ")
}

// File is the migration file an applied entry reports. A --format template
// written for the community binary reads a file's version as `.File.Version`.
func (f *atlasMigrateApplyAppliedFile) File() atlasMigrateApplyFile {
	return f.atlasMigrateApplyFile
}

// atlasMigrateApplyTally counts a run's migrations, checks and statements by
// outcome.
type atlasMigrateApplyTally struct {
	passedFiles, failedFiles   int
	passedChecks, failedChecks int
	passedStmts, failedStmts   int
}

// add counts one applied entry.
//
// Checks count as the report lists them: up to and including the first group
// that refused, whose checks after the refusal did not run.
//
// A failed migration failed at a check or at a statement. A check failure ran
// no statement of the file, so only a failure no check claims counts a failed
// statement. The community binary takes that statement off the passed count,
// because its list always ends with it. Ptah's list holds it only when the
// statement reached the database, so a refusal Ptah raised before sending it
// leaves the passed count alone. A failure that names no statement, such as a
// refused commit, still counts one failed statement: the file failed outside
// its checks.
func (t *atlasMigrateApplyTally) add(file *atlasMigrateApplyAppliedFile) {
	checkFailed := false
	for _, group := range file.Checks {
		outcome := atlasMigrateApplyChecksOutcomeOf(group)
		t.passedChecks += outcome.passed
		t.failedChecks += outcome.failed
		if outcome.refused {
			checkFailed = true
			break
		}
	}
	t.passedStmts += len(file.Applied)
	if file.Error == nil {
		t.passedFiles++
		return
	}
	t.failedFiles++
	if checkFailed {
		return
	}
	t.failedStmts++
	if file.errorListed {
		t.passedStmts--
	}
}

// lines is the summary's count lines, migrations first and statements last.
func (t atlasMigrateApplyTally) lines() []string {
	var lines []string
	lines = append(lines, atlasMigrateApplyOutcome("migration", t.passedFiles, t.failedFiles)...)
	lines = append(lines, atlasMigrateApplyOutcome("check", t.passedChecks, t.failedChecks)...)
	return append(lines, atlasMigrateApplyOutcome("sql statement", t.passedStmts, t.failedStmts)...)
}

// atlasMigrateApplyOutcome is the summary line for one kind of item, such as
// `2 sql statements ok, 1 with errors`, or nothing when there is none.
func atlasMigrateApplyOutcome(noun string, passed, failed int) []string {
	switch {
	case passed > 0 && failed > 0:
		return []string{fmt.Sprintf("%s ok, %d with errors", pluralize(passed, noun, noun+"s"), failed)}
	case passed > 0:
		return []string{pluralize(passed, noun, noun+"s")}
	case failed > 0:
		return []string{pluralize(failed, noun, noun+"s") + " with errors"}
	}
	return nil
}
