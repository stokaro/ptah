package capabilityprobe

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema"
)

// Outcome is the verdict for one capability row.
//
// There are three, and the third is the point of the deliverable. A harness
// with only AGREES and DISAGREES has to put every capability it did not
// measure into one of them, and whichever it picks is a lie: folded into
// agreement it manufactures evidence, folded into disagreement it cries wolf.
type Outcome string

const (
	// Agrees: the server was measured and behaves the way the preset says.
	Agrees Outcome = "AGREES"
	// Disagrees: the server was measured and does not. This is a failure.
	Disagrees Outcome = "DISAGREES"
	// Undecidable: this run did not establish what the server does, or
	// established it and cannot credit it to this release line. Reason is
	// always populated and it never counts as agreement.
	Undecidable Outcome = "UNDECIDABLE"
)

// Row is one capability measured against one server.
type Row struct {
	Capability capability.Capability
	Dialect    string
	Version    string
	// PresetSays is what the capability set Ptah plans with for this server
	// claims.
	PresetSays bool
	// ServerDoes is what the server was observed doing. Valid only when
	// Observed is true.
	ServerDoes bool
	// Observed is true when the deciding statements ran and answered, even if
	// the row is undecidable because the answer cannot be credited to this
	// release line. Keeping the observation is what stops an unattributable
	// row from also being an invisible one.
	Observed bool
	Outcome  Outcome
	// Reason is why the row is undecidable. Empty otherwise.
	Reason string
	// Note is context that belongs on the row whatever its outcome.
	Note string
	// Attempts are the statements this row was decided from, with the
	// server's own answers.
	Attempts []Attempt
}

// Mismatch reports whether the observation contradicts the preset. It is
// independent of Outcome, so a contradiction observed on a line the matrix
// cannot credit is still visible instead of vanishing into UNDECIDABLE.
func (r Row) Mismatch() bool {
	return r.Observed && r.ServerDoes != r.PresetSays
}

// Report is one probe run.
type Report struct {
	// URL is the target with its secrets redacted.
	URL string
	// Dialect is the dialect the connection resolved to, which may differ
	// from the one the URL declared: a CockroachDB server behind a
	// postgres:// URL announces itself in the banner.
	Dialect string
	// Banner is the raw server version string.
	Banner string
	// Version is the corrected product version.
	Version Version
	// Cell is the matrix cell this server fell on.
	Cell Cell
	// Matched is false when no cell covers this release line.
	Matched bool
	// Resolution is what capability.ResolveServerVersion answered for the
	// banner.
	Resolution capability.VersionResolution
	// SessionCapabilities is the set Ptah actually plans with once a physical
	// session is pinned. It can differ from the version resolution: MySQL 8.4+
	// reads its foreign-key reference policy from a session variable, so the
	// same version answers differently depending on how the server is
	// configured.
	SessionCapabilities capability.Capabilities
	// SessionDeltas names the keys where SessionCapabilities differs from the
	// version resolution.
	SessionDeltas []capability.Capability
	// Rows is one entry per registered capability, sorted by key.
	Rows []Row
	// Decidable is how many rows this run was obliged to decide: the
	// registered keys the dialect's plan did NOT declare undecidable in
	// advance, on a line the matrix can credit an observation to. It is zero
	// on an unattributable line, where every row is undecidable by
	// construction and the count would be a demand no run can meet.
	//
	// It is derived from the plan rather than written down per dialect
	// because a number somebody maintains by hand drifts the moment a
	// capability is added, and the drift is silent in the direction that
	// lowers the bar.
	Decidable int
	// Planned is false when the probe has no statement table for this
	// dialect, so nothing was executed at all.
	Planned bool
	// Control is the statement the server had to refuse for any acceptance in
	// this run to be worth reading.
	Control Attempt
	// Namespace is the throwaway schema or database the run used.
	Namespace string
	// Cleanup records the teardown statements.
	Cleanup []Attempt
}

// Count returns how many rows carry an outcome.
func (r *Report) Count(outcome Outcome) int {
	n := 0
	for _, row := range r.Rows {
		if row.Outcome == outcome {
			n++
		}
	}
	return n
}

// Decided returns how many rows this run actually decided.
func (r *Report) Decided() int {
	return r.Count(Agrees) + r.Count(Disagrees)
}

// Mismatches returns every row whose observation contradicts the preset,
// including rows the matrix cannot credit to this line.
func (r *Report) Mismatches() []Row {
	var out []Row
	for _, row := range r.Rows {
		if row.Mismatch() {
			out = append(out, row)
		}
	}
	return out
}

// Err returns the reason this run must fail, or nil.
//
// Failing on "decided nothing" is not defensive padding. A skipped check that
// reads as a passed check is how a matrix comes to certify lines nobody ever
// probed, and this repository has been bitten by that shape before. Failing on
// "decided less than the plan promised" is the same argument one step further
// in: a floor of one row lets twenty-three of twenty-four go quietly
// unmeasured while the run still exits zero, and coverage that can erode
// without turning anything red is coverage nobody is holding.
func (r *Report) Err() error {
	var problems []error

	switch {
	case !r.Planned:
		problems = append(problems, fmt.Errorf(
			"the probe has no statement table for the %s dialect, so it executed nothing against this server", r.Dialect))
	case r.Control.Statement == "":
		problems = append(problems, errors.New("the refusal control never ran"))
	case r.Control.Accepted:
		problems = append(problems, fmt.Errorf(
			"the server ACCEPTED the nonsense control %q, so no acceptance in this run distinguishes "+
				"a supported statement from a connection that agrees with everything",
			collapse(r.Control.Statement)))
	}
	if !r.Matched {
		problems = append(problems, fmt.Errorf(
			"no matrix cell covers %s %s: this release line is not in the matrix, "+
				"so nothing here promotes it to measured (add a cell in cells.go)",
			r.Dialect, r.Version))
	}
	problems = append(problems, r.cellProblems()...)

	for _, row := range r.Mismatches() {
		problems = append(problems, fmt.Errorf(
			"%s: preset says %t, server does %t", row.Capability, row.PresetSays, row.ServerDoes))
	}
	if r.Decided() < r.floor() {
		problems = append(problems, r.coverageProblem())
	}
	return errors.Join(problems...)
}

// floor is the fewest rows a run may decide and still be allowed to report
// success.
//
// On a line the matrix can credit it is every row the plan promised to answer,
// so a run that quietly stopped deciding most of them fails. Everywhere else
// it is one, which keeps the original guard: a probe that decided nothing must
// never read as a probe that passed.
func (r *Report) floor() int {
	return max(1, r.Decidable)
}

// coverageProblem states the shortfall in the terms of whichever floor applied.
func (r *Report) coverageProblem() error {
	if r.Decidable == 0 {
		return fmt.Errorf(
			"this run decided 0 of %d capability rows; a probe that measured nothing must not read as a probe that passed",
			len(r.Rows))
	}
	return fmt.Errorf(
		"this run decided %d of %d capability rows, %d fewer than the %d the %s plan promised to answer; "+
			"coverage that erodes without failing anything is coverage nobody is holding",
		r.Decided(), len(r.Rows), r.Decidable-r.Decided(), r.Decidable, r.Dialect)
}

// cellProblems reports the ways a matched cell can be a lie about the server
// it matched.
func (r *Report) cellProblems() []error {
	if !r.Matched {
		return nil
	}
	var problems []error
	if !r.Cell.Measured() {
		problems = append(problems, fmt.Errorf(
			"matrix cell %s has no measured capability preset (%s)", r.Cell, r.Cell.Note))
		return problems
	}
	if r.Resolution.Saturated {
		problems = append(problems, fmt.Errorf(
			"matrix cell %s claims a measured preset, but the resolver reports the server as past the "+
				"newest measured line (%s): a saturated version receives the dialect default, not this line's answer",
			r.Cell, r.Resolution.NewestMeasured))
	}
	if !maps.Equal(r.Resolution.Capabilities, r.Cell.Preset()) {
		problems = append(problems, fmt.Errorf(
			"matrix cell %s names preset %s, but the resolver handed this server a different set",
			r.Cell, r.Cell.PresetName))
	}
	return problems
}

// Run probes one live server and returns one row per registered capability.
//
// It never wraps the measurement in a transaction. PostgreSQL refuses CREATE
// INDEX CONCURRENTLY inside an explicit transaction block, so BEGIN/ROLLBACK
// isolation would report two true capabilities as false; the run works in a
// throwaway namespace and drops it instead.
func Run(ctx context.Context, dbURL string) (*Report, error) {
	if len(Cells) == 0 {
		return nil, errors.New("the capability matrix declares no cells; refusing to report a vacuous pass")
	}
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", dbschema.FormatDatabaseURL(dbURL), err)
	}
	defer dbschema.CloseAndWarn(conn)

	report := &Report{
		URL:     dbschema.FormatDatabaseURL(dbURL),
		Dialect: platform.NormalizeDialect(conn.Info().Dialect),
		Banner:  conn.Info().Version,
	}
	report.Version, err = ParseVersion(report.Dialect, report.Banner, productVersion(ctx, conn, report.Dialect))
	if err != nil {
		return nil, fmt.Errorf("read the product version of %s: %w", report.URL, err)
	}
	report.Cell, report.Matched = CellFor(report.Dialect, report.Version)
	report.Resolution = capability.ResolveServerVersion(report.Dialect, report.Banner)

	if err := conn.WithSession(ctx, func(pinned *dbschema.DatabaseConnection) error {
		report.SessionCapabilities = pinned.Info().Capabilities
		report.SessionDeltas = deltas(report.Resolution.Capabilities, report.SessionCapabilities)
		return measure(ctx, pinned, report)
	}); err != nil {
		return nil, err
	}
	return report, nil
}

// productVersion asks the server for a version surface cleaner than its
// banner, where one exists. Today only SQL Server has one, and it is the fix
// for the marketing-year parse: SERVERPROPERTY('ProductVersion') answers
// 17.0.4065.4 where @@VERSION opens with "Microsoft SQL Server 2025".
func productVersion(ctx context.Context, conn *dbschema.DatabaseConnection, dialect string) string {
	if dialect != platform.SQLServer {
		return ""
	}
	var value string
	if err := conn.QueryRowContext(ctx, "SELECT CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion'))").Scan(&value); err != nil {
		return ""
	}
	return value
}

// deltas names the keys where two capability sets differ.
func deltas(before, after capability.Capabilities) []capability.Capability {
	var out []capability.Capability
	for _, key := range capability.All() {
		if before.Has(key) != after.Has(key) {
			out = append(out, key)
		}
	}
	slices.Sort(out)
	return out
}

// measure runs the plan inside a throwaway namespace and fills in the rows.
func measure(ctx context.Context, pinned *dbschema.DatabaseConnection, report *Report) error {
	dialectPlan, planned := planFor(report.Dialect)
	report.Planned = planned
	if !planned {
		report.Rows = rowsWithoutPlan(report)
		return nil
	}

	namespace, err := newNamespace()
	if err != nil {
		return err
	}
	report.Namespace = namespace
	s := &session{conn: pinned, dialect: report.Dialect, namespace: namespace}

	enter, leave := namespaceSQL(report.Dialect, namespace)
	if attempts, ok := s.runAll(ctx, enter); !ok {
		return fmt.Errorf("create the throwaway probe namespace: %s", attempts[len(attempts)-1].ServerErr)
	}
	defer func() {
		report.Cleanup = append(s.dropRoles(ctx), s.exec(ctx, leave))
	}()

	report.Control = s.exec(ctx, nonsenseControl)
	observations, attempts := runPlan(ctx, s, dialectPlan)
	if s.broken != nil {
		// A dead session cannot run its own teardown, so the namespace stays
		// behind. Name it: a probe that loses a connection and then silently
		// litters the server is how the next run inherits objects it did not
		// create and reads them as findings.
		return fmt.Errorf("%w (the throwaway namespace %s could not be dropped and is still on the server)",
			s.broken, namespace)
	}
	report.Rows = assemble(report, observations, attempts)
	report.Decidable = decidable(report, dialectPlan)
	return nil
}

// decidable counts the rows this run had to decide for its coverage to be
// intact: every registered key the plan did not declare undecidable in advance.
//
// On a line no observation can be credited to it is zero rather than that
// count. Every row there is undecidable by construction — the resolver hands
// each release of the engine the same preset and the line has not been
// measured directly — so demanding decisions would turn a correctly reported
// limitation into a permanent failure and teach readers to ignore the exit
// code.
func decidable(report *Report, p plan) int {
	if lineReason(report) != "" {
		return 0
	}
	n := 0
	for _, key := range capability.All() {
		if _, declared := p.undecided[key]; !declared {
			n++
		}
	}
	return n
}

// runPlan executes a dialect's experiments in order and collects one
// observation per key.
func runPlan(ctx context.Context, s *session, p plan) (map[capability.Capability]observation, map[capability.Capability][]Attempt) {
	observations := make(map[capability.Capability]observation, len(capability.All()))
	attempts := make(map[capability.Capability][]Attempt, len(capability.All()))

	for key, reason := range p.undecided {
		observations[key] = observation{undecidable: reason}
	}
	for _, current := range p.experiments {
		record := func(obs observation, ran []Attempt) {
			for _, key := range current.decides {
				observations[key] = obs
				attempts[key] = ran
			}
		}
		if missing, ok := unmetRequirement(current, observations); !ok {
			record(cannotDecide(
				"this key presupposes %q, which this run did not decide true, so the deciding statement "+
					"would be refused for the missing statement rather than for this key",
				missing), nil)
			continue
		}
		ran, prepared := s.runAll(ctx, current.setup)
		if !prepared {
			record(cannotDecide(
				"the precondition %q was refused (%s), so the deciding statement never had anything to decide against",
				collapse(ran[len(ran)-1].Statement), collapse(ran[len(ran)-1].ServerErr)), ran)
			continue
		}
		results, decidingAttempts := current.decide(ctx, s)
		executed := make([]Attempt, 0, len(ran)+len(decidingAttempts))
		executed = append(executed, ran...)
		executed = append(executed, decidingAttempts...)
		for _, key := range current.decides {
			observations[key] = results[key]
			attempts[key] = executed
		}
	}
	return observations, attempts
}

// unmetRequirement returns the first requirement that was not decided true.
func unmetRequirement(e experiment, observations map[capability.Capability]observation) (capability.Capability, bool) {
	for _, required := range e.requires {
		obs, seen := observations[required]
		if !seen || obs.undecidable != "" || !obs.does {
			return required, false
		}
	}
	return "", true
}

// rowsWithoutPlan builds the all-undecidable row set for a dialect the probe
// has no statements for.
func rowsWithoutPlan(report *Report) []Row {
	reason := fmt.Sprintf("no probe plan exists for the %s dialect, so no statement was executed for this key", report.Dialect)
	rows := make([]Row, 0, len(capability.All()))
	for _, key := range sortedCapabilities() {
		rows = append(rows, Row{
			Capability: key,
			Dialect:    report.Dialect,
			Version:    report.Version.String(),
			PresetSays: report.SessionCapabilities.Has(key),
			Outcome:    Undecidable,
			Reason:     reason,
		})
	}
	return rows
}

// assemble turns observations into rows, applying the two undecidability
// layers in order: what the run could not decide, then what the matrix cannot
// credit to this line.
func assemble(report *Report, observations map[capability.Capability]observation, attempts map[capability.Capability][]Attempt) []Row {
	unattributable := lineReason(report)
	rows := make([]Row, 0, len(observations))
	for _, key := range sortedCapabilities() {
		obs, seen := observations[key]
		row := Row{
			Capability: key,
			Dialect:    report.Dialect,
			Version:    report.Version.String(),
			PresetSays: report.SessionCapabilities.Has(key),
			Note:       obs.note,
			Attempts:   attempts[key],
		}
		switch {
		case !seen:
			row.Outcome = Undecidable
			row.Reason = "the probe plan for this dialect does not answer for this key"
		case obs.undecidable != "":
			row.Outcome = Undecidable
			row.Reason = obs.undecidable
		default:
			row.Observed = true
			row.ServerDoes = obs.does
			row.Outcome = outcomeFor(row, unattributable)
			row.Reason = unattributable
		}
		rows = append(rows, row)
	}
	return rows
}

func outcomeFor(row Row, unattributable string) Outcome {
	if unattributable != "" {
		return Undecidable
	}
	if row.ServerDoes == row.PresetSays {
		return Agrees
	}
	return Disagrees
}

// lineReason says why an observation cannot be credited to the cell's release
// line, or returns "" when it can.
//
// This is the mechanism issue #1339 predicts will produce whole columns of
// undecidable rows, and that first output is the correct result rather than a
// defect to paper over: for several dialects the resolver hands every release
// the same set, so an observation taken on one release can be credited only
// when the matrix cell explicitly records that release-line measurement.
func lineReason(report *Report) string {
	if !report.Matched {
		return fmt.Sprintf(
			"no matrix cell covers %s %s, so this observation belongs to no declared release line",
			report.Dialect, report.Version)
	}
	if !report.Cell.Measured() {
		return fmt.Sprintf("matrix cell %s has no measured capability preset: %s", report.Cell, report.Cell.Note)
	}
	switch report.Cell.Refinement {
	case RefinedByBanner:
		return fmt.Sprintf(
			"the %s preset is selected by a banner substring before any version is parsed, so every "+
				"release of this engine receives %s and an observation on one release cannot be credited to this line",
			report.Dialect, report.Cell.PresetName)
	case NotRefined:
		return fmt.Sprintf(
			"the %s dialect has no version ladder: the resolver parses the version and discards it, so "+
				"every release receives %s and an observation on one release cannot be credited to this line",
			report.Dialect, report.Cell.PresetName)
	case RefinedByVersion, RefinedByMeasuredLine:
		return ""
	default:
		return fmt.Sprintf("unknown refinement %q for matrix cell %s", report.Cell.Refinement, report.Cell)
	}
}

func sortedCapabilities() []capability.Capability {
	keys := capability.All()
	slices.SortFunc(keys, func(a, b capability.Capability) int {
		return strings.Compare(string(a), string(b))
	})
	return keys
}
