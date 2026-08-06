package atlasreport

import (
	"fmt"
	"io"
	"strconv"
)

// The default `migrate status` report is the one verb output a pipeline parses
// with a machine rather than reads, so its field names, sentinel strings and
// value encodings are the interface (stokaro/ptah#1102). Every literal below is
// measured against the pinned community binary v1.3.0 on a hashed Atlas
// directory over a SQLite database, byte for byte:
//
//	Migration Status: PENDING
//	  -- Current Version: No migration applied yet
//	  -- Next Version:    1
//	  -- Executed Files:  0
//	  -- Pending Files:   2
//
// The two columns below are the padding that lines the values up, and both were
// read off the measured bytes rather than chosen: "Current Version:" is 16
// characters and carries one trailing space while "Next Version:" is 13 and
// carries four, so the field column is 17; "SQL:" carries three trailing spaces
// against "ERROR:"'s one, so the error column is 7.
const (
	migrateStatusFieldColumn = 17
	migrateStatusErrorColumn = 7
)

// WriteMigrateStatusText writes the default (no --format) `migrate status`
// report for the compat surface.
//
// Native `ptah migrations status` keeps its own block: only the compat surface
// is a contract with an existing pipeline, and the native one answers to a
// reader. See cmd/atlas/migrate_status.go for why that split is deliberate.
func WriteMigrateStatusText(w io.Writer, opts MigrateStatusOptions) error {
	report, err := NewMigrateStatus(opts)
	if err != nil {
		return err
	}
	writeMigrateStatusReport(w, report)
	return nil
}

func writeMigrateStatusReport(w io.Writer, report MigrateStatus) {
	fmt.Fprintf(w, "Migration Status: %s\n", report.Status)
	writeMigrateStatusField(w, "Current Version:", report.Current+migrateStatusApplied(report))
	writeMigrateStatusField(w, "Next Version:", migrateStatusNextField(report))
	writeMigrateStatusField(w, "Executed Files:", migrateStatusExecuted(report))
	writeMigrateStatusField(w, "Pending Files:", migrateStatusPendingField(report))
	writeMigrateStatusError(w, report)
	writeMigrateStatusOutOfOrder(w, report)
}

// migrateStatusPartial reports whether the highest revision row is half-applied.
//
// Total is the failed attempt's statement count, written only for such a row
// (see applyMigrateStatusPartial), so a zero here means "no half-applied row"
// rather than "a row with no statements": the three annotations below are all
// absent together, which is what the pinned community binary does.
func migrateStatusPartial(report MigrateStatus) bool {
	return report.Total > 0
}

// migrateStatusApplied annotates Current Version with the statements the failed
// attempt did commit. It is "statements" even at one, which is the community
// binary's wording and not a typo.
func migrateStatusApplied(report MigrateStatus) string {
	if !migrateStatusPartial(report) {
		return ""
	}
	return fmt.Sprintf(" (%d statements applied)", report.Count)
}

func migrateStatusLeft(report MigrateStatus) string {
	if !migrateStatusPartial(report) {
		return ""
	}
	return fmt.Sprintf(" (%d statements left)", report.Total-report.Count)
}

func migrateStatusExecuted(report MigrateStatus) string {
	count := strconv.Itoa(len(report.Applied))
	if !migrateStatusPartial(report) {
		return count
	}
	return count + " (last one partially)"
}

// writeMigrateStatusError prints the failing statement and its error under the
// counts. This is the block that keeps a wedged database distinguishable from a
// healthy one with work outstanding (stokaro/ptah#966): without it, Current
// Version names the half-applied version and nothing says the attempt failed.
func writeMigrateStatusError(w io.Writer, report MigrateStatus) {
	if report.Error == "" {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Last migration attempt had errors:")
	writeMigrateStatusLine(w, migrateStatusErrorColumn, "SQL:", report.SQL)
	writeMigrateStatusLine(w, migrateStatusErrorColumn, "ERROR:", report.Error)
}

func writeMigrateStatusField(w io.Writer, label, value string) {
	writeMigrateStatusLine(w, migrateStatusFieldColumn, label, value)
}

func writeMigrateStatusLine(w io.Writer, column int, label, value string) {
	fmt.Fprintf(w, "  -- %-*s%s\n", column, label, value)
}

// migrateStatusNextField answers "what runs next", and answers UNKNOWN when a
// pending file sorts below an applied one.
//
// Measured against the pinned community binary v1.3.0 on a directory where
// 20240401000001 and 20240403000000 are applied and 20240402000000 is then
// added and re-hashed: it prints `-- Next Version:    UNKNOWN`. Naming the
// out-of-order file there would be worse than saying nothing, because under
// linear execution that file is exactly the one that will not run.
func migrateStatusNextField(report MigrateStatus) string {
	if len(report.OutOfOrder) > 0 {
		return "UNKNOWN"
	}
	return report.Next + migrateStatusLeft(report)
}

// migrateStatusPendingField counts pending files and annotates the count when
// any of them is out of order, matching `1 (out of order)` from the binary.
func migrateStatusPendingField(report MigrateStatus) string {
	count := strconv.Itoa(len(report.Pending))
	if len(report.OutOfOrder) > 0 {
		return count + " (out of order)"
	}
	return count
}

// writeMigrateStatusOutOfOrder writes the trailing ERROR block the pinned
// binary prints for a non-linear directory, one line per offending file.
//
// This block is the reason the mirror had to cover this state rather than
// leaving it: a pipeline that greps the report for `ERROR:` to decide whether a
// directory is safe would otherwise read a clean, correctly-shaped report on
// exactly the directory the binary flags. A partial mirror is more dangerous
// than none, because the shared shape is what makes the omission invisible.
func writeMigrateStatusOutOfOrder(w io.Writer, report MigrateStatus) {
	for _, file := range report.OutOfOrder {
		fmt.Fprintln(w)
		fmt.Fprintf(w,
			"  ERROR: migration file %s was added out of order. See: https://atlasgo.io/versioned/apply#non-linear-error\n",
			file.Name)
	}
}
