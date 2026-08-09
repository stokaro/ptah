package capabilityprobe

import (
	"fmt"
	"io"
	"strings"
)

// WriteCells prints the matrix: every release line the probe covers, in
// declaration order.
func WriteCells(w io.Writer) {
	fmt.Fprintf(w, "capability matrix cells (%d)\n\n", len(Cells))
	fmt.Fprintf(w, "%-13s %-8s %-16s %-17s %s\n", "DIALECT", "LINE", "PRESET", "REFINEMENT", "IMAGE")
	for _, cell := range Cells {
		preset := cell.PresetName
		if preset == "" {
			preset = "(none)"
		}
		fmt.Fprintf(w, "%-13s %-8s %-16s %-17s %s\n", cell.Dialect, cell.Line, preset, cell.Refinement, cell.Image)
		if cell.Note != "" {
			fmt.Fprintf(w, "%-13s %-8s %s\n", "", "", "note: "+cell.Note)
		}
	}
}

// WriteReport prints one probe run: the table, the summary, and why each
// undecidable row is undecidable.
func WriteReport(w io.Writer, r *Report) {
	writeHeader(w, r)
	fmt.Fprintf(w, "\n%-38s %-12s %-12s %s\n", "CAPABILITY", "PRESET SAYS", "SERVER DOES", "OUTCOME")
	for _, row := range r.Rows {
		fmt.Fprintf(w, "%-38s %-12t %-12s %s\n", row.Capability, row.PresetSays, serverDoes(row), row.Outcome)
	}
	writeSummary(w, r)
	writeAnnotations(w, r)
}

// WriteEvidence prints every statement the run executed and the server's own
// answer to it. It is separate from WriteReport because the table is the
// deliverable and the evidence is the appeal: a reader who doubts a row should
// be able to re-execute it by hand from these lines.
func WriteEvidence(w io.Writer, r *Report) {
	fmt.Fprintf(w, "\nevidence:\n")
	for _, row := range r.Rows {
		if len(row.Attempts) == 0 {
			continue
		}
		fmt.Fprintf(w, "  %s\n", row.Capability)
		for _, attempt := range row.Attempts {
			fmt.Fprintf(w, "      %s\n", attempt)
		}
	}
	if len(r.Cleanup) > 0 {
		fmt.Fprintf(w, "  cleanup\n")
		for _, attempt := range r.Cleanup {
			fmt.Fprintf(w, "      %s\n", attempt)
		}
	}
}

func serverDoes(row Row) string {
	if !row.Observed {
		return "-"
	}
	return fmt.Sprintf("%t", row.ServerDoes)
}

func writeHeader(w io.Writer, r *Report) {
	fmt.Fprintf(w, "capability probe\n")
	fmt.Fprintf(w, "  target       %s\n", r.URL)
	fmt.Fprintf(w, "  dialect      %s\n", r.Dialect)
	fmt.Fprintf(w, "  banner       %s\n", r.Banner)
	fmt.Fprintf(w, "  version      %s (%s)\n", r.Version, r.Version.Source)
	if r.Matched {
		fmt.Fprintf(w, "  matrix cell  %s [preset %s, %s]\n", r.Cell, presetOrNone(r.Cell), r.Cell.Refinement)
	} else {
		fmt.Fprintf(w, "  matrix cell  NONE — no cell in cells.go covers this release line\n")
	}
	fmt.Fprintf(w, "  resolution   version-specific=%t saturated=%t newest-measured=%q\n",
		r.Resolution.VersionSpecific, r.Resolution.Saturated, r.Resolution.NewestMeasured)
	writeSessionDeltas(w, r)
	if r.Namespace != "" {
		fmt.Fprintf(w, "  namespace    %s\n", r.Namespace)
	}
	if r.Control.Statement == "" {
		fmt.Fprintf(w, "  control      NOT RUN — nothing was executed against this server\n")
		return
	}
	fmt.Fprintf(w, "  control      %s\n", r.Control)
}

func presetOrNone(cell Cell) string {
	if cell.PresetName == "" {
		return "none"
	}
	return cell.PresetName
}

// writeSessionDeltas names the keys the pinned session changed. MySQL 8.4+
// reads its foreign-key reference policy from restrict_fk_on_non_standard_key,
// so the same version answers differently depending on how the operator
// configured the server, and a matrix row that did not say so would be a
// version claim standing on a session fact.
func writeSessionDeltas(w io.Writer, r *Report) {
	if len(r.SessionDeltas) == 0 {
		fmt.Fprintf(w, "  session      preset unchanged by the pinned session\n")
		return
	}
	names := make([]string, 0, len(r.SessionDeltas))
	for _, key := range r.SessionDeltas {
		names = append(names, fmt.Sprintf("%s=%t", key, r.SessionCapabilities.Has(key)))
	}
	fmt.Fprintf(w, "  session      pinned session changed %s\n", strings.Join(names, ", "))
}

// writeSummary prints the counts and the floor the run had to clear. The floor
// belongs on the same line as the count it is compared against: a reader who
// sees "decided 22" cannot tell an intact run from an eroded one without it.
func writeSummary(w io.Writer, r *Report) {
	fmt.Fprintf(w, "\nsummary: %d rows — %d AGREES, %d DISAGREES, %d UNDECIDABLE; decided %d, floor %d\n",
		len(r.Rows), r.Count(Agrees), r.Count(Disagrees), r.Count(Undecidable), r.Decided(), r.floor())
}

func writeAnnotations(w io.Writer, r *Report) {
	writeUndecidable(w, r)
	writeMismatches(w, r)
	noted := rowsWith(r, func(row Row) bool { return row.Note != "" })
	if len(noted) > 0 {
		fmt.Fprintf(w, "\nnotes:\n")
		for _, row := range noted {
			fmt.Fprintf(w, "  %s\n      %s\n", row.Capability, row.Note)
		}
	}
}

// writeUndecidable groups undecidable rows by reason. A whole dialect can
// share one reason — six of the nine share the "no version ladder" one — and
// repeating it per row buries the two or three rows whose reason is their own.
func writeUndecidable(w io.Writer, r *Report) {
	undecidable := rowsWith(r, func(row Row) bool { return row.Outcome == Undecidable })
	if len(undecidable) == 0 {
		return
	}
	fmt.Fprintf(w, "\nundecidable rows and why:\n")
	var order []string
	grouped := map[string][]Row{}
	for _, row := range undecidable {
		if _, seen := grouped[row.Reason]; !seen {
			order = append(order, row.Reason)
		}
		grouped[row.Reason] = append(grouped[row.Reason], row)
	}
	for _, reason := range order {
		rows := grouped[reason]
		fmt.Fprintf(w, "  %s\n", reason)
		for _, row := range rows {
			if row.Observed {
				fmt.Fprintf(w, "      %-38s observed anyway: server does %-5t (preset says %t)\n",
					row.Capability, row.ServerDoes, row.PresetSays)
				continue
			}
			fmt.Fprintf(w, "      %s\n", row.Capability)
		}
	}
}

// writeMismatches prints every observation that contradicts the preset,
// including the ones whose row is undecidable because the line cannot be
// credited. An unattributable contradiction is still a contradiction; letting
// UNDECIDABLE swallow it would be the same error as letting AGREES swallow it.
func writeMismatches(w io.Writer, r *Report) {
	mismatches := r.Mismatches()
	if len(mismatches) == 0 {
		return
	}
	fmt.Fprintf(w, "\npreset and server disagree:\n")
	for _, row := range mismatches {
		fmt.Fprintf(w, "  %-38s preset says %-5t server does %-5t [%s]\n",
			row.Capability, row.PresetSays, row.ServerDoes, row.Outcome)
	}
}

func rowsWith(r *Report, keep func(Row) bool) []Row {
	var out []Row
	for _, row := range r.Rows {
		if keep(row) {
			out = append(out, row)
		}
	}
	return out
}
