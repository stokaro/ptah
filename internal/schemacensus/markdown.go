package schemacensus

import (
	"fmt"
	"io"
	"strings"
)

// meanings is the one-line sense of each disposition, for the generated table.
// The long form lives on the constants in disposition.go; this is what a reader
// of the register needs beside a count.
var meanings = map[Disposition]string{
	DDL:          "reaches rendered SQL on at least one target",
	Comparison:   "read when two schemas are compared, and written into no statement",
	Planning:     "read while a change set is assembled or ordered",
	Derived:      "computed from other fields rather than authored",
	SourceOrigin: "identifies the source text the declaration was read from",
	Export:       "what a generated document carries, or reports that it cannot",
	Data:         "reference or seed rows, which are not DDL",
}

// WriteMarkdown renders the register: what each disposition means and how many
// fields carry it, the fields that should render and do not, and the whole
// field-by-field table.
//
// The counts are computed rather than written down, so the prose above a table
// cannot describe a table that has changed underneath it.
func WriteMarkdown(w io.Writer) error {
	entries := Registry()

	counts := make(map[Disposition]int)
	for _, entry := range entries {
		counts[entry.Disposition]++
	}

	var out strings.Builder

	fmt.Fprintf(&out, "%d fields are reachable from the desired schema, and each one carries\n", len(entries))
	out.WriteString("exactly one disposition.\n\n")
	out.WriteString("| Disposition | Fields | What it means |\n| --- | --- | --- |\n")
	for _, disposition := range Dispositions() {
		fmt.Fprintf(&out, "| `%s` | %d | %s |\n", disposition, counts[disposition], meanings[disposition])
	}

	out.WriteString("\n### Fields that should render and do not\n\n")
	gaps := 0
	for _, entry := range entries {
		if entry.Gap != "" {
			gaps++
		}
	}
	if gaps == 0 {
		out.WriteString("None.\n")
	} else {
		fmt.Fprintf(&out, "%d, each recorded against the issue that tracks the repair. The gate\n", gaps)
		out.WriteString("refuses a gap that has started rendering, so a repair fails the build until\n")
		out.WriteString("its entry is reclassified.\n\n")
		out.WriteString("| Field | Issue |\n| --- | --- |\n")
		for _, entry := range entries {
			if entry.Gap != "" {
				fmt.Fprintf(&out, "| `%s` | %s |\n", entry.Field, entry.Gap)
			}
		}
	}

	writeRoundTripMarkdown(&out)

	out.WriteString("\n### Every field\n\n")
	out.WriteString("| Field | Disposition | Why it is not rendered |\n| --- | --- | --- |\n")
	for _, entry := range entries {
		reason := entry.Reason
		if reason == "" {
			reason = "—"
		}
		fmt.Fprintf(&out, "| `%s` | `%s` | %s |\n", entry.Field, entry.Disposition, reason)
	}

	_, err := io.WriteString(w, out.String())
	return err
}

// writeRoundTripMarkdown renders the round trip's registers: the fixtures
// whose round trip plans a change, the compared fields a change of which
// plans nothing, the ones that by design do not, and the ones it cannot ask
// about.
func writeRoundTripMarkdown(out *strings.Builder) {
	losses := RoundTripLosses()
	out.WriteString("\n### Round trips that plan a change\n\n")
	fmt.Fprintf(out, "%d recorded losses. Each is a fixture that, compared with its own conversion, plans a\n", len(losses))
	out.WriteString("migration nobody asked for.\n\n")
	out.WriteString("| Fixtures | Dialects | Directions | Why |\n| --- | --- | --- | --- |\n")
	for _, loss := range losses {
		fmt.Fprintf(out, "| %s | %s | %s | %s |\n",
			fixtureList(loss.Fixtures), codeList(loss.Dialects), directionList(loss.Directions), loss.Reason)
	}

	for _, register := range []struct {
		title, lead string
		entries     []Unreached
	}{
		{
			title:   "Compared fields a change of which plans nothing",
			lead:    "known gaps: a conversion or the comparison drops each one on the dialects named",
			entries: RoundTripGaps(),
		},
		{
			title:   "Compared fields that do not reach a plan by design",
			lead:    "exemptions, each with the reason it is right",
			entries: RoundTripExemptions(),
		},
	} {
		fmt.Fprintf(out, "\n### %s\n\n%d %s.\n\n", register.title, len(register.entries), register.lead)
		out.WriteString("| Field | Dialects | Directions | Why |\n| --- | --- | --- | --- |\n")
		for _, entry := range register.entries {
			dialects := "any"
			if entry.Dialects != nil {
				dialects = codeList(entry.Dialects)
			}
			fmt.Fprintf(out, "| `%s` | %s | %s | %s |\n", entry.Field, dialects, directionList(entry.Directions), entry.Reason)
		}
	}

	unmeasured := RoundTripUnmeasured()
	out.WriteString("\n### Compared fields the round trip cannot ask about\n\n")
	fmt.Fprintf(out, "%d, besides the ones only a recorded loss hides.\n\n", len(unmeasured))
	out.WriteString("| Field | Why |\n| --- | --- |\n")
	for _, entry := range unmeasured {
		fmt.Fprintf(out, "| `%s` | %s |\n", entry.Field, entry.Reason)
	}
}

// fixtureList names a few fixtures, or counts many.
func fixtureList(fixtures []string) string {
	if len(fixtures) > 4 {
		return fmt.Sprintf("%d fixtures", len(fixtures))
	}
	return codeList(fixtures)
}

func codeList(values []string) string {
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = "`" + value + "`"
	}
	return strings.Join(quoted, ", ")
}

func directionList(directions []Direction) string {
	if directions == nil {
		return "both"
	}
	names := make([]string, len(directions))
	for index, direction := range directions {
		names[index] = string(direction)
	}
	return strings.Join(names, ", ")
}
