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
	Export:       "a name or shape an exported API document carries",
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
