package capmatrix

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
)

// Badge is a shields.io endpoint document.
//
// shields.io renders whatever this says, which is what makes a per-engine badge
// possible at all: a `github/actions/workflow/status` badge reports a whole
// workflow, and every release line is a job inside one, so no built-in route
// addresses an engine.
//
// The badge and the status file are rendered from the same aggregate on
// purpose. Two generators reading the same results still drift the day one is
// re-run and the other is not, and a green badge beside a table saying
// otherwise is worse than no badge.
type Badge struct {
	SchemaVersion int    `json:"schemaVersion"`
	Label         string `json:"label"`
	Message       string `json:"message"`
	Color         string `json:"color"`
}

// BadgeFor renders one engine's badge from a tier aggregate.
//
// The denominator counts the release lines this tier can run for the engine,
// and a declared line the tier cannot run is named separately rather than
// folded into the ratio: counting it as passing would claim a measurement
// nobody made, and counting it as failing would report a red engine over a
// line that was never meant to run here.
func BadgeFor(a Aggregate, dialect string) Badge {
	runnable, passed := 0, 0
	for _, cell := range a.Verdicts() {
		if cell.Dialect != dialect {
			continue
		}
		runnable++
		if cell.Verdict == Passed {
			passed++
		}
	}
	skipped := 0
	for _, cell := range a.Matrix.Skipped {
		if cell.Dialect == dialect {
			skipped++
		}
	}

	badge := Badge{SchemaVersion: 1, Label: dialect}
	if runnable == 0 {
		// Every line this engine declares is one the tier cannot run. "0/0
		// passing" would read as a measurement, and appending the skipped count
		// to it renders "not run, 3 not run". The engine was not measured here
		// at all, and the status file carries the reason line by line.
		badge.Message, badge.Color = "not probed", "lightgrey"
		return badge
	}

	badge.Message = fmt.Sprintf("%d/%d passing", passed, runnable)
	badge.Color = "red"
	if passed == runnable {
		badge.Color = "brightgreen"
	}
	if skipped > 0 {
		badge.Message = fmt.Sprintf("%s, %d not probed", badge.Message, skipped)
	}
	return badge
}

// BadgeDialects names every engine the matrix declares, runnable or not, in a
// stable order.
//
// Iterating the declaration rather than the results is the same rule the status
// file follows: an engine whose every cell failed to start would otherwise lose
// its badge entirely, and a badge that disappears reads as an engine that was
// never claimed.
func BadgeDialects(a Aggregate) []string {
	seen := make(map[string]bool)
	var dialects []string
	for _, cell := range slices.Concat(a.Matrix.Cells, a.Matrix.Skipped) {
		if !seen[cell.Dialect] {
			seen[cell.Dialect] = true
			dialects = append(dialects, cell.Dialect)
		}
	}
	slices.SortFunc(dialects, cmp.Compare)
	return dialects
}

// MarshalBadge renders a badge document as the bytes to commit: indented, and
// newline-terminated, so a diff of two runs shows the field that changed rather
// than one long line.
func MarshalBadge(badge Badge) ([]byte, error) {
	encoded, err := json.MarshalIndent(badge, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal badge for %q: %w", badge.Label, err)
	}
	return append(encoded, '\n'), nil
}
