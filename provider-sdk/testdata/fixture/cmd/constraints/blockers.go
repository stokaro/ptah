package main

import (
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// blockerKind is the kind of the removal blockers the fixture reports.
const blockerKind = "fixture_stream"

// parseBlockers reads FIXTURE_REMOVAL_BLOCKERS: entries separated by
// semicolons, each `table=name` for a stream that names the table whole, or
// `table=name:column,column` for one that names some of its columns. The
// fixture keeps no such object in SQLite; the variable stands for what a
// server outside the fixture's model would hold.
func parseBlockers(value string) (map[string][]*protocol.RemovalBlocker, error) {
	result := make(map[string][]*protocol.RemovalBlocker)
	for entry := range strings.SplitSeq(value, ";") {
		table, rest, found := strings.Cut(entry, "=")
		if !found || table == "" || rest == "" {
			return nil, fmt.Errorf("a removal blocker needs table=name, not %q", entry)
		}
		name, columns, _ := strings.Cut(rest, ":")
		blocker := &protocol.RemovalBlocker{Kind: blockerKind, Name: name}
		if columns != "" {
			blocker.Columns = strings.Split(columns, ",")
		}
		result[table] = append(result[table], blocker)
	}
	return result, nil
}

// blockedRemoval says why the plan of table drops what a removal blocker the
// host sent with it blocks: the table, when the plan drops it, or a column a
// blocker names, when the plan keeps the table without that column. It
// returns the empty string when nothing the plan drops is blocked. The
// refusal comes before any step, as a provider's would that reads a server
// which refuses the drop.
func blockedRemoval(table *protocol.PlanningTable) string {
	held := table.GetCurrent()
	switch table.GetAction() {
	case protocol.PlanningTable_DROP:
		if blockers := held.GetRemovalBlockers(); len(blockers) != 0 {
			return fmt.Sprintf("dropping table %s: %s %s names it, and the fixture refuses to drop a table one names", held.GetName(), blockers[0].GetKind(), blockers[0].GetName())
		}
	case protocol.PlanningTable_ALTER:
		for _, blocker := range held.GetRemovalBlockers() {
			for _, column := range blocker.GetColumns() {
				if !slices.ContainsFunc(table.GetDesired().GetColumns(), func(declared *protocol.DeclaredColumn) bool { return declared.GetName() == column }) {
					return fmt.Sprintf("dropping column %s of table %s: %s %s names it, and the fixture refuses to drop a column one names", column, held.GetName(),
						blocker.GetKind(), blocker.GetName())
				}
			}
		}
	}
	return ""
}
