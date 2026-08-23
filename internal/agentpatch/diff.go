package agentpatch

import (
	"fmt"
	"slices"
	"strings"
)

// diffContextLines is how much unchanged text surrounds each hunk. Three is
// what every diff tool prints, and a reviewer reading an unfamiliar format is
// one more thing between them and noticing what the patch does.
const diffContextLines = 3

// maxDiffLines bounds the line-by-line comparison.
//
// The comparison is quadratic in the number of lines, and the content limit is
// a megabyte, so a pathological file could be a hundred thousand lines. Past
// this bound the diff degrades to a stated summary rather than to a slow
// answer, and it says which it is.
const maxDiffLines = 4000

// unifiedDiff renders the change to one file in the format every reviewer
// already reads.
//
// A nil before is a creation and a nil after is a deletion, spelled as
// /dev/null on the corresponding side, exactly as diff(1) does.
func unifiedDiff(name string, before, after []byte) string {
	beforeLines := splitLines(before)
	afterLines := splitLines(after)
	if len(beforeLines) > maxDiffLines || len(afterLines) > maxDiffLines {
		return summaryDiff(name, before, after, beforeLines, afterLines)
	}

	hunks := hunksFor(beforeLines, afterLines)
	if len(hunks) == 0 {
		return ""
	}
	out := &strings.Builder{}
	fmt.Fprintf(out, "--- %s\n", diffSide(name, before))
	fmt.Fprintf(out, "+++ %s\n", diffSide(name, after))
	for _, hunk := range hunks {
		out.WriteString(hunk.render())
	}
	return out.String()
}

// diffSide names one side of the header, or /dev/null where the file does not
// exist on that side.
func diffSide(name string, content []byte) string {
	if content == nil {
		return "/dev/null"
	}
	return name
}

// summaryDiff states the shape of a change too large to render.
//
// It says what it is doing rather than printing a truncated diff: a diff that
// silently stops halfway is one a reviewer reads as complete.
func summaryDiff(name string, before, after []byte, beforeLines, afterLines []string) string {
	return fmt.Sprintf(
		"%s: %d lines (%d bytes) becomes %d lines (%d bytes); "+
			"the file is too large for a line-by-line diff\n",
		name, len(beforeLines), len(before), len(afterLines), len(after))
}

// splitLines splits content into lines without their terminators. A nil input
// is no lines; content ending in a newline does not produce a trailing empty
// line, so appending to a file shows as one added line rather than two.
func splitLines(content []byte) []string {
	if content == nil {
		return nil
	}
	text := string(content)
	if text == "" {
		return make([]string, 0)
	}
	text = strings.TrimSuffix(text, "\n")
	return strings.Split(text, "\n")
}

// edit is one line's fate in the comparison.
type edit struct {
	// kind is ' ' for context, '-' for a removed line, '+' for an added one.
	kind byte
	text string
}

// hunk is a run of edits with its position on both sides.
type hunk struct {
	beforeStart int
	beforeCount int
	afterStart  int
	afterCount  int
	edits       []edit
}

// render prints one hunk in unified format.
func (h hunk) render() string {
	out := &strings.Builder{}
	fmt.Fprintf(out, "@@ -%d,%d +%d,%d @@\n",
		h.beforeStart, h.beforeCount, h.afterStart, h.afterCount)
	for _, item := range h.edits {
		out.WriteByte(item.kind)
		out.WriteString(item.text)
		out.WriteByte('\n')
	}
	return out.String()
}

// hunksFor computes the edit script and groups it into hunks with context.
func hunksFor(before, after []string) []hunk {
	edits := editScript(before, after)
	return groupHunks(edits)
}

// editScript walks the longest-common-subsequence table into an edit list.
//
// The table is the textbook dynamic program rather than Myers' algorithm: the
// inputs are bounded at [maxDiffLines] and the simpler code is the one whose
// output a reader can check by hand.
func editScript(before, after []string) []edit {
	lengths := lcsTable(before, after)
	edits := make([]edit, 0, len(before)+len(after))
	i, j := 0, 0
	for i < len(before) && j < len(after) {
		switch {
		case before[i] == after[j]:
			edits = append(edits, edit{kind: ' ', text: before[i]})
			i++
			j++
		case lengths[i+1][j] >= lengths[i][j+1]:
			edits = append(edits, edit{kind: '-', text: before[i]})
			i++
		default:
			edits = append(edits, edit{kind: '+', text: after[j]})
			j++
		}
	}
	for ; i < len(before); i++ {
		edits = append(edits, edit{kind: '-', text: before[i]})
	}
	for ; j < len(after); j++ {
		edits = append(edits, edit{kind: '+', text: after[j]})
	}
	return edits
}

// lcsTable builds the suffix-length table the edit walk reads.
func lcsTable(before, after []string) [][]int {
	table := make([][]int, len(before)+1)
	for i := range table {
		table[i] = make([]int, len(after)+1)
	}
	for i, beforeLine := range slices.Backward(before) {
		for j, afterLine := range slices.Backward(after) {
			if beforeLine == afterLine {
				table[i][j] = table[i+1][j+1] + 1
				continue
			}
			table[i][j] = max(table[i+1][j], table[i][j+1])
		}
	}
	return table
}

// groupHunks turns a flat edit list into hunks, each carrying up to
// [diffContextLines] of unchanged text on either side.
func groupHunks(edits []edit) []hunk {
	changed := changedIndexes(edits)
	if len(changed) == 0 {
		return nil
	}
	hunks := make([]hunk, 0, 4)
	start := 0
	for start < len(changed) {
		end := start
		for end+1 < len(changed) && changed[end+1]-changed[end] <= 2*diffContextLines {
			end++
		}
		first := max(changed[start]-diffContextLines, 0)
		last := min(changed[end]+diffContextLines, len(edits)-1)
		hunks = append(hunks, buildHunk(edits, first, last))
		start = end + 1
	}
	return hunks
}

// changedIndexes lists the positions of the added and removed lines.
func changedIndexes(edits []edit) []int {
	changed := make([]int, 0, len(edits))
	for index, item := range edits {
		if item.kind != ' ' {
			changed = append(changed, index)
		}
	}
	return changed
}

// buildHunk numbers one span of the edit list on both sides.
func buildHunk(edits []edit, first, last int) hunk {
	beforeLine, afterLine := 1, 1
	for _, item := range edits[:first] {
		if item.kind != '+' {
			beforeLine++
		}
		if item.kind != '-' {
			afterLine++
		}
	}
	built := hunk{beforeStart: beforeLine, afterStart: afterLine}
	for _, item := range edits[first : last+1] {
		built.edits = append(built.edits, item)
		if item.kind != '+' {
			built.beforeCount++
		}
		if item.kind != '-' {
			built.afterCount++
		}
	}
	return built
}
