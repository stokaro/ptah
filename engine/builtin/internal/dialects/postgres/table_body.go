package postgres

import "ptah.run/core/ast"

// Complete source order takes precedence over slice order. Programmatic nodes
// may omit Elements; partial metadata must never omit a declared table member.
func orderedTableBody(elements []ast.TableElement, lines []string, positions map[any]int) []string {
	var ordered []string
	seen := make(map[int]bool)
	for _, element := range elements {
		var node any
		switch {
		case element.Column != nil:
			node = element.Column
		case element.Constraint != nil:
			node = element.Constraint
		case element.OwnedConstraint != nil:
			node = element.OwnedConstraint
		default:
			continue
		}
		position, found := positions[node]
		if found && !seen[position] {
			ordered = append(ordered, lines[position])
			seen[position] = true
		}
	}
	if len(ordered) == len(lines) {
		return ordered
	}
	return lines
}
