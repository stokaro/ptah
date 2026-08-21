package lineage

import (
	"fmt"
	"sort"
	"strings"
)

// Formats a lineage report can be rendered as. They are the two the schema
// diagram already offers, so a reader needs no new tool for the second graph.
const (
	FormatDOT     = "dot"
	FormatMermaid = "mermaid"
)

// Render draws a lineage result as a graph description.
//
// Unresolved columns are drawn rather than omitted, with the reason on the node
// and no incoming edge. That is the visual form of what the derivation says:
// the column exists and its source was not established. Leaving them out would
// draw a schema that looks fully understood.
func Render(result Result, format string) ([]byte, error) {
	switch format {
	case FormatDOT:
		return []byte(renderDOT(result)), nil
	case FormatMermaid:
		return []byte(renderMermaid(result)), nil
	default:
		return nil, fmt.Errorf("unsupported lineage format %q: expected dot or mermaid", format)
	}
}

func renderDOT(result Result) string {
	var out strings.Builder
	out.WriteString("digraph lineage {\n")
	out.WriteString("  rankdir=LR;\n")
	out.WriteString("  node [shape=box, fontname=\"Helvetica\"];\n")
	for _, node := range sourceNodes(result) {
		fmt.Fprintf(&out, "  %q [label=%q];\n", node, node)
	}
	for _, unresolved := range result.Unresolved {
		id := unresolvedNodeID(unresolved)
		fmt.Fprintf(&out, "  %q [label=%q, style=dashed];\n", id, unresolvedLabel(unresolved))
	}
	for _, edge := range result.Edges {
		fmt.Fprintf(&out, "  %q -> %q;\n", qualify(edge.FromTable, edge.FromColumn), qualify(edge.ToView, edge.ToColumn))
	}
	out.WriteString("}\n")
	return out.String()
}

func renderMermaid(result Result) string {
	var out strings.Builder
	out.WriteString("flowchart LR\n")
	ids := make(map[string]string)
	for _, node := range sourceNodes(result) {
		id := mermaidID(node, ids)
		fmt.Fprintf(&out, "  %s[%q]\n", id, node)
	}
	for _, unresolved := range result.Unresolved {
		id := mermaidID(unresolvedNodeID(unresolved), ids)
		fmt.Fprintf(&out, "  %s[%q]\n", id, mermaidLabel(unresolved))
	}
	for _, edge := range result.Edges {
		from := mermaidID(qualify(edge.FromTable, edge.FromColumn), ids)
		to := mermaidID(qualify(edge.ToView, edge.ToColumn), ids)
		fmt.Fprintf(&out, "  %s --> %s\n", from, to)
	}
	return out.String()
}

// sourceNodes lists every node an edge touches, ordered and deduplicated.
func sourceNodes(result Result) []string {
	seen := make(map[string]bool)
	var nodes []string
	for _, edge := range result.Edges {
		for _, node := range []string{
			qualify(edge.FromTable, edge.FromColumn),
			qualify(edge.ToView, edge.ToColumn),
		} {
			if !seen[node] {
				seen[node] = true
				nodes = append(nodes, node)
			}
		}
	}
	sort.Strings(nodes)
	return nodes
}

// unresolvedNodeID is the node an unresolved entry draws. A body nobody could
// read has no column, so the view alone names it.
func unresolvedNodeID(unresolved Unresolved) string {
	if unresolved.Column == "" {
		return unresolved.View
	}
	return qualify(unresolved.View, unresolved.Column)
}

// unresolvedLabel is the DOT label, where a newline breaks the line.
func unresolvedLabel(unresolved Unresolved) string {
	return unresolvedNodeID(unresolved) + "\n" + unresolved.Reason
}

// mermaidLabel is the same label for Mermaid, which breaks a line on <br/> and
// would print an escaped newline literally.
func mermaidLabel(unresolved Unresolved) string {
	return unresolvedNodeID(unresolved) + "<br/>" + unresolved.Reason
}

func qualify(owner, column string) string {
	return owner + "." + column
}

// mermaidID assigns each node a stable identifier, because Mermaid node ids
// cannot carry the dots and spaces a qualified column name has.
func mermaidID(node string, assigned map[string]string) string {
	if id, known := assigned[node]; known {
		return id
	}
	id := fmt.Sprintf("n%d", len(assigned))
	assigned[node] = id
	return id
}
