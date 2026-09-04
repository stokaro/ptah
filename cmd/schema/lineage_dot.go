package schema

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"go.5x5.cz/ptah/internal/schemalineage"
)

// writeLineageDOT renders the lineage as a Graphviz digraph.
//
// The graph exists because the table and the JSON answer different questions
// from the one a reader has in front of a multi-hop dependency: which columns
// reach this view, and through what. A list of edges is the same data and does
// not show the shape (stokaro/ptah#2576).
//
// Two properties are load-bearing rather than decorative.
//
// It is DETERMINISTIC. Every node and every edge is sorted before it is
// written, so the output is a review artifact a repository can commit and diff
// rather than a picture that changes when a map is walked in a different
// order.
//
// It PRESERVES WHAT WAS NOT RESOLVED. A view or a routine this analysis could
// not attribute is drawn, dashed, carrying its reason, rather than left out.
// A graph that silently omitted them would be the most confident-looking
// output Ptah produces and the least honest: the reader would see a complete
// dependency picture with the unanswerable parts invisible.
func writeLineageDOT(w io.Writer, document lineageDocument) error {
	graph := newLineageGraph()
	graph.addViewEdges(document.Edges)
	graph.addUndecidedViews(document.Undecided)
	graph.addRoutineEdges(document.Routines.Edges)
	graph.addRoutineReads(document.Routines.Reads)
	graph.addRoutineWrites(document.Routines.Writes)
	graph.addUndecidedRoutines(document.Routines.Undecided)
	return graph.write(w)
}

// lineageGraph accumulates what the digraph will say.
//
// Nodes are keyed by name so one object declared by several edges is drawn
// once, and the attribute text is kept beside the key so the last writer does
// not win by accident: a node is only ever upgraded from a plain table to the
// kind that names it.
type lineageGraph struct {
	nodes map[string]string
	edges map[string]struct{}
}

func newLineageGraph() *lineageGraph {
	return &lineageGraph{
		nodes: make(map[string]string),
		edges: make(map[string]struct{}),
	}
}

// table records a source object, which is the shape everything else points
// away from.
func (g *lineageGraph) table(name string) {
	if _, seen := g.nodes[name]; seen {
		return
	}
	g.nodes[name] = `shape=box, label="` + dotEscape(name) + `"`
}

// consumer records a view, materialized view or routine, replacing a plain
// table node written earlier for the same name.
//
// It takes no "is it resolved" flag: an object this analysis could not
// attribute goes through [lineageGraph.unresolved] instead, which has a reason
// to carry and a different shape to draw. One function with a boolean would
// have made the caller say which of two drawings it wanted, and the callers
// already know.
func (g *lineageGraph) consumer(name, kind string) {
	g.nodes[name] = `shape=ellipse, label="` + dotLabel(name, kind) + `"`
}

// unresolved records an object this analysis could not attribute, with the
// reason on the node itself. The reason is a tooltip as well as a label so a
// long sentence stays readable in a rendered graph and legible in the source.
func (g *lineageGraph) unresolved(name, kind, reason string) {
	g.nodes[name] = `shape=ellipse, style=dashed, label="` +
		dotLabel(name, kind+", unresolved") +
		`", tooltip="` + dotEscape(reason) + `"`
}

func (g *lineageGraph) edge(from, to, label, style string) {
	line := `  "` + dotEscape(from) + `" -> "` + dotEscape(to) + `" [label="` + dotEscape(label) + `"`
	if style != "" {
		line += ", " + style
	}
	g.edges[line+"];"] = struct{}{}
}

func (g *lineageGraph) addViewEdges(edges []schemalineage.Edge) {
	for _, edge := range edges {
		kind := "view"
		if edge.Materialized {
			kind = "materialized view"
		}
		g.table(edge.FromTable)
		g.consumer(edge.ToView, kind)
		g.edge(edge.FromTable, edge.ToView, edge.FromColumn+" → "+edge.ToColumn, "")
	}
}

func (g *lineageGraph) addUndecidedViews(undecided []schemalineage.Undecided) {
	for _, item := range undecided {
		kind := "view"
		if item.Materialized {
			kind = "materialized view"
		}
		g.unresolved(item.View, kind, item.Reason)
	}
}

func (g *lineageGraph) addRoutineEdges(edges []schemalineage.RoutineEdge) {
	for _, edge := range edges {
		g.table(edge.FromTable)
		g.consumer(edge.ToRoutine, routineKind(edge.Kind))
		g.edge(edge.FromTable, edge.ToRoutine, edge.FromColumn, "")
	}
}

// addRoutineReads draws what a routine reads and how. The statement is on the
// edge because "select" and "delete" are different facts about the same pair.
func (g *lineageGraph) addRoutineReads(reads []schemalineage.RoutineRead) {
	for _, read := range reads {
		g.table(read.Table)
		g.consumer(read.ByRoutine, routineKind(read.Kind))
		label := read.Column
		if read.Statement != "" {
			label += " (" + read.Statement + ")"
		}
		g.edge(read.Table, read.ByRoutine, label, "style=dotted")
	}
}

// addRoutineWrites draws the other direction, because a write goes from the
// routine to the table and an arrow that pointed the other way would say the
// opposite of what happens.
func (g *lineageGraph) addRoutineWrites(writes []schemalineage.RoutineWrite) {
	for _, write := range writes {
		g.table(write.Table)
		g.consumer(write.ByRoutine, routineKind(write.Kind))
		label := write.Column
		if label == "" {
			label = "writes"
		}
		g.edge(write.ByRoutine, write.Table, label, "style=bold")
	}
}

func (g *lineageGraph) addUndecidedRoutines(undecided []schemalineage.UndecidedRoutine) {
	for _, item := range undecided {
		g.unresolved(item.Routine, routineKind(item.Kind), item.Reason)
	}
}

// routineKind names what a routine is, falling back to the word that is true
// of both when the analysis did not separate them.
func routineKind(kind string) string {
	if strings.TrimSpace(kind) == "" {
		return "routine"
	}
	return kind
}

func (g *lineageGraph) write(w io.Writer) error {
	names := make([]string, 0, len(g.nodes))
	for name := range g.nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	lines := make([]string, 0, len(g.edges))
	for line := range g.edges {
		lines = append(lines, line)
	}
	sort.Strings(lines)

	var builder strings.Builder
	builder.WriteString("digraph lineage {\n")
	builder.WriteString("  rankdir=LR;\n")
	builder.WriteString("  node [fontname=\"sans-serif\"];\n")
	builder.WriteString("  edge [fontname=\"sans-serif\"];\n")
	for _, name := range names {
		fmt.Fprintf(&builder, "  %q [%s];\n", name, g.nodes[name])
	}
	for _, line := range lines {
		builder.WriteString(line + "\n")
	}
	builder.WriteString("}\n")
	_, err := io.WriteString(w, builder.String())
	return err
}

// dotLabel builds a two-line node label out of parts that are escaped
// SEPARATELY.
//
// The line break is a DOT escape rather than a byte, so it has to be joined in
// AFTER each part is escaped: escaping the assembled label would turn the
// backslash of that escape into a literal one and render `name\n(view)` on a
// single line, backslash included.
func dotLabel(name, kind string) string {
	return dotEscape(name) + `\n(` + dotEscape(kind) + `)`
}

// dotEscape quotes what a DOT string literal cannot carry raw.
//
// The backslash goes first: escaping it after the quote would double the
// backslash this function had just written. A newline is written as the DOT
// escape rather than as a byte, because a literal newline inside a quoted
// attribute is what turns a graph into a parse error.
func dotEscape(value string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`"`, `\"`,
		"\n", `\n`,
		"\r", `\r`,
	)
	return replacer.Replace(value)
}
