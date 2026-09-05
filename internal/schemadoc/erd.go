package schemadoc

import (
	"fmt"
	"sort"
	"strings"
)

// Diagram geometry. They are constants rather than options because a diagram
// whose proportions vary between documents cannot be compared between them.
const (
	nodeHeight     = 34
	nodeCharWidth  = 7.4
	nodePadding    = 22
	nodeMinWidth   = 96
	layerGap       = 96
	rowGap         = 18
	diagramPadding = 16
	maxLayerDepth  = 64
)

// node is one table placed on the canvas.
type node struct {
	Name       string
	X, Y, W, H float64
	Layer, Row int
}

// renderERD draws the tables and their foreign keys as an SVG element.
//
// The layout is layered left to right by dependency: a table sits one layer
// right of everything it points at, so reading the diagram left to right reads
// the order the tables have to be created in. That is the question a schema
// diagram is usually opened to answer.
//
// It is deterministic -- layers by dependency, rows by name -- so the same
// schema draws the same picture and two documents can be compared.
func renderERD(doc document) string {
	if len(doc.Tables) == 0 {
		return ""
	}
	nodes := placeNodes(doc)
	width, height := canvasSize(nodes)

	var out strings.Builder
	fmt.Fprintf(&out, `<svg viewBox="0 0 %.0f %.0f" width="%.0f" height="%.0f" role="img" aria-label="Entity relationship diagram">`,
		width, height, width, height)
	out.WriteString(`<defs><marker id="a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto">` +
		`<path class="arrow" d="M0 0 L8 4 L0 8 z"/></marker></defs>`)

	byName := make(map[string]node, len(nodes))
	for _, n := range nodes {
		byName[n.Name] = n
	}
	for _, rel := range doc.Relations {
		from, okFrom := byName[rel.From]
		to, okTo := byName[rel.To]
		if !okFrom || !okTo || rel.From == rel.To {
			continue
		}
		out.WriteString(edgePath(from, to))
	}
	for _, n := range nodes {
		fmt.Fprintf(&out, `<rect class="node" x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="2"/>`, n.X, n.Y, n.W, n.H)
		fmt.Fprintf(&out, `<text class="label" x="%.1f" y="%.1f" text-anchor="middle" dominant-baseline="middle">%s</text>`,
			n.X+n.W/2, n.Y+n.H/2, escapeText(n.Name))
	}
	out.WriteString(`</svg>`)
	return out.String()
}

// edgePath draws one dependency. The curve leaves the dependent's left edge and
// arrives at the target's right edge, because the target sits to the left.
func edgePath(from, to node) string {
	x1, y1 := from.X, from.Y+from.H/2
	x2, y2 := to.X+to.W, to.Y+to.H/2
	control := (x1 - x2) / 2
	if control < 24 {
		control = 24
	}
	return fmt.Sprintf(`<path class="edge" d="M%.1f %.1f C%.1f %.1f %.1f %.1f %.1f %.1f" marker-end="url(#a)"/>`,
		x1, y1, x1-control, y1, x2+control, y2, x2+6, y2)
}

// placeNodes assigns every table a layer and a row, then turns those into
// coordinates.
func placeNodes(doc document) []node {
	depth := layerDepths(doc)
	rows := make(map[int][]string)
	for _, table := range doc.Tables {
		layer := depth[table.Name]
		rows[layer] = append(rows[layer], table.Name)
	}
	layers := make([]int, 0, len(rows))
	for layer := range rows {
		sort.Strings(rows[layer])
		layers = append(layers, layer)
	}
	sort.Ints(layers)

	widths := make(map[int]float64, len(layers))
	for _, layer := range layers {
		for _, name := range rows[layer] {
			if w := nodeWidth(name); w > widths[layer] {
				widths[layer] = w
			}
		}
	}

	var nodes []node
	x := float64(diagramPadding)
	for _, layer := range layers {
		y := float64(diagramPadding)
		for row, name := range rows[layer] {
			nodes = append(nodes, node{
				Name: name, X: x, Y: y, W: widths[layer], H: nodeHeight,
				Layer: layer, Row: row,
			})
			y += nodeHeight + rowGap
		}
		x += widths[layer] + layerGap
	}
	return nodes
}

// layerDepths assigns each table one more layer than the deepest table it
// points at.
//
// A cycle has no such number, so the walk stops at maxLayerDepth and the
// tables in it land on the same layer. A diagram that draws a cycle flat is a
// worse picture than one that does not draw at all is a worse document.
func layerDepths(doc document) map[string]int {
	targets := make(map[string][]string, len(doc.Relations))
	for _, rel := range doc.Relations {
		if rel.From == rel.To {
			continue
		}
		targets[rel.From] = append(targets[rel.From], rel.To)
	}
	depth := make(map[string]int, len(doc.Tables))
	var resolve func(name string, seen int) int
	resolve = func(name string, seen int) int {
		if known, ok := depth[name]; ok {
			return known
		}
		if seen > maxLayerDepth {
			return 0
		}
		best := 0
		for _, target := range targets[name] {
			if candidate := resolve(target, seen+1) + 1; candidate > best {
				best = candidate
			}
		}
		depth[name] = best
		return best
	}
	for _, table := range doc.Tables {
		resolve(table.Name, 0)
	}
	return depth
}

func canvasSize(nodes []node) (width, height float64) {
	for _, n := range nodes {
		if right := n.X + n.W + diagramPadding; right > width {
			width = right
		}
		if bottom := n.Y + n.H + diagramPadding; bottom > height {
			height = bottom
		}
	}
	return width, height
}

func nodeWidth(name string) float64 {
	width := float64(len([]rune(name)))*nodeCharWidth + nodePadding*2
	if width < nodeMinWidth {
		return nodeMinWidth
	}
	return width
}
