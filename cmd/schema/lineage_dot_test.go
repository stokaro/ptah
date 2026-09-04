package schema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `--format dot` answers the question the table and the JSON do not: what the
// dependency graph looks like, rather than what is in it. See
// stokaro/ptah#2576.

func TestSchemaLineageDOTDrawsEveryEdgeKind(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{
			name: "a view edge names both columns",
			want: `"users" -> "active_users" [label="email → contact"]`,
		},
		{
			name: "a routine that reads is drawn from the table it reads",
			want: `"users" -> "all_emails"`,
		},
		{
			name: "a routine that writes is drawn toward the table it writes",
			want: `"orders" -> "order_count"`,
		},
		{
			name: "a table is a box",
			want: `"users" [shape=box`,
		},
		{
			name: "a view says it is one",
			want: `"active_users" [shape=ellipse, label="active_users\n(view)"]`,
		},
	}

	c := qt.New(t)
	out := runLineage(c, "--format", "dot")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(out, qt.Contains, test.want)
		})
	}
}

// TestSchemaLineageDOTKeepsWhatItCouldNotResolve is the requirement that
// separates this from a picture.
//
// A view this analysis cannot attribute is drawn dashed and carries its reason,
// because a graph that omitted it would be the most confident-looking output
// Ptah produces and the least honest: the reader would see a complete
// dependency picture with the unanswerable parts invisible.
func TestSchemaLineageDOTKeepsWhatItCouldNotResolve(t *testing.T) {
	c := qt.New(t)

	out := runLineage(c, "--format", "dot")

	c.Assert(out, qt.Contains, `"joined" [shape=ellipse, style=dashed`)
	c.Assert(out, qt.Contains, `unresolved`)
	c.Assert(out, qt.Contains, `tooltip="`)
}

// TestSchemaLineageDOTIsDeterministic keeps the output usable as a committed
// review artifact. Nodes and edges come out of maps, so without sorting the
// same schema would render differently between runs and every diff would be
// noise.
func TestSchemaLineageDOTIsDeterministic(t *testing.T) {
	c := qt.New(t)

	first := runLineage(c, "--format", "dot")
	second := runLineage(c, "--format", "dot")

	c.Assert(second, qt.Equals, first)
	c.Assert(strings.Count(first, "digraph lineage {"), qt.Equals, 1)
}

// TestSchemaLineageDOTIsWellFormed pins the frame a Graphviz parser needs.
func TestSchemaLineageDOTIsWellFormed(t *testing.T) {
	c := qt.New(t)

	out := runLineage(c, "--format", "dot")

	c.Assert(strings.HasPrefix(out, "digraph lineage {\n"), qt.IsTrue)
	c.Assert(strings.HasSuffix(out, "}\n"), qt.IsTrue)
	c.Assert(strings.Count(out, "{"), qt.Equals, 1)
	c.Assert(strings.Count(out, "}"), qt.Equals, 1)
}

// TestSchemaLineageKeepsItsOtherFormats is the control the issue asks for by
// name: the graph is added without changing the text and JSON contracts.
func TestSchemaLineageKeepsItsOtherFormats(t *testing.T) {
	c := qt.New(t)

	table := runLineage(c)
	document := runLineage(c, "--format", "json")

	c.Assert(table, qt.Contains, "SOURCE")
	c.Assert(table, qt.Not(qt.Contains), "digraph")
	c.Assert(document, qt.Contains, `"edges"`)
	c.Assert(document, qt.Not(qt.Contains), "digraph")
}
