package embedpg

// White-box testing required: the two verification query builders are
// unexported, and what they must produce cannot be measured through the
// database. PostgreSQL returns a table's rows in whatever order the scan
// produced, which for a small freshly written relation is usually the order
// they were inserted -- so an end-to-end test of a chunked corpus passes with
// the ORDER BY missing about as often as it fails. Measured: removing the
// ordinal from the ordering reddened the suite once and passed the next run
// against the same code.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedgen"
)

// orderingSpec is the smallest specification either builder accepts.
func orderingSpec(layout embedgen.TargetLayout, targetTable string) embedgen.Spec {
	return embedgen.Spec{
		Source: embedgen.Source{
			Schema: "public", Table: "articles",
			KeyFields: []string{"id"}, InputFields: []string{"body"},
			VersionStrategy: embedgen.VersionUpdatedAt, VersionField: "updated_at",
		},
		Target: embedgen.Target{
			Schema: "public", Table: targetTable, Column: "embedding",
			Representation: "vector", Metric: embedgen.MetricCosine, Layout: layout,
		},
	}
}

// TestVerificationQuery_OrdersAChunkSetByItsOrdinal is what the walk depends
// on and cannot check for itself.
//
// The corpus fold requires a source key's stored rows to arrive as ordinals
// 0, 1, 2 ... in order, and reports a key whose rows do not. Without the
// ordinal in the ORDER BY a well-formed set arrives shuffled and every chunked
// key is reported as holding rows its set does not declare -- measured, three
// source keys and thirty correct stored rows produced three blocking findings.
//
// The JOINED assertion is the shipping one. A chunked corpus always takes that
// builder, because the layout that holds a set is refused when it names the
// source relation, so the two sides are never one. The single-relation row
// asserts the shared ordering helper on the branch that path cannot reach --
// worth having, and not the same claim.
func TestVerificationQuery_OrdersAChunkSetByItsOrdinal(t *testing.T) {
	c := qt.New(t)
	spec := orderingSpec(embedgen.LayoutOwnTable, "article_chunks")

	one := oneRelationVerificationQuery(spec, &Source{spec: spec})
	joined := joinedVerificationQuery(spec, &Source{spec: spec})

	c.Assert(orderByOf(one), qt.Contains, `"embedding_chunk_ordinal"`)
	c.Assert(orderByOf(joined), qt.Contains, "ptah_stored_ordinal")
}

// TestVerificationQuery_OrdersByNoLiteralWhereThereIsNoOrdinal is the pair,
// and it pins a refusal rather than a preference.
//
// The layout that stores one vector per source row has no ordinal column, and
// the expression for it is the literal zero. A bare integer in an ORDER BY is
// a COLUMN POSITION to PostgreSQL, so ordering by it refused every
// verification of every unchunked generation with
// `ORDER BY position 0 is not in select list`. There is also nothing to order:
// that layout stores one row per key.
func TestVerificationQuery_OrdersByNoLiteralWhereThereIsNoOrdinal(t *testing.T) {
	c := qt.New(t)
	spec := orderingSpec(embedgen.LayoutSourceColumns, "articles")

	one := oneRelationVerificationQuery(spec, &Source{spec: spec})
	joined := joinedVerificationQuery(
		orderingSpec(embedgen.LayoutSourceColumns, "article_vectors"), &Source{spec: spec})

	c.Assert(orderByOf(one), qt.Equals, `"id"`)
	c.Assert(orderByOf(joined), qt.Not(qt.Contains), "ptah_stored_ordinal")
}

// orderByOf is the query's ordering clause.
func orderByOf(query string) string {
	_, ordering, found := strings.Cut(query, " ORDER BY ")
	if !found {
		return ""
	}
	return ordering
}
