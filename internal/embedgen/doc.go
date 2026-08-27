// Package embedgen models an embedding generation: what produced a set of
// vectors, and what makes two sets of vectors comparable or incomparable.
//
// # Why an identity rather than a model name
//
// Changing an embedding model is a migration, and the thing being migrated is
// not "the model". A vector's meaning depends on the model, on its revision, on
// the output dimension, on how the source row was turned into text, on whether
// that text was truncated, on how the result was normalized, and on the metric
// the index will compare it under. Two sets of vectors that differ in any of
// those are not interchangeable, and a system that treats them as
// interchangeable answers queries with silent nonsense rather than an error
// (stokaro/ptah#2068, Decision 5).
//
// So a generation carries an identity derived from every load-bearing part of
// the transformation, and the epic names both halves of that: what MUST change
// it, and what must NOT. A request id, a timestamp, a worker count or a display
// name changing must leave the identity alone, or every run produces a new
// generation and nothing is ever comparable to anything.
//
// # What this package does not do
//
// It computes no embeddings and contacts no provider. It is the description of
// a transformation and the identity that follows from it; the engine that
// executes one is built on top.
package embedgen
