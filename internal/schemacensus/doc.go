// Package schemacensus enumerates every field of the desired schema and records
// what each one is for, so a fact an author declares cannot disappear on the way
// to SQL without somebody having decided that it should.
//
// The enumeration is reflection over [go.5x5.cz/ptah/core/schemamodel.Database]
// rather than a written list, so a field added to the model appears here the day
// it is added. [Registry] is the hand-written half: one disposition per field,
// with the reason for every field that is not rendered. [Measure] is the
// measured half: it ablates one field at a time out of a fixture, re-renders on
// every declared matrix cell, and reports where the output changed. A field
// declared [DDL] that no ablation can be seen through is a fact nothing reads.
//
// It measures whether a field is READ, not whether what it produces is right.
// An ablation that changes the SQL proves the renderer consulted the field; the
// SQL being correct is what the per-dialect tests are for.
//
// [MeasureEmissions] is the second thing the corpus is for, and it asks a
// different question of the same renders: not whether a declaration was read,
// but whether a physical object was created more than once. That is ADR 0015's
// second invariant -- one semantic owner and one DDL emission path per object --
// and its observable form is that no render creates the same named object
// twice. It reports what it could not classify beside what it found, because a
// statement shape it cannot read is a hole a clean report would cover.
package schemacensus
