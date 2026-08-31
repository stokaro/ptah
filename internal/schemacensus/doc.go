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
package schemacensus
