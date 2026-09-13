// Package nodedispatch holds the one question every dialect renderer's
// VisitNode dispatcher has to answer identically.
//
// What the eight renderers emit differs by design. Whether a caller handed them
// a node at all does not, and a predicate written eight times agrees on the day
// the eighth is written and stops agreeing the first time one is extended.
//
// The error texts stay with each renderer: they carry that dialect's own
// sentinel and its own RenderError shape, which a shared constructor would have
// to flatten.
package nodedispatch

import "ptah.run/core/ast"

// IsAbsent reports whether a caller handed the renderer no node at all.
//
// It answers only for a nil interface. A non-nil interface holding a nil
// pointer is left to the type switch, which matches that kind's case and hands
// the handler the nil pointer -- which is what a handler's own nil check
// answers, in words that name the kind. A dispatcher catching the typed nil
// first replaces every one of those messages with one naming no kind: SQL
// Server answers `upsert node is nil` for a nil upsert, and a typed-nil guard
// ahead of the switch turns that into `AST node is nil`.
//
// A handler with no nil check dereferences the pointer, exactly as it does when
// the same typed nil arrives through Accept. This predicate does not change that
// either way; it decides only which layer answers.
func IsAbsent(node ast.Node) bool { return node == nil }
