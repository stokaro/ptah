// Package readscope says which object kinds a run does not depend on, for a
// schema reader that has to answer for what it could not see.
//
// A comparison reads a missing row as a missing object, so a MySQL-family read
// refuses when the connecting account holds no privilege that shows a kind. A
// run that leaves the kind out, with an exclude selector for example, depends on
// nothing the account cannot see, and that refusal would be false. The selectors
// apply after the read and the read sits behind [ptah.run/catalog.SchemaReader],
// so the fact travels in the context from the run that knows it to the reader
// that needs it.
//
// The zero answer is that a run depends on every kind, so a caller that says
// nothing gets the refusal.
package readscope

import (
	"context"
	"slices"
)

// Kind is a family of objects a reader needs a privilege to see.
type Kind string

// The kinds a run can leave out.
const (
	// Tables is base tables with their columns and indexes.
	Tables Kind = "tables"
	// Views is views with their definitions.
	Views Kind = "views"
	// Constraints is primary keys, unique keys, foreign keys and CHECK
	// constraints.
	Constraints Kind = "constraints"
	// Triggers is triggers.
	Triggers Kind = "triggers"
	// Routines is stored functions and procedures.
	Routines Kind = "routines"
)

type contextKey struct{}

// Without returns ctx saying that the run does not depend on kinds, on top of
// what ctx already says.
func Without(ctx context.Context, kinds ...Kind) context.Context {
	if len(kinds) == 0 {
		return ctx
	}
	current, _ := ctx.Value(contextKey{}).([]Kind)
	return context.WithValue(ctx, contextKey{}, slices.Concat(current, kinds))
}

// Needs reports whether the run ctx belongs to depends on kind.
func Needs(ctx context.Context, kind Kind) bool {
	left, _ := ctx.Value(contextKey{}).([]Kind)
	return !slices.Contains(left, kind)
}

type authoredKey struct{}

// Authored returns ctx saying that the read covers what this run created with
// this account, on a dev database or a shadow database. The account that created
// a trigger, a routine or a constraint is shown it, whatever it holds on the
// database, so the read has nothing to prove about those kinds. It still has to
// answer for what the account was shown: a column list needs a privilege of its
// own, and a view or a routine with another definer comes back without a body.
func Authored(ctx context.Context) context.Context {
	return context.WithValue(ctx, authoredKey{}, true)
}

// IsAuthored reports whether ctx carries [Authored].
func IsAuthored(ctx context.Context) bool {
	authored, _ := ctx.Value(authoredKey{}).(bool)
	return authored
}
