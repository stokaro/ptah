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
//
// The context also says how a read names what it reports: by default an object
// of the connection's current schema carries no schema, and a run whose
// statements name every schema asks for it with [Qualified], or with
// [FullyQualified] when a connection scoped to one schema must name it too.
//
// A selector can leave out single objects too, and a read that has to send a
// statement naming one object, beyond the catalog queries that list them all,
// asks [Keeps] first and sends nothing for an object the run leaves out.
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
	// Events is MySQL-family events. Nothing compares one, so a read that cannot
	// list them says so rather than refuses, and a run that left them out has
	// nothing to say about them.
	Events Kind = "events"
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

type qualifiedKey struct{}

// Qualified returns ctx saying that the statements the run plans from a read
// name the schema of every object, so the read names it too: an object of the
// connection's current schema carries that schema rather than none. A
// realm-scoped plan on an engine that resolves names along a search path is
// written that way, as the pinned community binary writes it. A drop or an
// alter takes its name from the read, and from a read that left the current
// schema out it would name the object bare.
//
// A reader honors it only on a connection that resolves a bare name along a
// search path and describes every schema. On a connection scoped to one schema
// a bare name resolves where the read found the object, and a reader that names
// every object by its schema anyway has nothing to change.
func Qualified(ctx context.Context) context.Context {
	return context.WithValue(ctx, qualifiedKey{}, true)
}

// IsQualified reports whether ctx carries [Qualified].
func IsQualified(ctx context.Context) bool {
	qualified, _ := ctx.Value(qualifiedKey{}).(bool)
	return qualified
}

type fullyQualifiedKey struct{}

// FullyQualified returns ctx asking a read to name every object's schema,
// including on a connection scoped to one schema. A migration file needs
// those names even when its planning URL pins search_path: PostgreSQL still
// searches pg_catalog before that schema for a bare reference.
func FullyQualified(ctx context.Context) context.Context {
	return context.WithValue(Qualified(ctx), fullyQualifiedKey{}, true)
}

// IsFullyQualified reports whether ctx asks for qualified names even on a
// connection scoped to one schema.
func IsFullyQualified(ctx context.Context) bool {
	qualified, _ := ctx.Value(fullyQualifiedKey{}).(bool)
	return qualified
}

// Object is one object a read found, named as the read reports it.
type Object struct {
	// Kind is the object's kind.
	Kind Kind
	// Schema is the schema the read names the object with, and "" for an
	// object the read leaves unqualified.
	Schema string
	// Table is the table a trigger belongs to, and "" for an object of any
	// other kind.
	Table string
	// Name is the object's name.
	Name string
}

// Read is what a reader asks a [Selector] about: the objects it found on a
// connection of Dialect, where Current names the schema whose objects the
// read leaves unqualified, and "" for a read that qualifies every object.
// Objects holds the ones it may send a statement for and every object a
// selector could name to keep or leave out one of them: a trigger leaves with
// its table.
type Read struct {
	Dialect string
	Current string
	Objects []Object
}

// Selector reports, for each object of a read, whether the run keeps it. It
// answers true where it cannot tell, since a read that leaves out an object
// the run keeps describes it wrong.
type Selector func(read Read) []bool

type selectorsKey struct{}

// Selecting returns ctx saying that the run keeps only the objects selector
// keeps, on top of what ctx already says. A nil selector leaves ctx as it
// was.
func Selecting(ctx context.Context, selector Selector) context.Context {
	if selector == nil {
		return ctx
	}
	current, _ := ctx.Value(selectorsKey{}).([]Selector)
	return context.WithValue(ctx, selectorsKey{}, append(slices.Clip(current), selector))
}

// Keeps reports, for each object of read, whether the run ctx belongs to
// keeps it: every selector [Selecting] gave keeps it. Where none was given,
// and in a read ctx marks [Authored], it keeps every object. A read of what
// the run created names that database the way the run names its own, which
// is not always the way the selectors count, and an object there is the
// run's own to read.
func Keeps(ctx context.Context, read Read) []bool {
	kept := make([]bool, len(read.Objects))
	for index := range kept {
		kept[index] = true
	}
	if IsAuthored(ctx) {
		return kept
	}
	selectors, _ := ctx.Value(selectorsKey{}).([]Selector)
	for _, selector := range selectors {
		answer := selector(read)
		for index := range kept {
			kept[index] = kept[index] && (index >= len(answer) || answer[index])
		}
	}
	return kept
}
