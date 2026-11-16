// Package planrefusal declares the refusals a planner raises that a plan's
// machine-readable report names by a code.
//
// A planner sits below the command that reports its answer, and a report never
// reads a reason from message text, so the type carries the reason up the error
// chain. internal/atlasschema turns each type here into a refusal code. Each one
// keeps the error it wraps, so the message, the exit code and what errors.Is finds
// are the same with it and without it.
//
// A refusal code promises a caller that the target was read and that nothing
// reached it. Raise these only from a planner, or from the plan computation
// around the comparison it runs first, which both run after the read and before
// any statement is sent.
package planrefusal

// ViewOption is one security option, or a MySQL or MariaDB view's SQL SECURITY
// mode, that a plan would take from a view.
type ViewOption struct {
	// View is the view, as the schema spells it.
	View string `json:"view"`
	// Option is the option's name, or sql_security for the mode.
	Option string `json:"option"`
}

// LostViewSecurityError is the refusal of a plan that would take a security
// option, or SQL SECURITY INVOKER, from a view because the declaration does not
// state it.
type LostViewSecurityError struct {
	// Options are the options the plan would take, in the order the comparison
	// lists the views, and each view's options in name order.
	Options []ViewOption
	// Err is the refusal as the command reports it.
	Err error
}

// Error returns the message of the wrapped refusal.
func (e *LostViewSecurityError) Error() string { return e.Err.Error() }

// Unwrap returns the wrapped refusal.
func (e *LostViewSecurityError) Unwrap() error { return e.Err }

// ViewDefiner is one view a plan would replace as another account than the one
// the view runs as.
type ViewDefiner struct {
	// View is the view, as the schema spells it.
	View string `json:"view"`
	// Definer is the account the view runs as, as the catalog spells it, or
	// empty where the read did not record it.
	Definer string `json:"definer"`
	// Account is the account the plan's read ran as, which the replacement
	// makes the view's definer, or empty where the read did not record it.
	Account string `json:"account"`
}

// ViewDefinerError is the refusal of a plan that would replace a view running
// with its definer's privileges and make another account its definer, or that
// cannot tell whether it would because the read did not record one of them.
type ViewDefinerError struct {
	// Views are the views the plan would replace, in the order the comparison
	// lists them.
	Views []ViewDefiner
	// Err is the refusal as the command reports it.
	Err error
}

// Error returns the message of the wrapped refusal.
func (e *ViewDefinerError) Error() string { return e.Err.Error() }

// Unwrap returns the wrapped refusal.
func (e *ViewDefinerError) Unwrap() error { return e.Err }

// DependentObjectError is the refusal of a plan that drops an object something
// else depends on, where a drop would stop on the dependent or take it with
// nothing to create it again.
type DependentObjectError struct {
	// Err is the refusal as the command reports it, naming both objects.
	Err error
}

// Error returns the message of the wrapped refusal.
func (e *DependentObjectError) Error() string { return e.Err.Error() }

// Unwrap returns the wrapped refusal.
func (e *DependentObjectError) Unwrap() error { return e.Err }

// DeclarationError is the refusal of a declaration by the comparison a plan
// starts with: a check of what the schema states completed and found that this
// target cannot host it or plan it.
//
// The errors it wraps are raised on other paths too, and `schema diff` raises
// them over two documents with no target at all. So the plan computation sets
// this type at its comparison, and a report reads this type rather than the ones
// under it.
type DeclarationError struct {
	// Err is the refusal as the command reports it, naming what the schema
	// states.
	Err error
}

// Error returns the message of the wrapped refusal.
func (e *DeclarationError) Error() string { return e.Err.Error() }

// Unwrap returns the wrapped refusal.
func (e *DeclarationError) Unwrap() error { return e.Err }
