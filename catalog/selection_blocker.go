package catalog

// SelectionBlocker records one object a selection left outside a
// description, for a schema or a table that holds it. Dropping the schema or
// the table would remove the object too, so a comparison does not plan the
// drop: it reports it as blocked, and the commands that plan or apply SQL
// refuse it.
//
// It is diagnostic data, not an identifier to parse.
type SelectionBlocker struct {
	// Object names what the drop would remove, as diagnostics print it, such
	// as `table db.t` or `column db.u.c`. For a cause that left a whole kind
	// out of the read it names the kind, such as `triggers`.
	Object string `json:"object"`
	// Cause says how the selection left the object out.
	Cause SelectionCause `json:"cause"`
	// Selector is the pattern that left a whole kind out of the read, as the
	// user wrote it. It is empty for the other causes.
	Selector string `json:"selector,omitempty"`
	// Pattern names the database or the table the blocker is recorded for as
	// a selector names it on the run that recorded it, for a remedy to quote:
	// `db` for a database, and `db.u` or `u` for a table, as the run counts a
	// pattern from the realm or from the connection's schema.
	Pattern string `json:"pattern,omitempty"`
}

// SelectionCause says how a selection left an object outside a description.
type SelectionCause string

const (
	// SelectionExcluded is an object an --exclude pattern names.
	SelectionExcluded SelectionCause = "excluded"
	// SelectionNotIncluded is an object no --include pattern selects.
	SelectionNotIncluded SelectionCause = "not-included"
	// SelectionKindExcluded is a kind an --exclude pattern removes whole, such
	// as `*[type=trigger]`. A reader may skip that kind's catalog query, so
	// the read cannot show that a schema or a table holds none.
	SelectionKindExcluded SelectionCause = "kind-excluded"
	// SelectionKindNotIncluded is a kind no --include pattern can select, so
	// the read leaves it out as it does for [SelectionKindExcluded].
	SelectionKindNotIncluded SelectionCause = "kind-not-included"
)
