package catalog

// UnmodeledObject records an object of a kind the server has and Ptah does not
// model, such as a MySQL event, or the whole kind where a read could not list
// it. No schema source can declare one and no comparison reads one, so a report
// that finds every other object in place says nothing about it, and names it
// here so that nobody reads the report as covering it.
type UnmodeledObject struct {
	// Kind names the kind with a singular noun, such as "event".
	Kind string `json:"kind"`
	// Schema is the database the object is in, on a read of a whole server. It
	// is empty on a read of one database.
	Schema string `json:"schema,omitempty"`
	// Name names the object. It is empty on a record of every object of Kind in
	// Schema, which is the honest answer when the read could not list them.
	Name string `json:"name,omitempty"`
	// Reason says why nothing compares the object, in the reader's words: one
	// line, which a report prints after its name.
	Reason string `json:"reason"`
}
