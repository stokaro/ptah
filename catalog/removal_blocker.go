package catalog

// RemovalBlocker records an object outside the model that names a table and
// blocks its removal, as a read found it, such as a Spanner change stream that
// names the table in its FOR clause. The database refuses to drop the table
// while the object names it, and refuses to drop each column the object
// names. A blocker that names no column blocks the removal of no column.
//
// Nothing compares a blocker and no document records one: the planning service
// of the table's engine reads it to refuse a plan that drops what it blocks,
// before a statement runs.
type RemovalBlocker struct {
	// Schema and Table name the table the object names, as the read spelled
	// them.
	Schema string `json:"schema"`
	Table  string `json:"table"`
	// Kind says what the object is, as a lowercase token such as
	// change_stream.
	Kind string `json:"kind"`
	// Name is the object's name, as the engine spells it.
	Name string `json:"name"`
	// Columns are the table's columns the object names, each once; empty when
	// it names the table as a whole.
	Columns []string `json:"columns,omitempty"`
}
