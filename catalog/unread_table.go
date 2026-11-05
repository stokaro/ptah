package catalog

import "ptah.run/core/objectidentity"

// UnreadTable records a table a read found and does not describe, with the
// reason the reader gave. The table exists, so its absence from
// [Database.Tables] is not the absence of a table: a comparison refuses a
// schema that declares a table at its identity, or a foreign key that
// references one, with the reason, and plans nothing else about it.
type UnreadTable struct {
	// Identity names the table, with its source and normalized components.
	Identity objectidentity.ID `json:"identity"`
	// Reason says why the read leaves the table out, in the reader's words:
	// one line, which a report prints after the table's name.
	Reason string `json:"reason"`
}
