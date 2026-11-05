// Package unreadtables writes the note a read command prints when its read
// found tables it does not describe, such as a YDB replica table no
// replication writes any more. The read keeps them in
// [ptah.run/catalog.Database.UnreadTables], and no document made from it
// names them, so without the note the output cannot tell such a table from
// one the database does not have.
package unreadtables

import (
	"fmt"
	"io"
	"strings"

	"ptah.run/catalog"
)

// Report writes one note naming each table the read found and does not
// describe, with the reader's reason, and nothing when there is none. One
// table fits on the note's line; several follow it, one per line.
//
// w may be nil, which is how the read surfaces spell "no diagnostics
// stream". Write errors are dropped: a diagnostic that fails to print must
// not fail a read that succeeded.
func Report(w io.Writer, schema *catalog.Database) {
	if w == nil || schema == nil || len(schema.UnreadTables) == 0 {
		return
	}
	entries := make([]string, 0, len(schema.UnreadTables))
	for _, table := range schema.UnreadTables {
		entries = append(entries, table.Identity.Path()+": "+table.Reason)
	}
	if len(entries) == 1 {
		_, _ = fmt.Fprintf(w, "note: 1 table this read does not describe: %s\n", entries[0])
		return
	}
	_, _ = fmt.Fprintf(w, "note: %d tables this read does not describe:\n  %s\n", len(entries), strings.Join(entries, "\n  "))
}
