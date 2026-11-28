// Package portablegrants rewrites the grants of a one-database read so that
// statements rendered from them name the database's objects without the
// database. A MySQL-family read of the database a connection selected reports
// each grant on that database's objects with the database's name
// ([catalog.Database.CurrentDatabase]). Rendered as read, a grant names the
// database it was read from, and a file replayed into a database of another
// name fails on objects that do not exist there. A checkpoint reads a shadow
// database, whose name is never the name of the database the checkpoint is
// replayed into.
package portablegrants

import (
	"slices"

	"ptah.run/catalog"
)

// Unqualified returns schema with each grant on a table, a column or a
// routine of the database the read describes naming its object alone, as the
// read names the database's tables. A grant on another database's object
// keeps its name, and so does a grant on the database itself, which names the
// database as its object rather than as a schema: the MySQL family has no
// grant target that names the database a statement runs in and that a
// description can carry.
//
// schema is not changed. A schema with no current database, from a read of a
// whole server or of a dialect whose objects carry a schema, is returned as
// it is, and so is nil.
func Unqualified(schema *catalog.Database) *catalog.Database {
	if schema == nil || schema.CurrentDatabase == "" {
		return schema
	}
	out := *schema
	out.Grants = slices.Clone(schema.Grants)
	for position, grant := range out.Grants {
		if grant.Schema == schema.CurrentDatabase {
			out.Grants[position].Schema = ""
		}
	}
	return &out
}
