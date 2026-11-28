// Package portablegrants rewrites the grants of a one-database read so that
// statements rendered from them name the database's objects without the
// database. A MySQL-family read of the database a connection selected reports
// each grant on that database's objects with the database's name
// ([catalog.Database.CurrentDatabase]). Rendered as read, a grant names the
// database it was read from, and a file replayed into a database of another
// name fails on objects that do not exist there, or grants on the database it
// was read from. A checkpoint reads a shadow database, and a migration is
// planned against a dev database, whose name is never the name of the
// database the file is applied to.
package portablegrants

import (
	"slices"
	"strings"

	"ptah.run/catalog"
)

// Relative returns schema with each grant on the database the read
// describes, or on a table, a column or a routine of it, naming that
// database as the one a statement runs in. A grant on a table, a column or a
// routine names its object alone, as the read names the database's tables. A
// grant on the database itself becomes a grant on the database the session
// selected, an object type of DATABASE with no object name, which the MySQL
// family renders as `GRANT ... ON *`. A grant on another database or on its
// objects keeps its name. The current database stays as read, so a
// comparison still knows which database a grant on the database names.
//
// schema is not changed. A schema with no current database, from a read of a
// whole server or of a dialect whose objects carry a schema, is returned as
// it is, and so is nil.
func Relative(schema *catalog.Database) *catalog.Database {
	if schema == nil || schema.CurrentDatabase == "" {
		return schema
	}
	out := *schema
	out.Grants = slices.Clone(schema.Grants)
	for position, grant := range out.Grants {
		switch {
		case strings.EqualFold(grant.ObjectType, "SCHEMA"):
			if grant.ObjectName == schema.CurrentDatabase {
				out.Grants[position].ObjectType, out.Grants[position].ObjectName = "DATABASE", ""
			}
		case grant.Schema == schema.CurrentDatabase:
			out.Grants[position].Schema = ""
		}
	}
	return &out
}
