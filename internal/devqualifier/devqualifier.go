// Package devqualifier takes the name of a dev database off what a read of it
// reports, for a migration file written from that read. It sits between the
// readers, which record the database an object lives in, and `migrate diff`,
// which writes a file for some other database.
package devqualifier

import (
	"ptah.run/catalog"
	"ptah.run/internal/sqlqualifier"
)

// Remove takes the name of the dev database off the objects a read of it
// qualifies with it.
//
// The MySQL-family readers record the database a view, a routine
// or a sequence lives in as its schema, and the server prints a view's body with
// that database in front of every relation. A file written from that read names
// the dev database, as in DROP VIEW `dev`.`v`, so it cannot run on the database
// it is for: the statement stops there, or, with a guard, does nothing and leaves
// the view behind. A table carries no such name in a read of one database.
//
// A read of several schemas keeps every name, since there the qualifier is what
// tells the objects apart.
func Remove(schema *catalog.Database, dialect, database string, readNames []string) {
	if schema == nil || database == "" || len(readNames) != 1 || readNames[0] != database {
		return
	}
	unqualified := func(owner *string) bool {
		if *owner != database {
			return false
		}
		*owner = ""
		return true
	}
	for i := range schema.Views {
		if unqualified(&schema.Views[i].Schema) {
			schema.Views[i].Body = sqlqualifier.ViewBody(schema.Views[i].Body, dialect, database, "")
		}
	}
	for i := range schema.MatViews {
		if unqualified(&schema.MatViews[i].Schema) {
			schema.MatViews[i].Body = sqlqualifier.ViewBody(schema.MatViews[i].Body, dialect, database, "")
		}
	}
	for i := range schema.Functions {
		unqualified(&schema.Functions[i].Schema)
	}
	for i := range schema.Sequences {
		unqualified(&schema.Sequences[i].Schema)
	}
}
