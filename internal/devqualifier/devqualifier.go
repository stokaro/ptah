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
// The MySQL-family and ClickHouse readers name every object of the one
// database they read without it, and take it off a view body, so for them
// nothing here applies. The Oracle reader names nothing of its account either,
// and Oracle never reaches this path, because `migrate diff` refuses a dev
// replay on it. For a reader that records the schema it read on a view, a
// materialized view, a routine or a sequence, the file would name the dev
// database without this call, as in DROP VIEW DEV.V, so it could not run on
// the database it is for: the statement stops there, or, with a guard, does
// nothing and leaves the object behind.
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
