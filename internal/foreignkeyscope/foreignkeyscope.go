// Package foreignkeyscope decides whether a foreign key names a table in a
// schema its description holds nothing of.
//
// A key may reference a table in another schema: `REFERENCES crm.customers
// (id)` from a table of `public`, or on MySQL from a table of the database
// `shop`. A description of one schema cannot hold that table, so the renderer
// writes such a key as declared instead of refusing it as unknown, and the
// comparison refuses it only where the database it compares covers that
// schema. Both ask [Outside], so the two cannot disagree about which keys point
// out of a description.
package foreignkeyscope

import (
	"cmp"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/tableref"
)

// unqualifiedTableSchema is the schema a table declared with none is in, on
// each dialect whose foreign key may name a table outside its description. A
// dialect missing here has no such key: the renderer refuses it as a reference
// to an unknown table.
//
// On MySQL and MariaDB a database is the schema, and a description of one
// database names none. On PostgreSQL and CockroachDB an unqualified table is
// in `public`, so a key into `public.customers` beside an unqualified
// `customers` is a key into the description, not out of it. These are the
// engines measured: each accepts the key, and reads it back with the other
// schema's name (stokaro/ptah#3891, stokaro/ptah#3906).
var unqualifiedTableSchema = map[string]string{
	platform.MySQL:       "",
	platform.MariaDB:     "",
	platform.Postgres:    "public",
	platform.CockroachDB: "public",
}

// Outside reports whether reference names a table in a schema database holds
// nothing of, and which schema that is.
//
// A reference is not outside when it is unqualified, when the dialect is not
// one that accepts such a key, or when database holds anything of the schema
// it names: `crm.missing` beside a described `crm` is a table that is not
// there.
func Outside(database schemamodel.Database, dialect, reference string) (schema string, outside bool) {
	unqualified, accepted := unqualifiedTableSchema[platform.NormalizeDialect(dialect)]
	if !accepted {
		return "", false
	}
	ref, ok := tableref.Parse(reference)
	if !ok || !ref.Qualified {
		return "", false
	}
	for _, declared := range database.Schemas {
		if declared.Name == ref.Schema {
			return "", false
		}
	}
	for _, table := range database.Tables {
		if cmp.Or(strings.TrimSpace(table.Schema), unqualified) == ref.Schema {
			return "", false
		}
	}
	return ref.Schema, true
}

// Reference is one foreign key a description declares: what declares it and
// the table it names.
type Reference struct {
	// Declaration names the key the way a refusal names it: `field "x"` for
	// a key declared on a column, `constraint "x"` for a table constraint.
	Declaration string
	// Table is the referenced table as declared.
	Table string
}

// References lists the foreign keys database declares, those on columns
// first and then the table constraints, each in declaration order.
func References(database schemamodel.Database) []Reference {
	var references []Reference
	for _, field := range schemamodel.ProcessEmbeddedFields(database.EmbeddedFields, database.Fields) {
		if field.Foreign == "" {
			continue
		}
		references = append(references, Reference{
			Declaration: fmt.Sprintf("field %q", field.Name),
			Table:       schemaprep.ParseForeignKeyReference(field.Foreign).Table,
		})
	}
	for _, constraint := range database.Constraints {
		if !strings.EqualFold(strings.TrimSpace(constraint.Type), "FOREIGN KEY") {
			continue
		}
		references = append(references, Reference{
			Declaration: fmt.Sprintf("constraint %q", constraint.Name),
			Table:       constraint.ForeignTable,
		})
	}
	return references
}
