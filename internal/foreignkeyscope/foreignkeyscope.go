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
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

// unqualifiedTableSchema is the schema a table declared with none is in, on a
// dialect whose foreign key may name a table outside its description, and
// whether the dialect is one: its descriptor states ForeignKeysOutside. On any
// other dialect the renderer refuses such a key as a reference to an unknown
// table.
//
// On MySQL and MariaDB a database is the schema, and a description of one
// database names none. On PostgreSQL and CockroachDB an unqualified table is
// in `public`, so a key into `public.customers` beside an unqualified
// `customers` is a key into the description, not out of it. These are the
// engines measured: each accepts the key, and reads it back with the other
// schema's name (stokaro/ptah#3891, stokaro/ptah#3906).
func unqualifiedTableSchema(dialect string) (string, bool) {
	described, found := platform.Lookup(dialect)
	if !found || !described.Comparison.ForeignKeysOutside {
		return "", false
	}
	return described.DefaultSchema, true
}

// Outside reports whether reference names a table in a schema database holds
// nothing of, and which schema that is.
//
// A reference is not outside when it is unqualified, when the dialect is not
// one that accepts such a key, or when database holds anything of the schema
// it names: `crm.missing` beside a described `crm` is a table that is not
// there. Schemas compare as dialect compares them ([SchemaKey]), so on Oracle
// `CRM.accounts` is a key into a described crm.
func Outside(database schemamodel.Database, dialect, reference string) (schema string, outside bool) {
	unqualified, accepted := unqualifiedTableSchema(dialect)
	if !accepted {
		return "", false
	}
	ref, ok := tableref.Parse(reference)
	if !ok || !ref.Qualified {
		return "", false
	}
	semantics := identifier.ForDialect(dialect)
	named := SchemaKey(semantics, spelledSchema(semantics, reference, ref))
	for _, declared := range database.Schemas {
		if SchemaKey(semantics, declared.Name) == named {
			return "", false
		}
	}
	for _, table := range database.Tables {
		if SchemaKey(semantics, cmp.Or(table.Schema, unqualified)) == named {
			return "", false
		}
	}
	return ref.Schema, true
}

// SchemaKey is the key two spellings of one schema share under semantics:
// on Oracle crm, CRM and "CRM" share one, and "crm" has its own, since Oracle
// stores an unquoted name in upper case.
func SchemaKey(semantics identifier.Semantics, spelled string) string {
	return semantics.TableIdentityKey(strings.TrimSpace(spelled))
}

// StoredSchemaKey is [SchemaKey] for a schema as a catalog stores it, such as
// the connected Oracle account APP or a table's schema in a read.
func StoredSchemaKey(semantics identifier.Semantics, dialect, stored string) string {
	return SchemaKey(semantics, sqlident.CatalogName(dialect, stored))
}

// ReferencedSchemaKey is the [SchemaKey] of the schema reference names, or
// the empty string for an unqualified reference or one that does not parse.
func ReferencedSchemaKey(semantics identifier.Semantics, reference string) string {
	ref, ok := tableref.Parse(reference)
	if !ok || !ref.Qualified {
		return ""
	}
	return SchemaKey(semantics, spelledSchema(semantics, reference, ref))
}

// IntoSchema reports whether reference names a table of the schema whose key
// is schema, and whether database declares that table, unqualified or in that
// schema. An unqualified reference names no schema.
//
// A comparison asks it with the schema of the connection, where an
// unqualified table is: on Oracle `REFERENCES app.t1`, `APP.T1` and
// `"APP".T1` read as the account APP are each a key into the description's
// own T1, which the description declares or the plan drops.
func IntoSchema(database schemamodel.Database, semantics identifier.Semantics, reference, schema string) (into, declared bool) {
	ref, ok := tableref.Parse(reference)
	if schema == "" || !ok || !ref.Qualified || SchemaKey(semantics, spelledSchema(semantics, reference, ref)) != schema {
		return false, false
	}
	name := SchemaKey(semantics, spelledTable(semantics, reference, ref))
	for _, table := range database.Tables {
		if (table.Schema == "" || SchemaKey(semantics, table.Schema) == schema) && SchemaKey(semantics, table.Name) == name {
			return true, true
		}
	}
	return true, false
}

// IntoTableOf reports whether reference names one of tables, each named as a
// read spells its schema and table in [ptah.run/catalog.QualifyTableName]. An
// unqualified reference, and a table the read names with no schema, are in
// current, the schema the connection resolves an unqualified name in. Names
// compare as dialect compares them, so on Oracle `app.t1` names the stored
// APP.T1.
//
// A comparison asks it with the tables an exclude selector took out of the
// read: such a table stays on the server and out of the plan, so a key into it
// neither names a table the description lacks nor one the plan drops.
func IntoTableOf(dialect, reference, current string, tables []string) bool {
	ref, ok := tableref.Parse(reference)
	if !ok || len(tables) == 0 {
		return false
	}
	semantics := identifier.ForDialect(dialect)
	schema := StoredSchemaKey(semantics, dialect, current)
	if ref.Qualified {
		schema = SchemaKey(semantics, spelledSchema(semantics, reference, ref))
	}
	if schema == "" {
		return false
	}
	name := SchemaKey(semantics, spelledTable(semantics, reference, ref))
	for _, table := range tables {
		held, parsed := tableref.Parse(table)
		if parsed && StoredSchemaKey(semantics, dialect, cmp.Or(held.Schema, current)) == schema &&
			StoredSchemaKey(semantics, dialect, held.Name) == name {
			return true
		}
	}
	return false
}

// spelledSchema is the schema part of reference as written: ref.Schema, with
// its double quotes where semantics fold an unquoted name, so that a quoted
// name the fold would change keeps apart from the unquoted one.
func spelledSchema(semantics identifier.Semantics, reference string, ref tableref.Ref) string {
	return spelledPart(semantics, reference, 0, ref.Schema)
}

// spelledTable is [spelledSchema] for the table part.
func spelledTable(semantics identifier.Semantics, reference string, ref tableref.Ref) string {
	return spelledPart(semantics, reference, 1, ref.Name)
}

// spelledPart is part index of a two-part reference as written where it is
// double-quoted and semantics fold an unquoted name, and parsed otherwise.
func spelledPart(semantics identifier.Semantics, reference string, index int, parsed string) string {
	if semantics.UnquotedFold == "" {
		return parsed
	}
	parts := sqlident.SplitQualified(strings.TrimSpace(reference))
	if len(parts) != 2 {
		return parsed
	}
	if raw := strings.TrimSpace(parts[index]); strings.HasPrefix(raw, `"`) {
		return raw
	}
	return parsed
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
