package atlasfilter

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlproperty"
)

// propertyAddress is a SQL Server extended property's schema, table and
// name as its identity carries them: the schema and the name as written, and
// the table folded. The schema is empty on a database property and the table
// on a schema property.
func propertyAddress(ref objectidentity.ID) (schema, table, name string) {
	address, _ := mssqlproperty.RefAddress(ref)
	return ref.Schema.Source, address.Table, ref.Name.Source
}

// filterPropertyFeatures drops the extended properties an exclusion selector
// names, and the ones whose owner is excluded.
//
// The second rule is what makes this more than a name match. An extended
// property is not an independent object: SQL Server drops it with the table it
// hangs off, so a description that excluded `docs` and kept a property
// addressed to `docs` would carry a row naming an object the same description
// says is not there, and a comparison would then plan sp_dropextendedproperty
// against a table that is not in the plan. The property is still offered to
// the patterns under its own name, so `--exclude ptah_flag` is reported as
// having matched something.
//
// A database property is in no schema, so excluding a schema does not exclude
// it: asking schemaExcluded("") would resolve the empty name to the
// connection's own schema, which is a different object entirely.
func (s *exclusionState) filterPropertyFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage) {
	keep := func(ref objectidentity.ID) bool {
		if ref.Kind != objectidentity.Kind(mssqlproperty.Kind) {
			return true
		}
		schema, table, name := propertyAddress(ref)
		switch {
		case s.matches("extended_property", s.nameCandidates(schema, name)...):
			return false
		case schema != "" && s.schemaExcluded(schema):
			return false
		default:
			return table == "" || !s.tableExcluded(schema, table)
		}
	}
	return objects.Select(keep), coverage.SelectSubjects(keep)
}

// selectPropertyFeatures keeps the extended properties a scope selects: one
// selected on its own name, and one on a table the scope keeps. A property on
// a table the scope drops goes with it, since SQL Server drops the property
// with the table. A database property is in no schema, so the schema universe
// does not reach it; narrowing it away would plan sp_dropextendedproperty for
// a property the declaration still names.
func (s *scopeSelection) selectPropertyFeatures(objects schemaext.Objects, coverage schemaext.Coverage, tableKept func(schema, table string) bool) (schemaext.Objects, schemaext.Coverage) {
	keep := func(ref objectidentity.ID) bool {
		if ref.Kind != objectidentity.Kind(mssqlproperty.Kind) {
			return true
		}
		schema, table, name := propertyAddress(ref)
		switch {
		case schema == "":
			return s.selectedDatabaseProperty(name)
		case table != "" && !tableKept(schema, table):
			return false
		default:
			return s.selected(typeList("extended_property"), schema, name) || table != ""
		}
	}
	return objects.Select(keep), coverage.SelectSubjects(keep)
}
