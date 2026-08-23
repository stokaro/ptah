package mssql

import (
	"database/sql"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
)

// extendedPropertyQuery reads the SQL Server extended properties Ptah
// describes.
//
// sys.extended_properties addresses a property by a class and up to two ids,
// and the three arms below are the three classes this description covers.
// class 0 is the DATABASE, whose ids are both 0 and which therefore has no
// schema to report. class 3 is a schema, whose major_id is a schema_id.
// class 1 is OBJECT_OR_COLUMN, whose major_id is an object_id and whose
// minor_id is 0 for the object itself or the column's id for one of its
// columns -- which is why the join to sys.columns is a LEFT one and why
// minor_id = 0 produces no column name.
//
// The database arm is NOT scoped by the schema predicate, for the reason
// [Reader.SetSchemas] cannot answer: a database-scoped property is in no
// schema, so narrowing the read to one does not exclude it any more than it
// excludes the database itself. That is the rule an extension already follows
// -- placement is not ownership -- and the alternative is worse in a way that
// only shows up on apply: a description that dropped it while the declaration
// still carried one would plan sp_dropextendedproperty for a property the
// operator declared.
//
// Three exclusions, and each is a row that would otherwise be described as
// something Ptah manages when it is not.
//
//   - MS_Description, at every class. The reader already turns it into the
//     object's Comment, and reporting it here as well would let the comment
//     comparator and the property comparator plan the same change from two
//     places, each unaware of the other.
//   - a class 1 property whose major_id is not a table. sys.extended_properties
//     addresses views, procedures, functions, indexes and types through the
//     same class, and each takes a different @level1type in the statement that
//     writes it. The join to sys.tables is what restricts this to the level
//     the renderer can write, rather than describing a property nothing could
//     re-emit.
//
// The value is converted twice on purpose. sp_addextendedproperty takes a
// sql_variant, so the stored base type is a fact of its own -- measured on SQL
// Server 2022, @value=42 stores `int` and a DATE stores `date` -- and
// SQL_VARIANT_PROPERTY is the only way to ask for it. CONVERT alone would
// answer `Jan  2 2026` for that date and lose both the type and, with the
// locale, the value.
const extendedPropertyQuery = `
	SELECT
		N'' AS schema_name,
		N'' AS table_name,
		N'' AS column_name,
		ep.name,
		CONVERT(NVARCHAR(MAX), ep.value),
		CONVERT(NVARCHAR(128), SQL_VARIANT_PROPERTY(ep.value, 'BaseType'))
	FROM sys.extended_properties AS ep
	WHERE ep.class = 0
	  AND ep.name <> N'MS_Description'

	UNION ALL

	SELECT
		s.name,
		N'' AS table_name,
		N'' AS column_name,
		ep.name,
		CONVERT(NVARCHAR(MAX), ep.value),
		CONVERT(NVARCHAR(128), SQL_VARIANT_PROPERTY(ep.value, 'BaseType'))
	FROM sys.extended_properties AS ep
	JOIN sys.schemas AS s ON s.schema_id = ep.major_id
	WHERE ep.class = 3
	  AND ep.name <> N'MS_Description'
	  AND (` + schemaPredicatePlaceholder + `)

	UNION ALL

	SELECT
		s.name,
		t.name,
		COALESCE(c.name, N''),
		ep.name,
		CONVERT(NVARCHAR(MAX), ep.value),
		CONVERT(NVARCHAR(128), SQL_VARIANT_PROPERTY(ep.value, 'BaseType'))
	FROM sys.extended_properties AS ep
	JOIN sys.tables AS t ON t.object_id = ep.major_id
	JOIN sys.schemas AS s ON s.schema_id = t.schema_id
	LEFT JOIN sys.columns AS c
	  ON c.object_id = ep.major_id AND c.column_id = ep.minor_id
	WHERE ep.class = 1
	  AND ep.name <> N'MS_Description'
	  AND t.is_ms_shipped = 0
	  AND t.name NOT IN ('schema_migrations', 'atlas_schema_revisions')
	  AND (` + schemaPredicatePlaceholder + `)

	ORDER BY 1, 2, 3, 4`

// readExtendedProperties reads the schema-, table- and column-scoped extended
// properties of the schemas this read covers.
func (r *Reader) readExtendedProperties() ([]types.DBExtendedProperty, error) {
	rows, err := r.db.Query(r.queryWithSchemaPredicate(extendedPropertyQuery), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var properties []types.DBExtendedProperty
	for rows.Next() {
		var property types.DBExtendedProperty
		var value sql.NullString
		var baseType sql.NullString
		var scannedSchema string
		if err := rows.Scan(&scannedSchema, &property.Table, &property.Column,
			&property.Name, &value, &baseType); err != nil {
			return nil, err
		}
		property.Schema = scannedSchema
		property.ValueType = strings.ToLower(strings.TrimSpace(baseType.String))
		property.ValueNotRepresentable = !representableExtendedPropertyType(property.ValueType)
		if !property.ValueNotRepresentable {
			property.Value = value.String
		}
		properties = append(properties, property)
	}
	return properties, rows.Err()
}

// representableExtendedPropertyType reports whether a sql_variant base type is
// one the renderer can write back.
//
// The renderer emits the value as an N” literal, which is nvarchar. A
// property stored as anything else would come back with a different type after
// a round trip, so the four character types are the list, and everything else
// -- int, date, bit, uniqueidentifier, and the rest of sql_variant's range --
// is reported as present and left alone rather than rewritten.
//
// The empty string is not representable either. It is what SQL_VARIANT_PROPERTY
// answers for a value it cannot describe, and treating "no answer" as a
// character type would write an N” literal over a value nobody read.
func representableExtendedPropertyType(baseType string) bool {
	switch baseType {
	case "nvarchar", "varchar", "nchar", "char":
		return true
	default:
		return false
	}
}
