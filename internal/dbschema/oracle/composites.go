package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/oratype"
)

// compositeQuery reads the types this schema owns, one row per type, with the
// facts that decide whether the object type model can describe it.
//
// The model describes an object type that is a list of attributes and nothing
// else. Each of the other shapes was measured on 23.26.2.0.0 and 21.3.0.0.0:
//
//   - TYPECODE other than OBJECT is a collection type (VARRAY, TABLE), a
//     different shape with no field list.
//   - METHODS above 0 is an object type with behavior.
//     `CREATE TYPE m AS OBJECT (a NUMBER, MEMBER FUNCTION doubled RETURN NUMBER)`
//     reports METHODS = 1, and describing it as its attributes alone would say
//     a replay produces the same type when it produces one that has lost its
//     methods.
//   - SUPERTYPE_NAME is a subtype, whose full attribute list is its parent's
//     plus its own and whose declaration is UNDER rather than AS.
//   - INCOMPLETE = 'YES' is a shell. PostgreSQL's own spelling,
//     `CREATE TYPE t AS (a NUMBER)`, is ACCEPTED here and leaves exactly that:
//     ATTRIBUTES 0, INCOMPLETE YES, and USER_OBJECTS reporting INVALID.
//
// Every such type is read rather than filtered out in SQL, because the read
// records it: a type the read leaves out is one the schema does not have, and
// a declaration of that name would plan a CREATE OR REPLACE TYPE over it.
const compositeQuery = `
SELECT t.type_name, t.typecode, t.methods, t.supertype_name, t.incomplete
FROM all_types t
WHERE t.owner = :1
ORDER BY t.type_name`

// compositeAttributeQuery reads the attributes of this schema's object catalog.
//
// ATTR_NO is the declaration order, and it is the ORDER BY rather than the
// name: a composite's fields are positional, and reading them alphabetically
// would describe a different type that happens to have the same names.
//
// CHAR_USED says whether a VARCHAR2 or CHAR length counts characters (C) or
// bytes (B). ALL_TYPE_ATTRS carries it, unlike ALL_DOMAIN_COLS.
const compositeAttributeQuery = `
SELECT a.type_name, a.attr_name, a.attr_type_name, a.length, a.precision, a.scale, a.char_used
FROM all_type_attrs a
WHERE a.owner = :1
ORDER BY a.type_name, a.attr_no`

// readComposites reads the schema's object types into db as the Oracle type
// owner's observations, with the knowledge that the read looked.
//
// A type the model cannot describe is recorded as unrepresentable rather than
// left out, and so is an object type whose attributes this read finds none of.
// Measured on 23.26.2.0.0, every incomplete type reports ATTRIBUTES 0 and has
// no ALL_TYPE_ATTRS row -- a forward declaration `CREATE TYPE t;`, a body
// naming a type that does not exist, and a body where only one attribute does
// -- so the two rules overlap today. Both are kept because they answer
// different questions: one is about what the type IS, the other about what the
// read FOUND, and describing a type with no attributes would plan a CREATE OR
// REPLACE that empties it.
func (r *Reader) readComposites(ctx context.Context, db *catalog.Database) error {
	attributes, err := r.readCompositeAttributes(ctx)
	if err != nil {
		return err
	}

	rows, err := r.db.QueryContext(ctx, compositeQuery, r.schema)
	if err != nil {
		return err
	}
	defer rows.Close()

	objects := db.FeatureObjects
	var limits []schemaext.SubjectCoverage
	for rows.Next() {
		var (
			name                            string
			typeCode, supertype, incomplete sql.NullString
			methods                         sql.NullInt64
		)
		if err := rows.Scan(&name, &typeCode, &methods, &supertype, &incomplete); err != nil {
			return err
		}
		ref := oratype.Ref(oratype.ObjectTypeKind, "", name)
		shape := typeShape{code: typeCode.String, methods: methods.Int64, supertype: supertype.String,
			incomplete: incomplete.String, attributes: len(attributes[name])}
		if reason := shape.undescribed(); reason != "" {
			limits = append(limits, unrepresentable(oratype.ObjectTypeKind, ref, reason))
			continue
		}
		object, err := oratype.ObservedObject(ref, &oratype.ObservedObjectType{Attributes: attributes[name]})
		if err != nil {
			return err
		}
		if objects, err = objects.With(object); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return recordTypes(db, objects, oratype.ObjectTypeKind, limits)
}

// typeShape is what ALL_TYPES reports about a type, and the number of
// attributes the attribute read returned for it.
type typeShape struct {
	code       string
	methods    int64
	supertype  string
	incomplete string
	attributes int
}

// undescribed says why the object type model cannot describe the type, or is
// empty for one it can.
func (s typeShape) undescribed() string {
	switch {
	case !strings.EqualFold(strings.TrimSpace(s.code), "OBJECT"):
		return fmt.Sprintf("the type is a %s type, which the object type model cannot describe", strings.ToLower(strings.TrimSpace(s.code)))
	case strings.EqualFold(strings.TrimSpace(s.incomplete), "YES"):
		return "the object type is incomplete"
	case strings.TrimSpace(s.supertype) != "":
		return fmt.Sprintf("the object type is a subtype of %s, which the object type model cannot describe", s.supertype)
	case s.methods > 0:
		return "the object type has methods, which the object type model cannot describe"
	case s.attributes == 0:
		return "the read found no attribute of the object type"
	}
	return ""
}

// readCompositeAttributes groups the attributes by type name, in declaration
// order.
func (r *Reader) readCompositeAttributes(ctx context.Context) (map[string][]oratype.Attribute, error) {
	rows, err := r.db.QueryContext(ctx, compositeAttributeQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	attributes := make(map[string][]oratype.Attribute)
	for rows.Next() {
		var typeName, attrName, dataType string
		var length, precision, scale sql.NullInt64
		var charUsed sql.NullString
		if err := rows.Scan(&typeName, &attrName, &dataType, &length, &precision, &scale, &charUsed); err != nil {
			return nil, err
		}
		attributes[typeName] = append(attributes[typeName], oratype.Attribute{
			Name: attrName,
			// ALL_TYPE_ATTRS carries no vector description, so an attribute of that
			// type reads as a bare VECTOR here.
			Type: formatAttributeType(strings.TrimSpace(dataType), length, precision, scale, strings.TrimSpace(charUsed.String)),
		})
	}
	return attributes, rows.Err()
}

// formatAttributeType composes the spelling a declaration would have written
// for an object type attribute.
//
// It is the column read's composition, and for the same reason: ATTR_TYPE_NAME
// alone answers VARCHAR2 for a VARCHAR2(10) and NUMBER for a NUMBER(5,2), so a
// description built from it would compare unequal to the declaration that
// created it, forever. Four more facts were measured on 23.26.3.0.0. A
// TIMESTAMP attribute reports its fractional precision in SCALE, so
// TIMESTAMP(3) read as TIMESTAMP. The time zone forms are named TIMESTAMP WITH
// TZ and TIMESTAMP WITH LOCAL TZ, which no statement accepts. An INTERVAL
// reports its precisions in PRECISION and SCALE, so INTERVAL DAY(3) TO
// SECOND(2) read as INTERVAL DAY TO SECOND. And a length in characters reports
// CHAR_USED C, which VARCHAR2(10 CHAR) and VARCHAR2(10) differ by alone.
func formatAttributeType(dataType string, length, precision, scale sql.NullInt64, charUsed string) string {
	switch dataType {
	case "TIMESTAMP":
		return "TIMESTAMP" + parenthesized(scale)
	case "TIMESTAMP WITH TZ":
		return "TIMESTAMP" + parenthesized(scale) + " WITH TIME ZONE"
	case "TIMESTAMP WITH LOCAL TZ":
		return "TIMESTAMP" + parenthesized(scale) + " WITH LOCAL TIME ZONE"
	case "INTERVAL DAY TO SECOND":
		return "INTERVAL DAY" + parenthesized(precision) + " TO SECOND" + parenthesized(scale)
	case "INTERVAL YEAR TO MONTH":
		return "INTERVAL YEAR" + parenthesized(precision) + " TO MONTH"
	case "VARCHAR2", "CHAR":
		if charUsed == "C" && length.Valid && length.Int64 > 0 {
			return fmt.Sprintf("%s(%d CHAR)", dataType, length.Int64)
		}
	}
	return formatColumnType(dataType, sql.NullString{}, length, precision, scale)
}

// parenthesized is a catalog number in parentheses, or nothing for NULL.
func parenthesized(value sql.NullInt64) string {
	if !value.Valid {
		return ""
	}
	return fmt.Sprintf("(%d)", value.Int64)
}
