package oracle

import (
	"database/sql"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
)

// compositeQuery reads the object types this schema owns that Ptah's composite
// model can describe.
//
// The predicates are the model's limits written as SQL, and each one was
// measured on 23.26.2.0.0 and 21.3.0.0.0:
//
//   - TYPECODE = 'OBJECT' keeps out the collection types (VARRAY, TABLE),
//     which are a different shape with no field list.
//   - METHODS = 0 keeps out an object type with behavior.
//     `CREATE TYPE m AS OBJECT (a NUMBER, MEMBER FUNCTION doubled RETURN NUMBER)`
//     reports METHODS = 1, and describing it as its attributes alone would say
//     a replay produces the same type when it produces one that has lost its
//     methods.
//   - SUPERTYPE_NAME IS NULL keeps out a subtype, whose full attribute list is
//     its parent's plus its own and whose declaration is UNDER rather than AS.
//   - INCOMPLETE = 'NO' keeps out the shells. PostgreSQL's own spelling,
//     `CREATE TYPE t AS (a NUMBER)`, is ACCEPTED here and leaves exactly that:
//     ATTRIBUTES 0, INCOMPLETE YES, and USER_OBJECTS reporting INVALID.
//
// The last predicate overlaps [Reader.readComposites]'s own guard on an empty
// attribute list, and that is said here rather than left for the next reader to
// discover: measured on 23.26.2.0.0, EVERY incomplete type reports ATTRIBUTES 0
// and has no ALL_TYPE_ATTRS row -- a forward declaration `CREATE TYPE t;`, a
// body naming a type that does not exist, and a body where only one attribute
// does. So neither rule can be observed without the other today. Both are kept
// because they answer different questions: this one is about what the type IS,
// the other about what the read FOUND, and a catalog that ever reports an
// attribute for an incomplete type would otherwise turn a shell into a
// described composite.
const compositeQuery = `
SELECT t.type_name
FROM all_types t
WHERE t.owner = :1
  AND t.typecode = 'OBJECT'
  AND t.methods = 0
  AND t.supertype_name IS NULL
  AND t.incomplete = 'NO'
ORDER BY t.type_name`

// compositeAttributeQuery reads the attributes of this schema's object types.
//
// ATTR_NO is the declaration order, and it is the ORDER BY rather than the
// name: a composite's fields are positional, and reading them alphabetically
// would describe a different type that happens to have the same names.
const compositeAttributeQuery = `
SELECT a.type_name, a.attr_name, a.attr_type_name, a.length, a.precision, a.scale
FROM all_type_attrs a
WHERE a.owner = :1
ORDER BY a.type_name, a.attr_no`

// readComposites reads the schema's composite types.
//
// A type whose attributes this read finds none of is left out rather than
// described as empty: the query above already excludes the incomplete shells,
// so an object type with no attribute row is one the attribute read could not
// see, and describing it as a composite with no fields would plan a
// CREATE OR REPLACE that empties it.
func (r *Reader) readComposites() ([]types.DBComposite, error) {
	attributes, err := r.readCompositeAttributes()
	if err != nil {
		return nil, err
	}

	rows, err := r.db.Query(compositeQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var composites []types.DBComposite
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		fields := attributes[name]
		if len(fields) == 0 {
			continue
		}
		composites = append(composites, types.DBComposite{Name: name, Fields: fields})
	}
	return composites, rows.Err()
}

// readCompositeAttributes groups the attributes by type name, in declaration
// order.
func (r *Reader) readCompositeAttributes() (map[string][]types.DBCompositeField, error) {
	rows, err := r.db.Query(compositeAttributeQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	attributes := make(map[string][]types.DBCompositeField)
	for rows.Next() {
		var typeName, attrName, dataType string
		var length, precision, scale sql.NullInt64
		if err := rows.Scan(&typeName, &attrName, &dataType, &length, &precision, &scale); err != nil {
			return nil, err
		}
		attributes[typeName] = append(attributes[typeName], types.DBCompositeField{
			Name: attrName,
			// The same composition the column read uses, and for the same
			// reason: ATTR_TYPE_NAME alone answers VARCHAR2 for a
			// VARCHAR2(10) and NUMBER for a NUMBER(5,2), so a description
			// built from it would compare unequal to the declaration that
			// created it, forever.
			Type: formatColumnType(strings.TrimSpace(dataType), length, precision, scale),
		})
	}
	return attributes, rows.Err()
}
