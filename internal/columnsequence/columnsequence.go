// Package columnsequence answers which sequence is part of a PostgreSQL
// column: the one a serial or an identity column creates.
//
// Such a sequence is never described as a sequence of its own, because
// writing the column back creates it. Something that names it -- a grant, or
// a selector -- therefore reaches it through the column. On a database the
// catalog says which column owns it, and [catalog.Column.OwnedSequence]
// carries that answer. A desired schema has no catalog, and a column that is
// not created yet has no sequence, so this package answers there with the name
// PostgreSQL gives the sequence when it creates the column.
package columnsequence

import (
	"strings"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/pgname"
)

// Declared answers the name of the sequence field creates as a column of
// table, and whether it creates one. A serial type creates one, and so does an
// identity clause; the name is [pgname.Sequence] of the bare table and column
// names. A sequence a column draws from through an ordinary nextval default is
// declared on its own and is not this column's.
func Declared(table string, field schemamodel.Field) (string, bool) {
	if !createsSequence(field) {
		return "", false
	}
	return pgname.Sequence(table, field.Name), true
}

// createsSequence reports whether PostgreSQL creates a sequence for field.
//
// The type spellings are the ones the PostgreSQL renderer writes as a serial
// type: the three serial names, their numbered aliases, and the AUTO_INCREMENT
// spellings it maps to SERIAL and BIGSERIAL.
func createsSequence(field schemamodel.Field) bool {
	if strings.TrimSpace(field.IdentityGeneration) != "" {
		return true
	}
	switch strings.ToUpper(strings.TrimSpace(field.Type)) {
	case "SMALLSERIAL", "SERIAL", "BIGSERIAL", "SERIAL2", "SERIAL4", "SERIAL8",
		"AUTO_INCREMENT", "BIGINT AUTO_INCREMENT":
		return true
	default:
		return false
	}
}
