package schemaprep

import "ptah.run/core/schemamodel"

// NeedsPrimaryKeyConstraint reports whether common key metadata requires a
// table constraint rather than column flags. Lowering and target preparation
// share this decision so both derive the same ordered key from a declaration.
func NeedsPrimaryKeyConstraint(table schemamodel.Table, fields []schemamodel.Field) bool {
	// A NAME is a reason on its own. An inline `PRIMARY KEY` has nowhere to
	// carry one, so a named single-column key written that way reached the
	// server as a generated name and nothing reported the difference
	// (stokaro/ptah#2180).
	if table.PrimaryKeyName != "" && (len(table.PrimaryKey) > 0 || len(table.PrimaryKeyParts) > 0) {
		return true
	}
	if len(table.PrimaryKeyInclude) > 0 && (len(table.PrimaryKey) > 0 || len(table.PrimaryKeyParts) > 0) {
		return true
	}
	// So is a deferral, which the column spelling can carry on PostgreSQL and
	// cannot on every target that has the table spelling.
	if table.PrimaryKeyDeferrable && (len(table.PrimaryKey) > 0 || len(table.PrimaryKeyParts) > 0) {
		return true
	}
	// So is an access method: `PRIMARY KEY USING HASH` has no column spelling.
	if (table.PrimaryKeyMethod != "" || table.PrimaryKeyComment != "" || table.PrimaryKeyBlockSize != 0) &&
		(len(table.PrimaryKey) > 0 || len(table.PrimaryKeyParts) > 0) {
		return true
	}
	// UNIQUE on the same column is a reason of the same kind. The column
	// spelling has one slot for a key and the source declared two, and folding
	// them is not a formatting choice: measured on MariaDB 11.8,
	// `a INT PRIMARY KEY UNIQUE` produces the primary key ALONE, while the
	// source's own `a INT UNIQUE, PRIMARY KEY (a)` produces the primary key and
	// a secondary unique index. MySQL 8.4 keeps both either way, so writing the
	// key as a table constraint is what makes one rendering right on both --
	// and it is what the source wrote (stokaro/ptah#2787).
	if primaryKeyColumnIsUnique(table, fields) {
		return true
	}
	if len(table.PrimaryKeyParts) > 0 {
		return len(table.PrimaryKeyParts) > 1 || primaryKeyPartsHaveAttributes(table.PrimaryKeyParts)
	}
	return len(table.PrimaryKey) > 1
}

// primaryKeyColumnIsUnique reports whether a single-column primary key names a
// column the same table declares UNIQUE.
//
// Single-column only, because a composite key already renders as a table
// constraint and a column inside one carries no key of its own to collide with.
// The name comparison is the one the surrounding conversion uses: a field
// belongs to this table when its StructName matches.
func primaryKeyColumnIsUnique(table schemamodel.Table, fields []schemamodel.Field) bool {
	if len(table.PrimaryKey) != 1 {
		return false
	}
	for _, field := range fields {
		if field.StructName == table.StructName && field.Name == table.PrimaryKey[0] {
			return field.Unique
		}
	}
	return false
}

func primaryKeyPartsHaveAttributes(parts []schemamodel.PrimaryKeyPart) bool {
	for _, part := range parts {
		if part.Prefix != "" || part.Desc {
			return true
		}
	}
	return false
}
