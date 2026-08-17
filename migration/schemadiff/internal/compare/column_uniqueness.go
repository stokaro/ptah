package compare

import (
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/tableref"
)

// columnIdentity and tableIdentity are the shared identity model's comparison
// value, under this package's own names.
//
// They were two structs private to this package, duplicated byte for byte in
// internal/atlasfilter, and each object family had grown its own key on top.
// Four closed defects came from that -- see the objectidentity package doc --
// and the fix is one model rather than one more private key
// (stokaro/ptah#1345).
//
// The alias is [objectidentity.Key] and not [objectidentity.ID] because these
// names are used as map keys throughout this package, and a Key is exactly the
// part of an identity equality is decided on. An ID additionally carries the
// spelling each side wrote, which a diagnostic and a renderer need and a
// comparison must never see: two spellings of one table are one table.
type (
	columnIdentity = objectidentity.Key
	tableIdentity  = objectidentity.Key
	// objectIdentity is the same value under the name a family that is not a
	// table reads better with. A sequence keyed as `tableIdentity` invites the
	// next reader to key it AS a table, which is the reuse that makes two
	// families merge the day they share a map.
	objectIdentity = objectidentity.Key
)

// collectGeneratedObjectOwnedUniqueColumns identifies desired single-column
// uniqueness whose lifecycle is explicitly represented by an index or table
// constraint. Live objects alone cannot suppress a column difference because
// database-side index filters may intentionally exclude backing indexes.
func collectGeneratedObjectOwnedUniqueColumns(
	generated *goschema.Database,
	semantics identifier.Semantics,
) map[columnIdentity]struct{} {
	columns := make(map[columnIdentity]struct{})
	collectGeneratedUniqueIndexColumns(columns, generated, semantics)
	collectGeneratedUniqueConstraintColumns(columns, generated, semantics)
	return columns
}

func collectGeneratedUniqueIndexColumns(
	columns map[columnIdentity]struct{},
	generated *goschema.Database,
	semantics identifier.Semantics,
) {
	owners := goschema.ResolveIndexTableNames(generated.Indexes, generated.Tables)
	for position, index := range generated.Indexes {
		column, ok := singleGeneratedUniqueIndexColumn(index)
		if !ok || owners[position] == "" {
			continue
		}
		table, ok := generatedIndexTable(index, owners[position], generated.Tables)
		if !ok {
			continue
		}
		columns[newColumnIdentityForTable(
			table.Schema,
			table.Name,
			column,
			semantics,
		)] = struct{}{}
	}
}

func generatedIndexTable(
	index goschema.Index,
	owner string,
	tables []goschema.Table,
) (goschema.Table, bool) {
	var match goschema.Table
	matchCount := 0
	for _, table := range tables {
		if table.QualifiedName() != owner {
			continue
		}
		match = table
		matchCount++
	}
	if matchCount == 1 {
		return match, true
	}
	if matchCount == 0 || index.StructName == "" {
		return goschema.Table{}, false
	}

	matchCount = 0
	for _, table := range tables {
		if table.QualifiedName() != owner || table.StructName != index.StructName {
			continue
		}
		match = table
		matchCount++
	}
	return match, matchCount == 1
}

func singleGeneratedUniqueIndexColumn(index goschema.Index) (string, bool) {
	if !index.Unique || strings.TrimSpace(index.Condition) != "" {
		return "", false
	}
	if len(index.Parts) > 0 {
		if len(index.Parts) != 1 || index.Parts[0].Name == "" || index.Parts[0].Expr != "" {
			return "", false
		}
		return index.Parts[0].Name, true
	}
	if len(index.Fields) != 1 {
		return "", false
	}
	return index.Fields[0], true
}

func collectGeneratedUniqueConstraintColumns(
	columns map[columnIdentity]struct{},
	generated *goschema.Database,
	semantics identifier.Semantics,
) {
	for _, constraint := range generated.Constraints {
		table, ok := generatedConstraintTable(constraint, generated.Tables)
		if !strings.EqualFold(constraint.Type, "UNIQUE") ||
			!ok ||
			len(constraint.Columns) != 1 {
			continue
		}
		columns[newColumnIdentityForTable(
			table.Schema,
			table.Name,
			constraint.Columns[0],
			semantics,
		)] = struct{}{}
	}
}

func generatedConstraintTableName(
	constraint goschema.Constraint,
	tables []goschema.Table,
) string {
	table, ok := generatedConstraintTable(constraint, tables)
	if !ok {
		return strings.TrimSpace(constraint.Table)
	}
	return table.QualifiedName()
}

func generatedConstraintTable(
	constraint goschema.Constraint,
	tables []goschema.Table,
) (goschema.Table, bool) {
	tableName := strings.TrimSpace(constraint.Table)
	var owner goschema.Table
	found := false
	for _, table := range tables {
		if constraint.StructName != "" && table.StructName != constraint.StructName {
			continue
		}
		if tableName != "" &&
			table.Name != tableName &&
			table.QualifiedName() != tableName {
			continue
		}
		if found {
			return goschema.Table{}, false
		}
		owner = table
		found = true
	}
	return owner, found
}

func newColumnIdentityForTable(
	schema string,
	table string,
	column string,
	semantics identifier.Semantics,
) columnIdentity {
	return objectidentity.NewBuilder(semantics).ColumnParts(schema, table, column).Key()
}

func newTableIdentity(
	schema string,
	table string,
	semantics identifier.Semantics,
) tableIdentity {
	return objectidentity.NewBuilder(semantics).TableParts(schema, table).Key()
}

// newObjectIdentity is [newTableIdentity] for a family whose name is unique
// within a schema without being a table: a sequence, an enum, a domain, a view.
//
// The kind is carried rather than borrowed from tables. These families live in
// separate maps today, so the kind changes no answer now -- it is what stops a
// later map that holds two of them from merging a sequence with the table it
// was named after.
func newObjectIdentity(
	kind objectidentity.Kind,
	schema string,
	name string,
	semantics identifier.Semantics,
) objectIdentity {
	return objectidentity.NewBuilder(semantics).SchemaScopedParts(kind, schema, name).Key()
}

// newQualifiedObjectIdentity is [newObjectIdentity] for a name that arrives as
// one string, which is how the desired schema reports several of these
// families.
//
// Parsing is delegated to tableref for the reason
// [newQualifiedTableIdentity] gives: a name whose own text contains a dot must
// not be mistaken for a qualified one.
func newQualifiedObjectIdentity(
	kind objectidentity.Kind,
	name string,
	semantics identifier.Semantics,
) objectIdentity {
	ref, ok := tableref.Parse(name)
	if !ok {
		return newObjectIdentity(kind, "", name, semantics)
	}
	return newObjectIdentity(kind, ref.Schema, ref.Name, semantics)
}
