package compare

import (
	"maps"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/objectidentity"
	"ptah.run/internal/tableref"
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

// columnUniqueness says how the column comparison reads the uniqueness of a
// column that a declared unique index or UNIQUE constraint also covers.
type columnUniqueness struct {
	// objectOwned are the columns whose uniqueness such an object holds. The
	// column comparison leaves their uniqueness out, and the object's own
	// comparison creates and drops it.
	objectOwned map[columnIdentity]struct{}
	// ownKey are the columns that declare UNIQUE themselves beside such an
	// object, on an engine that builds the two as two keys, with whether the
	// database holds the column's own key. The column comparison reads the
	// database's uniqueness of such a column from it.
	ownKey map[columnIdentity]bool
}

// readColumnUniqueness identifies desired single-column uniqueness whose
// lifecycle an index or table constraint represents. Live objects alone cannot
// suppress a column difference because database-side index filters may
// intentionally exclude backing indexes.
//
// A column that declares UNIQUE itself beside such an object is a second key,
// and its own key is compared rather than left out. Measured on MySQL 8.4.11
// and 26.7.0, MariaDB 11.8.9 and 12.3.3 and PostgreSQL 18.6, with Atlas CE
// v1.3.0 comparing a database that holds only the object:
//
//	declaration                                        MySQL, MariaDB  PostgreSQL
//	a int UNIQUE, CONSTRAINT uq_a UNIQUE (a)           two keys        one key
//	a int UNIQUE; ALTER TABLE c ADD CONSTRAINT uq_a    two keys        two keys
//	UNIQUE (a)
//	a int UNIQUE; CREATE UNIQUE INDEX ux ON c (a)      two keys        two keys
//
// Atlas CE adds the column's own key to a database that holds only the object,
// and so does this (stokaro/ptah#3784, stokaro/ptah#3812). PostgreSQL builds
// one key from the pair in one CREATE TABLE, and the SQL reader folds that
// pair into the named key before it reaches the model; see
// [schemaprep.FoldedIndexConstraints]. So a column that still declares UNIQUE
// beside an equal constraint in the model is two keys on every engine this
// covers. The other engines are not measured, and keep the column's uniqueness
// with the object.
func readColumnUniqueness(
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
) columnUniqueness {
	byIndex := make(map[columnIdentity]struct{})
	collectGeneratedUniqueIndexColumns(byIndex, desired, semantics)
	byConstraint := make(map[columnIdentity]struct{})
	collectGeneratedUniqueConstraintColumns(byConstraint, desired, semantics)
	uniqueness := columnUniqueness{
		objectOwned: make(map[columnIdentity]struct{}, len(byIndex)+len(byConstraint)),
		ownKey:      make(map[columnIdentity]bool),
	}
	maps.Copy(uniqueness.objectOwned, byIndex)
	maps.Copy(uniqueness.objectOwned, byConstraint)
	if database == nil {
		return uniqueness
	}
	held := readColumnKeys(desired, database, dialect, semantics).held
	tables := make(map[string]schemamodel.Table, len(desired.Tables))
	for _, table := range desired.Tables {
		if _, seen := tables[table.StructName]; !seen {
			tables[table.StructName] = table
		}
	}
	for _, field := range desired.Fields {
		table, ok := tables[field.StructName]
		if !field.Unique || !ok {
			continue
		}
		identity := newColumnIdentityForTable(table.Schema, table.Name, field.Name, semantics)
		_, besideIndex := byIndex[identity]
		_, besideConstraint := byConstraint[identity]
		declared := besideIndex || besideConstraint
		if !declared || !buildsColumnKeyBesideDeclaredKey(dialect) {
			continue
		}
		_, holds := held[newTableMemberKey(table.QualifiedName(), field.Name, semantics)]
		uniqueness.ownKey[identity] = holds
	}
	return uniqueness
}

// compared answers both sides of the column at key with their uniqueness as
// the column comparison reads it: the database's own key of a column that
// declares one beside a separate object, and nothing for a column whose
// uniqueness an object holds.
func (u columnUniqueness) compared(
	key columnIdentity, desired schemamodel.Field, database catalog.Column,
) (schemamodel.Field, catalog.Column) {
	if holds, separate := u.ownKey[key]; separate {
		database.IsUnique = holds
		return desired, database
	}
	if _, owned := u.objectOwned[key]; owned {
		desired.Unique = false
		database.IsUnique = false
	}
	return desired, database
}

// buildsColumnKeyBesideDeclaredKey reports whether a column's UNIQUE and a
// unique index or UNIQUE constraint the model declares over the same column
// are two keys on dialect; see [readColumnUniqueness].
func buildsColumnKeyBesideDeclaredKey(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB, platform.Postgres:
		return true
	default:
		return false
	}
}

func collectGeneratedUniqueIndexColumns(
	columns map[columnIdentity]struct{},
	desired *schemamodel.Database,
	semantics identifier.Semantics,
) {
	owners := schemamodel.ResolveIndexTableNames(desired.Indexes, desired.Tables)
	for position, index := range desired.Indexes {
		column, ok := singleGeneratedUniqueIndexColumn(index)
		if !ok || owners[position] == "" {
			continue
		}
		table, ok := generatedIndexTable(index, owners[position], desired.Tables)
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
	index schemamodel.Index,
	owner string,
	tables []schemamodel.Table,
) (schemamodel.Table, bool) {
	var match schemamodel.Table
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
		return schemamodel.Table{}, false
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

func singleGeneratedUniqueIndexColumn(index schemamodel.Index) (string, bool) {
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
	desired *schemamodel.Database,
	semantics identifier.Semantics,
) {
	for _, constraint := range desired.Constraints {
		table, ok := generatedConstraintTable(constraint, desired.Tables)
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
	constraint schemamodel.Constraint,
	tables []schemamodel.Table,
) string {
	table, ok := generatedConstraintTable(constraint, tables)
	if !ok {
		return strings.TrimSpace(constraint.Table)
	}
	return table.QualifiedName()
}

func generatedConstraintTable(
	constraint schemamodel.Constraint,
	tables []schemamodel.Table,
) (schemamodel.Table, bool) {
	tableName := strings.TrimSpace(constraint.Table)
	var owner schemamodel.Table
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
			return schemamodel.Table{}, false
		}
		owner = table
		found = true
	}
	return owner, found
}

func newColumnIdentityForTable(
	schema,
	table,
	column string,
	semantics identifier.Semantics,
) columnIdentity {
	return objectidentity.NewBuilder(semantics).ColumnParts(schema, table, column).Key()
}

func newTableIdentity(
	schema,
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
	schema,
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
