package compare

import (
	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/columnsequence"
	"ptah.run/internal/objectidentity"
	"ptah.run/migration/schemadiff/difftypes"
)

// grantObjectTypeSequence is the object type of a grant on a sequence.
const grantObjectTypeSequence = "SEQUENCE"

// columnSequences says which column owns the sequence a grant names, where that
// sequence is part of its column: the one a serial or identity column creates.
//
// Such a sequence has two names. The database holds the name the column was
// created with, and renaming the table or the column leaves it alone: a table
// created as items and renamed to products keeps items_id_seq. A description
// writes the column as its shorthand, so replaying it creates the sequence
// under the name PostgreSQL gives it, products_id_seq, and the description's
// grant names that one. Keyed by name, a grant on the sequence therefore never
// matched across the two, and keyed by the column it matches whichever name
// either side uses (stokaro/ptah#4064).
//
// The database side answers from [catalog.Column.OwnedSequence]. The desired
// side answers with the name PostgreSQL gives the sequence of each column it
// creates, and a grant written with the database's name is placed through the
// database, when the desired schema creates a sequence for the same column.
type columnSequences struct {
	desired  map[tableIdentity]tableIdentity
	database map[tableIdentity]tableIdentity
	// declared holds the columns the desired schema creates a sequence for.
	declared map[tableIdentity]bool
	// current is the target a database grant names each column's sequence by.
	current map[tableIdentity]string
}

func newColumnSequences(
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
) columnSequences {
	builder := objectidentity.NewBuilder(semantics)
	sequences := columnSequences{
		desired:  make(map[tableIdentity]tableIdentity),
		database: make(map[tableIdentity]tableIdentity),
		declared: make(map[tableIdentity]bool),
		current:  make(map[tableIdentity]string),
	}
	for _, table := range database.Tables {
		for _, column := range table.Columns {
			if column.OwnedSequence == "" {
				continue
			}
			owner := builder.ColumnParts(table.Schema, table.Name, column.Name).Key()
			sequences.database[newTableIdentity(table.Schema, column.OwnedSequence, semantics)] = owner
			sequences.current[owner] = catalog.QualifyTableName(table.Schema, column.OwnedSequence)
		}
	}
	tables := make(map[string]schemamodel.Table, len(desired.Tables))
	for _, table := range desired.Tables {
		tables[table.StructName] = table
	}
	for _, field := range desired.Fields {
		table, ok := tables[field.StructName]
		if !ok {
			continue
		}
		name, ok := columnsequence.Declared(table.Name, field)
		if !ok {
			continue
		}
		owner := builder.ColumnParts(table.Schema, table.Name, field.Name).Key()
		sequences.desired[newTableIdentity(table.Schema, name, semantics)] = owner
		sequences.declared[owner] = true
	}
	return sequences
}

// desiredKey is key for a desired grant, with a column's sequence replaced by
// the column.
func (s columnSequences) desiredKey(key grantIdentity) grantIdentity {
	if owner, ok := s.desiredOwner(key); ok {
		key.object = owner
	}
	return key
}

// databaseKey is key for a database grant, with a column's sequence replaced
// by the column.
func (s columnSequences) databaseKey(key grantIdentity) grantIdentity {
	if key.objectType != grantObjectTypeSequence {
		return key
	}
	if owner, ok := s.database[key.object]; ok {
		key.object = owner
	}
	return key
}

func (s columnSequences) desiredOwner(key grantIdentity) (tableIdentity, bool) {
	if key.objectType != grantObjectTypeSequence {
		return tableIdentity{}, false
	}
	if owner, ok := s.desired[key.object]; ok {
		return owner, true
	}
	owner, ok := s.database[key.object]
	return owner, ok && s.declared[owner]
}

// nameAsDatabase renames each planned desired grant on a column's sequence to
// the name the database holds for it, so the statement names a sequence that
// exists. A column the database does not have yet keeps the declared name,
// which is the one its CREATE gives the sequence.
func (s columnSequences) nameAsDatabase(refs []difftypes.GrantRef, semantics identifier.Semantics) {
	for i, ref := range refs {
		owner, ok := s.desiredOwner(newGrantIdentity(ref, semantics))
		if !ok {
			continue
		}
		if current, ok := s.current[owner]; ok {
			refs[i].ObjectName = current
		}
	}
}
