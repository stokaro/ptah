package atlasfilter

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/columnsequence"
)

// A sequence that is part of a column -- the one a serial or identity column
// creates -- is not a sequence either side describes: the column creates it.
// A grant on it is still a grant on a sequence, and it follows the column. So
// under a selection it stays where its table stays and leaves where its table
// or its column leaves. A selector can also name the sequence itself, under
// type sequence, and then the grant on it stays or leaves on its own.
//
// Each side answers which column a sequence belongs to with the facts it has.
// A database read carries the owner in [catalog.Column.OwnedSequence]. A
// desired schema answers with the name PostgreSQL gives the sequence when it
// creates the column, [columnsequence.Declared]. Without either answer, a
// grant on such a sequence named an object neither side holds: an include
// selection dropped it from both sides, and an exclusion kept it beside the
// table it excluded.

// sequenceIdentity is the identity of a sequence in schema, the blank schema
// meaning the default one.
func (s *scopeSelection) sequenceIdentity(schema, name string) tableIdentity {
	return exactIdentities.SchemaScopedParts(objectidentity.KindSequence, s.effectiveSchema(schema), name).Key()
}

// databaseColumnSequences collects the sequences that are part of a column of
// tables and survive the selection: every one of a kept table, and one a
// selector names.
func (s *scopeSelection) databaseColumnSequences(
	tables []catalog.Table,
	keptTables map[tableIdentity]struct{},
) map[tableIdentity]struct{} {
	kept := make(map[tableIdentity]struct{})
	for _, table := range tables {
		for _, column := range table.Columns {
			if column.OwnedSequence == "" {
				continue
			}
			named := s.selected(typeList("sequence"), table.Schema, column.OwnedSequence)
			if named || s.tableKept(keptTables, table.Schema, table.Name) {
				kept[s.sequenceIdentity(table.Schema, column.OwnedSequence)] = struct{}{}
			}
		}
	}
	return kept
}

// generatedColumnSequences is [scopeSelection.databaseColumnSequences] for a
// desired schema, whose fields name no sequence: each is the one PostgreSQL
// creates for the field.
func (s *scopeSelection) generatedColumnSequences(db, out *schemamodel.Database) map[tableIdentity]struct{} {
	tables := generatedTableByStruct(db.Tables)
	keptByStruct := generatedTableByStruct(out.Tables)
	kept := make(map[tableIdentity]struct{})
	for _, field := range db.Fields {
		table, ok := tables[field.StructName]
		if !ok {
			continue
		}
		name, ok := columnsequence.Declared(table.Name, field)
		if !ok {
			continue
		}
		named := s.selected(typeList("sequence"), table.Schema, name)
		_, tableKept := keptByStruct[field.StructName]
		if named || tableKept {
			kept[s.sequenceIdentity(table.Schema, name)] = struct{}{}
		}
	}
	return kept
}

// columnSequenceKept answers whether the sequence a grant names, written
// `schema.name` or `name`, is a column's sequence the selection kept.
func (s *scopeSelection) columnSequenceKept(kept map[tableIdentity]struct{}, schema, name string) bool {
	_, ok := kept[s.sequenceIdentity(schema, name)]
	return ok
}

// excludeColumnSequences records as excluded each sequence that is part of a
// column of tables whose table or column an exclusion removed, or that a
// selector names. tables is the read before the exclusion, since an excluded
// table takes its columns with it.
//
// The selector is asked for every such sequence, so one that names a sequence
// is reported as matched even where its table left anyway.
func (s *exclusionState) excludeColumnSequences(tables []catalog.Table) {
	for _, table := range tables {
		for _, column := range table.Columns {
			if column.OwnedSequence == "" {
				continue
			}
			named := s.matches("sequence", s.nameCandidates(table.Schema, column.OwnedSequence)...)
			if named || s.tableExcluded(table.Schema, table.Name) ||
				s.anyColumnExcluded(table.Schema, table.Name, []string{column.Name}) {
				s.excludeSequence(table.Schema, column.OwnedSequence)
			}
		}
	}
}

// excludeGeneratedColumnSequences is [exclusionState.excludeColumnSequences]
// for a desired schema, before its exclusion.
func (s *exclusionState) excludeGeneratedColumnSequences(tables []schemamodel.Table, fields []schemamodel.Field) {
	byStruct := generatedTableByStruct(tables)
	for _, field := range fields {
		table, ok := byStruct[field.StructName]
		if !ok {
			continue
		}
		name, ok := columnsequence.Declared(table.Name, field)
		if !ok {
			continue
		}
		named := s.matches("sequence", s.nameCandidates(table.Schema, name)...)
		if named || s.tableExcluded(table.Schema, table.Name) ||
			s.anyColumnExcluded(table.Schema, table.Name, []string{field.Name}) {
			s.excludeSequence(table.Schema, name)
		}
	}
}

// generatedSequenceKeyExcluded answers whether the sequence a desired grant
// names, written `schema.name` or `name`, was excluded: a sequence of its own,
// or one that is part of a column.
func generatedSequenceKeyExcluded(s *exclusionState, sequence string) bool {
	schema, name := splitQualified(sequence)
	return s.sequenceExcluded(schema, name)
}

// ownerExcluded answers whether the table or the column a sequence is OWNED BY
// was excluded. The include side keeps such a sequence with its table
// ([sequenceOwnerTable]), so the exclusion takes it with its table: without
// that, `--exclude items` planned a DROP SEQUENCE for a sequence OWNED BY
// items, which `--include other` leaves alone.
func (s *exclusionState) ownerExcluded(sequenceSchema, ownedBy string) bool {
	schema, table := sequenceOwnerTable(sequenceSchema, ownedBy)
	if table == "" {
		return false
	}
	parts := strings.Split(strings.TrimSpace(ownedBy), ".")
	column := strings.TrimSpace(parts[len(parts)-1])
	return s.tableExcluded(schema, table) || s.anyColumnExcluded(schema, table, []string{column})
}
