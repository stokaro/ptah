package atlashcl

import (
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/planner/tablelookup"
	"go.5x5.cz/ptah/internal/tableref"
)

// pendingForeignRef is one single-column foreign key whose target table still
// has to be read against the whole document.
//
// It cannot be resolved where it is parsed: a `foreign_key` block may name a
// table block that appears further down the file, so the reference is recorded
// during the walk and read once the walk is over.
type pendingForeignRef struct {
	// field indexes db.Fields. The field is already appended by the time the
	// reference is recorded, and nothing removes fields during the walk.
	field int
	// owner is the table declaring the foreign key. Its schema is both the
	// tie-breaker for a name several schemas carry and the schema this
	// reference does not have to spell out -- see [resolveDocumentTableName].
	owner  goschema.Table
	table  string
	column string
}

// resolveDocumentTableRefs restores the schema of every table reference that
// would otherwise lose it, reading it off the block the reference names.
//
// A reference in HCL names a block by its labels, so a table in another schema
// is named `table.users`, not `table.other.users` -- see the measurement on
// objectRef in internal/atlashclrender. goschema.Finalize already reads the
// schema back off the referenced block for constraints, indexes, policies,
// grants and triggers. Two positions it does not cover are handled here:
//
//   - A SINGLE-column foreign key, which the HCL parser records on the field as
//     `Foreign = "<table>(<column>)"` rather than as a Constraint. Measured on
//     the pinned Atlas community binary v1.3.0's OWN inspect output for a
//     cross-schema foreign key -- `ref_columns = [table.users.column.id]` with
//     `users` declared in schema `other` -- Ptah read it back as `users(id)`,
//     losing the schema, while the multi-column form of the same key read back
//     as `other.users`. Ptah could not correctly read that binary's output for
//     the shape it emits most.
//   - A `data` block's table, whose schema goschema.Finalize never touches and
//     whose block has no owning table to supply one.
//   - A `permission` target or a `trigger`'s `on` naming a VIEW, a MATERIALIZED
//     view or a SEQUENCE. Finalize reads a grant's or a trigger's schema off a
//     TABLE block and does nothing for these, by the note at the end of
//     goschema.Finalize, so the schema the short form drops would be dropped for
//     good. The renderer only writes the short form where the document declares
//     the block, so the block is right there to read it back off
//     (stokaro/ptah#1234).
//
// An ambiguous name is left exactly as written, matching what goschema and
// [tablelookup.ResolveReference] both do with one: two tables of a name in
// different schemas make the short form mean neither, and inventing a winner
// would silently point the reference at the wrong table.
func (p *parser) resolveDocumentTableRefs() {
	for _, pending := range p.pendingForeignRefs {
		resolved := resolveDocumentTableName(p.db.Tables, pending.owner, pending.table)
		p.db.Fields[pending.field].Foreign = resolved + "(" + pending.column + ")"
	}
	for i := range p.db.ManagedData {
		data := &p.db.ManagedData[i]
		if data.Schema != "" {
			continue
		}
		// A data block has no owning table, so no schema is implicit in it and
		// the referenced block's is the only one there is.
		resolved := resolveDocumentTableName(p.db.Tables, goschema.Table{}, data.Table)
		ref, ok := tableref.Parse(resolved)
		if !ok || !ref.Qualified {
			continue
		}
		data.Schema = ref.Schema
		data.Table = ref.Name
	}
	for i := range p.db.Grants {
		grant := &p.db.Grants[i]
		grant.OnTable = p.qualifyFromRelationBlock(grant.OnTable)
		grant.OnSequence = p.qualifyFromRelationBlock(grant.OnSequence)
	}
	for i := range p.db.Triggers {
		trigger := &p.db.Triggers[i]
		trigger.Table = p.qualifyFromRelationBlock(trigger.Table)
	}
}

// qualifyFromRelationBlock writes back the schema of the `view`, `materialized`
// or `sequence` block a bare relation reference names.
//
// It answers only for a name no TABLE block claims, so it can never disagree
// with the resolution goschema.Finalize does for those: a relation name belongs
// to one namespace per schema, and where a table block carries the label the
// table path already restores whatever there is to restore.
//
// A block that declares no schema of its own restores nothing, which keeps a
// single-schema document exactly as it was written -- a bare name there is bare
// because there was never a schema on it, not because a spelling dropped one.
func (p *parser) qualifyFromRelationBlock(name string) string {
	ref, ok := tableref.Parse(name)
	if !ok || ref.Qualified {
		return name
	}
	for _, table := range p.db.Tables {
		if table.Name == ref.Name {
			return name
		}
	}
	schema := relationBlockSchema(p.db, ref.Name)
	if schema == "" {
		return name
	}
	return tableref.Canonical(schema, ref.Name)
}

// relationBlockSchema is the schema carried by the single view, materialized
// view or sequence block this document declares under a label, and empty where
// there is not exactly one.
func relationBlockSchema(db *goschema.Database, label string) string {
	var found []string
	for _, view := range db.Views {
		if ref, ok := tableref.Parse(view.Name); ok && ref.Qualified && ref.Name == label {
			found = append(found, ref.Schema)
		}
	}
	for _, view := range db.MaterializedViews {
		if ref, ok := tableref.Parse(view.Name); ok && ref.Qualified && ref.Name == label {
			found = append(found, ref.Schema)
		}
	}
	for _, sequence := range db.Sequences {
		if sequence.Name == label && sequence.Schema != "" {
			found = append(found, sequence.Schema)
		}
	}
	if len(found) != 1 {
		return ""
	}
	return found[0]
}

// resolveDocumentTableName reads the schema of an unqualified table reference
// off the table block the document declares, and writes it into the name only
// when the reference cannot be read without it.
//
// The owner's schema is the one the reader already has in hand, so a target in
// it keeps the bare name it was written with. Qualifying that case instead
// would be a change with nothing behind it -- every consumer of Field.Foreign
// resolves a bare name through [tablelookup.ResolveReference] against the same
// tables and reaches the same table -- and it is not free: measured by
// qualifying unconditionally first, it renders `REFERENCES "main"."users"
// ("id")` on SQLite, which that engine refuses with `near ".": syntax error`,
// and TestInspectSource_FileExportThenDevInspectionRoundTrip went red on
// exactly that statement.
//
// A reference that already carries a schema keeps it. The author wrote it, and
// this is a restore of what a spelling drops, not a normalizer.
func resolveDocumentTableName(tables []goschema.Table, owner goschema.Table, name string) string {
	ref, ok := tableref.Parse(name)
	if !ok || ref.Qualified {
		return name
	}
	resolved, ok := tableref.Parse(tablelookup.ResolveReference(tables, owner, name))
	if !ok || !resolved.Qualified || resolved.Schema == strings.TrimSpace(owner.Schema) {
		return name
	}
	return tableref.Canonical(resolved.Schema, resolved.Name)
}
