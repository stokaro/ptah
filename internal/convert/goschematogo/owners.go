package goschematogo

import (
	"context"
	"fmt"
	"slices"
	"strings"

	annotationext "ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/annotationmeta"
	"ptah.run/internal/dialectscope"
)

// ownerPlace is where the source carries an owner's directive: a placement
// of the struct the standalone objects hang off, or a placement at a table.
type ownerPlace struct {
	placement annotationext.Placement
	table     annotationext.Table
}

// encodeOwners hands the selected owners what the schema holds of their
// models and keeps the directives they write, as the comments the source
// carries, for the places they stand. A directive the grammar would not read
// back is refused: one of a name, at a scope or with an attribute the
// frontend does not know, one that leaves out an attribute the directive
// requires, and one with targets where the directive takes none.
func (ctx *renderContext) encodeOwners(requestContext context.Context) error {
	catalog, err := annotationmeta.NewCatalog(ctx.opts.Annotations)
	if err != nil {
		return err
	}
	objects, err := ctx.db.FeatureObjects.All()
	if err != nil {
		return err
	}
	encoded, err := ctx.opts.Annotations.Encode(requestContext, annotationext.EncodeRequest{
		Objects: objects, Tables: ctx.sourceTables(), Slots: ctx.owned.slots, Coverage: ctx.db.FeatureCoverage,
	})
	if err != nil {
		return err
	}
	ctx.ownerDirectives = make(map[ownerPlace][]string)
	for _, directive := range encoded.Directives {
		if err := checkOwnerDirective(catalog, directive); err != nil {
			return err
		}
		attrs := make([]attr, 0, len(directive.Attributes)+1)
		for _, attribute := range directive.Attributes {
			attrs = append(attrs, attr{name: attribute.Name, value: attribute.Value, set: true})
		}
		place := ownerPlace{placement: directive.Placement}
		if directive.Table != nil {
			place.table = *directive.Table
		}
		ctx.ownerDirectives[place] = append(ctx.ownerDirectives[place],
			annotation(directive.Directive, append(attrs, dialectsAttr(directive.Targets))...))
	}
	return nil
}

// checkOwnerDirective refuses a directive the frontend would not read back as
// the source writes it: a struct comment.
func checkOwnerDirective(catalog annotationmeta.Catalog, directive annotationext.EncodedDirective) error {
	known, found := catalog.Lookup(directive.Directive)
	if !found || !annotationmeta.AllowsScope(known, annotationext.ScopeStruct) {
		return fmt.Errorf("%w: an owner wrote directive %q, which the frontend does not read on a struct",
			schemaext.ErrInvalidValue, directive.Directive)
	}
	written := make([]string, 0, len(directive.Attributes)+1)
	for _, attribute := range directive.Attributes {
		written = append(written, attribute.Name)
	}
	if len(directive.Targets) > 0 {
		written = append(written, dialectscope.Attribute)
	}
	for _, name := range written {
		if !catalog.AllowsAttribute(directive.Directive, name) {
			return fmt.Errorf("%w: an owner wrote attribute %q of directive %q, which the frontend does not read",
				schemaext.ErrInvalidValue, name, directive.Directive)
		}
	}
	for _, name := range catalog.RequiredAttributes(directive.Directive) {
		if !slices.Contains(written, name) {
			return fmt.Errorf("%w: an owner wrote directive %q without its required attribute %q",
				schemaext.ErrInvalidValue, directive.Directive, name)
		}
	}
	return nil
}

// writeOwnerDirectives writes the owners' directives that stand at place.
func (ctx *renderContext) writeOwnerDirectives(w *sourceWriter, place ownerPlace) {
	for _, text := range ctx.ownerDirectives[place] {
		w.writeComment(text)
	}
}

// hasOwnerGlobals reports an owner's directive on the struct the standalone
// objects hang off.
func (ctx *renderContext) hasOwnerGlobals() bool {
	for place, texts := range ctx.ownerDirectives {
		if place.table == (annotationext.Table{}) && len(texts) > 0 {
			return true
		}
	}
	return false
}

// sourceTable is table as the source declares it, which an owner's directive
// names to stand beside it.
func sourceTable(table schemamodel.Table) annotationext.Table {
	return annotationext.Table{Schema: table.Schema, Name: table.Name, Struct: table.StructName}
}

// sourceTables are the tables the source declares, in the order it writes
// them.
func (ctx *renderContext) sourceTables() annotationext.Tables {
	tables := make(annotationext.Tables, 0, len(ctx.db.Tables))
	for _, table := range sortedTables(ctx.db.Tables) {
		tables = append(tables, sourceTable(table))
	}
	return tables
}

// Directives of the frontend's own whose declarations hold facets.
const (
	tableDirective      = "ptah:schema:table"
	fieldDirective      = "ptah:schema:field"
	indexDirective      = "ptah:schema:index"
	constraintDirective = "ptah:schema:constraint"
	matviewDirective    = "ptah:schema:matview"
)

// ownerSlots are the declarations of the frontend's own directives in the
// schema that hold a facet, as the owners are handed them: each table and its
// primary key, in the order the source writes them; then the fields, indexes
// and constraints of each table's struct in that order too, and those of each
// struct that declares no table, whose table names only the struct; then the
// materialized views. handed addresses each of their facet slots, which an
// owner's facet reaches from there: a facet of a model an owner encodes is
// the owner's to write or refuse, so none disappears from the export because
// the source writes its declaration nowhere.
type ownerSlots struct {
	slots  []annotationext.SlotFacets
	handed map[*schemaext.Facets]bool
}

// collectOwnerSlots addresses the declarations the owners are handed.
func (ctx *renderContext) collectOwnerSlots() ownerSlots {
	collected := ownerSlots{handed: make(map[*schemaext.Facets]bool)}
	add := func(slot annotationext.Slot, facets *schemaext.Facets) {
		collected.handed[facets] = true
		if facets.Len() > 0 {
			collected.slots = append(collected.slots, annotationext.SlotFacets{Slot: slot, Facets: *facets})
		}
	}
	db := ctx.db
	declared := make(map[string]annotationext.Table, len(db.Tables))
	tables := make([]int, 0, len(db.Tables))
	for i := range db.Tables {
		tables = append(tables, i)
	}
	slices.SortStableFunc(tables, func(a, b int) int {
		return strings.Compare(db.Tables[a].QualifiedName(), db.Tables[b].QualifiedName())
	})
	structs := make([]string, 0, len(tables))
	for _, i := range tables {
		table := sourceTable(db.Tables[i])
		add(annotationext.Slot{Directive: tableDirective, Table: table}, &db.Tables[i].Facets)
		add(annotationext.Slot{Directive: tableDirective, Table: table, PrimaryKey: true}, &db.Tables[i].PrimaryKeyFacets)
		if _, seen := declared[table.Struct]; !seen {
			declared[table.Struct] = table
			structs = append(structs, table.Struct)
		}
	}
	for _, name := range append(structs, ctx.undeclaredStructs(declared)...) {
		table, found := declared[name]
		if !found {
			table = annotationext.Table{Struct: name}
		}
		for i := range db.Fields {
			if db.Fields[i].StructName == name {
				add(annotationext.Slot{Directive: fieldDirective, Table: table, Name: db.Fields[i].Name}, &db.Fields[i].Facets)
			}
		}
		for i := range db.Indexes {
			if indexOwner(db.Indexes[i]) == name {
				add(annotationext.Slot{Directive: indexDirective, Table: table, Name: db.Indexes[i].Name}, &db.Indexes[i].Facets)
			}
		}
		for i := range db.Constraints {
			if constraintOwner(db.Constraints[i]) == name {
				add(annotationext.Slot{Directive: constraintDirective, Table: table, Name: db.Constraints[i].Name}, &db.Constraints[i].Facets)
			}
		}
	}
	views := make([]int, 0, len(db.MaterializedViews))
	for i := range db.MaterializedViews {
		views = append(views, i)
	}
	slices.SortStableFunc(views, func(a, b int) int {
		return strings.Compare(db.MaterializedViews[a].Name, db.MaterializedViews[b].Name)
	})
	for _, i := range views {
		add(annotationext.Slot{Directive: matviewDirective, Name: db.MaterializedViews[i].Name}, &db.MaterializedViews[i].Facets)
	}
	return collected
}

// undeclaredStructs are, sorted, the structs a field, an index or a
// constraint names that declare no table.
func (ctx *renderContext) undeclaredStructs(declared map[string]annotationext.Table) []string {
	var names []string
	note := func(name string) {
		if _, found := declared[name]; !found && !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	for name := range ctx.fieldsByTable {
		note(name)
	}
	for name := range ctx.indexesByTable {
		note(name)
	}
	for name := range ctx.constraintsByTable {
		note(name)
	}
	slices.Sort(names)
	return names
}
