package goschematogo

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	annotationext "ptah.run/core/annotation"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
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
		Objects: objects, Tables: ctx.sourceTables(), Slots: ctx.owned.slots, Fields: ctx.owned.fields, Coverage: ctx.db.FeatureCoverage,
		Target: platform.NormalizeDialect(ctx.opts.Dialect),
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
	for _, limit := range encoded.Limits {
		ctx.featureLimitAnnotations = append(ctx.featureLimitAnnotations, annotation("ptah:schema:notdescribed",
			attr{name: "kind", value: limit.Kind, set: true}, attr{name: "name", value: limit.Name, set: limit.Name != ""}))
	}
	ctx.ownerAttributes = make(map[ownerSlot][]attr)
	claimed := make(map[annotationext.Slot][]string)
	for _, slot := range encoded.Attributes {
		if err := checkOwnerAttributes(catalog, slot); err != nil {
			return err
		}
		place := ownerSlot{slot: slot.Slot, position: slot.Position}
		for _, attribute := range slot.Attributes {
			ctx.ownerAttributes[place] = append(ctx.ownerAttributes[place], attr{name: attribute.Name, value: attribute.Value, set: true})
		}
		claimed[slot.Slot] = append(claimed[slot.Slot], slot.Options...)
	}
	ctx.fieldTypes = make(map[annotationext.Slot]annotationext.FieldType, len(encoded.FieldTypes))
	for _, fieldType := range encoded.FieldTypes {
		ctx.fieldTypes[fieldType.Slot] = fieldType
	}
	return ctx.refuseUnclaimedOptions(claimed)
}

// refuseUnclaimedOptions refuses an index's WITH options no owner writes, such
// as PostgreSQL's fillfactor: left out, the comparator would plan a rebuild
// without them wherever the source is applied.
func (ctx *renderContext) refuseUnclaimedOptions(claimed map[annotationext.Slot][]string) error {
	for _, slot := range ctx.owned.slots {
		for name := range slot.Options {
			if !slices.Contains(claimed[slot.Slot], name) {
				return fmt.Errorf("%w: index %q has WITH options %s, which a Go annotation cannot represent",
					ptaherr.ErrUnsupportedFeature, slot.Slot.Name, strings.Join(slices.Sorted(maps.Keys(slot.Options)), ", "))
			}
		}
	}
	return nil
}

// ownerSlot is where in a declaration of the frontend's own the attributes
// an owner adds stand.
type ownerSlot struct {
	slot     annotationext.Slot
	position annotationext.Position
}

// checkOwnerAttributes refuses attributes the writer has no place for:
// attributes after the platform properties of a directive that carries none,
// and a platform property the directive does not read. Set.Encode has
// checked that every other attribute is one its owner adds to the directive.
func checkOwnerAttributes(catalog annotationmeta.Catalog, slot annotationext.SlotAttributes) error {
	carriesProperties := slot.Slot.Directive == indexDirective || slot.Slot.Directive == tableDirective && !slot.Slot.PrimaryKey
	if slot.Position == annotationext.AfterProperties && !carriesProperties {
		return fmt.Errorf("%w: an owner wrote attributes after the platform properties of %s, which has none", schemaext.ErrInvalidValue, slot.Slot)
	}
	for _, attribute := range slot.Attributes {
		if !catalog.AllowsAttribute(slot.Slot.Directive, attribute.Name) {
			return fmt.Errorf("%w: an owner wrote attribute %q of %s, which the frontend does not read",
				schemaext.ErrInvalidValue, attribute.Name, slot.Slot)
		}
	}
	return nil
}

// ownerAttrs are the attributes the owners add to the declaration slot at
// position.
func (ctx *renderContext) ownerAttrs(slot annotationext.Slot, position annotationext.Position) []attr {
	return ctx.ownerAttributes[ownerSlot{slot: slot, position: position}]
}

// validateMaterializedViewFacets refuses a materialized view setting no
// selected owner writes. The matview directive carries only the attributes an
// owner adds to it, such as a ClickHouse refresh schedule, and the owner
// writes, or refuses, the settings of its models.
func validateMaterializedViewFacets(views []schemamodel.MaterializedView, owners annotationext.Set) error {
	for _, view := range views {
		for _, kind := range view.Facets.DeclaredKinds() {
			if !owners.Encodes(kind) {
				return fmt.Errorf("%w: materialized view %q carries setting %q, which a Go annotation cannot represent",
					ptaherr.ErrUnsupportedFeature, view.Name, kind)
			}
		}
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
	written := make(map[string]string, len(directive.Attributes)+1)
	for _, attribute := range directive.Attributes {
		written[attribute.Name] = attribute.Value
	}
	if len(directive.Targets) > 0 {
		written[dialectscope.Attribute] = strings.Join(directive.Targets, ",")
	}
	for _, name := range slices.Sorted(maps.Keys(written)) {
		if !catalog.AllowsAttribute(directive.Directive, name) {
			return fmt.Errorf("%w: an owner wrote attribute %q of directive %q, which the frontend does not read",
				schemaext.ErrInvalidValue, name, directive.Directive)
		}
	}
	for _, name := range catalog.MissingRequiredAttributes(directive.Directive, written) {
		return fmt.Errorf("%w: an owner wrote directive %q without its required attribute %q",
			schemaext.ErrInvalidValue, directive.Directive, name)
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
	// fields are every field of the schema, in its order, as an owner may
	// type it.
	fields []annotationext.FieldDeclaration
	// tables names, by struct, the table a struct's fields, indexes and
	// constraints are handed under.
	tables map[string]annotationext.Table
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
				collected.addIndex(annotationext.Slot{Directive: indexDirective, Table: table, Name: db.Indexes[i].Name}, &db.Indexes[i])
			}
		}
		for i := range db.Constraints {
			if constraintOwner(db.Constraints[i]) == name {
				collected.addConstraint(table, &db.Constraints[i])
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
	for i := range db.Functions {
		function := &db.Functions[i]
		add(routineAnnotationSlot(*function), &function.Facets)
	}
	collected.fields = ownerFieldDeclarations(db, declared)
	collected.tables = declared
	return collected
}

// ownerFieldDeclarations describes field types and nullability independently
// of whether a field holds any owner facets.
func ownerFieldDeclarations(db *schemamodel.Database, declared map[string]annotationext.Table) []annotationext.FieldDeclaration {
	keys := make(map[string][]string, len(db.Tables))
	for _, table := range db.Tables {
		keys[table.StructName] = table.PrimaryKey
	}
	var fields []annotationext.FieldDeclaration
	for _, field := range db.Fields {
		table, found := declared[field.StructName]
		if !found {
			table = annotationext.Table{Struct: field.StructName}
		}
		fields = append(fields, annotationext.FieldDeclaration{
			Slot: annotationext.Slot{Directive: fieldDirective, Table: table, Name: field.Name}, Type: field.Type,
			Nullable: field.Nullable, Key: field.Primary || slices.Contains(keys[field.StructName], field.Name)})
	}
	return fields
}

// addIndex hands an index's declaration with its facets, its options, and
// the attributes of its own an owner may read beside its own, such as its
// type, so an owner can write the options it reads back as the index's.
func (o *ownerSlots) addIndex(slot annotationext.Slot, index *schemamodel.Index) {
	o.handed[&index.Facets] = true
	reads := make(map[string]string)
	for _, attribute := range indexDefinitionAttrs(*index) {
		if attribute.set {
			reads[attribute.name] = attribute.value
		}
	}
	o.slots = append(o.slots, annotationext.SlotFacets{Slot: slot, Facets: index.Facets,
		Options: maps.Clone(index.StorageParams), Reads: reads})
}

// constraintSlot is the declaration of constraint in the struct of table. A
// PRIMARY KEY constraint is marked as a primary key, as the renderer
// recognizes one, so an owner can tell a key's settings from another
// constraint's.
func constraintSlot(table annotationext.Table, constraint schemamodel.Constraint) annotationext.Slot {
	return annotationext.Slot{Directive: constraintDirective, Table: table, Name: constraint.Name,
		PrimaryKey: strings.EqualFold(strings.TrimSpace(constraint.Type), ast.PrimaryKeyConstraint.String())}
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

func routineAnnotationSlot(function schemamodel.Function) annotationext.Slot {
	directive := "ptah:schema:function"
	if function.IsProcedure() {
		directive = "ptah:schema:procedure"
	}
	return annotationext.Slot{Directive: directive, Name: function.Name, Parameters: function.Parameters}
}

func (o *ownerSlots) addConstraint(table annotationext.Table, key *schemamodel.Constraint) {
	o.handed[&key.Facets] = true
	if key.Facets.Len() > 0 {
		o.slots = append(o.slots, annotationext.SlotFacets{Slot: constraintSlot(table, *key), Facets: key.Facets, Reads: map[string]string{"type": key.Type}})
	}
}
