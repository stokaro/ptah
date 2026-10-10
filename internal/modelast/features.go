package modelast

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/tableref"
)

// validateFeatureLowering accounts for feature state before the first node is
// visited. Table facets and named children travel on their CREATE TABLE, index
// facets on their index node, and materialized view facets on their CREATE
// MATERIALIZED VIEW, where the selected renderer consumes or refuses them. Other placements need a selected owner lowering service;
// dropping them would leave a successful AST that no downstream renderer can
// refuse.
//
// A setting attached to a table, index or materialized view that the source
// could not capture, such as a refresh schedule whose stored clause a reader
// could not read, does not stop the lowering: the object is still lowered, and
// the returned notes say what its statement does not carry.
func validateFeatureLowering(database schemamodel.Database, dialect string) ([]ast.Node, error) {
	notes, err := validateLoweringCoverage(database.FeatureCoverage)
	if err != nil {
		return nil, err
	}
	if err := validateFacetLowering(&database, dialect); err != nil {
		return nil, err
	}
	if err := schemaprep.ValidateFeatureParents(&database, dialect); err != nil {
		return nil, err
	}
	for _, ref := range database.FeatureObjects.Refs() {
		if database.FeatureCoverage.Lookup(schemaext.Kind(ref.Kind), ref).State == schemaext.Absent {
			return nil, fmt.Errorf("%w: declared feature object %s is marked absent", schemaext.ErrInvalidValue, ref)
		}
	}
	return notes, nil
}

func validateFacetLowering(database *schemamodel.Database, dialect string) error {
	builder := objectidentity.NewBuilder(identifier.ForDialect(dialect))
	owners := make(map[*schemaext.Facets]objectidentity.ID, len(database.Tables)+len(database.Indexes))
	for i := range database.Tables {
		table := &database.Tables[i]
		owners[&table.Facets] = builder.TableParts(table.Schema, table.Name)
	}
	for i, owner := range schemamodel.ResolveIndexOwners(database.Indexes, database.Tables, database.MaterializedViews) {
		if ref, valid := tableref.Parse(owner); valid {
			owners[&database.Indexes[i].Facets] = builder.IndexParts(ref.Schema, ref.Name, database.Indexes[i].Name)
		}
	}
	for i := range database.MaterializedViews {
		view := &database.MaterializedViews[i]
		owners[&view.Facets] = builder.SchemaScoped(objectidentity.KindMatView, view.Name)
	}
	// A column's settings are lowered with the column definition that holds
	// them, so a field is an owner where it belongs to a declared table.
	tables := make(map[string]*schemamodel.Table, len(database.Tables))
	for i := range database.Tables {
		tables[database.Tables[i].StructName] = &database.Tables[i]
	}
	for i := range database.Fields {
		field := &database.Fields[i]
		if table, found := tables[field.StructName]; found {
			owners[&field.Facets] = builder.ColumnParts(table.Schema, table.Name, field.Name)
		}
	}
	for _, facets := range database.FacetSlots() {
		if facets.IsZero() {
			continue
		}
		subject, supported := owners[facets]
		if !supported {
			return fmt.Errorf("%w: no schema-to-AST lowering for feature facet %q", ptaherr.ErrUnsupportedFeature, facets.DeclaredKinds()[0])
		}
		for _, kind := range facets.Kinds() {
			if database.FeatureCoverage.Lookup(kind, subject).State == schemaext.Absent {
				return fmt.Errorf("%w: declared feature facet %q on %s is marked absent", schemaext.ErrInvalidValue, kind, subject)
			}
		}
	}
	return nil
}

func validateLoweringCoverage(coverage schemaext.Coverage) ([]ast.Node, error) {
	if !coverage.IsZero() && coverage.Representation() != schemaext.Desired {
		return nil, fmt.Errorf("%w: schema-to-AST requires desired feature coverage", schemaext.ErrInvalidValue)
	}
	// Knowledge is not an executable object. Complete and absent claims can
	// accompany declarations without creating DDL. Default requests and known
	// representation loss require owner interpretation before this boundary,
	// except a setting of one owner the source could not capture: the owner
	// is lowered without it, and a note says so.
	for _, record := range coverage.KindRecords() {
		if err := validateLoweringKnowledge(record.Model.Kind, record.Knowledge); err != nil {
			return nil, err
		}
	}
	var notes []ast.Node
	for _, record := range coverage.SubjectRecords() {
		if note := uncapturedSetting(record); note != nil {
			notes = append(notes, note)
			continue
		}
		if err := validateLoweringKnowledge(record.Kind, record.Knowledge); err != nil {
			return nil, err
		}
	}
	return notes, nil
}

// uncapturedSetting is the note for a table, index or materialized view whose
// attached setting the source could not capture, or nil for any other
// record.
func uncapturedSetting(record schemaext.SubjectCoverage) ast.Node {
	switch record.Subject.Kind {
	case objectidentity.KindTable, objectidentity.KindIndex, objectidentity.KindMatView:
	default:
		return nil
	}
	if record.Knowledge.State != schemaext.Unrepresentable && record.Knowledge.State != schemaext.Uninspected {
		return nil
	}
	name := record.Subject.Name.Source
	if record.Subject.Schema.Source != "" {
		name = record.Subject.Schema.Source + "." + name
	}
	return ast.NewComment(fmt.Sprintf("%s %s: %s was not captured (%s), and its statement does not carry it",
		record.Subject.Kind, name, record.Kind, record.Knowledge.Reason))
}

func validateLoweringKnowledge(kind schemaext.Kind, knowledge schemaext.Knowledge) error {
	if knowledge.State == schemaext.Defaulted || knowledge.State == schemaext.Unrepresentable {
		return fmt.Errorf("%w: feature %q requires owner lowering for %q knowledge", ptaherr.ErrUnsupportedFeature, kind, knowledge.State)
	}
	return nil
}
