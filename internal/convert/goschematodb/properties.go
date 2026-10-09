package goschematodb

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/core/schemaproperties"
)

// Runtime selects source decoding, CREATE prediction, and representation
// conversion together. Document projection never selects a built-in provider.
type Runtime interface {
	schemaext.ConversionRuntime
	schemaprojection.TableCreationService
	schemaproperties.Runtime
}

// A current-side document describes the table its CREATE would produce. Resolve
// its source properties under creation rules before projecting that state.
// This is a prediction from a document, not evidence that a table was inspected.
func prepareSourceTables(ctx context.Context, source *schemamodel.Database, target string, runtime Runtime) (*schemamodel.Database, []schemaprojection.TableCreation, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, nil, err
	}
	selected, err := runtime.ResolveTarget(target)
	if err != nil {
		return nil, nil, err
	}
	scoped, err := schemamodel.ScopeToTarget(source, selected)
	if err != nil {
		return nil, nil, err
	}
	database, err := schemaproperties.DecodeTables(ctx, scoped, selected.Name(), runtime)
	if err != nil {
		return nil, nil, err
	}
	database, err = schemaproperties.DecodeIndexes(ctx, database, selected.Name(), runtime)
	if err != nil {
		return nil, nil, err
	}
	semantics := identifier.ForDialect(selected.Name())
	identities := objectidentity.NewBuilder(semantics)
	request := schemaprojection.TableCreationRequest{Target: selected.Name(), Identifiers: semantics, Capabilities: capability.ForDialect(selected.Name())}
	for _, table := range database.Tables {
		request.Tables = append(request.Tables, schemaprojection.TableCreationInput{
			Subject: identities.TableParts(table.Schema, table.Name), Declaration: schemacapture.DeclareTable(database, table, semantics),
		})
	}
	result, err := runtime.ProjectTableCreations(ctx, request.Clone())
	if err != nil {
		return nil, nil, err
	}
	projection, err := schemaprojection.AcceptTableCreations(request, result)
	if err != nil {
		return nil, nil, err
	}
	slots, err := creationFacetSlots(database, semantics)
	if err != nil {
		return nil, nil, err
	}
	if err := applyCreationFacets(slots, projection.Tables, selected.Name()); err != nil {
		return nil, nil, err
	}
	database.FeatureCoverage, err = projectedFacetCoverage(database.FeatureCoverage, slots, runtime.Codecs())
	if err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return database, projection.Tables, nil
}

// An unenrolled declaration gains knowledge only for successfully projected
// values. Explicit source limitations are retained even when a value is present.
func projectedFacetCoverage(source schemaext.Coverage, slots []creationFacetSlot, registry schemaext.Registry) (schemaext.Coverage, error) {
	kinds := source.KindRecords()
	subjects := source.SubjectRecords()
	enrolled := make(map[schemaext.Kind]bool)
	added := make(map[schemaext.Kind]bool)
	definitions := make(map[schemaext.Kind]schemaext.CodecIdentity)
	for _, record := range kinds {
		enrolled[record.Model.Kind] = true
	}
	for _, definition := range registry.Definitions() {
		if definition.Representation == schemaext.Desired {
			definitions[definition.Kind] = definition
		}
	}
	for _, slot := range slots {
		for _, kind := range slot.values.Kinds() {
			if enrolled[kind] {
				continue
			}
			model, found := definitions[kind]
			if !found {
				return schemaext.Coverage{}, fmt.Errorf("%w: projected facet %q", schemaext.ErrUnknownCodec, kind)
			}
			if !added[kind] {
				kinds = append(kinds, schemaext.KindCoverage{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only projected declarations describe these settings"}})
				added[kind] = true
			}
			subjects = append(subjects, schemaext.SubjectCoverage{Kind: kind, Subject: slot.subject, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
		}
	}
	if len(kinds) == 0 {
		return source, nil
	}
	return schemaext.NewCoverage(schemaext.Desired, kinds, subjects)
}

func applyPreparedColumnKeys(db *catalog.Database, tables []schemaprojection.TableCreation) {
	for i, table := range tables {
		if !table.ColumnPrimaryKeysPrepared {
			continue
		}
		keys := make(map[string]bool, len(table.ColumnPrimaryKeys))
		for _, name := range table.ColumnPrimaryKeys {
			keys[name] = true
		}
		for j := range db.Tables[i].Columns {
			db.Tables[i].Columns[j].IsPrimaryKey = keys[db.Tables[i].Columns[j].Name]
		}
	}
}
