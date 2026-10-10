package atlasreport

import (
	"context"
	"fmt"
	"io"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/sqlite/sqlitetable"
	"ptah.run/internal/featurereport"
	"ptah.run/internal/schemaexportloss"
)

// JSON projects common envelopes and omits all feature payloads. Keep their
// destinations alongside one batch so a process-backed reporter receives the
// same request as a local reporter, independently of template traversal.
func attachFeatureJSONLoss(ctx context.Context, realm *atlasSchemaInspectJSONRealm, db *schemamodel.Database, info catalog.ServerInfo, writer io.Writer, runtime schemaext.ReportingRuntime) error {
	if db == nil {
		return nil
	}
	projection := featureJSONProjection(realm, db, info, writer)
	objects, err := db.FeatureObjects.All()
	if err != nil {
		return err
	}
	owners := featurereport.NewTableOwners(db.Tables)
	var values []schemaext.Value
	var destinations []*inspectJSONLoss
	for _, object := range objects {
		position, err := owners.Resolve(object.Ref)
		if err != nil {
			return err
		}
		destination := projection.root
		if position >= 0 {
			destination = projection.tables[position]
		}
		values = append(values, object.Value)
		destinations = append(destinations, destination)
	}
	for _, facets := range db.FacetSlots() {
		attached, err := facets.Values()
		if err != nil {
			return err
		}
		destination := projection.facets[facets]
		if destination == nil {
			destination = projection.root
		}
		for _, value := range attached {
			if representedInJSON(value) || reportedByInspect(value) {
				continue
			}
			values = append(values, value)
			destinations = append(destinations, destination)
		}
	}
	labels, err := schemaexportloss.FeatureLabels(ctx, info.Dialect, values, runtime)
	if err != nil {
		return err
	}
	counts := make(map[*inspectJSONLoss]map[string]int)
	for i, label := range labels {
		destination := destinations[i]
		if counts[destination] == nil {
			counts[destination] = make(map[string]int)
		}
		counts[destination][label]++
	}
	for destination, labels := range counts {
		destination.omitted = append(destination.omitted, schemaexportloss.Descriptions(labels)...)
		slices.Sort(destination.omitted)
	}
	return ctx.Err()
}

// representedInJSON reports a facet value the document writes in full: MySQL
// column settings that state only a character set, which a column carries as
// its charset attribute. An ON UPDATE clause has no attribute there and is
// reported as left out.
func representedInJSON(value schemaext.Value) bool {
	settings, ok := mysqlschema.ValueSettings(value)
	return ok && settings.OnUpdate == ""
}

// reportedByInspect reports a facet value whose loss the inspect command
// answers for itself: a SQLite virtual table's module declaration, which every
// format but SQL drops, and which the command names with its module and the
// format that keeps it. A second warning here would say the same thing twice.
func reportedByInspect(value schemaext.Value) bool {
	return value.Kind() == sqlitetable.VirtualKind
}

type jsonFeatureProjection struct {
	root   *inspectJSONLoss
	tables []*inspectJSONLoss
	facets map[*schemaext.Facets]*inspectJSONLoss
}

func featureJSONProjection(realm *atlasSchemaInspectJSONRealm, db *schemamodel.Database, info catalog.ServerInfo, writer io.Writer) jsonFeatureProjection {
	projection := jsonFeatureProjection{
		root:   jsonLossDestination(&realm.loss, writer, ""),
		facets: make(map[*schemaext.Facets]*inspectJSONLoss),
	}
	tables := make(map[inspectJSONIdentity]*atlasSchemaInspectJSONTable)
	for s := range realm.Schemas {
		schema := &realm.Schemas[s]
		for i := range db.Schemas {
			if db.Schemas[i].Name == schema.Name {
				projection.facets[&db.Schemas[i].Facets] = jsonLossDestination(&schema.loss, writer, fmt.Sprintf(" from schema %q", schema.Name))
			}
		}
		for t := range schema.Tables {
			table := &schema.Tables[t]
			tables[inspectJSONIdentity{schema.Name, table.Name}] = table
		}
	}
	for i := range db.Tables {
		source := &db.Tables[i]
		table := tables[inspectJSONIdentity{atlasSchemaInspectSchemaName(source.Schema, info), source.Name}]
		destination := projection.root
		if table != nil {
			destination = jsonLossDestination(&table.loss, writer, fmt.Sprintf(" from table %q", source.QualifiedName()))
			attachFeatureMemberDestinations(projection.facets, source, table, db, writer)
		}
		projection.tables = append(projection.tables, destination)
		projection.facets[&source.Facets] = destination
	}
	return projection
}

func attachFeatureMemberDestinations(destinations map[*schemaext.Facets]*inspectJSONLoss, source *schemamodel.Table, table *atlasSchemaInspectJSONTable, db *schemamodel.Database, writer io.Writer) {
	for i := range db.Fields {
		field := &db.Fields[i]
		if field.StructName != source.StructName {
			continue
		}
		for j := range table.Columns {
			column := &table.Columns[j]
			if column.Name == field.Name {
				destinations[&field.Facets] = jsonLossDestination(&column.loss, writer, fmt.Sprintf(" from column %q of table %q", field.Name, source.QualifiedName()))
			}
		}
	}
	for i := range db.Indexes {
		index := &db.Indexes[i]
		if index.StructName != source.StructName {
			continue
		}
		for j := range table.Indexes {
			projected := &table.Indexes[j]
			if projected.Name == index.Name {
				destinations[&index.Facets] = jsonLossDestination(&projected.loss, writer, fmt.Sprintf(" from index %q of table %q", index.Name, source.QualifiedName()))
			}
		}
	}
}

func jsonLossDestination(slot **inspectJSONLoss, writer io.Writer, scope string) *inspectJSONLoss {
	if *slot == nil {
		*slot = &inspectJSONLoss{writer: writer, scope: scope}
	}
	return *slot
}
