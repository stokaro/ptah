package atlasfilter

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/internal/readscope"
)

// partialSchemas is [catalog.Database.PartialSchemas] for filtered, a
// description a selection made of read: the schemas filtered keeps that read
// held more objects of, with those read already held only part of. leftOut is
// the kinds the selection kept out of the read, as [KindsLeftOut] names them.
// The read then cannot show that a schema holds none of them, so every schema
// filtered keeps is partial.
//
// It compares the two descriptions rather than asking each rule what it took
// out, as [outOfScopeParts] does, so an object every rule removes counts
// alike: a table a selector names, a trigger that left with its table, an
// event the include selection did not reach. A filter only removes objects, so
// a schema with fewer objects of one kind than the read lost one.
func partialSchemas(read, filtered *catalog.Database, leftOut []readscope.Kind) []string {
	kept := make(map[string]bool, len(filtered.Schemas))
	for _, schema := range filtered.Schemas {
		kept[schema.Name] = true
	}
	partial := make(map[string]bool)
	for _, name := range read.PartialSchemas {
		if kept[name] {
			partial[name] = true
		}
	}
	for _, schemas := range objectSchemas {
		held, left := countBySchema(schemas(read)), countBySchema(schemas(filtered))
		for name, count := range held {
			if kept[name] && left[name] < count {
				partial[name] = true
			}
		}
	}
	if len(leftOut) > 0 {
		maps.Copy(partial, kept)
	}
	if len(partial) == 0 {
		return nil
	}
	return slices.Sorted(maps.Keys(partial))
}

// objectSchemas lists, for each kind of object a database holds, the schema
// of every object of that kind in a description, once per object. A schema
// drop takes each of them.
var objectSchemas = []func(*catalog.Database) []string{
	func(db *catalog.Database) []string { return schemasOf(db.Tables, tableSchema) },
	func(db *catalog.Database) []string { return schemasOf(db.Views, viewSchema) },
	func(db *catalog.Database) []string { return schemasOf(db.MatViews, matViewSchema) },
	func(db *catalog.Database) []string { return schemasOf(db.Sequences, sequenceSchema) },
	func(db *catalog.Database) []string { return schemasOf(db.Functions, functionSchema) },
	func(db *catalog.Database) []string { return schemasOf(db.Enums, enumSchema) },
	func(db *catalog.Database) []string { return schemasOf(db.Triggers, triggerSchema) },
	func(db *catalog.Database) []string { return schemasOf(db.Indexes, indexSchema) },
	func(db *catalog.Database) []string { return schemasOf(db.Constraints, constraintSchema) },
	func(db *catalog.Database) []string { return schemasOf(db.UnmodeledObjects, unmodeledSchema) },
	featureSchemas,
}

func tableSchema(o catalog.Table) string               { return o.Schema }
func viewSchema(o catalog.View) string                 { return o.Schema }
func matViewSchema(o catalog.MaterializedView) string  { return o.Schema }
func sequenceSchema(o catalog.Sequence) string         { return o.Schema }
func functionSchema(o catalog.Function) string         { return o.Schema }
func enumSchema(o catalog.Enum) string                 { return o.Schema }
func triggerSchema(o catalog.Trigger) string           { return o.Schema }
func indexSchema(o catalog.Index) string               { return o.Schema }
func constraintSchema(o catalog.Constraint) string     { return o.Schema }
func unmodeledSchema(o catalog.UnmodeledObject) string { return o.Schema }

// featureSchemas is the schema of each owner-described object db holds.
func featureSchemas(db *catalog.Database) []string {
	refs := db.FeatureObjects.Refs()
	schemas := make([]string, 0, len(refs))
	for _, ref := range refs {
		schemas = append(schemas, ref.Schema.Authored())
	}
	return schemas
}

func schemasOf[T any](objects []T, schema func(T) string) []string {
	schemas := make([]string, 0, len(objects))
	for _, object := range objects {
		schemas = append(schemas, schema(object))
	}
	return schemas
}

// countBySchema counts the names in schemas, leaving out an object read
// without a schema, which belongs to no schema a comparison drops.
func countBySchema(schemas []string) map[string]int {
	counts := make(map[string]int, len(schemas))
	for _, schema := range schemas {
		if strings.TrimSpace(schema) != "" {
			counts[schema]++
		}
	}
	return counts
}

// kindsLeftOutBy names the kinds patterns exclude every object of, which
// [KindsLeftOut] keeps out of the read.
func kindsLeftOutBy(patterns []resourcePattern) []readscope.Kind {
	var kinds []readscope.Kind
	for _, pattern := range patterns {
		kinds = append(kinds, pattern.wholeKinds()...)
	}
	return kinds
}
