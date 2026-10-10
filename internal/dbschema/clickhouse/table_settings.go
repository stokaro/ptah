package clickhouse

import (
	"fmt"
	"sync"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// Catalog properties retain their own spelling and inspected absence. Equal
// sorting and primary keys remain separate facts; neither implies the other.
func observedTableSettings(engineFull, sortingKey, primaryKey, partitionKey, samplingKey string) (schemaext.Facets, error) {
	clauses := parseEngineFull(engineFull)
	value := &chschema.ObservedTable{
		Engine: clauses.Engine, OrderBy: sortingKey, PrimaryKey: primaryKey,
		PartitionBy: partitionKey, SampleBy: samplingKey, TTL: clauses.TTL, Settings: clauses.Settings,
	}
	if err := chschema.ValidateObserved(value); err != nil {
		return schemaext.Facets{}, err
	}
	facets, err := schemaext.NewFacets(value)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return facets.WithTargetScope(chschema.TableKind, platform.ClickHouse)
}

// observedIndexSettings records an index's type, with its parameters, and its
// granularity as the owner's observation. An incomplete row is an error rather
// than an observation with a fabricated default.
func observedIndexSettings(indexType string, granularity uint64) (schemaext.Facets, error) {
	value := &chschema.ObservedIndex{IndexType: indexType, Granularity: granularity}
	if err := chschema.ValidateObservedIndex(value); err != nil {
		return schemaext.Facets{}, err
	}
	facets, err := schemaext.NewFacets(value)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return facets.WithTargetScope(chschema.IndexKind, platform.ClickHouse)
}

// storageRegistry builds the model registry observed coverage names once.
// Building one validates and hashes every codec definition, and coverage is
// built on every read.
var storageRegistry = sync.OnceValues(func() (schemaext.Registry, error) {
	var codecs []schemaext.OwnedCodec
	for _, codec := range append(chschema.Codecs(), chschema.IndexCodecs()...) {
		codecs = append(codecs, schemaext.OwnedCodec{Owner: "ptah.run/clickhouse", Codec: codec})
	}
	return schemaext.NewRegistry(codecs...)
})

// The table query excludes unsupported engines and materialized-view storage.
// Only returned, retained tables establish complete settings observations.
// Index knowledge applies to the whole database; readSkippingIndexes says why.
func observedCoverage(tables []catalog.Table, index schemaext.Knowledge) (schemaext.Coverage, error) {
	registry, err := storageRegistry()
	if err != nil {
		return schemaext.Coverage{}, err
	}
	identities := objectidentity.NewBuilder(identifier.ForDialect(platform.ClickHouse))
	subjects := make([]schemaext.SubjectCoverage, 0, len(tables))
	for _, table := range tables {
		subjects = append(subjects, schemaext.SubjectCoverage{
			Kind: chschema.TableKind, Subject: identities.TableParts(table.Schema, table.Name),
			Knowledge: schemaext.Knowledge{State: schemaext.Complete},
		})
	}
	knowledge := map[schemaext.Kind]schemaext.Knowledge{
		chschema.TableKind: {State: schemaext.Uninspected, Reason: "only returned tables have inspected ClickHouse settings"},
		chschema.IndexKind: index,
	}
	var kinds []schemaext.KindCoverage
	for _, model := range registry.Definitions() {
		if claim, found := knowledge[model.Kind]; found && model.Representation == schemaext.Observed {
			kinds = append(kinds, schemaext.KindCoverage{Model: model, Knowledge: claim})
		}
	}
	if len(kinds) != len(knowledge) {
		return schemaext.Coverage{}, fmt.Errorf("%w: no observed ClickHouse storage codec", schemaext.ErrUnknownCodec)
	}
	return schemaext.NewCoverage(schemaext.Observed, kinds, subjects)
}
