package clickhouse

import (
	"fmt"

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

// The table query excludes unsupported engines and materialized-view storage.
// Only returned, retained tables establish complete settings observations.
func observedTableCoverage(tables []catalog.Table) (schemaext.Coverage, error) {
	var codecs []schemaext.OwnedCodec
	for _, codec := range chschema.Codecs() {
		codecs = append(codecs, schemaext.OwnedCodec{Owner: "ptah.run/clickhouse", Codec: codec})
	}
	registry, err := schemaext.NewRegistry(codecs...)
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
	for _, model := range registry.Definitions() {
		if model.Kind == chschema.TableKind && model.Representation == schemaext.Observed {
			return schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{
				Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables have inspected ClickHouse settings"},
			}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: no observed ClickHouse table codec", schemaext.ErrUnknownCodec)
}
