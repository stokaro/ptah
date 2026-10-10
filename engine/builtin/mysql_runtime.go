package builtin

import (
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlast"
	"ptah.run/dialect/mysql/mysqlcompare"
	"ptah.run/dialect/mysql/mysqlconvert"
	"ptah.run/dialect/mysql/mysqldiff"
	"ptah.run/dialect/mysql/mysqlplan"
	"ptah.run/dialect/mysql/mysqlreport"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
	"ptah.run/engine"
)

// registerMySQLServices assembles the MySQL-family owner on the mysql
// target's provider, for the mysql and mariadb targets alike: the options a
// table and an index are created with, read from platform properties and the
// common engine, an index's block-size hint, and the character set and ON
// UPDATE clause of a column. Every stage is registered together, so no stage
// can accept a value another would refuse or ignore.
//
// The column settings and the index options have no change or reversal
// services: their comparison never reports a change. A column that changes
// for another reason carries its settings in its own statement, and an index
// is created with its options. A changed block-size hint is planned as a
// replacement of its index, and reversed by another.
func registerMySQLServices(provider *engine.Provider) {
	provider.Targets[0].Creations = mysqlconvert.CreationService{}
	provider.Annotations = append(provider.Annotations, mysqlsource.Annotations())
	provider.YAML = append(provider.YAML, mysqlsource.YAML())
	provider.Codecs = append(provider.Codecs, slices.Concat(
		mysqlschema.TableCodecs(), []schemaext.Codec{mysqldiff.TableCodec()},
		mysqlschema.IndexCodecs(),
		mysqlschema.IndexBlockSizeCodecs(), []schemaext.Codec{mysqldiff.IndexBlockSizeCodec()}, mysqlast.Codecs(),
		mysqlschema.ColumnSettingsCodecs())...)
	tables := []schemaext.Kind{mysqlschema.TableKind}
	indexes := []schemaext.Kind{mysqlschema.IndexKind}
	blockSizes := []schemaext.Kind{mysqlschema.IndexBlockSizeKind}
	columns := []schemaext.Kind{mysqlschema.ColumnSettingsKind}
	for _, target := range mysqlschema.Targets() {
		provider.Properties = append(provider.Properties,
			engine.PropertySource{Target: target, Format: schemaext.TablePlatformProperties, Definitions: mysqlsource.Definitions(), Service: mysqlsource.Service{}},
			engine.PropertySource{Target: target, Format: schemaext.IndexPlatformProperties, Definitions: mysqlsource.IndexDefinitions(), Service: mysqlsource.IndexService{}},
			engine.PropertySource{Target: target, Format: schemaext.ColumnPlatformProperties, Definitions: mysqlsource.ColumnDefinitions(), Service: mysqlsource.ColumnService{}},
		)
		provider.Conversions = append(provider.Conversions,
			engine.Conversion{Target: target, Kinds: tables, Service: mysqlconvert.TableService{}},
			engine.Conversion{Target: target, Kinds: indexes, Service: mysqlconvert.IndexService{}},
			engine.Conversion{Target: target, Kinds: blockSizes, Service: mysqlconvert.IndexBlockSizeService{}},
			engine.Conversion{Target: target, Kinds: columns, Service: mysqlconvert.ColumnService{}},
		)
		provider.FacetComparisons = append(provider.FacetComparisons,
			engine.FacetComparison{
				Target: target, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
				Kinds: tables, ChangeKinds: []schemaext.Kind{mysqldiff.TableKind}, Service: mysqlcompare.TableService{},
			},
			engine.FacetComparison{
				Target: target, OwnerKinds: []objectidentity.Kind{objectidentity.KindIndex},
				Kinds: indexes, Service: mysqlcompare.IndexService{},
			},
			engine.FacetComparison{
				Target: target, OwnerKinds: []objectidentity.Kind{objectidentity.KindIndex},
				Kinds: blockSizes, ChangeKinds: []schemaext.Kind{mysqldiff.IndexBlockSizeKind}, Service: mysqlcompare.IndexBlockSizeService{},
			},
			engine.FacetComparison{
				Target: target, OwnerKinds: []objectidentity.Kind{objectidentity.KindColumn},
				Kinds: columns, Service: mysqlcompare.ColumnService{},
			},
		)
		provider.Planning = append(provider.Planning,
			engine.Planning{Target: target, ParentKinds: tables, Service: mysqlplan.TableService{}},
			engine.Planning{Target: target, ParentKinds: indexes, Service: mysqlplan.IndexService{}},
			engine.Planning{Target: target, Kinds: []schemaext.Kind{mysqldiff.IndexBlockSizeKind}, ParentKinds: blockSizes,
				OperationKinds: []schemaext.Kind{mysqlast.ReplaceIndexKind}, Service: mysqlplan.IndexBlockSizeService{}},
			engine.Planning{Target: target, ParentKinds: columns, Service: mysqlplan.ColumnService{}},
		)
		provider.Reversals = append(provider.Reversals,
			engine.Reversal{Target: target, Kinds: []schemaext.Kind{mysqldiff.IndexBlockSizeKind}, Service: mysqlplan.ReversalService{}})
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting,
			engine.Reporting{Representation: representation, Definitions: mysqlreport.TableDefinitions(), Service: mysqlreport.TableService{}},
			engine.Reporting{Representation: representation, Definitions: mysqlreport.IndexDefinitions(), Service: mysqlreport.IndexService{}},
			engine.Reporting{Representation: representation, Definitions: mysqlreport.IndexBlockSizeDefinitions(), Service: mysqlreport.IndexBlockSizeService{}},
			engine.Reporting{Representation: representation, Definitions: mysqlreport.ColumnDefinitions(), Service: mysqlreport.ColumnService{}},
		)
	}
}
