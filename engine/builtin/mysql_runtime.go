package builtin

import (
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
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
// table is created with, read from platform properties and the common engine,
// and the character set and ON UPDATE clause of a column. Every stage is
// registered together, so no stage can accept a value another would refuse or
// ignore.
//
// The column settings have no change or reversal services: their comparison
// never reports a change, and a column that changes for another reason
// carries them in its own statement.
func registerMySQLServices(provider *engine.Provider) {
	provider.Codecs = append(provider.Codecs,
		slices.Concat(mysqlschema.TableCodecs(), []schemaext.Codec{mysqldiff.TableCodec()}, mysqlschema.ColumnSettingsCodecs())...)
	tables := []schemaext.Kind{mysqlschema.TableKind}
	columns := []schemaext.Kind{mysqlschema.ColumnSettingsKind}
	for _, target := range mysqlschema.Targets() {
		provider.Properties = append(provider.Properties,
			engine.PropertySource{Target: target, Format: schemaext.TablePlatformProperties, Definitions: mysqlsource.Definitions(), Service: mysqlsource.Service{}},
			engine.PropertySource{Target: target, Format: schemaext.ColumnPlatformProperties, Definitions: mysqlsource.ColumnDefinitions(), Service: mysqlsource.ColumnService{}},
		)
		provider.Conversions = append(provider.Conversions,
			engine.Conversion{Target: target, Kinds: tables, Service: mysqlconvert.TableService{}},
			engine.Conversion{Target: target, Kinds: columns, Service: mysqlconvert.ColumnService{}},
		)
		provider.FacetComparisons = append(provider.FacetComparisons,
			engine.FacetComparison{
				Target: target, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
				Kinds: tables, ChangeKinds: []schemaext.Kind{mysqldiff.TableKind}, Service: mysqlcompare.TableService{},
			},
			engine.FacetComparison{
				Target: target, OwnerKinds: []objectidentity.Kind{objectidentity.KindColumn},
				Kinds: columns, Service: mysqlcompare.ColumnService{},
			},
		)
		provider.Planning = append(provider.Planning,
			engine.Planning{Target: target, ParentKinds: tables, Service: mysqlplan.TableService{}},
			engine.Planning{Target: target, ParentKinds: columns, Service: mysqlplan.ColumnService{}},
		)
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting,
			engine.Reporting{Representation: representation, Definitions: mysqlreport.TableDefinitions(), Service: mysqlreport.TableService{}},
			engine.Reporting{Representation: representation, Definitions: mysqlreport.ColumnDefinitions(), Service: mysqlreport.ColumnService{}},
		)
	}
}
