package builtin

import (
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
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
// table is created with, read from platform properties and the common engine.
// Every stage is registered together, so no stage can accept a value another
// would refuse or ignore.
func registerMySQLServices(provider *engine.Provider) {
	provider.Codecs = append(provider.Codecs, slices.Concat(mysqlschema.TableCodecs(), []schemaext.Codec{mysqldiff.TableCodec()})...)
	tables := []schemaext.Kind{mysqlschema.TableKind}
	for _, target := range []string{platform.MySQL, platform.MariaDB} {
		provider.Properties = append(provider.Properties, engine.PropertySource{
			Target: target, Format: schemaext.TablePlatformProperties, Definitions: mysqlsource.Definitions(), Service: mysqlsource.Service{},
		})
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: tables, Service: mysqlconvert.TableService{}})
		provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{
			Target: target, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
			Kinds: tables, ChangeKinds: []schemaext.Kind{mysqldiff.TableKind}, Service: mysqlcompare.TableService{},
		})
		provider.Planning = append(provider.Planning, engine.Planning{Target: target, ParentKinds: tables, Service: mysqlplan.TableService{}})
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{
			Representation: representation, Definitions: mysqlreport.TableDefinitions(), Service: mysqlreport.TableService{},
		})
	}
}
