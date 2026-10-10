package builtin

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/sqlite/sqlitetable"
	"ptah.run/engine"
)

// registerSQLiteServices assembles the SQLite owner on the sqlite target's
// provider: the options a table is created with, STRICT and WITHOUT ROWID,
// read from the table's platform properties. Every stage is registered
// together, so no stage can accept a value another would refuse or ignore.
func registerSQLiteServices(provider *engine.Provider) {
	target := platform.SQLite
	tables := []schemaext.Kind{sqlitetable.TableKind}
	provider.Codecs = append(provider.Codecs, append(sqlitetable.TableCodecs(), sqlitetable.ChangeCodec())...)
	provider.Properties = append(provider.Properties, engine.PropertySource{
		Target: target, Format: schemaext.TablePlatformProperties, Definitions: sqlitetable.PropertyDefinitions(), Service: sqlitetable.PropertyService{},
	})
	provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: tables, Service: sqlitetable.ConvertService{}})
	provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{
		Target: target, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
		Kinds: tables, ChangeKinds: []schemaext.Kind{sqlitetable.ChangeKind}, Service: sqlitetable.CompareService{},
	})
	provider.Planning = append(provider.Planning, engine.Planning{Target: target, ParentKinds: tables, Service: sqlitetable.PlanService{}})
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{
			Representation: representation, Definitions: sqlitetable.ReportDefinitions(), Service: sqlitetable.ReportService{},
		})
	}
}
