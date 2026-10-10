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
// read from the table's platform properties, and the module declaration that
// makes a table virtual. Every stage is registered together, so no stage can
// accept a value another would refuse or ignore.
func registerSQLiteServices(provider *engine.Provider) {
	target := platform.SQLite
	tables := []schemaext.Kind{sqlitetable.TableKind}
	virtual := []schemaext.Kind{sqlitetable.VirtualKind}
	provider.Codecs = append(provider.Codecs, append(sqlitetable.TableCodecs(), sqlitetable.ChangeCodec())...)
	provider.Codecs = append(provider.Codecs, append(sqlitetable.VirtualCodecs(), sqlitetable.VirtualChangeCodec())...)
	provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: virtual, Service: sqlitetable.VirtualConvertService{}})
	provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{
		Target: target, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
		Kinds: virtual, ChangeKinds: []schemaext.Kind{sqlitetable.VirtualChangeKind}, Service: sqlitetable.VirtualCompareService{},
	})
	provider.Planning = append(provider.Planning, engine.Planning{Target: target, ParentKinds: virtual, Service: sqlitetable.VirtualPlanService{}})
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{
			Representation: representation, Definitions: sqlitetable.VirtualReportDefinitions(), Service: sqlitetable.VirtualReportService{},
		})
	}
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
