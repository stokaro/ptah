package builtin

import (
	"slices"

	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlast"
	"ptah.run/dialect/mssql/mssqlcompare"
	"ptah.run/dialect/mssql/mssqlconvert"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlplan"
	"ptah.run/dialect/mssql/mssqlprobe"
	"ptah.run/dialect/mssql/mssqlrelation"
	"ptah.run/dialect/mssql/mssqlreport"
	"ptah.run/dialect/mssql/mssqlreverse"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine"
	"ptah.run/internal/mssqlpolicysource"
	"ptah.run/internal/mssqlroutinesource"
)

// mssqlProvider assembles the SQL Server security policy owner of ADR 0020 on
// the SQL Server target. Its rendering joins the target's composition in
// [ownersFor]. The Go source declares the policies scoped to SQL Server and
// the SQL Server reader reports the ones a database holds; a connected
// comparison asks the server to spell the declared predicates it would
// otherwise leave undecided.
func mssqlProvider() engine.Provider {
	models := []schemaext.Kind{mssqlschema.SecurityPolicyKind}
	changes := []schemaext.Kind{mssqldiff.SecurityPolicyKind}
	operations := []schemaext.Kind{mssqlast.SecurityPolicyKind}
	target := platform.SQLServer
	provider := engine.Provider{
		ID:          mssqlschema.Owner,
		Codecs:      slices.Concat(mssqlschema.Codecs(), mssqldiff.Codecs(), mssqlast.Codecs()),
		Conversions: []engine.Conversion{{Target: target, Kinds: models, Service: mssqlconvert.Service{}}},
		Comparisons: []engine.ObjectComparison{{Target: target, Kinds: models, ChangeKinds: changes, Service: mssqlcompare.Service{}}},
		Reversals:   []engine.Reversal{{Target: target, Kinds: changes, Service: mssqlreverse.Service{}}},
		Planning:    []engine.Planning{{Target: target, Kinds: changes, OperationKinds: operations, Service: mssqlplan.Service{}}},
		Declarations: []engine.DeclarationPlanning{{Target: target, Kinds: models, OperationKinds: operations,
			Service: mssqlplan.Service{}}},
		Normalizations: []engine.Normalization{{Target: target, Kinds: models, Service: mssqlprobe.Service{}}},
		Annotations:    []annotation.Extension{mssqlpolicysource.Annotations()},
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Relations = append(provider.Relations, engine.RelationDiscovery{Target: target,
			Representation: representation, Kinds: models, Service: mssqlrelation.Service{}})
		provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation,
			Definitions: mssqlreport.Definitions(), Service: mssqlreport.Service{}})
	}
	return provider
}

// registerSQLServerRoutineServices gives the SQL Server target's provider the
// options a routine's header writes: the facet's codecs, its conversion, its
// comparison, the reversal of its change, and the SQL syntax that reads it.
// A change of it replaces the routine, which the host plans as the routine's
// CREATE OR ALTER, so no owner plan is registered for it.
func registerSQLServerRoutineServices(provider *engine.Provider) {
	target := platform.SQLServer
	provider.Codecs = slices.Concat(provider.Codecs, mssqlschema.RoutineOptionsCodecs(), []schemaext.Codec{mssqldiff.RoutineOptionsCodec()})
	provider.Conversions = append(provider.Conversions, engine.Conversion{
		Target:  target,
		Kinds:   []schemaext.Kind{mssqlschema.RoutineOptionsKind},
		Service: mssqlconvert.Service{},
	})
	provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{
		Target:      target,
		OwnerKinds:  []objectidentity.Kind{objectidentity.KindFunction, objectidentity.KindProcedure},
		Kinds:       []schemaext.Kind{mssqlschema.RoutineOptionsKind},
		ChangeKinds: []schemaext.Kind{mssqldiff.RoutineOptionsKind},
		Service:     mssqlcompare.RoutineOptionsService{},
	})
	provider.Reversals = append(provider.Reversals, engine.Reversal{
		Target:  target,
		Kinds:   []schemaext.Kind{mssqldiff.RoutineOptionsKind},
		Service: mssqlreverse.RoutineOptionsService{},
	})
	provider.SQL = append(provider.SQL, mssqlroutinesource.SQL())
}
