package builtin

import (
	"slices"

	"ptah.run/core/annotation"
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
