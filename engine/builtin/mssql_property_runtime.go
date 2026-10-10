package builtin

import (
	"ptah.run/core/annotation"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlproperty"
	"ptah.run/dialect/mssql/mssqlrender"
	"ptah.run/engine"
	"ptah.run/feature/synonym"
)

// mssqlPropertyProvider assembles the SQL Server extended property owner on
// the SQL Server target. Its rendering joins the target's composition in
// [ownersFor] through [mssqlRegistry].
func mssqlPropertyProvider() engine.Provider {
	models := []schemaext.Kind{mssqlproperty.Kind}
	changes := []schemaext.Kind{mssqlproperty.ChangeKind}
	operations := []schemaext.Kind{mssqlproperty.OperationKind}
	target := platform.SQLServer
	provider := engine.Provider{
		ID:          mssqlproperty.Owner,
		Annotations: []annotation.Extension{mssqlproperty.Annotations()},
		Codecs:      mssqlproperty.Codecs(),
		Conversions: []engine.Conversion{{Target: target, Kinds: models, Service: mssqlproperty.ConvertService{}}},
		Comparisons: []engine.ObjectComparison{{Target: target, Kinds: models, ChangeKinds: changes, Service: mssqlproperty.CompareService{}}},
		Reversals:   []engine.Reversal{{Target: target, Kinds: changes, Service: mssqlproperty.ReverseService{}}},
		Planning:    []engine.Planning{{Target: target, Kinds: changes, OperationKinds: operations, Service: mssqlproperty.PlanService{}}},
		Declarations: []engine.DeclarationPlanning{{Target: target, Kinds: models, OperationKinds: operations,
			Service: mssqlproperty.PlanService{}}},
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation,
			Definitions: mssqlproperty.Definitions(), Service: mssqlproperty.ReportService{}})
	}
	return provider
}

// mssqlRegistry is the SQL Server owners' handler registry: the security
// policy's, the extended properties' and the synonyms'.
func mssqlRegistry() (renderer.Extensions, error) {
	handlers := append(mssqlrender.Handlers(), mssqlproperty.Handlers()...)
	return renderer.NewExtensions(append(handlers, synonym.Handlers()...)...)
}
