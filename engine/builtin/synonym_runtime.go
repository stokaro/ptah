package builtin

import (
	"ptah.run/core/annotation"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/synonym"
)

// synonymProvider assembles the synonym owner on the targets that have
// synonyms, SQL Server and Oracle. Its rendering joins the targets'
// compositions in [ownersFor] through [mssqlRegistry] and [oracleRegistry].
func synonymProvider() engine.Provider {
	models := []schemaext.Kind{synonym.Kind}
	changes := []schemaext.Kind{synonym.ChangeKind}
	operations := []schemaext.Kind{synonym.OperationKind}
	provider := engine.Provider{
		ID:          synonym.Owner,
		Annotations: []annotation.Extension{synonym.Annotations()},
		Codecs:      synonym.Codecs(),
	}
	for _, target := range synonym.Targets() {
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: models, Service: synonym.ConvertService{}})
		provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: target, Kinds: models, ChangeKinds: changes,
			Service: synonym.CompareService{}})
		provider.Reversals = append(provider.Reversals, engine.Reversal{Target: target, Kinds: changes, Service: synonym.ReverseService{}})
		provider.Planning = append(provider.Planning, engine.Planning{Target: target, Kinds: changes, OperationKinds: operations,
			Service: synonym.PlanService{}})
		provider.Declarations = append(provider.Declarations, engine.DeclarationPlanning{Target: target, Kinds: models,
			OperationKinds: operations, Service: synonym.PlanService{}})
	}
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation,
			Definitions: synonym.Definitions(), Service: synonym.ReportService{}})
	}
	return provider
}

// oracleRegistry is the Oracle owners' handler registry, which is the
// synonym owner's alone.
func oracleRegistry() (renderer.Extensions, error) {
	return renderer.NewExtensions(synonym.Handlers()...)
}
