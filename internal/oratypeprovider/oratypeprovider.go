// Package oratypeprovider assembles the Oracle user type owner of package
// oratype as one engine provider: its codecs, its conversion, comparison,
// planning, declaration and reversal services on the Oracle target, its
// reports in both representations, and the Go, HCL and SQL source adapters
// of package oratypesource.
package oratypeprovider

import (
	"slices"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/core/sqlext"
	"ptah.run/engine"
	"ptah.run/feature/oratype"
	"ptah.run/internal/oratypesource"
)

// Provider returns a fresh descriptor of the owner. The runtime that selects
// it must also select a provider of the Oracle target.
//
// A type on another target than Oracle is refused, naming the scope that keeps
// it to Oracle: a column declared with the type would otherwise name
// something the server has no definition of.
func Provider() engine.Provider {
	models := oratype.Kinds()
	return engine.Provider{
		ID:     oratype.Owner,
		Codecs: slices.Concat(oratype.Codecs(), oratype.ChangeCodecs(), oratype.OperationCodecs()),
		// A description that does not describe a kind, such as a document
		// whose blocks of that kind the compatibility surface left out,
		// describes none of the owner's types of the kind either.
		CoverageLimits: []schemaext.LimitMapping{
			{Header: coverage.Domain, Model: oratype.DomainKind, Names: typeNames},
			{Header: coverage.Composite, Model: oratype.ObjectTypeKind, Names: typeNames},
		},
		Selections:  []selectext.Extension{oratype.Selections()},
		HCL:         []hclext.Extension{oratypesource.HCL()},
		Annotations: []annotation.Extension{oratypesource.Annotations()},
		SQL:         []sqlext.Extension{oratypesource.SQL()},
		Conversions: []engine.Conversion{{Target: platform.Oracle, Kinds: models, Service: oratype.ConvertService{}}},
		Comparisons: []engine.ObjectComparison{{Target: platform.Oracle, Kinds: models, ChangeKinds: oratype.ChangeKinds(),
			Service: oratype.CompareService{}}},
		Planning: []engine.Planning{{Target: platform.Oracle, Kinds: oratype.ChangeKinds(), OperationKinds: oratype.OperationKinds(),
			Service: oratype.PlanService{}}},
		Declarations: []engine.DeclarationPlanning{{Target: platform.Oracle, Kinds: models, OperationKinds: oratype.OperationKinds(),
			Service: oratype.PlanService{}}},
		Reversals: []engine.Reversal{{Target: platform.Oracle, Kinds: oratype.ChangeKinds(), Service: oratype.ReverseService{}}},
		OffTarget: []engine.OffTargetPolicy{
			{Kinds: []schemaext.Kind{oratype.DomainKind}, Action: schemaext.OffTargetRefuse, Label: "Oracle domain"},
			{Kinds: []schemaext.Kind{oratype.ObjectTypeKind}, Action: schemaext.OffTargetRefuse, Label: "Oracle object type"},
		},
		Reporting: []engine.Reporting{
			{Representation: schemaext.Desired, Definitions: oratype.Definitions(), Service: oratype.ReportService{}},
			{Representation: schemaext.Observed, Definitions: oratype.Definitions(), Service: oratype.ReportService{}},
		},
	}
}

// typeNames are the names a not-described record may use for a type: its
// name qualified by the schema the source wrote, and its bare name.
func typeNames(ref objectidentity.ID) []string {
	return slices.Compact([]string{oratype.QualifiedName(ref), ref.Name.Source})
}
