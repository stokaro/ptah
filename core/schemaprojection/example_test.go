package schemaprojection_test

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
)

// ExampleTableState_Clone shows how a projector changes its prediction without
// changing the captured input that another direction or service still needs.
func ExampleTableState_Clone() {
	before := schemaprojection.TableState{
		Table: catalog.Table{Name: "items", Columns: []catalog.Column{{Name: "id", IsNullable: "YES"}}},
	}
	after := before.Clone()
	after.Table.Columns[0].IsNullable = "NO"
	fmt.Println(before.Table.Columns[0].IsNullable)
	fmt.Println(after.Table.Columns[0].IsNullable)
	// Output:
	// YES
	// NO
}

// ExampleTableCreationInput_FacetOwners shows how a transport preserves eligible
// facet slots even when the declaration has no properties on those objects.
func ExampleTableCreationInput_FacetOwners() {
	rules := identifier.Semantics{TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact,
		IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable}
	input := schemaprojection.TableCreationInput{Subject: objectidentity.NewBuilder(rules).Table("items"),
		Declaration: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "items"}, Indexes: []schemamodel.Index{{Name: "by_id"}}}}
	owners, err := input.FacetOwners(rules)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, owner := range owners {
		fmt.Println(owner.Kind)
	}
	// Output:
	// index
	// primary key
	// table
}
