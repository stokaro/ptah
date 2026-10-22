package schemaprojection_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
)

func TestCreationFacetOwnersIncludeEmptyDeclaredSlots(t *testing.T) {
	for _, namespace := range []identifier.IndexNamespace{identifier.IndexNamespaceTable, identifier.IndexNamespaceSchema} {
		t.Run(string(namespace), func(t *testing.T) {
			c := qt.New(t)
			rules := identifier.Semantics{DefaultSchema: "public", TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: namespace}
			builder := objectidentity.NewBuilder(rules)
			parent := builder.TableParts("public", "orders")
			input := schemaprojection.TableCreationInput{Subject: parent, Declaration: schemacapture.TableDeclaration{
				Table:       schemamodel.Table{Name: "orders", Schema: "public"},
				Fields:      []schemamodel.Field{{Name: "id", Type: "INTEGER"}},
				Indexes:     []schemamodel.Index{{Name: "by_id"}},
				Constraints: []schemamodel.Constraint{{Name: "check_id", Type: "CHECK"}, {Name: "named_pk", Type: "PRIMARY KEY"}, {Type: "CHECK"}},
			}}
			owners, err := input.FacetOwners(rules)
			c.Assert(err, qt.IsNil)
			constraint, found := schemacapture.ConstraintSubject(parent, "CHECK", "check_id", rules)
			c.Assert(found, qt.IsTrue)
			want := []objectidentity.ID{parent, objectidentity.PrimaryKeyOf(parent), builder.IndexParts("public", "orders", "by_id"), constraint}
			slices.SortFunc(want, schemaext.CompareRefs)
			c.Assert(owners, qt.DeepEquals, want)
			input.Declaration.Indexes = append(input.Declaration.Indexes, input.Declaration.Indexes[0])
			owners, err = input.FacetOwners(rules)
			c.Assert(err, qt.ErrorIs, schemaprojection.ErrInvalid)
			c.Assert(owners, qt.IsNil)
		})
	}
}
