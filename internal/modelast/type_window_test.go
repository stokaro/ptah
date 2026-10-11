package modelast_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
)

// typeDefiningValue is an owner's object that defines the domain its
// identity names.
type typeDefiningValue struct{}

func (*typeDefiningValue) Kind() schemaext.Kind { return loweringKind }
func (*typeDefiningValue) Clone() schemaext.Value {
	return &typeDefiningValue{}
}
func (*typeDefiningValue) Equal(other schemaext.Value) bool {
	_, ok := other.(*typeDefiningValue)
	return ok
}
func (*typeDefiningValue) DefinedType(ref objectidentity.ID) schemaext.DefinedType {
	ref.Kind = objectidentity.KindDomain
	return schemaext.DefinedType{ID: ref, References: []string{"text"}}
}

// typeWindowDatabase declares an enum, a routine whose signature names the
// type zip, a table, and the owner's object named zip carrying value.
func typeWindowDatabase(value schemaext.Value) schemamodel.Database {
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(objectidentity.Kind(loweringKind), "", "zip")
	return schemamodel.Database{
		Enums: []schemamodel.Enum{{Name: "status", Values: []string{"on"}}},
		Functions: []schemamodel.Function{{Name: "to_zip", Parameters: "value text", Returns: "zip", Language: "plpgsql",
			Body: "BEGIN RETURN value; END;"}},
		Tables:         []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields:         []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}},
		FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: value})),
	}
}

// typeWindowKinds names each statement a walk visited: an enum, a routine, a
// table, or an owner's statement.
func typeWindowKinds(nodes []ast.Node) []string {
	kinds := make([]string, 0, len(nodes))
	for _, node := range nodes {
		switch typed := node.(type) {
		case *ast.EnumNode:
			kinds = append(kinds, "enum "+typed.Name)
		case *ast.CreateFunctionNode:
			kinds = append(kinds, "routine "+typed.Name)
		case *ast.CreateTableNode:
			kinds = append(kinds, "table")
		case *ast.ExtensionStatement:
			kinds = append(kinds, "owner")
		default:
			kinds = append(kinds, fmt.Sprintf("%T", node))
		}
	}
	return kinds
}

// TestStandaloneLoweringPlacesAType pins the type window of a whole-schema
// render. A type an owner's object defines is created after the enums and
// before the tables, and a routine whose signature names it waits for it, as
// a migration plan places both.
func TestStandaloneLoweringPlacesAType(t *testing.T) {
	tests := []struct {
		name  string
		phase featureplan.Phase
		value schemaext.Value
		want  []string
	}{
		{name: "a type", phase: featureplan.PhaseType, value: &typeDefiningValue{},
			want: []string{"enum status", "owner", "routine to_zip", "table"}},
		{name: "control: an object that defines no type leaves the routine before the types", phase: featureplan.PhaseType,
			value: &loweringValue{Name: "zip"}, want: []string{"routine to_zip", "enum status", "owner", "table"}},
		{name: "control: a step without a phase follows its dependencies alone", phase: featureplan.PhaseDefault,
			value: &typeDefiningValue{}, want: []string{"owner", "enum status", "routine to_zip", "table"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var visited []ast.Node

			err := modelast.WalkDatabase(typeWindowDatabase(test.value), "postgres", func(node ast.Node) error {
				visited = append(visited, node)
				return nil
			}, modelast.Lowering{Context: t.Context(), Runtime: phasedDeclarationRuntime{phase: test.phase}})

			c.Assert(err, qt.IsNil)
			c.Assert(typeWindowKinds(visited), qt.DeepEquals, test.want)
		})
	}
}
