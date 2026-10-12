package featurehost_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/modelast"
	"ptah.run/internal/planner/dialects/clickhouse"
	"ptah.run/internal/planner/dialects/mysql"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

const contextOwner = "example.org/type-context"

type contextValue struct{ References []string }

func (*contextValue) Kind() schemaext.Kind { return contextOwner + "/model" }
func (v *contextValue) Clone() schemaext.Value {
	return &contextValue{References: slices.Clone(v.References)}
}
func (v *contextValue) Equal(other schemaext.Value) bool {
	typed, ok := other.(*contextValue)
	return ok && typed != nil && slices.Equal(v.References, typed.References)
}
func (v *contextValue) DefinedType(ref objectidentity.ID) schemaext.DefinedType {
	ref.Kind = objectidentity.KindComposite
	return schemaext.DefinedType{ID: ref, References: slices.Clone(v.References)}
}

type contextChange struct{}

func (*contextChange) Kind() schemaext.Kind               { return contextOwner + "/change" }
func (*contextChange) CloneChange() schemaext.ChangeValue { return &contextChange{} }

type contextService struct {
	plans        []featureplan.Request
	declarations []featureplan.DeclarationRequest
}

func (s *contextService) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	s.plans = append(s.plans, request)
	result := featureplan.Result{Complete: true}
	for _, change := range request.Changes {
		result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: change.Subject, Kind: change.Value.Kind(), Strategy: "context probe"})
	}
	return result, nil
}

func (s *contextService) PlanDeclarations(_ context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	s.declarations = append(s.declarations, request)
	result := featureplan.DeclarationResult{Complete: true}
	for _, object := range request.Objects {
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: object.Ref, Strategy: "context probe"})
	}
	return result, nil
}

func typeContextRuntime(target string, service *contextService) *engine.Runtime {
	definition := json.RawMessage(`{"type":"object"}`)
	return must.Must(engine.New(engine.Provider{
		ID: contextOwner, Targets: []engine.Target{{Name: target}},
		Codecs: []schemaext.Codec{
			schemaext.ModelCodec[*contextValue]{Prototype: &contextValue{}, Representation: schemaext.Desired, Version: 1, Definition: definition,
				Clone: func(v *contextValue) *contextValue { return v.Clone().(*contextValue) }}.Codec(),
			schemaext.ModelCodec[*contextChange]{Prototype: &contextChange{}, Representation: schemaext.Change, Version: 1, Definition: definition,
				Clone: func(v *contextChange) *contextChange { return &contextChange{} }}.Codec(),
			schemaext.ModelCodec[*operation]{Prototype: &operation{}, Representation: schemaext.Operation, Version: 1, Definition: definition,
				Clone: func(v *operation) *operation { return v.CloneExtension().(*operation) }}.Codec(),
		},
		Planning: []engine.Planning{{Target: target, Kinds: []schemaext.Kind{contextOwner + "/change"},
			OperationKinds: []schemaext.Kind{(&operation{}).Kind()}, Service: service}},
		Declarations: []engine.DeclarationPlanning{{Target: target, Kinds: []schemaext.Kind{contextOwner + "/model"},
			OperationKinds: []schemaext.Kind{(&operation{}).Kind()}, Service: service}},
	}))
}

func contextObjects(target, source string) schemaext.Objects {
	builder := objectidentity.NewBuilder(identifier.ForDialect(target))
	return must.Must(schemaext.NewObjects(
		schemaext.Object{Ref: builder.SchemaScopedParts(contextOwner+"/model", "app", `"Consumer"`), Value: &contextValue{References: []string{source, `"app"."Kept"`}}},
		schemaext.Object{Ref: builder.SchemaScopedParts(contextOwner+"/model", "app", `"Kept"`), Value: &contextValue{References: []string{"INTEGER"}}},
	))
}

type contextPlanner interface {
	GenerateMigrationAST(context.Context, featureplan.Runtime, *difftypes.SchemaDiff) ([]ast.Node, error)
}

// TestMigrationHostsSupplyUnchangedTypes drives the native hosts with a
// separate provider. The change itself defines no type, so deriving context
// from changes loses every definition this provider needs.
func TestMigrationHostsSupplyUnchangedTypes(t *testing.T) {
	for _, test := range []struct {
		target  string
		planner contextPlanner
	}{
		{platform.Oracle, mysql.NewForDialect(platform.Oracle, capability.Oracle23())},
		{platform.SQLServer, mysql.NewForDialect(platform.SQLServer, capability.ForDialect(platform.SQLServer))},
		{platform.MySQL, mysql.New()},
		{platform.MariaDB, mysql.NewForDialect(platform.MariaDB, capability.ForDialect(platform.MariaDB))},
		{platform.Postgres, postgres.New()},
		{platform.CockroachDB, postgres.NewForDialect(platform.CockroachDB, capability.ForDialect(platform.CockroachDB))},
		{platform.ClickHouse, clickhouse.New()},
		{platform.YDB, ydb.New()},
	} {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)
			service := &contextService{}
			desired, current := contextObjects(test.target, "NUMBER(8)"), contextObjects(test.target, "NUMBER(6)")
			ref := objectidentity.NewBuilder(identifier.ForDialect(test.target)).SchemaScopedParts(contextOwner+"/model", "app", "changed")
			diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ref, Value: &contextChange{}}},
				Features: difftypes.FeatureContext{DesiredObjects: desired, CurrentObjects: current}}
			_, err := test.planner.GenerateMigrationAST(t.Context(), typeContextRuntime(test.target, service), diff)
			c.Assert(err, qt.IsNil)
			c.Assert(service.plans, qt.HasLen, 1)
			c.Assert(service.plans[0].Types, qt.DeepEquals, featureplan.TypeContext{Desired: schemaext.DefinedTypes(desired), Current: schemaext.DefinedTypes(current)})
			c.Assert(service.plans[0].Types.Desired, qt.HasLen, 2)
		})
	}
}

func TestWholeSchemaRenderSuppliesTypeContext(t *testing.T) {
	c := qt.New(t)
	service := &contextService{}
	objects := contextObjects(platform.Oracle, "NUMBER(8)")
	var nodes []ast.Node
	err := modelast.WalkDatabase(schemamodel.Database{FeatureObjects: objects}, platform.Oracle, func(node ast.Node) error {
		nodes = append(nodes, node)
		return nil
	}, modelast.Lowering{Context: t.Context(), Runtime: typeContextRuntime(platform.Oracle, service)})
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 0)
	c.Assert(service.declarations, qt.HasLen, 1)
	c.Assert(service.declarations[0].Types, qt.DeepEquals, schemaext.DefinedTypes(objects))
	c.Assert(service.declarations[0].Types, qt.HasLen, 2)
}

// These hosts dispatch table changes separately from standalone objects. The
// table callback still needs the complete type context.
func TestTableHostsSupplyUnchangedTypes(t *testing.T) {
	for _, test := range []struct {
		target  string
		planner contextPlanner
	}{
		{platform.CockroachDB, postgres.NewForDialect(platform.CockroachDB, capability.ForDialect(platform.CockroachDB))},
		{platform.YDB, ydb.New()},
	} {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)
			service := &contextService{}
			desired, current := contextObjects(test.target, "NUMBER(8)"), contextObjects(test.target, "NUMBER(6)")
			ref := objectidentity.NewBuilder(identifier.ForDialect(test.target)).Table("items")
			diff := &difftypes.SchemaDiff{
				Features: difftypes.FeatureContext{DesiredObjects: desired, CurrentObjects: current},
				TablesModified: []difftypes.TableDiff{{TableName: "items",
					FeatureChanges: []schemaext.ChangeRecord{{Subject: ref, Value: &contextChange{}}},
					Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "items", StructName: "Item", PrimaryKey: []string{"id"}},
						Fields: []schemamodel.Field{{StructName: "Item", Name: "id", Type: "BIGINT", Primary: true}}},
					Current: schemacapture.TableObservation{Table: catalog.Table{Name: "items"}},
				}},
			}
			_, err := test.planner.GenerateMigrationAST(t.Context(), typeContextRuntime(test.target, service), diff)
			c.Assert(err, qt.IsNil)
			c.Assert(service.plans, qt.HasLen, 1)
			c.Assert(service.plans[0].Types, qt.DeepEquals, featureplan.TypeContext{Desired: schemaext.DefinedTypes(desired), Current: schemaext.DefinedTypes(current)})
		})
	}
}
