package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
)

// typedRoutine is a routine whose signature names the type zip, and plainRoutine
// one whose signature names nothing a plan creates.
var (
	typedRoutine = schemamodel.Function{Name: "to_zip", Parameters: "value text", Returns: "zip", Language: "plpgsql", Body: "BEGIN RETURN value; END;"}
	plainRoutine = schemamodel.Function{Name: "plain", Parameters: "", Returns: "integer", Language: "plpgsql", Body: "BEGIN RETURN 1; END;"}
)

// typeCreations is a plan that creates an enum, the two routines and a table,
// beside the owner's change of the type zip.
func typeCreations(change *phaseChange) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{{Subject: phaseSubject("zip"), Value: change}},
		EnumsAdded:     difftypes.EnumChanges{{Name: "status", Values: []string{"on", "off"}}},
		FunctionsAdded: difftypes.FunctionChanges{{Function: plainRoutine}, {Function: typedRoutine}},
		TablesAdded: difftypes.TableChanges{{Name: "orders", Table: schemamodel.Table{StructName: "Order", Name: "orders"},
			Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}}}},
	}
}

// typeRemovals is a plan that drops a table, a routine, a role and an enum,
// beside the owner's drop of the type zip.
func typeRemovals() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		FeatureChanges:   []schemaext.ChangeRecord{{Subject: phaseSubject("zip"), Value: &phaseChange{Action: plangraph.Drop}}},
		TablesRemoved:    difftypes.TableRemovals{{Name: "orders"}},
		FunctionsRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "tenant_of"}, Signature: new("")}},
		RolesRemoved:     difftypes.RoleChanges{{Name: "reader"}},
		EnumsRemoved:     difftypes.EnumChanges{{Name: "status"}},
	}
}

// TestPlanner_PlacesTypeFeatureOperations pins the type windows. A data type
// columns, routine signatures and other types may name is created after the
// enums and the routines that name no created type, and before the routines
// that name it and the tables. It is dropped after the tables, routines and
// roles, and before the enums it may name.
func TestPlanner_PlacesTypeFeatureOperations(t *testing.T) {
	tests := []struct {
		name  string
		phase featureplan.Phase
		diff  *difftypes.SchemaDiff
		want  []string
	}{
		{name: "a type creation", phase: featureplan.PhaseType, diff: typeCreations(&phaseChange{Type: objectidentity.KindDomain}),
			want: []string{"create function plain", "create enum status", "owner creates", "create function to_zip", "create table"}},
		{name: "control: a change that creates no type leaves the routine with the others", phase: featureplan.PhaseType,
			diff: typeCreations(&phaseChange{}),
			want: []string{"create function plain", "create function to_zip", "create enum status", "owner creates", "create table"}},
		{name: "control: a default creation", phase: featureplan.PhaseDefault, diff: typeCreations(&phaseChange{Type: objectidentity.KindDomain}),
			want: []string{"create function plain", "create enum status", "create function to_zip", "create table", "owner creates"}},
		{name: "a type removal", phase: featureplan.PhaseType, diff: typeRemovals(),
			want: []string{"drop table", "drop function", "drop role", "owner drops", "drop type status"}},
		{name: "control: a default removal", phase: featureplan.PhaseDefault, diff: typeRemovals(),
			want: []string{"owner drops", "drop table", "drop function", "drop role", "drop type status"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := phaseRuntime{Runtime: must.Must(builtin.New()), phase: test.phase}

			nodes, err := postgres.New().GenerateMigrationAST(context.Background(), runtime, test.diff)

			c.Assert(err, qt.IsNil)
			c.Assert(phaseOrder(nodes), qt.DeepEquals, test.want)
		})
	}
}

// rebuilt is a plan that drops the type zip and creates it again, the drop
// first, beside an enum and a table it may name.
func rebuilt() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{
			{Subject: phaseSubject("zip"), Value: &phaseChange{Action: plangraph.Drop}},
			{Subject: phaseSubject("zip"), Value: &phaseChange{Type: objectidentity.KindDomain}},
		},
		EnumsAdded: difftypes.EnumChanges{{Name: "status", Values: []string{"on"}}},
		TablesAdded: difftypes.TableChanges{{Name: "orders", Table: schemamodel.Table{StructName: "Order", Name: "orders"},
			Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}}}},
	}
}

// TestPlanner_RebuildsATypeInItsCreationWindow pins a rebuilt type: both
// halves ask for the type phase, and both join the creation window in the
// owner's order, so the type is back before the tables that may name it.
func TestPlanner_RebuildsATypeInItsCreationWindow(t *testing.T) {
	c := qt.New(t)
	runtime := phaseRuntime{Runtime: must.Must(builtin.New()), phase: featureplan.PhaseType, ordered: [][2]int{{0, 1}}}

	nodes, err := postgres.New().GenerateMigrationAST(context.Background(), runtime, rebuilt())

	c.Assert(err, qt.IsNil)
	c.Assert(phaseOrder(nodes), qt.DeepEquals, []string{"create enum status", "owner drops", "owner creates", "create table"})
}

// TestPlanner_RefusesASplitDefaultRebuild is the control: in the default phase
// the creation window runs before the removal window, so the same two halves
// are refused rather than run in the wrong order.
func TestPlanner_RefusesASplitDefaultRebuild(t *testing.T) {
	c := qt.New(t)
	runtime := phaseRuntime{Runtime: must.Must(builtin.New()), phase: featureplan.PhaseDefault, ordered: [][2]int{{0, 1}}}

	nodes, err := postgres.New().GenerateMigrationAST(context.Background(), runtime, rebuilt())

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*a replacement is one step, or both halves ask for the dependent, type or lifecycle phase.*`)
	c.Assert(nodes, qt.IsNil)
}

// typeOwner is a non-builtin owner of one data type model, reached through
// engine.New: it plans each change as one statement of the type phase.
const typeOwner = "example.org/types"

type typeOwnerChange struct {
	Name string `json:"name"`
	Drop bool   `json:"drop"`
}

func (*typeOwnerChange) Kind() schemaext.Kind { return typeOwner + "/change" }
func (v *typeOwnerChange) CloneChange() schemaext.ChangeValue {
	return &typeOwnerChange{Name: v.Name, Drop: v.Drop}
}
func (v *typeOwnerChange) CreatedType(subject objectidentity.ID) (schemaext.DefinedType, bool) {
	return schemaext.DefinedType{ID: typeOwnerDomain(subject)}, !v.Drop
}

type typeOwnerOperation struct {
	Name string `json:"name"`
	Drop bool   `json:"drop"`
}

func (*typeOwnerOperation) Kind() schemaext.Kind { return typeOwner + "/operation" }
func (v *typeOwnerOperation) CloneExtension() ast.ExtensionPayload {
	return &typeOwnerOperation{Name: v.Name, Drop: v.Drop}
}

// typeOwnerDomain is the common identity of the domain subject names.
func typeOwnerDomain(subject objectidentity.ID) objectidentity.ID {
	subject.Kind = objectidentity.KindDomain
	return subject
}

type typeOwnerService struct{}

func (typeOwnerService) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	result := featureplan.Result{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: typeOwner}
	for i, change := range request.Changes {
		value := change.Value.(*typeOwnerChange)
		action := map[bool]plangraph.Action{false: plangraph.Create, true: plangraph.Drop}[value.Drop]
		id := plangraph.StepID{Owner: typeOwner, Name: fmt.Sprintf("%06d", i)}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &typeOwnerOperation{Name: value.Name, Drop: value.Drop},
				Phase: featureplan.PhaseType},
			Effects:     []plangraph.Effect{{Subject: typeOwnerDomain(change.Subject), Action: action}},
			Transaction: plangraph.TransactionAllowed, Impact: schemaext.Effect{Impact: schemaext.Additive, Reason: "test"},
		})
		result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: change.Subject, Kind: change.Value.Kind(), Strategy: "test",
			Steps: []plangraph.StepID{id}})
	}
	result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	return result, nil
}

// typeOwnerRuntime selects the PostgreSQL target and the type owner alone.
func typeOwnerRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	object := json.RawMessage(`{"type":"object"}`)
	runtime, err := engine.New(
		engine.Provider{ID: "example.org/targets", Targets: []engine.Target{{Name: "postgres"}}},
		engine.Provider{ID: typeOwner,
			Codecs: []schemaext.Codec{
				schemaext.ModelCodec[*typeOwnerChange]{Prototype: &typeOwnerChange{}, Representation: schemaext.Change, Version: 1,
					Definition: object, Clone: func(v *typeOwnerChange) *typeOwnerChange { return v.CloneChange().(*typeOwnerChange) }}.Codec(),
				schemaext.ModelCodec[*typeOwnerOperation]{Prototype: &typeOwnerOperation{}, Representation: schemaext.Operation, Version: 1,
					Definition: object, Clone: func(v *typeOwnerOperation) *typeOwnerOperation { return v.CloneExtension().(*typeOwnerOperation) }}.Codec(),
			},
			Planning: []engine.Planning{{Target: "postgres", Kinds: []schemaext.Kind{typeOwner + "/change"},
				OperationKinds: []schemaext.Kind{typeOwner + "/operation"}, Service: typeOwnerService{}}},
		})
	c.Assert(err, qt.IsNil)
	return runtime
}

// TestPlanner_PlacesATypeOwnerReachedThroughTheEngine drives the type phase
// through a runtime engine.New builds, so the runtime accepts the phase and
// the planner places the owner's statement and the routine naming its type.
func TestPlanner_PlacesATypeOwnerReachedThroughTheEngine(t *testing.T) {
	c := qt.New(t)
	subject := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(typeOwner+"/domain", "", "zip")
	diff := &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{{Subject: subject, Value: &typeOwnerChange{Name: "zip"}}},
		EnumsAdded:     difftypes.EnumChanges{{Name: "status", Values: []string{"on"}}},
		FunctionsAdded: difftypes.FunctionChanges{{Function: plainRoutine}, {Function: typedRoutine}},
		TablesAdded: difftypes.TableChanges{{Name: "orders", Table: schemamodel.Table{StructName: "Order", Name: "orders"},
			Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}}}},
	}

	nodes, err := postgres.New().GenerateMigrationAST(t.Context(), typeOwnerRuntime(c), diff)

	c.Assert(err, qt.IsNil)
	c.Assert(phaseOrder(nodes), qt.DeepEquals,
		[]string{"create function plain", "create enum status", "type owner creates zip", "create function to_zip", "create table"})
}
