package postgres_test

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

const phaseOwner = "example.org/phase"

// phaseChange asks the phase owner to create or to drop one object.
type phaseChange struct{ Drop bool }

func (*phaseChange) Kind() schemaext.Kind { return phaseOwner + "/change" }
func (v *phaseChange) CloneChange() schemaext.ChangeValue {
	return &phaseChange{Drop: v.Drop}
}

// phaseOperation is the statement the phase owner contributes.
type phaseOperation struct{ Drop bool }

func (*phaseOperation) Kind() schemaext.Kind { return phaseOwner + "/operation" }
func (v *phaseOperation) CloneExtension() ast.ExtensionPayload {
	return &phaseOperation{Drop: v.Drop}
}

// phaseRuntime plans every change as one operation of phase, with no
// dependency of its own, so the window it joins decides where it lands.
type phaseRuntime struct {
	*engine.Runtime
	phase featureplan.Phase
}

func (r phaseRuntime) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	result := featureplan.Result{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: phaseOwner}
	for i, change := range request.Changes {
		drop := change.Value.(*phaseChange).Drop
		action := plangraph.Create
		if drop {
			action = plangraph.Drop
		}
		id := plangraph.StepID{Owner: phaseOwner, Name: fmt.Sprintf("%06d", i)}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &phaseOperation{Drop: drop}, Phase: r.phase},
			Effects: []plangraph.Effect{{Subject: change.Subject, Action: action}}, Transaction: plangraph.TransactionAllowed,
			Impact: schemaext.Effect{Impact: schemaext.Additive, Reason: "test"},
		})
		result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: change.Subject, Kind: change.Value.Kind(), Strategy: "test", Steps: []plangraph.StepID{id}})
	}
	result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	return result, nil
}

func phaseSubject(name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(objectidentity.Kind(phaseOwner+"/object"), "", name)
}

// phaseOrder names the statements a plan carries that the phase tests read,
// in plan order.
func phaseOrder(nodes []ast.Node) []string {
	var order []string
	for _, node := range nodes {
		switch typed := node.(type) {
		case *ast.CreateViewNode:
			order = append(order, "create view")
		case *ast.DropViewNode:
			order = append(order, "drop view")
		case *ast.AlterTableEnableRLSNode:
			order = append(order, "enable row security")
		case *ast.AlterRoleNode:
			order = append(order, "alter role")
		case *ast.GrantPrivilegeNode:
			order = append(order, "grant")
		case *ast.AlterTableDisableRLSNode:
			order = append(order, "disable row security")
		case *ast.DropFunctionNode:
			order = append(order, "drop function")
		case *ast.DropRoleNode:
			order = append(order, "drop role")
		case *ast.AlterTableNode:
			for _, operation := range typed.Operations {
				switch operation.(type) {
				case *ast.DropColumnOperation:
					order = append(order, "drop column")
				case *ast.DropConstraintOperation:
					order = append(order, "drop constraint")
				}
			}
		case *ast.ExtensionStatement:
			if operation, ok := typed.Payload.(*phaseOperation); ok {
				order = append(order, map[bool]string{false: "owner creates", true: "owner drops"}[operation.Drop])
			}
		}
	}
	return order
}

// TestPlanner_PlacesDependentFeatureOperations pins the two windows a feature
// operation can join. A default one is created before the views that may read
// it and dropped after them, which is where a TimescaleDB aggregate goes. A
// dependent one names objects of every family and nothing common reads it, so
// it is created after the views, the role changes and the row-security
// switches, before the grants, and dropped before row security is disabled
// and before any column, constraint, view, routine or role is.
//
// In both phases an object is created before another is dropped, so a
// replacement under a new name never leaves the table without either: a
// restrictive policy exchanged for another keeps hiding the rows while a plan
// runs without a transaction.
func TestPlanner_PlacesDependentFeatureOperations(t *testing.T) {
	created := &difftypes.SchemaDiff{
		FeatureChanges:    []schemaext.ChangeRecord{{Subject: phaseSubject("guarded"), Value: &phaseChange{}}},
		ViewsAdded:        difftypes.ViewChanges{{Name: "recent", Body: "SELECT 1"}},
		DeclaredViewLikes: difftypes.ViewLikeVocabulary{Views: []schemamodel.View{{Name: "recent", Body: "SELECT 1"}}},
		RolesModified: []difftypes.RoleDiff{{RoleName: "reader", Changes: map[string]string{"login": "false -> true"},
			Desired: schemamodel.Role{Name: "reader", Login: true}}},
		RLSEnabledTablesAdded: difftypes.RLSEnabledTableChanges{{Table: "orders"}},
		GrantsAdded:           []difftypes.GrantRef{{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "orders"}},
	}
	dropped := &difftypes.SchemaDiff{
		FeatureChanges:          []schemaext.ChangeRecord{{Subject: phaseSubject("guarded"), Value: &phaseChange{Drop: true}}},
		ViewsRemoved:            difftypes.ViewChanges{{Name: "recent"}},
		TablesModified:          []difftypes.TableDiff{{TableName: "orders", ColumnsRemoved: difftypes.ColumnChanges{{Name: "tenant"}}}},
		ConstraintsRemoved:      difftypes.ConstraintRemovals{{Name: "orders_tenant_check", TableName: "orders", Type: "CHECK"}},
		RLSEnabledTablesRemoved: difftypes.RLSEnabledTableChanges{{Table: "orders"}},
		FunctionsRemoved:        difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "tenant_of"}, Signature: new("")}},
		RolesRemoved:            difftypes.RoleChanges{{Name: "reader"}},
	}
	// The drop is the first change, so only the windows order it after the
	// creation.
	exchanged := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
		{Subject: phaseSubject("old"), Value: &phaseChange{Drop: true}}, {Subject: phaseSubject("new"), Value: &phaseChange{}},
	}}
	tests := []struct {
		name  string
		phase featureplan.Phase
		diff  *difftypes.SchemaDiff
		want  []string
	}{
		{name: "a default creation", phase: featureplan.PhaseDefault, diff: created,
			want: []string{"owner creates", "create view", "alter role", "enable row security", "grant"}},
		{name: "a dependent creation", phase: featureplan.PhaseDependent, diff: created,
			want: []string{"create view", "alter role", "enable row security", "owner creates", "grant"}},
		{name: "a default removal", phase: featureplan.PhaseDefault, diff: dropped,
			want: []string{"disable row security", "drop column", "drop constraint", "drop view", "owner drops", "drop function", "drop role"}},
		{name: "a dependent removal", phase: featureplan.PhaseDependent, diff: dropped,
			want: []string{"owner drops", "disable row security", "drop column", "drop constraint", "drop view", "drop function", "drop role"}},
		{name: "a default exchange", phase: featureplan.PhaseDefault, diff: exchanged, want: []string{"owner creates", "owner drops"}},
		{name: "a dependent exchange", phase: featureplan.PhaseDependent, diff: exchanged, want: []string{"owner creates", "owner drops"}},
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
