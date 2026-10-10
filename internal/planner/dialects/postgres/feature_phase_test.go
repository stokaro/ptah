package postgres_test

import (
	"cmp"
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

const phaseOwner = "example.org/phase"

// secondOwner is another owner, which reads an object the phase owner creates.
const secondOwner = "example.org/second"

// phaseChange asks an owner to create, drop or read one object; an empty
// Action creates it. An empty Owner is the phase owner.
type phaseChange struct {
	Action plangraph.Action
	Owner  string
}

func (*phaseChange) Kind() schemaext.Kind { return phaseOwner + "/change" }
func (v *phaseChange) CloneChange() schemaext.ChangeValue {
	return &phaseChange{Action: v.Action, Owner: v.Owner}
}

// phaseOperation is the statement an owner contributes.
type phaseOperation struct {
	Action plangraph.Action
	Second bool
}

func (*phaseOperation) Kind() schemaext.Kind { return phaseOwner + "/operation" }
func (v *phaseOperation) CloneExtension() ast.ExtensionPayload {
	return &phaseOperation{Action: v.Action, Second: v.Second}
}

// phaseRuntime plans every change as one operation of phase, in the
// contribution of the change's owner. ordered lists pairs of change indexes the
// owner orders, the first before the second; nothing else orders the steps,
// so the windows and the lifecycle decide where they land.
type phaseRuntime struct {
	*engine.Runtime
	phase   featureplan.Phase
	ordered [][2]int
}

func (r phaseRuntime) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	result := featureplan.Result{Complete: true}
	contributions := map[string]*plangraph.Contribution[featureplan.Operation]{
		phaseOwner: {Owner: phaseOwner}, secondOwner: {Owner: secondOwner},
	}
	ids := make([]plangraph.StepID, len(request.Changes))
	for i, change := range request.Changes {
		value := change.Value.(*phaseChange)
		owner := cmp.Or(value.Owner, phaseOwner)
		action := cmp.Or(value.Action, plangraph.Create)
		ids[i] = plangraph.StepID{Owner: owner, Name: fmt.Sprintf("%06d", i)}
		contributions[owner].Steps = append(contributions[owner].Steps, plangraph.Step[featureplan.Operation]{ID: ids[i],
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &phaseOperation{Action: action, Second: owner == secondOwner}, Phase: r.phase},
			Effects: []plangraph.Effect{{Subject: change.Subject, Action: action}}, Transaction: plangraph.TransactionAllowed,
			Impact: schemaext.Effect{Impact: schemaext.Additive, Reason: "test"},
		})
		result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: change.Subject, Kind: change.Value.Kind(), Strategy: "test", Steps: []plangraph.StepID{ids[i]}})
	}
	for _, pair := range r.ordered {
		contributions[ids[pair[0]].Owner].Dependencies = append(contributions[ids[pair[0]].Owner].Dependencies,
			plangraph.Dependency{Before: ids[pair[0]], After: ids[pair[1]]})
	}
	for _, owner := range []string{phaseOwner, secondOwner} {
		result.Contributions = append(result.Contributions, *contributions[owner])
	}
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
		case *ast.CreateTableNode:
			order = append(order, "create table")
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
		case *ast.DropTableNode:
			order = append(order, "drop table")
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
				order = append(order, map[bool]string{false: "owner", true: "second owner"}[operation.Second]+
					map[plangraph.Action]string{plangraph.Create: " creates", plangraph.Drop: " drops", plangraph.Read: " reads"}[operation.Action])
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
// and before any column, constraint, view, routine or role is. In the default
// phase an object is created before another is dropped.
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
		FeatureChanges:          []schemaext.ChangeRecord{{Subject: phaseSubject("guarded"), Value: &phaseChange{Action: plangraph.Drop}}},
		ViewsRemoved:            difftypes.ViewChanges{{Name: "recent"}},
		TablesModified:          []difftypes.TableDiff{{TableName: "orders", ColumnsRemoved: difftypes.ColumnChanges{{Name: "tenant"}}}},
		ConstraintsRemoved:      difftypes.ConstraintRemovals{{Name: "orders_tenant_check", TableName: "orders", Type: "CHECK"}},
		RLSEnabledTablesRemoved: difftypes.RLSEnabledTableChanges{{Table: "orders"}},
		FunctionsRemoved:        difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "tenant_of"}, Signature: new("")}},
		RolesRemoved:            difftypes.RoleChanges{{Name: "reader"}},
	}
	// The drop is the first change, so only the windows order it after the
	// creation.
	exchanged := exchange("old", "new")
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

// exchange drops the object named dropped and creates the one named created,
// the drop first.
func exchange(dropped, created string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
		{Subject: phaseSubject(dropped), Value: &phaseChange{Action: plangraph.Drop}},
		{Subject: phaseSubject(created), Value: &phaseChange{}},
	}}
}

// TestPlanner_LetsTheOwnerOrderDependentOperations pins the dependent window:
// creations and drops share it, so the order between them is the owner's. A
// policy replaced under its own name is dropped first, and whether a
// replacement under a new name drops or creates first depends on what the
// policy admits, which only its owner knows. An object one owner creates and
// another reads is ordered by its lifecycle, the creation first, though the
// reader is listed first.
func TestPlanner_LetsTheOwnerOrderDependentOperations(t *testing.T) {
	tests := []struct {
		name    string
		diff    *difftypes.SchemaDiff
		ordered [][2]int
		want    []string
	}{
		{name: "the owner drops first", diff: exchange("old", "new"), ordered: [][2]int{{0, 1}}, want: []string{"owner drops", "owner creates"}},
		{name: "the owner creates first", diff: exchange("old", "new"), ordered: [][2]int{{1, 0}}, want: []string{"owner creates", "owner drops"}},
		{name: "a replacement under one name", diff: exchange("guarded", "guarded"), ordered: [][2]int{{0, 1}},
			want: []string{"owner drops", "owner creates"}},
		{name: "an object another owner reads", diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
			{Subject: phaseSubject("guarded"), Value: &phaseChange{Action: plangraph.Read, Owner: secondOwner}},
			{Subject: phaseSubject("guarded"), Value: &phaseChange{}},
		}}, want: []string{"owner creates", "second owner reads"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := phaseRuntime{Runtime: must.Must(builtin.New()), phase: featureplan.PhaseDependent, ordered: test.ordered}

			nodes, err := postgres.New().GenerateMigrationAST(context.Background(), runtime, test.diff)

			c.Assert(err, qt.IsNil)
			c.Assert(phaseOrder(nodes), qt.DeepEquals, test.want)
		})
	}
}

// TestPlanner_RefusesSplitReplacements pins the replacements no window can
// order: one split over the two default windows, which would create the
// object before the drop it replaces, and one in the dependent window that its
// owner left unordered.
func TestPlanner_RefusesSplitReplacements(t *testing.T) {
	tests := []struct {
		name  string
		phase featureplan.Phase
		want  string
	}{
		{name: "in the default phase", phase: featureplan.PhaseDefault, want: `.*creates .*guarded, which step example\.org/phase/000000 drops; a replacement is one step.*`},
		{name: "unordered in the dependent phase", phase: featureplan.PhaseDependent, want: `.*unordered effects on .*guarded.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := phaseRuntime{Runtime: must.Must(builtin.New()), phase: test.phase}

			nodes, err := postgres.New().GenerateMigrationAST(context.Background(), runtime, exchange("guarded", "guarded"))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// removalRuntime accounts for every removed table it is asked about with one
// dependent-phase operation that drops an object of the phase owner.
type removalRuntime struct{ *engine.Runtime }

func (removalRuntime) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	result := featureplan.Result{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: phaseOwner}
	for i, table := range request.Tables {
		id := plangraph.StepID{Owner: phaseOwner, Name: fmt.Sprintf("table-%06d", i)}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &phaseOperation{Action: plangraph.Drop}, Phase: featureplan.PhaseDependent},
			Effects: []plangraph.Effect{{Subject: phaseSubject("guarded"), Action: plangraph.Drop}}, Transaction: plangraph.TransactionAllowed,
			Impact: schemaext.Effect{Impact: schemaext.Behavioral, Reason: "test"},
		})
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: tsschema.HypertableKind,
			Action: table.Action, Strategy: "test", Steps: []plangraph.StepID{id}})
	}
	result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	return result, nil
}

// TestPlanner_PlacesADependentOperationOfARemovedTable pins the removed-table
// path, which places an owner's operations at one position rather than in a
// window: a dependent operation is accepted there, ahead of the table's drop.
func TestPlanner_PlacesADependentOperationOfARemovedTable(t *testing.T) {
	c := qt.New(t)
	removal := difftypes.TableRemoval{Name: "orders"}
	removal.Current.Table = catalog.Table{Name: "orders", Facets: must.Must(schemaext.NewFacets(&tsschema.ObservedHypertable{Column: "ts"}))}

	nodes, err := postgres.New().GenerateMigrationAST(context.Background(), removalRuntime{Runtime: must.Must(builtin.New())},
		&difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{removal}})

	c.Assert(err, qt.IsNil)
	c.Assert(phaseOrder(nodes), qt.DeepEquals, []string{"owner drops", "drop table"})
}

// phaseChild is a named child of a table, owned by the phase owner.
type phaseChild struct{}

func (*phaseChild) Kind() schemaext.Kind             { return phaseOwner + "/child" }
func (*phaseChild) Clone() schemaext.Value           { return &phaseChild{} }
func (*phaseChild) Equal(other schemaext.Value) bool { _, ok := other.(*phaseChild); return ok }

// creationRuntime answers every table the plan creates with one dependent
// step that creates its children, and records the request it was sent.
type creationRuntime struct {
	*engine.Runtime
	received *featureplan.Request
}

func (r creationRuntime) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	*r.received = request
	result := featureplan.Result{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: phaseOwner}
	for i, table := range request.Tables {
		id := plangraph.StepID{Owner: phaseOwner, Name: fmt.Sprintf("children/%06d", i)}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: &phaseOperation{Action: plangraph.Create}, Phase: featureplan.PhaseDependent},
			Effects:     []plangraph.Effect{{Subject: phaseSubject("child"), Action: plangraph.Create}, {Subject: table.Subject, Action: plangraph.Read}},
			Transaction: plangraph.TransactionAllowed, Impact: schemaext.Effect{Impact: schemaext.Additive, Reason: "test"},
		})
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: phaseOwner + "/child", Action: table.Action,
			Strategy: "create the children", Steps: []plangraph.StepID{id}})
	}
	result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	return result, nil
}

// TestPlanner_HandsACreatedTablesChildrenToTheirOwner pins a table the plan
// creates with a named child: its CREATE TABLE does not carry the child, the
// owner is sent the table as a creation with its declaration, and the owner's
// creation step lands in the dependent window, after the views.
func TestPlanner_HandsACreatedTablesChildrenToTheirOwner(t *testing.T) {
	c := qt.New(t)
	table := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "orders")
	child := objectidentity.ID{Kind: objectidentity.Kind(phaseOwner + "/child"), Schema: table.Schema, Parent: table.Name,
		Name: objectidentity.Part{Source: "tenant", Normalized: "tenant"}}
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{Name: "orders", Table: schemamodel.Table{StructName: "Order", Name: "orders"},
			Fields:       []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}},
			OwnedObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: child, Value: &phaseChild{}}))}},
		ViewsAdded:        difftypes.ViewChanges{{Name: "recent", Body: "SELECT 1"}},
		DeclaredViewLikes: difftypes.ViewLikeVocabulary{Views: []schemamodel.View{{Name: "recent", Body: "SELECT 1"}}},
	}
	var received featureplan.Request

	nodes, err := postgres.New().GenerateMigrationAST(context.Background(), creationRuntime{Runtime: must.Must(builtin.New()), received: &received}, diff)

	c.Assert(err, qt.IsNil)
	c.Assert(phaseOrder(nodes), qt.DeepEquals, []string{"create table", "create view", "owner creates"})
	c.Assert(received.Tables, qt.HasLen, 1)
	c.Assert(received.Tables[0].Action, qt.Equals, featureplan.CreateTable)
	c.Assert(received.Tables[0].Subject, qt.DeepEquals, table)
	c.Assert(received.Tables[0].Desired.OwnedObjects.Len(), qt.Equals, 1)
	c.Assert(received.Tables[0].Current.HasTable(), qt.IsFalse)
}
