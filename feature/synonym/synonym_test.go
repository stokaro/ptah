package synonym_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/feature/synonym"
)

// TestTargetParts_HappyPath reads a target from the right, as SQL Server
// writes base_object_name: the last part is the object, and an absent middle
// part is an empty pair of brackets, so `[remote]..[dbo].[orders]` names a
// linked server and no database. A parser that assigned parts from the left
// would take the server for a database.
func TestTargetParts_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		want     [4]string
		declared string
	}{
		{name: "an unqualified object", target: "[orders]", want: [4]string{"", "", "", "orders"}, declared: "orders"},
		{name: "schema and object", target: "[dbo].[orders]", want: [4]string{"", "", "dbo", "orders"}, declared: "dbo.orders"},
		{name: "another database", target: "[sales].[dbo].[orders]", want: [4]string{"", "sales", "dbo", "orders"}, declared: "sales.dbo.orders"},
		{name: "a linked server", target: "[remote].[sales].[dbo].[orders]", want: [4]string{"remote", "sales", "dbo", "orders"},
			declared: "remote.sales.dbo.orders"},
		{name: "a linked server and no database", target: "[remote]..[dbo].[orders]", want: [4]string{"remote", "", "dbo", "orders"},
			declared: "remote..dbo.orders"},
		{name: "unquoted parts", target: "sales.dbo.orders", want: [4]string{"", "sales", "dbo", "orders"}, declared: "sales.dbo.orders"},
		{name: "a dot inside a quoted part", target: `[dbo].[order.lines]`, want: [4]string{"", "", "dbo", "order.lines"},
			declared: "dbo.[order.lines]"},
		{name: "a doubled closing bracket", target: `[dbo].[a]]b]`, want: [4]string{"", "", "dbo", "a]b"}, declared: "dbo.[a]]b]"},
		{name: "double quotes", target: `"dbo"."orders"`, want: [4]string{"", "", "dbo", "orders"}, declared: "dbo.orders"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parts := synonym.TargetParts(test.target)

			c.Assert(parts, qt.Equals, test.want)
			c.Assert(synonym.DeclaredTarget(parts), qt.Equals, test.declared)
			c.Assert(synonym.TargetParts(synonym.DeclaredTarget(parts)), qt.Equals, test.want,
				qt.Commentf("the declared spelling splits back into the same parts"))
		})
	}
}

// TestSameTarget compares targets without their quoting and without case,
// part by part: a declared `dbo.orders` and a stored `[dbo].[orders]` are one
// target, and a target in another database is not the local one of the same
// name.
func TestSameTarget(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{name: "the server's bracket quoting", a: "dbo.orders", b: "[dbo].[orders]", want: true},
		{name: "letter case", a: "DBO.Orders", b: "[dbo].[orders]", want: true},
		{name: "an empty middle part", a: "remote..dbo.orders", b: "[remote]..[dbo].[orders]", want: true},
		{name: "another object", a: "dbo.orders", b: "[dbo].[invoices]", want: false},
		{name: "another database", a: "other.dbo.orders", b: "[dbo].[orders]", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(synonym.SameTarget(test.a, test.b), qt.Equals, test.want)
		})
	}
}

// TestValidate_FailurePath refuses a synonym no target can hold.
func TestValidate_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		synonym synonym.Synonym
		want    string
	}{
		{name: "no name", synonym: synonym.Synonym{Target: "dbo.orders"}, want: `.*a synonym needs a name`},
		{name: "no target", synonym: synonym.Synonym{Schema: "dbo", Name: "s"}, want: `.*synonym "dbo\.s" needs a target`},
		{name: "five parts", synonym: synonym.Synonym{Name: "s", Target: "a.b.c.d.e"}, want: `.*target "a\.b\.c\.d\.e" has more than four parts`},
		{name: "no object part", synonym: synonym.Synonym{Name: "s", Target: "dbo."}, want: `.*target "dbo\." names no object`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := synonym.Validate(test.synonym)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

func render(c *qt.C, target string, caps capability.Capabilities, operation *synonym.Operation) []string {
	c.Helper()
	registry := must.Must(renderer.NewExtensions(synonym.Handlers()...))
	statements, err := registry.Render(renderer.ExtensionContext{Target: target, Capabilities: caps},
		ast.StatementExtension, operation)
	c.Assert(err, qt.IsNil)
	return statements
}

func operation(action synonym.Action, comment string, s synonym.Synonym) *synonym.Operation {
	return &synonym.Operation{Action: action, Synonym: synonym.DesiredSynonym{Synonym: s, Comment: comment}}
}

// TestOperation_WritesTheStatements pins the statements each action writes.
// Neither engine has ALTER SYNONYM and CREATE SYNONYM refuses a name that
// exists, so a retarget is the drop and then the create, and the declaration's
// comment stays above the create it documents.
func TestOperation_WritesTheStatements(t *testing.T) {
	orders := synonym.Synonym{Schema: "app", Name: "orders", Target: "sales.orders"}
	tests := []struct {
		name      string
		target    string
		caps      capability.Capabilities
		operation *synonym.Operation
		want      []string
	}{
		{name: "create", target: platform.SQLServer, operation: operation(synonym.Create, "", orders),
			want: []string{"CREATE SYNONYM [app].[orders] FOR [sales].[orders];"}},
		{name: "create with a comment", target: platform.SQLServer, operation: operation(synonym.Create, "why", orders),
			want: []string{"-- why", "CREATE SYNONYM [app].[orders] FOR [sales].[orders];"}},
		{name: "drop", target: platform.SQLServer, operation: operation(synonym.Drop, "", orders),
			want: []string{"DROP SYNONYM IF EXISTS [app].[orders];"}},
		{name: "retarget", target: platform.SQLServer, operation: operation(synonym.Retarget, "why", orders),
			want: []string{"DROP SYNONYM IF EXISTS [app].[orders];", "-- why", "CREATE SYNONYM [app].[orders] FOR [sales].[orders];"}},
		{name: "an alias in the default schema", target: platform.SQLServer,
			operation: operation(synonym.Create, "", synonym.Synonym{Name: "orders", Target: "dbo.orders"}),
			want:      []string{"CREATE SYNONYM [orders] FOR [dbo].[orders];"}},
		{name: "an Oracle drop with the guard", target: platform.Oracle, caps: capability.ForDialect(platform.Oracle),
			operation: operation(synonym.Drop, "", orders), want: []string{"DROP SYNONYM IF EXISTS app.orders;"}},
		{name: "an Oracle drop where the release has no guard", target: platform.Oracle,
			operation: operation(synonym.Drop, "", orders), want: []string{"DROP SYNONYM app.orders;"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(render(c, test.target, test.caps, test.operation), qt.DeepEquals, test.want)
		})
	}
}

// TestOperation_RefusesAnotherTarget refuses an operation on a target that has
// no synonyms.
func TestOperation_RefusesAnotherTarget(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(renderer.NewExtensions(synonym.Handlers()...))

	got, err := registry.Render(renderer.ExtensionContext{Target: platform.Postgres, Capabilities: capability.ForDialect(platform.Postgres)},
		ast.StatementExtension, operation(synonym.Create, "", synonym.Synonym{Name: "s", Target: "t"}))

	c.Assert(err, qt.ErrorMatches, `(?s).*postgres.*`)
	c.Assert(got, qt.IsNil)
}

func objects(c *qt.C, values ...schemaext.Object) schemaext.Objects {
	c.Helper()
	return must.Must(schemaext.NewObjects(values...))
}

func complete(representation schemaext.Representation) schemaext.Coverage {
	return must.Must(synonym.Coverage(representation, schemaext.Knowledge{State: schemaext.Complete}, nil))
}

func declared(s synonym.Synonym) schemaext.Object {
	return synonym.DeclaredObject(synonym.DesiredSynonym{Synonym: s})
}

func observed(s synonym.Synonym) schemaext.Object {
	return synonym.ObservedObject(synonym.ObservedSynonym{Synonym: s})
}

func compare(c *qt.C, desired, current schemaext.ObjectState) schemaext.ObjectComparisonResult {
	c.Helper()
	// A connected server's collation answers how names compare; the default
	// collation folds case.
	semantics := identifier.ForDialect(platform.SQLServer)
	semantics.TableNames = identifier.ComparisonASCIIInsensitive
	result, err := synonym.CompareService{}.CompareObjects(c.Context(), schemaext.ObjectComparisonRequest{
		Target: platform.SQLServer, Identifiers: semantics, Kinds: []schemaext.Kind{synonym.Kind},
		Desired: desired, Current: current,
	})
	c.Assert(err, qt.IsNil)
	return result
}

// changeTargets is each change of a comparison as its two targets, empty
// where the side is absent.
func changeTargets(result schemaext.ObjectComparisonResult) [][2]string {
	var targets [][2]string
	for _, record := range result.Changes {
		change := record.Value.(*synonym.Change)
		var pair [2]string
		if change.Before != nil {
			pair[0] = change.Before.Target
		}
		if change.After != nil {
			pair[1] = change.After.Target
		}
		targets = append(targets, pair)
	}
	return targets
}

// TestCompareService_Changes covers each comparison outcome. A synonym is
// matched by alias under the server's identifier rules, which fold case here
// and fill in the default schema; a target differing only in the server's
// quoting is no change; a changed target is one retarget; and a synonym the
// database holds and the declaration leaves out is dropped only where the
// source can declare one.
func TestCompareService_Changes(t *testing.T) {
	stored := synonym.Synonym{Schema: "app", Name: "orders", Target: "[sales].[orders]"}
	tests := []struct {
		name     string
		desired  schemaext.ObjectState
		current  schemaext.ObjectState
		changes  [][2]string
		declared int
	}{
		{name: "added",
			desired: schemaext.ObjectState{Objects: objects(qt.New(t), declared(synonym.Synonym{Schema: "app", Name: "orders", Target: "sales.orders"})),
				Coverage: complete(schemaext.Desired)},
			current: schemaext.ObjectState{Coverage: complete(schemaext.Observed)}, changes: [][2]string{{"", "sales.orders"}}, declared: 1},
		{name: "the same target in the server's quoting and another case",
			desired: schemaext.ObjectState{Objects: objects(qt.New(t), declared(synonym.Synonym{Schema: "APP", Name: "Orders", Target: "Sales.Orders"})),
				Coverage: complete(schemaext.Desired)},
			current: schemaext.ObjectState{Objects: objects(qt.New(t), observed(stored)), Coverage: complete(schemaext.Observed)}, declared: 1},
		{name: "an alias in the default schema, written with and without it",
			desired: schemaext.ObjectState{Objects: objects(qt.New(t), declared(synonym.Synonym{Schema: "dbo", Name: "orders", Target: "sales.orders"})),
				Coverage: complete(schemaext.Desired)},
			current: schemaext.ObjectState{Objects: objects(qt.New(t), observed(synonym.Synonym{Name: "orders", Target: "[sales].[orders]"})),
				Coverage: complete(schemaext.Observed)}, declared: 1},
		{name: "retargeted",
			desired: schemaext.ObjectState{Objects: objects(qt.New(t), declared(synonym.Synonym{Schema: "app", Name: "orders", Target: "sales.orders_v2"})),
				Coverage: complete(schemaext.Desired)},
			current: schemaext.ObjectState{Objects: objects(qt.New(t), observed(stored)), Coverage: complete(schemaext.Observed)},
			changes: [][2]string{{"[sales].[orders]", "sales.orders_v2"}}, declared: 1},
		{name: "dropped where the source declares synonyms", desired: schemaext.ObjectState{Coverage: complete(schemaext.Desired)},
			current: schemaext.ObjectState{Objects: objects(qt.New(t), observed(stored)), Coverage: complete(schemaext.Observed)},
			changes: [][2]string{{"[sales].[orders]", ""}}},
		{name: "adopted where the source cannot declare one",
			current: schemaext.ObjectState{Objects: objects(qt.New(t), observed(stored)), Coverage: complete(schemaext.Observed)}, declared: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := compare(c, test.desired, test.current)
			c.Assert(changeTargets(result), qt.DeepEquals, test.changes)
			c.Assert(result.Desired.Objects.Len(), qt.Equals, test.declared)
		})
	}
}

// TestPlanService_PlansEachChangeInTheDependentWindow plans each change as one
// step in the dependent window: after the objects a target may name are
// created and before any of them is dropped. Neither engine resolves the target
// when the alias is created, so the step reads nothing else.
func TestPlanService_PlansEachChangeInTheDependentWindow(t *testing.T) {
	before := &synonym.ObservedSynonym{Synonym: synonym.Synonym{Schema: "app", Name: "orders", Target: "sales.orders"}}
	after := &synonym.DesiredSynonym{Synonym: synonym.Synonym{Schema: "app", Name: "orders", Target: "sales.orders_v2"}}
	tests := []struct {
		name   string
		change *synonym.Change
		action synonym.Action
		effect plangraph.Action
	}{
		{name: "create", change: &synonym.Change{After: after}, action: synonym.Create, effect: plangraph.Create},
		{name: "drop", change: &synonym.Change{Before: before}, action: synonym.Drop, effect: plangraph.Drop},
		{name: "retarget", change: &synonym.Change{Before: before, After: after}, action: synonym.Retarget, effect: plangraph.Alter},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := synonym.PlanService{}.PlanFeatures(c.Context(), featureplan.Request{
				Target: platform.SQLServer, Identifiers: identifier.ForDialect(platform.SQLServer),
				Changes: []schemaext.ChangeRecord{{Subject: before.Ref(), Value: test.change}},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Contributions, qt.HasLen, 1)
			step := result.Contributions[0].Steps[0]
			c.Assert(step.Payload.Phase, qt.Equals, featureplan.PhaseDependent)
			c.Assert(step.Payload.Payload.(*synonym.Operation).Action, qt.Equals, test.action)
			c.Assert(step.Effects, qt.DeepEquals, []plangraph.Effect{{Subject: before.Ref(), Action: test.effect}})
		})
	}
}

// TestReverseService_PointsARetargetBack reverses a retarget to the target the
// database had before the change, which is what a rollback must recreate.
func TestReverseService_PointsARetargetBack(t *testing.T) {
	c := qt.New(t)
	before := &synonym.ObservedSynonym{Synonym: synonym.Synonym{Name: "orders", Target: "dbo.orders_v1"}}
	after := &synonym.DesiredSynonym{Synonym: synonym.Synonym{Name: "orders", Target: "dbo.orders_v2"}}

	reversals, err := synonym.ReverseService{}.ReverseChanges(c.Context(), schemaext.ReversalRequest{Target: platform.SQLServer,
		Changes: []schemaext.ChangeRecord{{Subject: before.Ref(), Value: &synonym.Change{Before: before, After: after}}}})

	c.Assert(err, qt.IsNil)
	c.Assert(reversals, qt.HasLen, 1)
	reversed := reversals[0].Change.Value.(*synonym.Change)
	c.Assert(reversed.After.Target, qt.Equals, "dbo.orders_v1")
	c.Assert(reversed.Before.Target, qt.Equals, "dbo.orders_v2")
}

// TestDeclaredObject_IsBoundToTheTargetsThatHaveSynonyms binds a declaration
// to SQL Server and Oracle, so a schema for another target leaves it out
// rather than refusing it.
func TestDeclaredObject_IsBoundToTheTargetsThatHaveSynonyms(t *testing.T) {
	c := qt.New(t)
	object := declared(synonym.Synonym{Name: "s", Target: "t"})
	c.Assert(object.Targets, qt.DeepEquals, []string{platform.SQLServer, platform.Oracle})
}

// TestValidateChange_AcceptsADefaultedSchema accepts a change whose read side
// left out the connection's default schema the declaration wrote, which a
// comparison pairs under the server's rules, and refuses two different
// schemas.
func TestValidateChange_AcceptsADefaultedSchema(t *testing.T) {
	c := qt.New(t)
	read := &synonym.ObservedSynonym{Synonym: synonym.Synonym{Name: "orders", Target: "dbo.orders_v1"}}
	inSales := &synonym.ObservedSynonym{Synonym: synonym.Synonym{Schema: "sales", Name: "orders", Target: "dbo.orders_v1"}}
	declared := &synonym.DesiredSynonym{Synonym: synonym.Synonym{Schema: "dbo", Name: "orders", Target: "dbo.orders_v2"}}

	c.Assert(synonym.ValidateChange(&synonym.Change{Before: read, After: declared}), qt.IsNil)
	c.Assert(synonym.ValidateChange(&synonym.Change{Before: inSales, After: declared}), qt.ErrorMatches, `.*operands name different synonyms`)
}
