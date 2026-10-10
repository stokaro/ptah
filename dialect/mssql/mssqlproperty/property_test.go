package mssqlproperty_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlproperty"
)

func property(schema, table, column, name, value string) mssqlproperty.Property {
	return mssqlproperty.Property{Schema: schema, Table: table, Column: column, Name: name, Value: value}
}

func render(c *qt.C, action mssqlproperty.Action, declared mssqlproperty.DesiredProperty) []string {
	registry := must.Must(renderer.NewExtensions(mssqlproperty.Handlers()...))
	statements, err := registry.Render(renderer.ExtensionContext{Target: platform.SQLServer, Capabilities: capability.SQLServer2022()},
		ast.StatementExtension, &mssqlproperty.Operation{Action: action, Property: declared})
	c.Assert(err, qt.IsNil)
	return statements
}

// TestOperation_WritesTheAddressLevelByLevel pins the procedure each action
// runs and the address it writes: no level for the database's own property,
// one more level for each of schema, table and column, every argument a
// string literal with its quotes doubled, and no value for a drop, which
// sp_dropextendedproperty refuses (`has too many arguments specified`).
func TestOperation_WritesTheAddressLevelByLevel(t *testing.T) {
	tests := []struct {
		name     string
		action   mssqlproperty.Action
		property mssqlproperty.Property
		want     string
	}{
		{name: "database scope passes no level", action: mssqlproperty.Add, property: property("", "", "", "ptah_db", "on"),
			want: "EXEC sp_addextendedproperty @name = N'ptah_db', @value = N'on';"},
		{name: "schema scope passes level 0 alone", action: mssqlproperty.Add, property: property("app", "", "", "ptah_flag", "on"),
			want: "EXEC sp_addextendedproperty @name = N'ptah_flag', @value = N'on', @level0type = N'SCHEMA', @level0name = N'app';"},
		{name: "column scope adds levels 1 and 2", action: mssqlproperty.Add, property: property("app", "docs", "title", "ptah_flag", "on"),
			want: "EXEC sp_addextendedproperty @name = N'ptah_flag', @value = N'on', @level0type = N'SCHEMA', @level0name = N'app', " +
				"@level1type = N'TABLE', @level1name = N'docs', @level2type = N'COLUMN', @level2name = N'title';"},
		{name: "an update names the update procedure", action: mssqlproperty.Update, property: property("app", "docs", "", "ptah_flag", "off"),
			want: "EXEC sp_updateextendedproperty @name = N'ptah_flag', @value = N'off', @level0type = N'SCHEMA', @level0name = N'app', " +
				"@level1type = N'TABLE', @level1name = N'docs';"},
		{name: "a drop passes no value", action: mssqlproperty.Drop, property: property("app", "docs", "", "ptah_flag", "ignored"),
			want: "EXEC sp_dropextendedproperty @name = N'ptah_flag', @level0type = N'SCHEMA', @level0name = N'app', " +
				"@level1type = N'TABLE', @level1name = N'docs';"},
		{name: "quotes are doubled in every literal", action: mssqlproperty.Add, property: property("o'brien", "d'oh", "c'est", "it's", "va'lue"),
			want: "EXEC sp_addextendedproperty @name = N'it''s', @value = N'va''lue', @level0type = N'SCHEMA', @level0name = N'o''brien', " +
				"@level1type = N'TABLE', @level1name = N'd''oh', @level2type = N'COLUMN', @level2name = N'c''est';"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(render(c, test.action, mssqlproperty.DesiredProperty{Property: test.property}), qt.DeepEquals, []string{test.want})
		})
	}
}

// TestOperation_WritesTheCommentAboveTheStatement keeps a declaration's
// documentation in the script.
func TestOperation_WritesTheCommentAboveTheStatement(t *testing.T) {
	c := qt.New(t)
	got := render(c, mssqlproperty.Add, mssqlproperty.DesiredProperty{Property: property("", "", "", "ptah_db", "on"), Comment: "why"})
	c.Assert(got, qt.DeepEquals, []string{"-- why", "EXEC sp_addextendedproperty @name = N'ptah_db', @value = N'on';"})
}

// TestOperation_FailurePath refuses an address no statement can compose and
// another target.
func TestOperation_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		target string
		op     *mssqlproperty.Operation
		want   string
	}{
		{name: "a table without its schema", target: platform.SQLServer,
			op:   &mssqlproperty.Operation{Action: mssqlproperty.Add, Property: mssqlproperty.DesiredProperty{Property: property("", "docs", "", "p", "v")}},
			want: `(?s).*names table "docs" and no schema.*`},
		{name: "a column without its table", target: platform.SQLServer,
			op:   &mssqlproperty.Operation{Action: mssqlproperty.Add, Property: mssqlproperty.DesiredProperty{Property: property("app", "", "title", "p", "v")}},
			want: `(?s).*names column "title" and no table.*`},
		{name: "an unknown action", target: platform.SQLServer,
			op:   &mssqlproperty.Operation{Action: "rename", Property: mssqlproperty.DesiredProperty{Property: property("app", "", "", "p", "v")}},
			want: `(?s).*has action "rename".*`},
		{name: "another target", target: platform.Postgres,
			op:   &mssqlproperty.Operation{Action: mssqlproperty.Add, Property: mssqlproperty.DesiredProperty{Property: property("app", "", "", "p", "v")}},
			want: `(?s).*postgres.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry := must.Must(renderer.NewExtensions(mssqlproperty.Handlers()...))
			got, err := registry.Render(renderer.ExtensionContext{Target: test.target, Capabilities: capability.ForDialect(test.target)}, ast.StatementExtension, test.op)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

func objects(c *qt.C, values ...schemaext.Object) schemaext.Objects {
	c.Helper()
	return must.Must(schemaext.NewObjects(values...))
}

func complete(representation schemaext.Representation, subjects ...schemaext.SubjectCoverage) schemaext.Coverage {
	return must.Must(mssqlproperty.Coverage(representation, schemaext.Knowledge{State: schemaext.Complete}, subjects))
}

func compare(c *qt.C, desired, current schemaext.ObjectState) schemaext.ObjectComparisonResult {
	c.Helper()
	result, err := mssqlproperty.CompareService{}.CompareObjects(c.Context(), schemaext.ObjectComparisonRequest{
		Target: platform.SQLServer, Identifiers: identifier.ForDialect(platform.SQLServer), Kinds: []schemaext.Kind{mssqlproperty.Kind},
		Desired: desired, Current: current,
	})
	c.Assert(err, qt.IsNil)
	return result
}

func declared(p mssqlproperty.Property) schemaext.Object {
	return mssqlproperty.DeclaredObject(mssqlproperty.DesiredProperty{Property: p})
}

func observed(p mssqlproperty.Property) schemaext.Object {
	return mssqlproperty.ObservedObject(mssqlproperty.ObservedProperty{Property: p, ValueType: "nvarchar"})
}

// TestCompareService_Changes covers each comparison outcome. A property is
// one property however a side spells its address, since SQL Server's default
// collation folds case; a changed value is a change in place; and a property
// the database holds and the declaration leaves out is dropped only where the
// source can declare one.
func TestCompareService_Changes(t *testing.T) {
	flag := property("app", "docs", "title", "ptah_flag", "on")
	shouted := property("APP", "Docs", "TITLE", "PTAH_FLAG", "on")
	changed := property("app", "docs", "title", "ptah_flag", "off")
	tests := []struct {
		name            string
		desired         schemaext.ObjectState
		current         schemaext.ObjectState
		changes         int
		before, after   *mssqlproperty.Property
		desiredProperty int
	}{
		{name: "added", desired: schemaext.ObjectState{Objects: objects(qt.New(t), declared(flag)), Coverage: complete(schemaext.Desired)},
			current: schemaext.ObjectState{Coverage: complete(schemaext.Observed)}, changes: 1, after: &flag, desiredProperty: 1},
		{name: "spelled in another case", desired: schemaext.ObjectState{Objects: objects(qt.New(t), declared(shouted)), Coverage: complete(schemaext.Desired)},
			current: schemaext.ObjectState{Objects: objects(qt.New(t), observed(flag)), Coverage: complete(schemaext.Observed)}, desiredProperty: 1},
		{name: "value changed", desired: schemaext.ObjectState{Objects: objects(qt.New(t), declared(changed)), Coverage: complete(schemaext.Desired)},
			current: schemaext.ObjectState{Objects: objects(qt.New(t), observed(flag)), Coverage: complete(schemaext.Observed)},
			changes: 1, before: &flag, after: &changed, desiredProperty: 1},
		{name: "dropped where the source declares properties", desired: schemaext.ObjectState{Coverage: complete(schemaext.Desired)},
			current: schemaext.ObjectState{Objects: objects(qt.New(t), observed(flag)), Coverage: complete(schemaext.Observed)}, changes: 1, before: &flag},
		{name: "adopted where the source cannot declare one",
			current: schemaext.ObjectState{Objects: objects(qt.New(t), observed(flag)), Coverage: complete(schemaext.Observed)}, desiredProperty: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := compare(c, test.desired, test.current)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Desired.Objects.Len(), qt.Equals, test.desiredProperty)
			for _, record := range result.Changes {
				change := record.Value.(*mssqlproperty.Change)
				c.Assert(change.Before != nil, qt.Equals, test.before != nil)
				c.Assert(change.After != nil, qt.Equals, test.after != nil)
			}
		})
	}
}

// TestCompareService_LeavesAnUnwritableValueAlone declines a property the read
// found under a value type Ptah cannot write back, in both directions: no add
// for a declaration of it, and no drop where the declaration leaves it out.
func TestCompareService_LeavesAnUnwritableValueAlone(t *testing.T) {
	flag := property("app", "docs", "", "ptah_flag", "on")
	current := schemaext.ObjectState{Coverage: complete(schemaext.Observed,
		schemaext.SubjectCoverage{Kind: mssqlproperty.Kind, Subject: flag.Ref(), Knowledge: mssqlproperty.UnrepresentableValue("int")})}
	for _, desired := range []schemaext.ObjectState{
		{Objects: objects(qt.New(t), declared(flag)), Coverage: complete(schemaext.Desired)},
		{Coverage: complete(schemaext.Desired)},
	} {
		c := qt.New(t)
		c.Assert(compare(c, desired, current).Changes, qt.HasLen, 0)
	}
}

// TestPlanService_OrdersAroundTheOwner plans each change as one statement in
// the dependent window that reads the table or column the property is on,
// which orders it after the owner's creation and before its drop.
func TestPlanService_OrdersAroundTheOwner(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect(platform.SQLServer)
	flag := mssqlproperty.DesiredProperty{Property: property("app", "docs", "title", "ptah_flag", "on")}
	result, err := mssqlproperty.PlanService{}.PlanFeatures(c.Context(), featureplan.Request{
		Target: platform.SQLServer, Identifiers: semantics,
		Changes: []schemaext.ChangeRecord{{Subject: flag.Ref(), Value: &mssqlproperty.Change{After: &flag}}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Contributions, qt.HasLen, 1)
	step := result.Contributions[0].Steps[0]
	c.Assert(step.Payload.Phase, qt.Equals, featureplan.PhaseDependent)
	c.Assert(step.Effects, qt.DeepEquals, []plangraph.Effect{
		{Subject: flag.Ref(), Action: plangraph.Create},
		{Subject: objectidentity.NewBuilder(semantics).ColumnParts("app", "docs", "title"), Action: plangraph.Read},
	})
}
