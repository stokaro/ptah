package ydbplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine"
)

func secretPlanningRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	codecs := append(ydbsecret.Codecs(), ydbdiff.Codecs()...)
	codecs = append(codecs, ydbast.SecretCodec())
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: codecs,
		Declarations: []engine.DeclarationPlanning{{Target: "ydb", Kinds: []schemaext.Kind{ydbsecret.Kind}, OperationKinds: []schemaext.Kind{ydbast.SecretKind}, Service: ydbplan.SecretService{}}},
		Planning:     []engine.Planning{{Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.SecretKind}, OperationKinds: []schemaext.Kind{ydbast.SecretKind}, Service: ydbplan.SecretService{}}}})
	c.Assert(err, qt.IsNil)
	return runtime
}

// hostChain is a host's common statements, each after the one before it, and
// the metadata a feature owner plans against. The YDB migration host writes
// under the owner's own identity, which is what lets either side write a
// scheme path the other hands over.
type hostChain struct {
	contribution plangraph.Contribution[featureplan.Operation]
	steps        []featureplan.CommonStep
}

func commonChain(effects ...[]plangraph.Effect) hostChain {
	var chain hostChain
	chain.contribution.Owner = "ptah.run/ydb"
	for i, effect := range effects {
		id := plangraph.StepID{Owner: "ptah.run/ydb", Name: string(rune('a' + i))}
		chain.contribution.Steps = append(chain.contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id, Effects: effect})
		if i > 0 {
			chain.contribution.Dependencies = append(chain.contribution.Dependencies, plangraph.Dependency{Before: chain.steps[i-1].ID, After: id})
		}
		chain.steps = append(chain.steps, featureplan.CommonStep{ID: id, Effects: effect})
	}
	return chain
}

func scheduledNames(c *qt.C, chain hostChain, result featureplan.Result) []string {
	c.Helper()
	plan, err := plangraph.Schedule(c.Context(), append([]plangraph.Contribution[featureplan.Operation]{chain.contribution}, result.Contributions...)...)
	c.Assert(err, qt.IsNil)
	var names []string
	for _, step := range plan.Steps {
		if operation, ok := step.Payload.Payload.(*ydbast.Secret); ok {
			names = append(names, string(operation.Operation)+" "+operation.Path())
			continue
		}
		names = append(names, step.ID.Name)
	}
	return names
}

// TestSecretPlan_LowersEachChange writes one statement per created, rotated
// or dropped secret, with the declared variable or the default one for the
// secret's path.
func TestSecretPlan_LowersEachChange(t *testing.T) {
	c := qt.New(t)
	request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		Changes: []schemaext.ChangeRecord{
			{Subject: ydbsecret.Ref("ext", "created"), Value: &ydbdiff.Secret{After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_CREATED"}}},
			{Subject: ydbsecret.Ref("", "restored.pw"), Value: &ydbdiff.Secret{After: &ydbsecret.Desired{}}},
			{Subject: ydbsecret.Ref("ext", "rotated"), Value: &ydbdiff.Secret{Before: &ydbsecret.Observed{}, After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_ROTATED"}}},
			{Subject: ydbsecret.Ref("ext", "dropped"), Value: &ydbdiff.Secret{Before: &ydbsecret.Observed{}}},
		}}

	result, err := secretPlanningRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	plan, err := plangraph.Schedule(t.Context(), result.Contributions...)
	c.Assert(err, qt.IsNil)
	var operations []*ydbast.Secret
	for _, step := range plan.Steps {
		operation := step.Payload.Payload.(*ydbast.Secret)
		operations = append(operations, operation)
		c.Assert(step.Transaction, qt.Equals, plangraph.TransactionForbidden)
		c.Assert(step.Effects[0].Subject, qt.DeepEquals, operation.Subject())
		c.Assert(step.Effects[1].Subject, qt.DeepEquals, ydbscheme.Path(operation.Schema, operation.Name))
		c.Assert(step.Impact, qt.DeepEquals, operation.Effect())
	}
	c.Assert(operations, qt.DeepEquals, []*ydbast.Secret{
		{Operation: ydbast.SecretCreate, Name: "restored.pw", ValueEnv: "PTAH_SECRET_RESTORED_PW"},
		{Operation: ydbast.SecretCreate, Schema: "ext", Name: "created", ValueEnv: "PTAH_SECRET_CREATED"},
		{Operation: ydbast.SecretDrop, Schema: "ext", Name: "dropped"},
		{Operation: ydbast.SecretRotate, Schema: "ext", Name: "rotated", ValueEnv: "PTAH_SECRET_ROTATED"},
	})
}

// TestSecretPlan_PlacesEachStatement orders a secret against the host's
// statements: first, unless a drop frees its path, and before everything that
// reads it by path.
func TestSecretPlan_PlacesEachStatement(t *testing.T) {
	slot := ydbscheme.Path("ext", "pw")
	reads := []plangraph.Effect{{Subject: ydbsecret.Ref("ext", "pw"), Action: plangraph.Read}}
	tests := []struct {
		name    string
		change  *ydbdiff.Secret
		effects [][]plangraph.Effect
		want    []string
	}{
		{name: "a creation runs first and before its reader",
			change:  &ydbdiff.Secret{After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW"}},
			effects: [][]plangraph.Effect{nil, nil, reads},
			want:    []string{"create ext/pw", "a", "b", "c"}},
		{name: "a creation follows the drop that frees its path",
			change:  &ydbdiff.Secret{After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW"}},
			effects: [][]plangraph.Effect{nil, {{Subject: slot, Action: plangraph.Drop}}, nil, reads},
			want:    []string{"a", "b", "create ext/pw", "c", "d"}},
		{name: "a drop runs before a creation at its path",
			change:  &ydbdiff.Secret{Before: &ydbsecret.Observed{}},
			effects: [][]plangraph.Effect{nil, {{Subject: slot, Action: plangraph.Create}}},
			want:    []string{"drop ext/pw", "a", "b"}},
		{name: "a rotation runs before its reader",
			change:  &ydbdiff.Secret{Before: &ydbsecret.Observed{}, After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW"}},
			effects: [][]plangraph.Effect{nil, reads},
			want:    []string{"rotate ext/pw", "a", "b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			chain := commonChain(test.effects...)
			request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
				CommonSteps: chain.steps, Changes: []schemaext.ChangeRecord{{Subject: ydbsecret.Ref("ext", "pw"), Value: test.change}}}
			result, err := secretPlanningRuntime(c).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			c.Assert(scheduledNames(c, chain, result), qt.DeepEquals, test.want)
		})
	}
}

// TestSecretPlan_RefusesTheWholeBatch returns no statement when one change is
// refused: a target without the secrets capability, a drop a statement of the
// plan still reads, and a creation at a path another object keeps.
func TestSecretPlan_RefusesTheWholeBatch(t *testing.T) {
	slot := ydbscheme.Path("ext", "pw")
	tests := []struct {
		name    string
		caps    capability.Capabilities
		change  *ydbdiff.Secret
		effects [][]plangraph.Effect
		code    schemavalidation.Code
		wantErr string
	}{
		{name: "a line without secrets", caps: capability.YDB251(), change: &ydbdiff.Secret{After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW"}},
			code: schemavalidation.UnsupportedFeature, wantErr: "secret first, which requires target capability secrets, unavailable on this ydb target"},
		{name: "a drop the plan still reads", caps: capability.YDB262(), change: &ydbdiff.Secret{Before: &ydbsecret.Observed{}},
			effects: [][]plangraph.Effect{{{Subject: ydbsecret.Ref("ext", "pw"), Action: plangraph.Read}}},
			code:    schemavalidation.InvalidSchema, wantErr: ".*secret ext/pw is dropped while a statement of this plan reads it by its path"},
		{name: "a path a table takes", caps: capability.YDB262(), change: &ydbdiff.Secret{After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW"}},
			effects: [][]plangraph.Effect{{{Subject: slot, Action: plangraph.Create}}},
			code:    schemavalidation.InvalidSchema, wantErr: ".*secret create conflicts with create at scheme path.*"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: test.caps,
				CommonSteps: commonChain(test.effects...).steps, Changes: []schemaext.ChangeRecord{
					{Subject: ydbsecret.Ref("", "first"), Value: &ydbdiff.Secret{After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_FIRST"}}},
					{Subject: ydbsecret.Ref("ext", "pw"), Value: test.change},
				}}
			result, err := secretPlanningRuntime(c).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Code, qt.Equals, test.code)
			c.Assert(result.Err(request), qt.ErrorMatches, test.wantErr)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// TestSecretDeclarations_CreateEachDeclaredSecret derives one CREATE SECRET
// per declared secret, before the statements that read it.
func TestSecretDeclarations_CreateEachDeclaredSecret(t *testing.T) {
	c := qt.New(t)
	chain := commonChain(nil, []plangraph.Effect{{Subject: ydbsecret.Ref("ext", "pw"), Action: plangraph.Read}})
	request := featureplan.DeclarationRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		CommonSteps: chain.steps, Objects: []schemaext.Object{
			{Ref: ydbsecret.Ref("ext", "pw"), Value: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW"}},
		}}

	result, err := secretPlanningRuntime(c).PlanDeclarations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Declarations, qt.HasLen, 1)
	c.Assert(result.Declarations[0].Subject, qt.DeepEquals, ydbsecret.Ref("ext", "pw"))
	c.Assert(scheduledNames(c, chain, featureplan.Result{Contributions: result.Contributions}), qt.DeepEquals, []string{"create ext/pw", "a", "b"})
}

// TestSecretDeclarations_RefusesAPathATableTakes refuses a declared secret
// whose path a declared table holds, since YDB keeps one object at a path
// (measured on 26.2.1.14: `unexpected path type ... EPathTypeTable`).
func TestSecretDeclarations_RefusesAPathATableTakes(t *testing.T) {
	c := qt.New(t)
	table := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("ext", "pw")
	chain := commonChain([]plangraph.Effect{{Subject: ydbscheme.Path("ext", "pw"), Action: plangraph.Create}, {Subject: table, Action: plangraph.Create}})
	request := featureplan.DeclarationRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		CommonSteps: chain.steps, Objects: []schemaext.Object{ydbsecret.DesiredObject("ext", "pw", "", "PTAH_SECRET_PW")}}

	result, err := secretPlanningRuntime(c).PlanDeclarations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Problem.Kind, qt.Equals, string(ydbsecret.Kind))
	c.Assert(result.Err(request), qt.ErrorMatches, ".*secret create conflicts with create at scheme path.*")
	c.Assert(result.Contributions, qt.HasLen, 0)
}

// TestSecretPlan_ReadersFollowTheSecretOnAnyHost orders every statement that
// reads a secret after the secret's creation even when the host leaves its
// statements unordered, so the order does not rest on the host chaining them.
func TestSecretPlan_ReadersFollowTheSecretOnAnyHost(t *testing.T) {
	c := qt.New(t)
	reads := []plangraph.Effect{{Subject: ydbsecret.Ref("ext", "pw"), Action: plangraph.Read}}
	chain := commonChain(nil, reads, reads)
	chain.contribution.Dependencies = nil
	request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		CommonSteps: chain.steps, Changes: []schemaext.ChangeRecord{
			{Subject: ydbsecret.Ref("ext", "pw"), Value: &ydbdiff.Secret{After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW"}}},
		}}

	result, err := secretPlanningRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(scheduledNames(c, chain, result), qt.DeepEquals, []string{"create ext/pw", "a", "b", "c"})
}
