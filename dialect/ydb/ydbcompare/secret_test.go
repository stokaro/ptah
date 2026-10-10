package ydbcompare_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine"
)

func secretRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}},
		Codecs: append(ydbsecret.Codecs(), ydbdiff.SecretCodec()),
		Comparisons: []engine.ObjectComparison{{Target: "ydb", Kinds: []schemaext.Kind{ydbsecret.Kind},
			ChangeKinds: []schemaext.Kind{ydbdiff.SecretKind}, Actions: []string{ydbsecret.RotateAction}, Service: ydbcompare.SecretService{}}},
	})
	c.Assert(err, qt.IsNil)
	return runtime
}

func secretState(c *qt.C, direction schemaext.Representation, knowledge schemaext.KnowledgeState, value schemaext.Value, limits ...schemaext.SubjectCoverage) schemaext.ObjectState {
	c.Helper()
	coverage, err := ydbsecret.Coverage(direction, schemaext.Knowledge{State: knowledge, Reason: "namespace evidence"}, limits)
	c.Assert(err, qt.IsNil)
	state := schemaext.ObjectState{Coverage: coverage}
	if value != nil {
		state.Objects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbsecret.Ref("ext", "pw"), Value: value}))
	}
	return state
}

func secretRequest(c *qt.C, desired, current schemaext.ObjectState) schemaext.ObjectComparisonRequest {
	c.Helper()
	return schemaext.ObjectComparisonRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
		Capabilities: capability.YDB262(), Kinds: []schemaext.Kind{ydbsecret.Kind}, Desired: desired, Current: current}
}

// dottedSecretLimit is a source that describes every secret but the root
// secret ext.pw.
func dottedSecretLimit(c *qt.C) schemaext.ObjectState {
	c.Helper()
	return secretState(c, schemaext.Desired, schemaext.Complete, nil, schemaext.SubjectCoverage{Kind: ydbsecret.Kind,
		Subject: ydbsecret.Ref("", "ext.pw"), Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not described"}})
}

// TestSecretComparison_FailurePath_ADottedLimitBesideItsDirectoryForm refuses
// a plan that drops the secret ext/pw when the source's limit is "ext.pw" and
// the database holds no root secret ext.pw: the source most likely means the
// secret the plan would drop, and its value cannot be read back.
func TestSecretComparison_FailurePath_ADottedLimitBesideItsDirectoryForm(t *testing.T) {
	c := qt.New(t)
	request := secretRequest(c, dottedSecretLimit(c), secretState(c, schemaext.Observed, schemaext.Complete, &ydbsecret.Observed{}))

	result, err := secretRuntime(c).CompareObjects(t.Context(), request)

	c.Assert(err, qt.ErrorMatches, `.*the secret limit "ext\.pw" names ext\.pw at the database root, which the database does not hold, `+
		`while this plan would drop ext/pw, which the same name written with a slash names\. Write the limit as "ext/pw" to keep that secret.*`)
	c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
}

// TestSecretComparison_ADottedLimitNamingAHeldRootSecret reads a dotted limit
// as written when the database holds the root secret it names: that secret
// is kept, and ext/pw, which the source does not describe, is dropped.
func TestSecretComparison_ADottedLimitNamingAHeldRootSecret(t *testing.T) {
	c := qt.New(t)
	current := secretState(c, schemaext.Observed, schemaext.Complete, &ydbsecret.Observed{})
	current.Objects = must.Must(current.Objects.With(schemaext.Object{Ref: ydbsecret.Ref("", "ext.pw"), Value: &ydbsecret.Observed{}}))

	result, err := secretRuntime(c).CompareObjects(t.Context(), secretRequest(c, dottedSecretLimit(c), current))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.DeepEquals, []schemaext.ChangeRecord{{Subject: ydbsecret.Ref("ext", "pw"), Value: &ydbdiff.Secret{Before: &ydbsecret.Observed{}}}})
}

// TestSecretComparison_ADottedLimitBesideADeclaredSecret creates the declared
// secret ext/pw beside the limit "ext.pw": only a planned drop beside a
// dotted limit is refused.
func TestSecretComparison_ADottedLimitBesideADeclaredSecret(t *testing.T) {
	c := qt.New(t)
	declared := &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_EXT_PW"}
	desired := dottedSecretLimit(c)
	desired.Objects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbsecret.Ref("ext", "pw"), Value: declared}))

	result, err := secretRuntime(c).CompareObjects(t.Context(), secretRequest(c, desired, secretState(c, schemaext.Observed, schemaext.Complete, nil)))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.DeepEquals, []schemaext.ChangeRecord{{Subject: ydbsecret.Ref("ext", "pw"), Value: &ydbdiff.Secret{After: declared}}})
}

// TestSecretComparison_ComparesByPresenceAndEvidence compares a secret by its
// path alone. A changed value cannot be observed, so a secret both sides hold
// is equal unless the caller requests a rotation; a creation is planned only
// where the read established absence, and a removal only where the source
// describes secrets. An unread secret is reported only to a source that makes
// a claim about it.
func TestSecretComparison_ComparesByPresenceAndEvidence(t *testing.T) {
	declared := &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW"}
	observed := &ydbsecret.Observed{}
	limit := []schemaext.SubjectCoverage{{Kind: ydbsecret.Kind, Subject: ydbsecret.Ref("ext", "pw"),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "target capability secrets is unavailable"}}}
	tests := []struct {
		name                               string
		desired, current                   schemaext.Value
		desiredKnowledge, currentKnowledge schemaext.KnowledgeState
		currentLimits                      []schemaext.SubjectCoverage
		rotate                             []string
		want                               []schemaext.ChangeRecord
		undecided, objects                 int
	}{
		{name: "create", desired: declared, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, objects: 1,
			want: []schemaext.ChangeRecord{{Subject: ydbsecret.Ref("ext", "pw"), Value: &ydbdiff.Secret{After: declared}}}},
		{name: "a rotation of an absent secret is a creation", desired: declared, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, objects: 1,
			rotate: []string{"ext/pw"}, want: []schemaext.ChangeRecord{{Subject: ydbsecret.Ref("ext", "pw"), Value: &ydbdiff.Secret{After: declared}}}},
		{name: "drop", current: observed, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete,
			want: []schemaext.ChangeRecord{{Subject: ydbsecret.Ref("ext", "pw"), Value: &ydbdiff.Secret{Before: observed}}}},
		{name: "equal by presence", desired: declared, current: observed, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, objects: 1},
		{name: "rotate only when asked", desired: declared, current: observed, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, objects: 1,
			rotate: []string{"ext/pw"}, want: []schemaext.ChangeRecord{{Subject: ydbsecret.Ref("ext", "pw"), Value: &ydbdiff.Secret{Before: observed, After: declared}}}},
		{name: "an unread secret is not rotated", desired: declared, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete,
			currentLimits: limit, rotate: []string{"ext/pw"}, objects: 1, undecided: 1},
		{name: "a source that cannot describe secrets keeps the held one", current: observed, desiredKnowledge: schemaext.Uninspected, currentKnowledge: schemaext.Complete, objects: 1},
		{name: "an unread namespace withholds the creation", desired: declared, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Uninspected, objects: 1, undecided: 2},
		{name: "an unread secret withholds its removal", desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, currentLimits: limit, undecided: 1},
		{name: "an unread secret is nothing to a source without a claim", desiredKnowledge: schemaext.Uninspected, currentKnowledge: schemaext.Complete, currentLimits: limit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := secretRequest(c,
				secretState(c, schemaext.Desired, test.desiredKnowledge, test.desired),
				secretState(c, schemaext.Observed, test.currentKnowledge, test.current, test.currentLimits...))
			request.Requests = must.Must(ydbsecret.RotationRequests(test.rotate))
			result, err := secretRuntime(c).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.DeepEquals, test.want)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Desired.Objects.Len(), qt.Equals, test.objects)
		})
	}
}

// TestSecretComparison_KeepsTheHeldSecretAsADeclaration adopts a secret the
// source does not describe as a declaration that names no variable, which
// selects the default one for its path.
func TestSecretComparison_KeepsTheHeldSecretAsADeclaration(t *testing.T) {
	c := qt.New(t)
	request := secretRequest(c, secretState(c, schemaext.Desired, schemaext.Uninspected, nil),
		secretState(c, schemaext.Observed, schemaext.Complete, &ydbsecret.Observed{}))
	result, err := secretRuntime(c).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	values, err := result.Desired.Objects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 1)
	c.Assert(values[0].Value, qt.DeepEquals, &ydbsecret.Desired{})
	c.Assert(result.Desired.Coverage.Lookup(ydbsecret.Kind, values[0].Ref).State, qt.Equals, schemaext.Complete)
}

// TestSecretComparison_PlansNothingOnALineWithoutSecrets compares secrets on
// a target without the secrets capability, and refuses nothing, when no
// statement would run: a secret both sides hold, one a source without a claim
// keeps, and an unread one, whether or not the source claims it.
func TestSecretComparison_PlansNothingOnALineWithoutSecrets(t *testing.T) {
	declared := &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW"}
	limit := []schemaext.SubjectCoverage{{Kind: ydbsecret.Kind, Subject: ydbsecret.Ref("ext", "pw"),
		Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "target capability secrets is unavailable"}}}
	tests := []struct {
		name             string
		desiredKnowledge schemaext.KnowledgeState
		desired, current schemaext.Value
		currentLimits    []schemaext.SubjectCoverage
		undecided        int
	}{
		{name: "equal by presence", desiredKnowledge: schemaext.Complete, desired: declared, current: &ydbsecret.Observed{}},
		{name: "kept by a source without a claim", desiredKnowledge: schemaext.Uninspected, current: &ydbsecret.Observed{}},
		{name: "unread, without a claim", desiredKnowledge: schemaext.Uninspected, currentLimits: limit},
		{name: "unread, with a claim", desiredKnowledge: schemaext.Complete, currentLimits: limit, undecided: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := secretRequest(c,
				secretState(c, schemaext.Desired, test.desiredKnowledge, test.desired),
				secretState(c, schemaext.Observed, schemaext.Complete, test.current, test.currentLimits...))
			request.Capabilities = capability.YDB251()
			result, err := secretRuntime(c).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
		})
	}
}

// TestSecretComparison_FailurePath refuses a request it cannot answer
// without returning part of an answer: a target without the secrets
// capability, named by the secret's path, another target, a rotation of a
// secret the source does not declare, a request the owner does not know, and
// a canceled context.
func TestSecretComparison_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		caps     capability.Capabilities
		requests []schemaext.ChangeRequest
		wantErr  string
		wantIs   error
	}{
		{name: "a line without secrets", target: "ydb", caps: capability.YDB251(),
			wantErr: "secret ext/pw, which requires target capability secrets, unavailable on this ydb target", wantIs: ptaherr.ErrUnsupportedFeature},
		{name: "another target", target: "postgres", caps: capability.YDB262(),
			wantErr: `.*YDB comparison on "postgres"`, wantIs: ptaherr.ErrUnsupportedDialect},
		{name: "a rotation of an undeclared secret", target: "ydb", caps: capability.YDB262(),
			requests: []schemaext.ChangeRequest{{Subject: ydbsecret.Ref("", "ext.pw"), Action: ydbsecret.RotateAction}},
			wantErr:  `rotate secret "ext.pw": the desired schema declares no such secret`, wantIs: ydbsecret.ErrRotateUndeclared},
		{name: "an unknown request", target: "ydb", caps: capability.YDB262(),
			requests: []schemaext.ChangeRequest{{Subject: ydbsecret.Ref("ext", "pw"), Action: "reset"}},
			wantErr:  `.*a secret accepts no "reset" request`, wantIs: schemaext.ErrInvalidValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := secretRequest(c, secretState(c, schemaext.Desired, schemaext.Complete, &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW"}),
				secretState(c, schemaext.Observed, schemaext.Complete, nil))
			request.Target, request.Capabilities, request.Requests = test.target, test.caps, test.requests
			result, err := (ydbcompare.SecretService{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
		})
	}
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (ydbcompare.SecretService{}).CompareObjects(ctx, schemaext.ObjectComparisonRequest{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
}
