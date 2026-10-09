package ydbreverse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/dialect/ydb/ydbsecret"
)

func secretReverseRequest(changes ...schemaext.ChangeRecord) schemaext.ReversalRequest {
	return schemaext.ReversalRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(), Changes: changes}
}

// TestSecretReversal_HappyPath drops what the change created, creates again
// what it dropped with the default variable for its path, and keeps a
// rotated value, reporting each value the rollback cannot restore.
func TestSecretReversal_HappyPath(t *testing.T) {
	ref := ydbsecret.Ref("ext", "pg.pw")
	tests := []struct {
		name        string
		change      *ydbdiff.Secret
		want        *ydbdiff.Secret
		forward     schemaext.Value
		limitations []string
	}{
		{name: "a creation is dropped", change: &ydbdiff.Secret{After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PG"}},
			want: &ydbdiff.Secret{Before: &ydbsecret.Observed{}}, forward: &ydbsecret.Observed{}},
		{name: "a drop is created again", change: &ydbdiff.Secret{Before: &ydbsecret.Observed{}},
			want:        &ydbdiff.Secret{After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_EXT_PG_PW"}},
			limitations: []string{"the dropped value of secret ext/pg.pw was never read; the rollback takes the value PTAH_SECRET_EXT_PG_PW holds when it runs"}},
		{name: "a rotation keeps the new value",
			change:      &ydbdiff.Secret{Before: &ydbsecret.Observed{}, After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PG", Rotate: true}},
			want:        &ydbdiff.Secret{Before: &ydbsecret.Observed{}, After: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PG"}},
			forward:     &ydbsecret.Observed{},
			limitations: []string{"the value secret ext/pg.pw held before the rotation was never read; the rollback keeps the rotated value"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreverse.SecretService{}).ReverseChanges(t.Context(), secretReverseRequest(schemaext.ChangeRecord{Subject: ref, Value: test.change}))
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.HasLen, 1)
			c.Assert(result[0].Change, qt.DeepEquals, schemaext.ChangeRecord{Subject: ref, Value: test.want})
			c.Assert(result[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: schemaext.ObjectPlacement, Kind: ydbsecret.Kind, Value: test.forward}})
			c.Assert(result[0].Limitations, qt.DeepEquals, test.limitations)
		})
	}
}

// TestSecretReversal_FailurePath refuses a target without the secrets
// capability and a change the owner does not recognize, with no partial
// result.
func TestSecretReversal_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ReversalRequest
	}{
		{name: "a line without secrets", request: schemaext.ReversalRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB251(),
			Changes: []schemaext.ChangeRecord{{Subject: ydbsecret.Ref("", "pw"), Value: &ydbdiff.Secret{Before: &ydbsecret.Observed{}}}}}},
		{name: "a change of another family", request: secretReverseRequest(schemaext.ChangeRecord{Subject: ydbsecret.Ref("", "pw"), Value: &ydbdiff.StreamingQuery{}})},
		{name: "an empty change", request: secretReverseRequest(schemaext.ChangeRecord{Subject: ydbsecret.Ref("", "pw"), Value: &ydbdiff.Secret{}})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreverse.SecretService{}).ReverseChanges(t.Context(), test.request)
			c.Assert(err, qt.IsNotNil)
			c.Assert(result, qt.IsNil)
		})
	}
}
