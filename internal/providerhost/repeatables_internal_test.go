package providerhost

// White-box testing required: negotiation, the transaction binding of each
// write and the wire conversion must be checked without starting a provider
// or trusting a remote receipt.

import (
	"errors"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

func repeatableSession(processFeatures, engineFeatures []string) *session {
	return &session{
		target: &target{
			engine: &protocol.Engine{Id: "fixture", Features: engineFeatures},
			host:   &Host{description: &protocol.Description{ProviderId: "example.org/fixture", Features: processFeatures}},
		},
		remote: &protocol.Session{Id: "session"},
	}
}

func TestRepeatableStore_HappyPath(t *testing.T) {
	t.Run("the session scope carries no transaction", func(t *testing.T) {
		c := qt.New(t)
		owner := repeatableSession([]string{provider.RepeatableRecordFeature}, []string{provider.RepeatableRecordFeature})
		store, err := owner.RepeatableStore()
		c.Assert(err, qt.IsNil)
		scope, err := store.(repeatableStore).scope(dbschema.MigrationMetadataTable{Schema: "main", Name: "_revisions_repeatable"})
		c.Assert(err, qt.IsNil)
		c.Assert(scope.GetSessionId(), qt.Equals, "session")
		c.Assert(scope.GetTable(), qt.Equals, "_revisions_repeatable")
		c.Assert(scope.GetTransactionId(), qt.Equals, "")
	})
	t.Run("a completion carries the transaction", func(t *testing.T) {
		c := qt.New(t)
		owner := repeatableSession([]string{provider.RepeatableRecordFeature}, []string{provider.RepeatableRecordFeature})
		store, err := (&transaction{session: owner, id: "transaction"}).RepeatableStore()
		c.Assert(err, qt.IsNil)
		scope, err := store.(repeatableStore).completionScope(dbschema.MigrationMetadataTable{Schema: "main", Name: "_revisions_repeatable"})
		c.Assert(err, qt.IsNil)
		c.Assert(scope.GetTransactionId(), qt.Equals, "transaction")
		c.Assert(store.MaxBodyBytes(), qt.Equals, provider.MaxRepeatableBodyBytes)
	})
}

func TestRepeatableStore_FailurePath(t *testing.T) {
	refusal := `unsupported feature: provider example.org/fixture keeps no record of repeatable migrations for engine fixture, ` +
		`because it does not list sessions.repeatable-record \(contract 25.44\); install a provider release that lists it`
	for _, test := range []struct {
		name                            string
		processFeatures, engineFeatures []string
	}{
		{"the process does not list the feature", nil, []string{provider.RepeatableRecordFeature}},
		{"the engine does not list the feature", []string{provider.RepeatableRecordFeature}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := repeatableSession(test.processFeatures, test.engineFeatures)
			store, err := owner.RepeatableStore()
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, refusal)
			c.Assert(store, qt.IsNil)
			bound, err := (&transaction{session: owner, id: "transaction"}).RepeatableStore()
			c.Assert(err, qt.ErrorMatches, refusal)
			c.Assert(bound, qt.IsNil)
		})
	}
	t.Run("a transaction carries only a completion", func(t *testing.T) {
		c := qt.New(t)
		owner := repeatableSession([]string{provider.RepeatableRecordFeature}, []string{provider.RepeatableRecordFeature})
		store, err := (&transaction{session: owner, id: "transaction"}).RepeatableStore()
		c.Assert(err, qt.IsNil)
		scope, err := store.(repeatableStore).scope(dbschema.MigrationMetadataTable{Schema: "main", Name: "_revisions_repeatable"})
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(scope, qt.IsNil)
	})
	t.Run("a dry run writes nothing", func(t *testing.T) {
		c := qt.New(t)
		owner := repeatableSession([]string{provider.RepeatableRecordFeature}, []string{provider.RepeatableRecordFeature})
		owner.SetDryRun(true)
		store, err := owner.RepeatableStore()
		c.Assert(err, qt.IsNil)
		table := dbschema.MigrationMetadataTable{Schema: "main", Name: "_revisions_repeatable"}
		c.Assert(store.Prepare(c.Context(), table), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		removed, err := store.Remove(c.Context(), table, "view")
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(removed, qt.IsFalse)
	})
}

func TestRepeatableWriteError(t *testing.T) {
	t.Run("a conflict names its holder", func(t *testing.T) {
		c := qt.New(t)
		err := repeatableWriteError(&provider.RepeatableError{Outcome: protocol.RepeatableResult_CONFLICT, Holder: "run-a"})
		conflict, ok := errors.AsType[*dbschema.RepeatableConflictError](err)
		c.Assert(ok, qt.IsTrue)
		c.Assert(conflict.Holder, qt.Equals, "run-a")
	})
	t.Run("a refusal is not a conflict", func(t *testing.T) {
		c := qt.New(t)
		err := repeatableWriteError(&provider.RepeatableError{Outcome: protocol.RepeatableResult_REFUSED})
		_, ok := errors.AsType[*dbschema.RepeatableConflictError](err)
		c.Assert(ok, qt.IsFalse)
		c.Assert(err, qt.ErrorMatches, "provider repeatable migration record outcome is REFUSED")
	})
}

func TestRepeatableErrorText(t *testing.T) {
	long := strings.Repeat("a", provider.MaxRepeatableErrorBytes-1) + "é"
	for _, test := range []struct{ name, text, want string }{
		{"kept as written", "syntax error", "syntax error"},
		{"empty", "", "the repeatable migration failed"},
		{"NUL and invalid UTF-8", "bad\x00\xffbyte", "bad��byte"},
		{"cut at a character boundary", long, strings.Repeat("a", provider.MaxRepeatableErrorBytes-1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(repeatableErrorText(test.text), qt.Equals, test.want)
		})
	}
}

func TestRepeatableRecordRow_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		row  *protocol.RepeatableRow
		want dbschema.RepeatableRow
	}{
		{
			name: "an applied row keeps its time",
			row: &protocol.RepeatableRow{
				Identity: "view", State: protocol.RepeatableRow_APPLIED, Checksum: "h1:a", AttemptChecksum: "h1:a", AttemptOrder: 2,
				Transactional: true, RunId: "run", AttemptedAtSeconds: 1700000000, AttemptedAtNanos: 5, Applied: true,
				AppliedAtSeconds: 1700000001, BodyKept: true,
			},
			want: dbschema.RepeatableRow{
				Identity: "view", State: dbschema.RepeatableApplied, Checksum: "h1:a", AttemptChecksum: "h1:a", AttemptOrder: 2,
				Transactional: true, RunID: "run", AttemptedAt: time.Unix(1700000000, 5).UTC(), AppliedAt: time.Unix(1700000001, 0).UTC(),
				BodyKept: true,
			},
		},
		{
			name: "a failed row that never applied has no applied time",
			row: &protocol.RepeatableRow{
				Identity: "view", State: protocol.RepeatableRow_FAILED, AttemptChecksum: "h1:b", AttemptOrder: 1, RunId: "run",
				AttemptedAtSeconds: 1700000000, AppliedAtSeconds: 1700000001, Error: "syntax error",
			},
			want: dbschema.RepeatableRow{
				Identity: "view", State: dbschema.RepeatableFailed, AttemptChecksum: "h1:b", AttemptOrder: 1, RunID: "run",
				AttemptedAt: time.Unix(1700000000, 0).UTC(), Error: "syntax error",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := repeatableRecordRow(test.row)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestRepeatableRecordRow_FailurePath(t *testing.T) {
	c := qt.New(t)
	got, err := repeatableRecordRow(&protocol.RepeatableRow{Identity: "view", State: protocol.RepeatableRow_STATE_UNSPECIFIED})
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(got, qt.DeepEquals, dbschema.RepeatableRow{})
}
