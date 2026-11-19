package provider_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// recordDescription declares the provider features and the engine features.
func recordDescription(providerFeatures, engineFeatures []string) *protocol.Description {
	return &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1",
		Features: slices.Clone(providerFeatures),
		Engines: []*protocol.Engine{{Id: "test", Features: slices.Clone(engineFeatures), Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT,
			ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}}
}

func recordScope() *protocol.RepeatableScope {
	return &protocol.RepeatableScope{SessionId: "session", Table: "schema_migrations_repeatable"}
}

func appliedRow() *protocol.RepeatableRow {
	return &protocol.RepeatableRow{Identity: "views", State: protocol.RepeatableRow_APPLIED, Checksum: "h1:new", AttemptChecksum: "h1:new",
		AttemptOrder: 1, Transactional: true, RunId: "run-1", AttemptedAtSeconds: 1_700_000_000, Applied: true,
		AppliedAtSeconds: 1_700_000_001, BodyKept: true}
}

func neverAppliedRow() *protocol.RepeatableRow {
	return &protocol.RepeatableRow{Identity: "views", State: protocol.RepeatableRow_FAILED, AttemptChecksum: "h1:new", AttemptOrder: 2,
		RunId: "run-1", AttemptedAtSeconds: 1_700_000_000, Error: "syntax error"}
}

// A provider and an engine that list the record beside their history are a
// valid description, and so is an engine that keeps no record beside a
// provider that does.
func TestRepeatableRecordFeatureDescription_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name             string
		providerFeatures []string
		engineFeatures   []string
	}{
		{"provider and engine", []string{provider.HistoryFeature, provider.RepeatableRecordFeature},
			[]string{provider.HistoryFeature, provider.RepeatableRecordFeature}},
		{"an engine that keeps no record", []string{provider.HistoryFeature, provider.RepeatableRecordFeature}, []string{provider.HistoryFeature}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateDescription(recordDescription(test.providerFeatures, test.engineFeatures), nil), qt.IsNil)
		})
	}
}

// The record sits beside the revision table, so neither side may list it
// without the history service, and an engine may not list it without the
// provider.
func TestRepeatableRecordFeatureDescription_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name             string
		providerFeatures []string
		engineFeatures   []string
		want             string
	}{
		{"a provider without history", []string{provider.RepeatableRecordFeature}, nil,
			`.*sessions\.repeatable-record requires the provider's history service`},
		{"an engine without the provider's", []string{provider.HistoryFeature}, []string{provider.HistoryFeature, provider.RepeatableRecordFeature},
			`.*engine sessions\.repeatable-record requires the provider's`},
		{"an engine without its history", []string{provider.HistoryFeature, provider.RepeatableRecordFeature}, []string{provider.RepeatableRecordFeature},
			`.*engine sessions\.repeatable-record requires the engine's history`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.ValidateDescription(recordDescription(test.providerFeatures, test.engineFeatures), nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// Every write the migrator sends is valid: a claim that inserts, a claim that
// expects the run it read, a completion inside the body's transaction, an
// empty body, a failure and a removal.
func TestValidateMutateRepeatableRequest_HappyPath(t *testing.T) {
	inTransaction := recordScope()
	inTransaction.TransactionId = "tx-1"
	for _, test := range []struct {
		name    string
		request *protocol.MutateRepeatableRequest
	}{
		{"an inserting claim", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Claim{
			Claim: &protocol.RepeatableClaim{Identity: "views", Insert: true, RunId: "run-2", AttemptChecksum: "h1:new", AttemptOrder: 1, AttemptedAtSeconds: 1}}}},
		{"a claim of a row read", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Claim{
			Claim: &protocol.RepeatableClaim{Identity: "views", ExpectedRunId: "run-1", RunId: "run-2", AttemptChecksum: "h1:new", AttemptOrder: 3, AttemptedAtSeconds: 1}}}},
		{"a completion in a transaction", &protocol.MutateRepeatableRequest{Scope: inTransaction, RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Complete{
			Complete: &protocol.RepeatableCompletion{Identity: "views", RunId: "run-2", Checksum: "h1:new", Body: []byte("CREATE VIEW v AS SELECT 1;"), AppliedAtSeconds: 1}}}},
		{"a completion of an empty file", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Complete{
			Complete: &protocol.RepeatableCompletion{Identity: "views", RunId: "run-2", Checksum: "h1:empty", AppliedAtSeconds: 1}}}},
		{"a failure", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Fail{
			Fail: &protocol.RepeatableFailure{Identity: "views", RunId: "run-2", Error: "syntax error"}}}},
		{"a failure at the error bound", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Fail{
			Fail: &protocol.RepeatableFailure{Identity: "views", RunId: "run-2", Error: strings.Repeat("e", provider.MaxRepeatableErrorBytes)}}}},
		{"a removal", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Remove{Remove: &protocol.RepeatableRemoval{Identity: "views"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateMutateRepeatableRequest(test.request), qt.IsNil)
		})
	}
}

// A write is refused before it is sent when it could not be stored as one
// row: no write, a transaction on anything but a completion, a claim that
// both inserts and expects a run, a body over the bound, and missing facts.
func TestValidateMutateRepeatableRequest_FailurePath(t *testing.T) {
	inTransaction := recordScope()
	inTransaction.TransactionId = "tx-1"
	for _, test := range []struct {
		name    string
		request *protocol.MutateRepeatableRequest
		want    string
	}{
		{"no write", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r"}, `.*missing repeatable migration write`},
		{"no request ID", &protocol.MutateRepeatableRequest{Scope: recordScope(), Mutation: &protocol.MutateRepeatableRequest_Remove{Remove: &protocol.RepeatableRemoval{Identity: "views"}}},
			`.*invalid repeatable migration write identity or size`},
		{"no session", &protocol.MutateRepeatableRequest{Scope: &protocol.RepeatableScope{Table: "t"}, RequestId: "r",
			Mutation: &protocol.MutateRepeatableRequest_Remove{Remove: &protocol.RepeatableRemoval{Identity: "views"}}}, `.*requires a session and valid store identifiers`},
		{"a removal in a transaction", &protocol.MutateRepeatableRequest{Scope: inTransaction, RequestId: "r",
			Mutation: &protocol.MutateRepeatableRequest_Remove{Remove: &protocol.RepeatableRemoval{Identity: "views"}}}, `.*only a completion writes the repeatable migration record inside a transaction`},
		{"a claim that inserts and expects a run", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Claim{
			Claim: &protocol.RepeatableClaim{Identity: "views", Insert: true, ExpectedRunId: "run-1", RunId: "run-2", AttemptChecksum: "h1:new", AttemptOrder: 1, AttemptedAtSeconds: 1}}},
			`.*either inserts a row or expects the run it read`},
		{"a claim that neither inserts nor expects a run", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Claim{
			Claim: &protocol.RepeatableClaim{Identity: "views", RunId: "run-2", AttemptChecksum: "h1:new", AttemptOrder: 1, AttemptedAtSeconds: 1}}},
			`.*either inserts a row or expects the run it read`},
		{"a claim without its order", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Claim{
			Claim: &protocol.RepeatableClaim{Identity: "views", Insert: true, RunId: "run-2", AttemptChecksum: "h1:new", AttemptedAtSeconds: 1}}},
			`.*a repeatable migration claim needs its run, digest, order and time`},
		{"a completion without its run", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Complete{
			Complete: &protocol.RepeatableCompletion{Identity: "views", Checksum: "h1:new", AppliedAtSeconds: 1}}},
			`.*a repeatable migration completion needs its run, digest and time`},
		{"a body over the bound", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Complete{
			Complete: &protocol.RepeatableCompletion{Identity: "views", RunId: "run-2", Checksum: "h1:new", Body: make([]byte, provider.MaxRepeatableBodyBytes+1), AppliedAtSeconds: 1}}},
			`.*repeatable migration body exceeds 2097152 bytes`},
		{"a failure without its error", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Fail{
			Fail: &protocol.RepeatableFailure{Identity: "views", RunId: "run-2"}}}, `.*a repeatable migration failure needs its run and error`},
		{"a failure over the error bound", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Fail{
			Fail: &protocol.RepeatableFailure{Identity: "views", RunId: "run-2", Error: strings.Repeat("e", provider.MaxRepeatableErrorBytes+1)}}},
			`.*repeatable migration error exceeds 4096 bytes`},
		{"a claim of a run over the bound", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Claim{
			Claim: &protocol.RepeatableClaim{Identity: "views", Insert: true, RunId: strings.Repeat("r", provider.MaxRepeatableTokenBytes+1), AttemptChecksum: "h1:new",
				AttemptOrder: 1, AttemptedAtSeconds: 1}}}, `.*a repeatable migration claim needs its run, digest, order and time`},
		{"a claim that expects a run over the bound", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Claim{
			Claim: &protocol.RepeatableClaim{Identity: "views", ExpectedRunId: strings.Repeat("r", provider.MaxRepeatableTokenBytes+1), RunId: "run-2",
				AttemptChecksum: "h1:new", AttemptOrder: 1, AttemptedAtSeconds: 1}}}, `.*either inserts a row or expects the run it read`},
		{"an identity with a NUL byte", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r",
			Mutation: &protocol.MutateRepeatableRequest_Remove{Remove: &protocol.RepeatableRemoval{Identity: "vi\x00ews"}}}, `.*invalid repeatable migration identity`},
		{"an identity over the key", &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r",
			Mutation: &protocol.MutateRepeatableRequest_Remove{Remove: &protocol.RepeatableRemoval{Identity: string(make([]byte, provider.MaxRepeatableIdentityBytes+1))}}}, `.*invalid repeatable migration identity`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.ValidateMutateRepeatableRequest(test.request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// A snapshot of an applied row and a row whose only attempt failed is
// accepted, and so is the answer that the record does not exist.
func TestValidateRepeatableSnapshotReply_HappyPath(t *testing.T) {
	request := &protocol.ReadRepeatablesRequest{Scope: recordScope(), MaxRows: provider.MaxRepeatableRows}
	failed := neverAppliedRow()
	failed.Identity = "grants"
	for _, test := range []struct {
		name     string
		snapshot *protocol.RepeatableSnapshot
	}{
		{"rows", &protocol.RepeatableSnapshot{Scope: recordScope(), Exists: true, Complete: true, Rows: []*protocol.RepeatableRow{appliedRow(), failed}}},
		{"no record", &protocol.RepeatableSnapshot{Scope: recordScope(), Complete: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateReadRepeatablesRequest(request), qt.IsNil)
			c.Assert(provider.ValidateRepeatableSnapshotReply(request, test.snapshot), qt.IsNil)
		})
	}
}

// A snapshot that cannot be read as the whole record is refused with no usable
// rows: an incomplete one, one that repeats an identity, rows from a record
// that does not exist, and a row that never applied but keeps a body.
func TestValidateRepeatableSnapshotReply_FailurePath(t *testing.T) {
	request := &protocol.ReadRepeatablesRequest{Scope: recordScope(), MaxRows: provider.MaxRepeatableRows}
	kept := neverAppliedRow()
	kept.BodyKept = true
	unnamed := appliedRow()
	unnamed.RunId = ""
	verbose := neverAppliedRow()
	verbose.Error = strings.Repeat("e", provider.MaxRepeatableErrorBytes+1)
	digested := appliedRow()
	digested.Checksum = strings.Repeat("h", provider.MaxRepeatableTokenBytes+1)
	otherScope := recordScope()
	otherScope.Table = "other_repeatable"
	for _, test := range []struct {
		name     string
		snapshot *protocol.RepeatableSnapshot
		want     string
	}{
		{"incomplete", &protocol.RepeatableSnapshot{Scope: recordScope(), Exists: true}, `.*repeatable migration snapshot is incomplete`},
		{"another scope", &protocol.RepeatableSnapshot{Scope: otherScope, Exists: true, Complete: true}, `.*repeatable migration snapshot changed its scope`},
		{"a repeated identity", &protocol.RepeatableSnapshot{Scope: recordScope(), Exists: true, Complete: true, Rows: []*protocol.RepeatableRow{appliedRow(), appliedRow()}},
			`.*duplicate repeatable migration identity`},
		{"rows of a missing record", &protocol.RepeatableSnapshot{Scope: recordScope(), Complete: true, Rows: []*protocol.RepeatableRow{appliedRow()}},
			`.*a missing repeatable migration record returned rows`},
		{"a body that never applied", &protocol.RepeatableSnapshot{Scope: recordScope(), Exists: true, Complete: true, Rows: []*protocol.RepeatableRow{kept}},
			`.*a repeatable migration that never applied keeps no digest, time or body`},
		{"a row without its run", &protocol.RepeatableSnapshot{Scope: recordScope(), Exists: true, Complete: true, Rows: []*protocol.RepeatableRow{unnamed}},
			`.*invalid repeatable migration attempt`},
		{"an error over the bound", &protocol.RepeatableSnapshot{Scope: recordScope(), Exists: true, Complete: true, Rows: []*protocol.RepeatableRow{verbose}},
			`.*invalid repeatable migration attempt`},
		{"a digest over the bound", &protocol.RepeatableSnapshot{Scope: recordScope(), Exists: true, Complete: true, Rows: []*protocol.RepeatableRow{digested}},
			`.*an applied repeatable migration needs its digest and time`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateRepeatableSnapshotReply(request, test.snapshot), qt.ErrorMatches, test.want)
		})
	}
}

// A body read answers with the bytes kept, an empty body that is kept, and a
// row that keeps none.
func TestValidateRepeatableBodyReply_HappyPath(t *testing.T) {
	request := &protocol.ReadRepeatableBodyRequest{Scope: recordScope(), Identity: "views"}
	for _, test := range []struct {
		name string
		body *protocol.RepeatableBody
	}{
		{"bytes", &protocol.RepeatableBody{Scope: recordScope(), Identity: "views", Found: true, Kept: true, Body: []byte("SELECT 1;")}},
		{"an empty body", &protocol.RepeatableBody{Scope: recordScope(), Identity: "views", Found: true, Kept: true}},
		{"no body", &protocol.RepeatableBody{Scope: recordScope(), Identity: "views", Found: true}},
		{"no row", &protocol.RepeatableBody{Scope: recordScope(), Identity: "views"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateReadRepeatableBodyRequest(request), qt.IsNil)
			c.Assert(provider.ValidateRepeatableBodyReply(request, test.body), qt.IsNil)
		})
	}
}

// A body read that answers another identity, or bytes that are not kept, is
// refused.
func TestValidateRepeatableBodyReply_FailurePath(t *testing.T) {
	request := &protocol.ReadRepeatableBodyRequest{Scope: recordScope(), Identity: "views"}
	for _, test := range []struct {
		name string
		body *protocol.RepeatableBody
		want string
	}{
		{"another identity", &protocol.RepeatableBody{Scope: recordScope(), Identity: "grants", Found: true, Kept: true},
			`.*repeatable migration body changed its request binding`},
		{"bytes not kept", &protocol.RepeatableBody{Scope: recordScope(), Identity: "views", Found: true, Body: []byte("x")},
			`.*a repeatable migration body that is not kept has no bytes`},
		{"a body of no row", &protocol.RepeatableBody{Scope: recordScope(), Identity: "views", Kept: true},
			`.*a repeatable migration body that is not kept has no bytes`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateRepeatableBodyReply(request, test.body), qt.ErrorMatches, test.want)
		})
	}
}

func removal() *protocol.MutateRepeatableRequest {
	return &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Remove{Remove: &protocol.RepeatableRemoval{Identity: "views"}}}
}

func insertingClaim() *protocol.MutateRepeatableRequest {
	return &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Claim{
		Claim: &protocol.RepeatableClaim{Identity: "views", Insert: true, RunId: "run-2", AttemptChecksum: "h1:new", AttemptOrder: 1, AttemptedAtSeconds: 1}}}
}

func claim() *protocol.MutateRepeatableRequest {
	return &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "r", Mutation: &protocol.MutateRepeatableRequest_Claim{
		Claim: &protocol.RepeatableClaim{Identity: "views", ExpectedRunId: "run-1", RunId: "run-2", AttemptChecksum: "h1:new", AttemptOrder: 1, AttemptedAtSeconds: 1}}}
}

// An applied write is accepted, a conflict carries the run that holds the
// row in a RepeatableError that wraps ErrExecution, a claim that read a row
// conflicts with no row once another run removed it, and a refusal carries no
// holder.
func TestCheckRepeatableResult_HappyPath(t *testing.T) {
	c := qt.New(t)
	applied := &protocol.RepeatableResult{Scope: recordScope(), RequestId: "r", Outcome: protocol.RepeatableResult_APPLIED, Removed: true}
	conflict := &protocol.RepeatableResult{Scope: recordScope(), RequestId: "r", Outcome: protocol.RepeatableResult_CONFLICT, HolderRunId: "run-3"}
	vanished := &protocol.RepeatableResult{Scope: recordScope(), RequestId: "r", Outcome: protocol.RepeatableResult_CONFLICT}
	refused := &protocol.RepeatableResult{Scope: recordScope(), RequestId: "r", Outcome: protocol.RepeatableResult_REFUSED}

	c.Assert(provider.ValidateRepeatableResult(insertingClaim(), conflict), qt.IsNil)
	c.Assert(provider.ValidateRepeatableResult(claim(), vanished), qt.IsNil)
	c.Assert(provider.ValidateRepeatableResult(insertingClaim(), refused), qt.IsNil)
	c.Assert(provider.CheckRepeatableResult(refused), qt.ErrorMatches, `provider repeatable migration record outcome is REFUSED`)
	c.Assert(provider.CheckRepeatableResult(refused), qt.ErrorIs, provider.ErrExecution)

	c.Assert(provider.ValidateRepeatableResult(removal(), applied), qt.IsNil)
	c.Assert(provider.CheckRepeatableResult(applied), qt.IsNil)
	c.Assert(provider.ValidateRepeatableResult(claim(), conflict), qt.IsNil)
	err := provider.CheckRepeatableResult(conflict)
	var failure *provider.RepeatableError
	c.Assert(err, qt.ErrorAs, &failure)
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(err, qt.ErrorMatches, `provider repeatable migration record outcome is CONFLICT: run "run-3" holds the row`)
	c.Assert(failure.Holder, qt.Equals, "run-3")
}

// A receipt that answers another request, names a holder outside a conflict,
// or removes a row it was not asked to is refused, and an UNKNOWN outcome
// wraps ErrOutcomeUnknown.
func TestCheckRepeatableResult_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		request *protocol.MutateRepeatableRequest
		result  *protocol.RepeatableResult
		want    string
	}{
		{"another request", claim(), &protocol.RepeatableResult{Scope: recordScope(), RequestId: "s", Outcome: protocol.RepeatableResult_APPLIED},
			`.*repeatable migration result changed its request binding`},
		{"a holder of an applied write", claim(), &protocol.RepeatableResult{Scope: recordScope(), RequestId: "r", Outcome: protocol.RepeatableResult_APPLIED,
			HolderRunId: "run-3"}, `.*only a conflicting write names the run that holds the row`},
		{"a holder of a removal", removal(), &protocol.RepeatableResult{Scope: recordScope(), RequestId: "r", Outcome: protocol.RepeatableResult_CONFLICT,
			HolderRunId: "run-3"}, `.*only a conflicting write names the run that holds the row`},
		{"a claim that removed", claim(), &protocol.RepeatableResult{Scope: recordScope(), RequestId: "r", Outcome: protocol.RepeatableResult_APPLIED,
			Removed: true}, `.*only a removal that applied removes a row`},
		{"an inserting claim that conflicts with no row", insertingClaim(), &protocol.RepeatableResult{Scope: recordScope(), RequestId: "r",
			Outcome: protocol.RepeatableResult_CONFLICT}, `.*an inserting claim conflicts only with a row, which names its run`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateRepeatableResult(test.request, test.result), qt.ErrorMatches, test.want)
		})
	}
	t.Run("unknown", func(t *testing.T) {
		c := qt.New(t)
		err := provider.CheckRepeatableResult(&protocol.RepeatableResult{Scope: recordScope(), RequestId: "r"})
		c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	})
}

// A log entry about a repeatable migration carries its identity and version
// 0, and a peer that negotiated the record accepts it; an entry about a
// version needs no negotiation.
func TestRepeatableMigrationLogRecord_HappyPath(t *testing.T) {
	c := qt.New(t)
	entry := &protocol.MigrationLogRecord{RunId: "run-1", Sequence: 1, Operation: "up", State: "failed", ActorSource: "unknown",
		AtSeconds: 1_700_000_000, Repeatable: "views", Checksum: "h1:new", Error: "syntax error"}
	versioned := &protocol.MigrationLogRecord{RunId: "run-1", Sequence: 2, Operation: "up", Version: 3, State: "applied", ActorSource: "unknown",
		AtSeconds: 1_700_000_000}
	negotiated := recordDescription([]string{provider.HistoryFeature, provider.RepeatableRecordFeature}, nil)

	c.Assert(provider.ValidateMigrationLogRecord(entry), qt.IsNil)
	c.Assert(provider.ValidateMigrationLogNegotiation(negotiated, entry, versioned), qt.IsNil)
	c.Assert(provider.ValidateMigrationLogNegotiation(recordDescription(nil, nil), versioned), qt.IsNil)
}

// An entry about a repeatable migration that also names a version is refused,
// and so is one sent to a peer that did not negotiate the field, which would
// read it as an entry about version 0.
func TestRepeatableMigrationLogRecord_FailurePath(t *testing.T) {
	c := qt.New(t)
	versioned := &protocol.MigrationLogRecord{RunId: "run-1", Sequence: 1, Operation: "up", Version: 3, State: "started", ActorSource: "unknown",
		AtSeconds: 1_700_000_000, Repeatable: "views"}
	entry := &protocol.MigrationLogRecord{RunId: "run-1", Sequence: 1, Operation: "up", State: "started", ActorSource: "unknown",
		AtSeconds: 1_700_000_000, Repeatable: "views"}

	c.Assert(provider.ValidateMigrationLogRecord(versioned), qt.ErrorMatches, `.*a migration log entry about a repeatable migration has version 0`)
	c.Assert(provider.ValidateMigrationLogNegotiation(recordDescription([]string{provider.HistoryFeature}, nil), entry),
		qt.ErrorMatches, `.*a migration log entry about a repeatable migration requires sessions\.repeatable-record`)
}
