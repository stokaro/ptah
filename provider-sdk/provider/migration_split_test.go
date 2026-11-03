package provider_test

import (
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

const splitBody = "DEFINE v = 1;\nINSERT INTO t VALUES (v);\nCREATE TABLE u (id INT);\n"

// splitDescription offers migration splits on one engine, with the
// transactions and history the host runs the units with, as negotiated with
// a host that lists the feature.
func splitDescription() *protocol.Description {
	d := description()
	features := []string{provider.ExecuteFeature, provider.TransactionsFeature, provider.HistoryFeature, provider.MigrationSplitFeature}
	d.Features = append(d.Features, features...)
	d.Engines[0].Features = append(d.Engines[0].Features, features[1:]...)
	return d
}

// splitExchange splits splitBody into a data unit and a scheme unit, which
// carries the definition the data unit made. The first assertion is
// read-only and the second is not.
func splitExchange() (*protocol.Description, *protocol.SplitMigrationRequest, *protocol.MigrationSplit) {
	d := splitDescription()
	request := &protocol.SplitMigrationRequest{Engine: d.Engines[0].Id, Body: splitBody, Assertions: []string{"SELECT 1", "DELETE FROM t"}}
	result := &protocol.MigrationSplit{
		Units: []*protocol.MigrationUnit{
			{Sql: "DEFINE v = 1;\nINSERT INTO t VALUES (v);", Source: "DEFINE v = 1;\nINSERT INTO t VALUES (v);", Placement: protocol.MigrationUnit_WITH_CHECKPOINT},
			{Sql: "DEFINE v = 1;\nCREATE TABLE u (id INT);", Source: "CREATE TABLE u (id INT);", Placement: protocol.MigrationUnit_OUTSIDE},
		},
		Assertions: []*protocol.AssertionVerdict{{ReadOnly: true}, {Reason: "the statement deletes rows"}},
	}
	return d, request, result
}

// splitUnits is splitExchange's reply.
func splitUnits() *protocol.MigrationSplit {
	_, _, result := splitExchange()
	return result
}

// splitRefused is splitExchange's reply refusing the body from start to end.
func splitRefused(start, end uint32) *protocol.MigrationSplit {
	result := splitUnits()
	result.Units = nil
	result.Refusal = &protocol.SplitRefusal{Start: start, End: end, Reason: "a scheme statement follows a data statement"}
	return result
}

// errorEndsWith matches an error whose text ends with want.
func errorEndsWith(want string) string {
	return ".*" + regexp.QuoteMeta(want)
}

func TestMigrationSplit_HappyPath(t *testing.T) {
	end := uint32(len(splitBody))
	for _, test := range []struct {
		name   string
		result *protocol.MigrationSplit
	}{
		{"units", splitUnits()},
		{"a refusal up to the end of the body", splitRefused(14, end)},
		{"a refusal of an empty range at the end", splitRefused(end, end)},
		{"a body with no statement", &protocol.MigrationSplit{Assertions: splitRefused(0, 0).Assertions}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := splitExchange()
			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			c.Assert(provider.ValidateSplitMigrationRequest(d, request), qt.IsNil)
			c.Assert(provider.ValidateMigrationSplit(d, request, test.result), qt.IsNil)
		})
	}
}

// Each row starts from a valid request and changes one thing, which the
// request validator and the reply validator both refuse.
func TestSplitMigrationRequest_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.SplitMigrationRequest) *protocol.Description
		want   string
	}{
		{"no engine", func(d *protocol.Description, r *protocol.SplitMigrationRequest) *protocol.Description {
			r.Engine = ""
			return d
		}, "the request names no engine"},
		{"no body", func(d *protocol.Description, r *protocol.SplitMigrationRequest) *protocol.Description {
			r.Body = ""
			return d
		}, "the request carries no body"},
		{"a host that did not list the feature", func(d *protocol.Description, _ *protocol.SplitMigrationRequest) *protocol.Description {
			return provider.NegotiatedDescription(d, nil)
		}, "the feature was not negotiated"},
		{"an engine the provider does not own", func(d *protocol.Description, r *protocol.SplitMigrationRequest) *protocol.Description {
			r.Engine = "other"
			return d
		}, "value engine is not owned by this provider"},
		{"an engine that does not split", func(d *protocol.Description, _ *protocol.SplitMigrationRequest) *protocol.Description {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.MigrationSplitFeature })
			return d
		}, "the engine does not split migrations"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := splitExchange()
			d = test.mutate(d, request)
			err := provider.ValidateSplitMigrationRequest(d, request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, errorEndsWith(test.want))
			err = provider.ValidateMigrationSplit(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, errorEndsWith(test.want))
		})
	}
}

// Each row starts from a valid reply and changes one thing.
func TestMigrationSplit_FailurePath(t *testing.T) {
	body := uint32(len(splitBody))
	for _, test := range []struct {
		name   string
		mutate func(*protocol.MigrationSplit)
		want   string
	}{
		{"a unit without text", func(r *protocol.MigrationSplit) { r.Units[0].Sql = " \n" }, "a unit carries no text to execute"},
		{"a unit without source", func(r *protocol.MigrationSplit) { r.Units[1].Source = "" }, "a unit carries no source"},
		{"a unit without placement", func(r *protocol.MigrationSplit) { r.Units[0].Placement = protocol.MigrationUnit_PLACEMENT_UNSPECIFIED }, "a unit needs a known placement"},
		{"an unknown placement", func(r *protocol.MigrationSplit) { r.Units[1].Placement = 99 }, "a unit needs a known placement"},
		{"units beside a refusal", func(r *protocol.MigrationSplit) { r.Refusal = splitRefused(0, 14).Refusal }, "a refused body has no units"},
		{"a refusal past the end of the body", func(r *protocol.MigrationSplit) { *r = *splitRefused(14, body+1) }, "a refusal names a range outside the body"},
		{"a refusal that ends before it starts", func(r *protocol.MigrationSplit) { *r = *splitRefused(14, 13) }, "a refusal names a range outside the body"},
		{"a refusal on two lines", func(r *protocol.MigrationSplit) {
			*r = *splitRefused(0, 14)
			r.Refusal.Reason = "a scheme statement\nfollows a data statement"
		}, "a refusal needs its reason on one line"},
		{"a refusal without a reason", func(r *protocol.MigrationSplit) {
			*r = *splitRefused(0, 14)
			r.Refusal.Reason = ""
		}, "a refusal needs its reason on one line"},
		{"fewer verdicts than assertions", func(r *protocol.MigrationSplit) { r.Assertions = r.Assertions[:1] }, "the reply does not give one verdict for each assertion"},
		{"more verdicts than assertions", func(r *protocol.MigrationSplit) {
			r.Assertions = append(r.Assertions, &protocol.AssertionVerdict{ReadOnly: true})
		}, "the reply does not give one verdict for each assertion"},
		{"a reason beside a read-only verdict", func(r *protocol.MigrationSplit) { r.Assertions[0].Reason = "it reads" }, "a read-only assertion carries a reason"},
		{"a refused verdict without a reason", func(r *protocol.MigrationSplit) { r.Assertions[1].Reason = "" }, "a refused assertion needs its reason on one line"},
		{"a refused verdict on two lines", func(r *protocol.MigrationSplit) { r.Assertions[1].Reason = "the statement\ndeletes rows" }, "a refused assertion needs its reason on one line"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := splitExchange()
			test.mutate(result)
			err := provider.ValidateMigrationSplit(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, errorEndsWith(test.want))
		})
	}
	c := qt.New(t)
	d, request, _ := splitExchange()
	err := provider.ValidateMigrationSplit(d, request, nil)
	c.Assert(err, qt.ErrorMatches, errorEndsWith("the reply is missing"))
}

// The feature needs the provider's transactions and history, and an engine
// that declares it needs the provider's feature and its own transactions and
// history.
func TestMigrationSplitDescription_FailurePath(t *testing.T) {
	without := func(features []string, feature string) []string {
		return slices.DeleteFunc(features, func(f string) bool { return f == feature })
	}
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
		want   string
	}{
		{"a provider without transactions", func(d *protocol.Description) { d.Features = without(d.Features, provider.TransactionsFeature) },
			"units.migration-split requires sessions.transactions"},
		{"a provider without history", func(d *protocol.Description) { d.Features = without(d.Features, provider.HistoryFeature) },
			"units.migration-split requires sessions.history"},
		{"an engine whose provider does not split", func(d *protocol.Description) { d.Features = without(d.Features, provider.MigrationSplitFeature) },
			"engine units.migration-split requires the provider's"},
		{"an engine without transactions", func(d *protocol.Description) {
			d.Engines[0].Features = without(d.Engines[0].Features, provider.TransactionsFeature)
		}, "engine units.migration-split requires engine sessions.transactions"},
		{"an engine without history", func(d *protocol.Description) {
			d.Engines[0].Features = without(d.Engines[0].Features, provider.HistoryFeature)
		}, "engine units.migration-split requires engine sessions.history"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d := splitDescription()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, errorEndsWith(test.want))
		})
	}
}

// A host that does not list the feature gets a description without it, from
// the provider and from the engine, and the description stays valid.
func TestMigrationSplitNegotiation_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.MigrationSplitFeature)
	listed := provider.NegotiatedDescription(splitDescription(), []string{provider.MigrationSplitFeature})
	c.Assert(listed.Features, qt.Contains, provider.MigrationSplitFeature)
	c.Assert(listed.Engines[0].Features, qt.Contains, provider.MigrationSplitFeature)
	unlisted := provider.NegotiatedDescription(splitDescription(), nil)
	c.Assert(unlisted.Features, qt.Not(qt.Contains), provider.MigrationSplitFeature)
	c.Assert(unlisted.Engines[0].Features, qt.Not(qt.Contains), provider.MigrationSplitFeature)
	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
}

func abortedExecution() (*protocol.ExecuteRequest, *protocol.Execution) {
	request := &protocol.ExecuteRequest{SessionId: "session", RequestId: "unit-1", TransactionId: "unit-1",
		Units: []*protocol.Unit{{ChangeId: "unit-1", Body: &protocol.Unit_Sql{Sql: "INSERT INTO t VALUES (1)"}}}}
	return request, &protocol.Execution{Outcome: protocol.Execution_NOT_APPLIED, TransactionAborted: true}
}

func abortedCommit() (*protocol.FinishTransactionRequest, *protocol.TransactionResult) {
	request := &protocol.FinishTransactionRequest{SessionId: "session", TransactionId: "unit-1", Action: protocol.FinishTransactionRequest_COMMIT}
	return request, &protocol.TransactionResult{Outcome: protocol.TransactionResult_ROLLED_BACK, Aborted: true}
}

// An abort is valid on a unit the engine did not apply in a caller
// transaction. A reply without the flag needs no negotiation.
func TestExecutionAbort_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := abortedExecution()
	c.Assert(provider.ValidateExecutionAbort(splitDescription(), request, result), qt.IsNil)
	result.TransactionAborted = false
	c.Assert(provider.ValidateExecutionAbort(&protocol.Description{}, request, result), qt.IsNil)
}

func TestExecutionAbort_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.ExecuteRequest, *protocol.Execution) *protocol.Description
		want   string
	}{
		{"a host that did not list the feature", func(d *protocol.Description, _ *protocol.ExecuteRequest, _ *protocol.Execution) *protocol.Description {
			return provider.NegotiatedDescription(d, nil)
		}, "aborts were not negotiated"},
		{"a unit outside a caller transaction", func(d *protocol.Description, r *protocol.ExecuteRequest, _ *protocol.Execution) *protocol.Description {
			r.TransactionId = ""
			return d
		}, "only a unit in a caller transaction is aborted"},
		{"another outcome", func(d *protocol.Description, _ *protocol.ExecuteRequest, e *protocol.Execution) *protocol.Description {
			e.Outcome = protocol.Execution_UNKNOWN
			return d
		}, "an aborted unit is NOT_APPLIED"},
		{"a stop at the statement timeout", func(d *protocol.Description, _ *protocol.ExecuteRequest, e *protocol.Execution) *protocol.Description {
			e.StatementTimedOut = true
			return d
		}, "an aborted unit did not stop at the statement timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := abortedExecution()
			d := test.mutate(splitDescription(), request, result)
			err := provider.ValidateExecutionAbort(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, errorEndsWith(test.want))
		})
	}
}

// An abort is valid on a commit the engine rolled back. A reply without the
// flag needs no negotiation.
func TestCommitAbort_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := abortedCommit()
	c.Assert(provider.ValidateTransactionAbort(splitDescription(), request, result), qt.IsNil)
	result.Aborted = false
	c.Assert(provider.ValidateTransactionAbort(&protocol.Description{}, request, result), qt.IsNil)
}

func TestCommitAbort_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.FinishTransactionRequest, *protocol.TransactionResult) *protocol.Description
		want   string
	}{
		{"a host that did not list the feature", func(d *protocol.Description, _ *protocol.FinishTransactionRequest, _ *protocol.TransactionResult) *protocol.Description {
			return provider.NegotiatedDescription(d, nil)
		}, "aborts were not negotiated"},
		{"a rollback", func(d *protocol.Description, r *protocol.FinishTransactionRequest, _ *protocol.TransactionResult) *protocol.Description {
			r.Action = protocol.FinishTransactionRequest_ROLLBACK
			return d
		}, "only a commit is aborted"},
		{"another outcome", func(d *protocol.Description, _ *protocol.FinishTransactionRequest, f *protocol.TransactionResult) *protocol.Description {
			f.Outcome = protocol.TransactionResult_COMMITTED
			return d
		}, "an aborted commit is ROLLED_BACK"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := abortedCommit()
			d := test.mutate(splitDescription(), request, result)
			err := provider.ValidateTransactionAbort(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, errorEndsWith(test.want))
		})
	}
}
