package provider_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

const seedChecksum = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

func seed() *protocol.Seed {
	return &protocol.Seed{Path: "base/rates.sql", Filename: "rates.sql", Checksum: seedChecksum, Text: "INSERT INTO rates VALUES ('CZ');\nUPSERT;\n", Env: "dev"}
}

func applySeedExchange() (*protocol.ApplySeedRequest, *protocol.SeedResult) {
	return &protocol.ApplySeedRequest{SessionId: "session", RequestId: "apply-1", Seed: seed()},
		&protocol.SeedResult{RequestId: "apply-1", Path: "base/rates.sql", Outcome: protocol.SeedResult_APPLIED}
}

func recordSeedExchange() (*protocol.RecordSeedRequest, *protocol.SeedResult) {
	recorded := seed()
	recorded.Text = ""
	return &protocol.RecordSeedRequest{SessionId: "session", RequestId: "record-1", Seed: recorded},
		&protocol.SeedResult{RequestId: "record-1", Path: "base/rates.sql", Outcome: protocol.SeedResult_APPLIED}
}

func TestValidateAppliedSeed_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.SeedResult)
	}{
		{"applied", func(*protocol.SeedResult) {}},
		{"key conflict without a reason", func(r *protocol.SeedResult) { r.Outcome = protocol.SeedResult_KEY_CONFLICT }},
		{"refused at a statement", func(r *protocol.SeedResult) {
			r.Outcome, r.Reason = protocol.SeedResult_REFUSED, "BATCH UPDATE runs only outside a transaction"
			r.Position = &protocol.SourcePosition{ByteOffset: 33, Line: 2, Column: 1}
		}},
		{"refused before any statement", func(r *protocol.SeedResult) {
			r.Outcome, r.Reason = protocol.SeedResult_REFUSED, "the ledger has a layout this provider does not own"
		}},
		{"not applied", func(r *protocol.SeedResult) {
			r.Outcome, r.Reason = protocol.SeedResult_NOT_APPLIED, "aborted three times"
		}},
		{"unknown", func(r *protocol.SeedResult) { r.Outcome = protocol.SeedResult_UNKNOWN }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := applySeedExchange()
			test.mutate(result)
			c.Assert(provider.ValidateAppliedSeed(request, result), qt.IsNil)
		})
	}
}

func TestValidateApplySeedRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ApplySeedRequest)
	}{
		{"no session", func(r *protocol.ApplySeedRequest) { r.SessionId = "" }},
		{"no request ID", func(r *protocol.ApplySeedRequest) { r.RequestId = "" }},
		{"no seed", func(r *protocol.ApplySeedRequest) { r.Seed = nil }},
		{"no path", func(r *protocol.ApplySeedRequest) { r.Seed.Path = "" }},
		{"no file name", func(r *protocol.ApplySeedRequest) { r.Seed.Filename = "" }},
		{"uppercase checksum", func(r *protocol.ApplySeedRequest) { r.Seed.Checksum = strings.ToUpper(seedChecksum) }},
		{"short checksum", func(r *protocol.ApplySeedRequest) { r.Seed.Checksum = seedChecksum[1:] }},
		{"environment of two lines", func(r *protocol.ApplySeedRequest) { r.Seed.Env = "dev\nprod" }},
		{"text that is not UTF-8", func(r *protocol.ApplySeedRequest) { r.Seed.Text = "\xff" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := applySeedExchange()
			test.mutate(request)
			c.Assert(provider.ValidateApplySeedRequest(request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateAppliedSeed_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.SeedResult)
	}{
		{"another request", func(r *protocol.SeedResult) { r.RequestId = "apply-2" }},
		{"another path", func(r *protocol.SeedResult) { r.Path = "base/other.sql" }},
		{"unknown outcome", func(r *protocol.SeedResult) { r.Outcome = 99 }},
		{"refused without a reason", func(r *protocol.SeedResult) { r.Outcome = protocol.SeedResult_REFUSED }},
		{"not applied without a reason", func(r *protocol.SeedResult) { r.Outcome = protocol.SeedResult_NOT_APPLIED }},
		{"a reason of two lines", func(r *protocol.SeedResult) {
			r.Outcome, r.Reason = protocol.SeedResult_KEY_CONFLICT, "duplicate\n'CZ'"
		}},
		{"a reason too long", func(r *protocol.SeedResult) {
			r.Outcome, r.Reason = protocol.SeedResult_NOT_APPLIED, strings.Repeat("a", 513)
		}},
		{"a position on an applied seed", func(r *protocol.SeedResult) { r.Position = &protocol.SourcePosition{Line: 1, Column: 1} }},
		{"a position past the text", func(r *protocol.SeedResult) {
			r.Outcome, r.Reason = protocol.SeedResult_REFUSED, "refused"
			r.Position = &protocol.SourcePosition{ByteOffset: 1000, Line: 1, Column: 1}
		}},
		{"a position on line zero", func(r *protocol.SeedResult) {
			r.Outcome, r.Reason = protocol.SeedResult_REFUSED, "refused"
			r.Position = &protocol.SourcePosition{Column: 1}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := applySeedExchange()
			test.mutate(result)
			c.Assert(provider.ValidateAppliedSeed(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	request, _ := applySeedExchange()
	c.Assert(provider.ValidateAppliedSeed(request, nil), qt.ErrorIs, provider.ErrProtocol)
}

func TestValidateRecordedSeed_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := recordSeedExchange()
	c.Assert(provider.ValidateRecordedSeed(request, result), qt.IsNil)
}

// A record runs nothing, so it carries no text, cannot hit a key and names
// no position in a text it did not send.
func TestValidateRecordedSeed_FailurePath(t *testing.T) {
	c := qt.New(t)
	request, _ := recordSeedExchange()
	request.Seed.Text = "INSERT INTO rates VALUES ('CZ');"
	c.Assert(provider.ValidateRecordSeedRequest(request), qt.ErrorIs, provider.ErrProtocol)
	request, result := recordSeedExchange()
	result.Outcome = protocol.SeedResult_KEY_CONFLICT
	c.Assert(provider.ValidateRecordedSeed(request, result), qt.ErrorIs, provider.ErrProtocol)
	request, result = recordSeedExchange()
	result.Outcome, result.Reason, result.Position = protocol.SeedResult_REFUSED, "refused", &protocol.SourcePosition{Line: 1, Column: 1}
	c.Assert(provider.ValidateRecordedSeed(request, result), qt.ErrorIs, provider.ErrProtocol)
}

func TestCheckSeedResult_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.CheckSeedResult(&protocol.SeedResult{Outcome: protocol.SeedResult_APPLIED}), qt.IsNil)
}

func TestCheckSeedResult_FailurePath(t *testing.T) {
	tests := []struct {
		outcome protocol.SeedResult_Outcome
		want    error
	}{
		{protocol.SeedResult_KEY_CONFLICT, provider.ErrExecution},
		{protocol.SeedResult_REFUSED, provider.ErrExecution},
		{protocol.SeedResult_NOT_APPLIED, provider.ErrExecution},
		{protocol.SeedResult_UNKNOWN, provider.ErrOutcomeUnknown},
	}
	for _, test := range tests {
		t.Run(test.outcome.String(), func(t *testing.T) {
			c := qt.New(t)
			err := provider.CheckSeedResult(&protocol.SeedResult{Outcome: test.outcome})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(err, qt.ErrorMatches, "provider seed outcome is "+test.outcome.String())
		})
	}
}

func seedLedgerExchange() (*protocol.ReadSeedLedgerRequest, *protocol.SeedLedger) {
	return &protocol.ReadSeedLedgerRequest{SessionId: "session", MaxEntries: 2}, &protocol.SeedLedger{Exists: true, Complete: true, Entries: []*protocol.SeedLedgerEntry{
		{Path: "base/rates.sql", Checksum: seedChecksum, Env: "dev", AppliedAtSeconds: 1767225600},
		{Path: "base/users.sql", Checksum: seedChecksum, AppliedAtNanos: 999999999},
	}}
}

func TestValidateSeedLedger_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := seedLedgerExchange()
	c.Assert(provider.ValidateSeedLedger(request, result), qt.IsNil)
	c.Assert(provider.ValidateSeedLedger(request, &protocol.SeedLedger{Complete: true}), qt.IsNil)
}

func TestValidateSeedLedger_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadSeedLedgerRequest, *protocol.SeedLedger)
		want   error
	}{
		{"no session", func(r *protocol.ReadSeedLedgerRequest, _ *protocol.SeedLedger) { r.SessionId = "" }, provider.ErrProtocol},
		{"no bound", func(r *protocol.ReadSeedLedgerRequest, _ *protocol.SeedLedger) { r.MaxEntries = 0 }, provider.ErrProtocol},
		{"bound above the limit", func(r *protocol.ReadSeedLedgerRequest, _ *protocol.SeedLedger) {
			r.MaxEntries = provider.MaxSeedLedgerEntries + 1
		}, provider.ErrProtocol},
		{"incomplete", func(_ *protocol.ReadSeedLedgerRequest, l *protocol.SeedLedger) { l.Complete = false }, provider.ErrIncomplete},
		{"more entries than the bound", func(r *protocol.ReadSeedLedgerRequest, _ *protocol.SeedLedger) { r.MaxEntries = 1 }, provider.ErrIncomplete},
		{"entries of a missing ledger", func(_ *protocol.ReadSeedLedgerRequest, l *protocol.SeedLedger) { l.Exists = false }, provider.ErrProtocol},
		{"a path listed twice", func(_ *protocol.ReadSeedLedgerRequest, l *protocol.SeedLedger) { l.Entries[1].Path = l.Entries[0].Path }, provider.ErrProtocol},
		{"an entry without a path", func(_ *protocol.ReadSeedLedgerRequest, l *protocol.SeedLedger) { l.Entries[0].Path = "" }, provider.ErrProtocol},
		{"an invalid checksum", func(_ *protocol.ReadSeedLedgerRequest, l *protocol.SeedLedger) { l.Entries[0].Checksum = "abc" }, provider.ErrProtocol},
		{"an environment of two lines", func(_ *protocol.ReadSeedLedgerRequest, l *protocol.SeedLedger) { l.Entries[0].Env = "a\nb" }, provider.ErrProtocol},
		{"nanoseconds out of range", func(_ *protocol.ReadSeedLedgerRequest, l *protocol.SeedLedger) { l.Entries[0].AppliedAtNanos = 1e9 }, provider.ErrProtocol},
		{"a year past 9999", func(_ *protocol.ReadSeedLedgerRequest, l *protocol.SeedLedger) {
			l.Entries[0].AppliedAtSeconds = 253402300800
		}, provider.ErrProtocol},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := seedLedgerExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateSeedLedger(request, result), qt.ErrorIs, test.want)
		})
	}
}

func TestValidateSeedTables_HappyPath(t *testing.T) {
	c := qt.New(t)
	request := &protocol.ListSeedTablesRequest{SessionId: "session", Schema: "app"}
	result := &protocol.SeedTables{Tables: []*protocol.Reference{rowsTable(), {Schema: "app", Kind: "table", Name: "users"}}}
	c.Assert(provider.ValidateSeedTables(request, result), qt.IsNil)
	request.Schema = ""
	c.Assert(provider.ValidateSeedTables(request, &protocol.SeedTables{}), qt.IsNil)
}

func TestValidateSeedTables_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		tables []*protocol.Reference
	}{
		{"a table without a name", []*protocol.Reference{{Schema: "app", Kind: "table"}}},
		{"a table in another schema", []*protocol.Reference{{Schema: "audit", Kind: "table", Name: "rates"}}},
		{"a table listed twice", []*protocol.Reference{rowsTable(), rowsTable()}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := &protocol.ListSeedTablesRequest{SessionId: "session", Schema: "app"}
			c.Assert(provider.ValidateSeedTables(request, &protocol.SeedTables{Tables: test.tables}), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	c.Assert(provider.ValidateListSeedTablesRequest(&protocol.ListSeedTablesRequest{}), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateSeedTables(&protocol.ListSeedTablesRequest{SessionId: "session"}, nil), qt.ErrorIs, provider.ErrProtocol)
}
