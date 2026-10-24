package runtime

import (
	"context"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// SeedTimeout bounds a seed call that has no earlier deadline. A seed file can
// write many rows, and the owner may run it again after an abort that proved
// nothing committed.
const SeedTimeout = 10 * time.Minute

var (
	seedTables = sessionCall[*protocol.ListSeedTablesRequest, *protocol.SeedTables]{
		feature: provider.SeedsFeature, operation: "list seed tables", timeout: CallTimeout,
		validateInput: provider.ValidateListSeedTablesRequest, validateOutput: provider.ValidateSeedTables,
	}
	seedApply = sessionCall[*protocol.ApplySeedRequest, *protocol.SeedResult]{
		feature: provider.SeedsFeature, operation: "apply seed", timeout: SeedTimeout,
		validateInput: provider.ValidateApplySeedRequest, validateOutput: provider.ValidateAppliedSeed, uncertain: unknownSeed,
	}
	seedRecord = sessionCall[*protocol.RecordSeedRequest, *protocol.SeedResult]{
		feature: provider.SeedsFeature, operation: "record seed", timeout: CallTimeout,
		validateInput: provider.ValidateRecordSeedRequest, validateOutput: provider.ValidateRecordedSeed, uncertain: unknownSeed,
	}
	seedLedger = sessionCall[*protocol.ReadSeedLedgerRequest, *protocol.SeedLedger]{
		feature: provider.SeedsFeature, operation: "read seed ledger", timeout: CallTimeout,
		validateInput: provider.ValidateReadSeedLedgerRequest, validateOutput: provider.ValidateSeedLedger,
	}
)

func unknownSeed() error { return &provider.SeedError{} }

// ListSeedTables lists the tables of one schema for the protected-table check.
func (c *Client) ListSeedTables(ctx context.Context, request *protocol.ListSeedTablesRequest) (*protocol.SeedTables, error) {
	return seedTables.call(ctx, c, request, c.rpc.ListSeedTables)
}

// ApplySeed runs one seed file and records it, once. A confirmed outcome other
// than APPLIED is returned with a provider.SeedError, so the caller can read
// the reason and act on KEY_CONFLICT. A lost or invalid reply exposes no
// result and leaves the outcome unknown; this method never retries it.
func (c *Client) ApplySeed(ctx context.Context, request *protocol.ApplySeedRequest) (*protocol.SeedResult, error) {
	result, err := seedApply.call(ctx, c, request, c.rpc.ApplySeed)
	if err != nil {
		return nil, err
	}
	return result, provider.CheckSeedResult(result)
}

// RecordSeed records a seed without running it, with the outcome rules of
// ApplySeed.
func (c *Client) RecordSeed(ctx context.Context, request *protocol.RecordSeedRequest) (*protocol.SeedResult, error) {
	result, err := seedRecord.call(ctx, c, request, c.rpc.RecordSeed)
	if err != nil {
		return nil, err
	}
	return result, provider.CheckSeedResult(result)
}

// ReadSeedLedger reads the complete ledger, or returns an error and no entry.
func (c *Client) ReadSeedLedger(ctx context.Context, request *protocol.ReadSeedLedgerRequest) (*protocol.SeedLedger, error) {
	return seedLedger.call(ctx, c, request, c.rpc.ReadSeedLedger)
}

func (s *serverAdapter) ListSeedTables(ctx context.Context, request *protocol.ListSeedTablesRequest) (*protocol.SeedTables, error) {
	if nilService(s.server.Seeds) {
		return nil, unavailable("seeds")
	}
	return seedTables.serve(ctx, s, request, s.server.Seeds.ListSeedTables)
}

func (s *serverAdapter) ApplySeed(ctx context.Context, request *protocol.ApplySeedRequest) (*protocol.SeedResult, error) {
	if nilService(s.server.Seeds) {
		return nil, unavailable("seeds")
	}
	return seedApply.serve(ctx, s, request, s.server.Seeds.ApplySeed)
}

func (s *serverAdapter) RecordSeed(ctx context.Context, request *protocol.RecordSeedRequest) (*protocol.SeedResult, error) {
	if nilService(s.server.Seeds) {
		return nil, unavailable("seeds")
	}
	return seedRecord.serve(ctx, s, request, s.server.Seeds.RecordSeed)
}

func (s *serverAdapter) ReadSeedLedger(ctx context.Context, request *protocol.ReadSeedLedgerRequest) (*protocol.SeedLedger, error) {
	if nilService(s.server.Seeds) {
		return nil, unavailable("seeds")
	}
	return seedLedger.serve(ctx, s, request, s.server.Seeds.ReadSeedLedger)
}
