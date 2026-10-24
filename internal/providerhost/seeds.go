package providerhost

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/dataext"
)

// Seeds returns the session's seed service: the provider runs a seed file and
// keeps the ledger of applied files in a layout of its own. A provider whose
// engine lacks data.seeds gets a service that refuses every call, so a seed
// run never falls back to the host's own statements and tracker table.
func (s *session) Seeds() dataext.Seeds { return seeds{session: s} }

type seeds struct{ session *session }

var (
	_ dataext.Seeds      = seeds{}
	_ dataext.SeedLedger = seeds{}
)

func (s seeds) require(operation string) error {
	if s.session.closed.Load() {
		return fmt.Errorf("provider session is closed")
	}
	return s.session.target.requireData(provider.SeedsFeature, operation)
}

// TableNames lists the tables of schema for the protected-table check.
func (s seeds) TableNames(ctx context.Context, _ dataext.Database, schema string) ([]string, error) {
	if err := s.require("list the tables a seed run guards"); err != nil {
		return nil, err
	}
	listed, err := s.session.target.host.client.ListSeedTables(ctx, &protocol.ListSeedTablesRequest{SessionId: s.session.remote.GetId(), Schema: schema})
	if err != nil {
		return nil, fmt.Errorf("query target tables: %w", err)
	}
	names := make([]string, 0, len(listed.GetTables()))
	for _, table := range listed.GetTables() {
		names = append(names, table.GetName())
	}
	return names, nil
}

// Apply runs a seed file and records it in the provider's ledger in one step.
// A key conflict wraps dataext.ErrKeyConflict; a refusal names where in the
// file the statement the provider would not run starts; an outcome the
// provider cannot confirm is never sent again.
func (s seeds) Apply(ctx context.Context, _ dataext.Database, seed dataext.Seed) error {
	if err := s.require("run seed files"); err != nil {
		return err
	}
	result, err := s.session.target.host.client.ApplySeed(ctx, &protocol.ApplySeedRequest{SessionId: s.session.remote.GetId(), RequestId: rand.Text(), Seed: wireSeed(seed)})
	return seedOutcome(result, err)
}

// Record writes the ledger entry of a seed without running the file.
func (s seeds) Record(ctx context.Context, _ dataext.Database, seed dataext.Seed) error {
	if err := s.require("record seed files"); err != nil {
		return err
	}
	result, err := s.session.target.host.client.RecordSeed(ctx, &protocol.RecordSeedRequest{SessionId: s.session.remote.GetId(), RequestId: rand.Text(), Seed: recordedSeed(seed)})
	return seedOutcome(result, err)
}

// Ledger reads the provider's ledger whole. A ledger the provider has not
// created yet holds no entry.
func (s seeds) Ledger(ctx context.Context, _ dataext.Database) (map[string]string, error) {
	if err := s.require("read the seed ledger"); err != nil {
		return nil, err
	}
	ledger, err := s.session.target.host.client.ReadSeedLedger(ctx, &protocol.ReadSeedLedgerRequest{SessionId: s.session.remote.GetId(), MaxEntries: provider.MaxSeedLedgerEntries})
	if err != nil {
		return nil, err
	}
	applied := make(map[string]string, len(ledger.GetEntries()))
	for _, entry := range ledger.GetEntries() {
		applied[entry.GetPath()] = entry.GetChecksum()
	}
	return applied, nil
}

// wireSeed is a seed file as ApplySeed sends it, with its text.
func wireSeed(seed dataext.Seed) *protocol.Seed {
	wire := recordedSeed(seed)
	wire.Text = seed.Text
	return wire
}

// recordedSeed is a seed file as RecordSeed sends it, without its text.
func recordedSeed(seed dataext.Seed) *protocol.Seed {
	return &protocol.Seed{Path: seed.Path, Filename: seed.Filename, Checksum: seed.Checksum, Env: seed.Env}
}

// seedOutcome is the error a seed call's outcome means for the seeder: none
// for APPLIED, one that wraps dataext.ErrKeyConflict for KEY_CONFLICT, and
// otherwise one that carries the provider's reason. An unknown outcome wraps
// provider.ErrOutcomeUnknown, so a caller can tell it from a confirmed
// refusal.
func seedOutcome(result *protocol.SeedResult, err error) error {
	if err == nil {
		return nil
	}
	refused, confirmed := errors.AsType[*provider.SeedError](err)
	if !confirmed {
		return fmt.Errorf("the provider did not confirm what the seed run did, and it is not run again: %w", err)
	}
	switch refused.Outcome {
	case protocol.SeedResult_KEY_CONFLICT:
		return fmt.Errorf("%w: %w", dataext.ErrKeyConflict, err)
	case protocol.SeedResult_REFUSED:
		position := result.GetPosition()
		return fmt.Errorf("the provider refuses the file before running it, at line %d, column %d: %s",
			position.GetLine(), position.GetColumn(), result.GetReason())
	case protocol.SeedResult_NOT_APPLIED:
		return fmt.Errorf("the provider applied nothing of the file: %s", result.GetReason())
	default:
		return fmt.Errorf("the provider did not confirm what the seed run did, and it is not run again: %w", err)
	}
}
