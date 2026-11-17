package migrator

import (
	"context"
	"fmt"
	"slices"
)

// appliedRepeatables returns the repeatable migrations a replay of the
// history runs: the ones whose file hashes to the digest recorded as applied,
// in execution order. A file with no record never ran and is left out. Any
// other record is an [UnreplayableHistoryError], since what that attempt left
// in the database is not what a file of the directory says.
func (m *Migrator) appliedRepeatables(ctx context.Context) ([]AppliedRepeatable, error) {
	state, err := m.readRepeatableState(ctx)
	if err != nil {
		return nil, fmt.Errorf("read the applied repeatable migrations: %w", err)
	}
	var applied []AppliedRepeatable
	for _, record := range state.records() {
		if err := unreplayableRepeatable(record); err != nil {
			return nil, err
		}
		if record.State != RepeatableStateApplied {
			continue
		}
		applied = append(applied, AppliedRepeatable{
			Identity:  record.Identity,
			Path:      record.Path,
			Checksum:  record.Checksum,
			Migration: state.repeatable(record.Identity).migration,
		})
	}
	return applied, nil
}

// unreplayableRepeatable is the refusal a record gives a replay, or nil for a
// record that is applied as its file says, or that has no row yet.
func unreplayableRepeatable(record RepeatableRecord) error {
	var reason, detail string
	switch {
	case record.dirty():
		reason, detail = UnreplayableDirty, fmt.Sprintf("its last attempt is %s, not cleanly applied", record.State)
	case record.Orphaned:
		reason, detail = UnreplayableMissingFile,
			"the database records it as applied and the migration directory has no file for it"
	case record.State == RepeatableStateChanged:
		reason, detail = UnreplayableChanged,
			fmt.Sprintf("%s changed after it applied, and the content the database ran is not in the directory", record.Path)
	default:
		return nil
	}
	return &UnreplayableHistoryError{Reason: reason, Repeatable: record.Identity, Detail: detail}
}

// ReplayProvider returns a provider holding what a replay of the history
// applies: Migrations, and Repeatables after them in their order. A migrator
// over it runs the repeatable migrations after the versioned ones, as an up
// run of the directory does, and records them in a record of its own.
//
// It refuses a repeatable migration with no Migration, which a history
// [Migrator.AppliedHistory] returns never holds.
func (h AppliedHistory) ReplayProvider() (MigrationProvider, error) {
	versioned := NewRegisteredMigrationProvider(h.Migrations...)
	if len(h.Repeatables) == 0 {
		return versioned, nil
	}
	provider := &replayProvider{RegisteredMigrationProvider: versioned}
	for _, repeatable := range h.Repeatables {
		if repeatable.Migration == nil {
			return nil, fmt.Errorf("repeatable migration %s has no body to replay", repeatable.Identity)
		}
		provider.repeatables = append(provider.repeatables, &repeatableMigration{
			identity:  repeatable.Identity,
			path:      repeatable.Path,
			checksum:  repeatable.Checksum,
			migration: repeatable.Migration,
		})
	}
	return provider, nil
}

// replayProvider is a registered provider that also holds repeatable
// migrations, which only a replay of an applied history assembles.
type replayProvider struct {
	*RegisteredMigrationProvider
	repeatables []*repeatableMigration
}

func (p *replayProvider) loadsRepeatableMigrations() bool { return true }

func (p *replayProvider) repeatableMigrations() []*repeatableMigration {
	return slices.Clone(p.repeatables)
}

// RepeatableTableIdentifier returns the dialect-quoted name of the record of
// repeatable migrations, or an empty string when this migrator cannot keep
// one.
//
// It exists for the callers [Migrator.MigrationLogTableIdentifier] exists
// for: a shadow replay that ran repeatable migrations leaves their record
// behind, and the record has to go before the schema is compared.
func (m *Migrator) RepeatableTableIdentifier() string {
	if m.refuseRepeatablesHere() != nil {
		return ""
	}
	return m.repeatableTable()
}
