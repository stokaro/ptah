package migrator

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// appliedRepeatables returns the repeatable migrations a replay of the
// history runs, in identity order, which is the order an up run executes
// them in: for each record, the content in effect in the database.
//
// That is the file when it hashes to the digest recorded as applied, and
// otherwise the body the record keeps for that digest, for a file that
// changed or is gone. A transactional attempt records its outcome in its own
// transaction, so a record whose last such attempt is unfinished or failed
// describes the content that applied before it, or none. A file with no
// record never ran and is left out. Where nothing says what the database
// holds, the answer is an [UnreplayableHistoryError]: an attempt outside a
// transaction that did not finish, and a record that keeps no body for the
// content it applied.
func (m *Migrator) appliedRepeatables(ctx context.Context) ([]AppliedRepeatable, error) {
	state, err := m.readRepeatableState(ctx)
	if err != nil {
		return nil, fmt.Errorf("read the applied repeatable migrations: %w", err)
	}
	reader, _ := m.migrationProvider.(recordedRepeatableSource)
	var applied []AppliedRepeatable
	for _, record := range state.records() {
		content, err := state.appliedContent(record, reader)
		if err != nil {
			return nil, err
		}
		if content == nil {
			continue
		}
		applied = append(applied, AppliedRepeatable{
			Identity:  record.Identity,
			Path:      content.path,
			Checksum:  content.checksum,
			Migration: content.migration,
		})
	}
	slices.SortFunc(applied, func(a, b AppliedRepeatable) int { return strings.Compare(a.Identity, b.Identity) })
	return applied, nil
}

// appliedContent is the content of record in effect in the database, nil
// when none is.
func (s *repeatableState) appliedContent(
	record RepeatableRecord,
	reader recordedRepeatableSource,
) (*repeatableMigration, error) {
	switch {
	case record.State == RepeatableStatePending:
		return nil, nil
	case record.State == RepeatableStateApplied:
		return s.repeatable(record.Identity), nil
	case (record.State == RepeatableStateRunning || record.State == RepeatableStateFailed) && !record.NoTransaction:
		if record.AppliedChecksum == "" {
			return nil, nil
		}
		return s.recordedContent(record, reader, UnreplayableDirty,
			fmt.Sprintf("its last attempt is %s and rolled back, leaving the content that applied before it", record.State))
	case record.dirty():
		return nil, &UnreplayableHistoryError{
			Reason: UnreplayableDirty, Repeatable: record.Identity,
			Detail: fmt.Sprintf("its last attempt is %s and ran outside a transaction, "+
				"so what it left in the database is unknown", record.State),
		}
	case record.Orphaned:
		return s.recordedContent(record, reader, UnreplayableMissingFile,
			"the database records it as applied and the migration directory has no file for it")
	default:
		return s.recordedContent(record, reader, UnreplayableChanged,
			fmt.Sprintf("%s changed after it applied", record.Path))
	}
}

// recordedContent is the content record applied, read from the directory when
// a file still hashes to it and from the record's body otherwise. reason and
// detail describe the record for the refusal a missing or damaged body gives.
func (s *repeatableState) recordedContent(
	record RepeatableRecord,
	reader recordedRepeatableSource,
	reason, detail string,
) (*repeatableMigration, error) {
	if file := s.repeatable(record.Identity); file != nil && file.checksum == record.AppliedChecksum {
		return file, nil
	}
	refuse := func(problem string) error {
		return &UnreplayableHistoryError{Reason: reason, Repeatable: record.Identity, Detail: detail + ", and " + problem}
	}
	body := s.rows[record.Identity].body
	switch {
	case body == nil:
		return nil, refuse("its record keeps no body for the content that applied")
	case repeatableChecksum(body) != record.AppliedChecksum:
		return nil, refuse("the body its record keeps does not hash to the digest recorded as applied")
	case reader == nil:
		return nil, refuse("the migration provider cannot read the body its record keeps")
	}
	return reader.repeatableFromRecord(record.Identity, record.Path, body)
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
