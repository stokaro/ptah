package migrator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"ptah.run/dbschema"
)

const providerMetadataPageSize uint32 = 256

func (m *Migrator) providerLogTable() dbschema.MigrationMetadataTable {
	return dbschema.MigrationMetadataTable{Schema: m.metadataTableSchemaName(), Name: m.migrationsTableName() + migrationLogTableSuffix}
}
func (m *Migrator) providerTagsTable() dbschema.MigrationMetadataTable {
	return dbschema.MigrationMetadataTable{Schema: m.metadataTableSchemaName(), Name: m.migrationTagsTableName()}
}

// readProviderMetadata keeps snapshot lifetime, completeness, and identity checks
// in one place. Log pairing and tag resolution remain common migrator policy.
func readProviderMetadata[T any](ctx context.Context, reader dbschema.MigrationMetadataReader[T], key func(T) (string, error)) (records []T, resultErr error) {
	if nilMetadataReader(reader) {
		return nil, fmt.Errorf("provider returned a nil metadata reader")
	}
	defer func() {
		cleanup, cancel := durableRevisionWriteContext(ctx)
		defer cancel()
		resultErr = errors.Join(resultErr, reader.Close(cleanup))
		if resultErr != nil {
			records = nil
		}
	}()
	seen := make(map[string]bool)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := reader.Next(ctx, providerMetadataPageSize)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !page.Exists && (!page.Complete || len(page.Records) != 0 || len(records) != 0) || len(page.Records) > int(providerMetadataPageSize) || !page.Complete && len(page.Records) == 0 {
			return nil, fmt.Errorf("provider returned inconsistent or unbounded metadata pages")
		}
		for _, record := range page.Records {
			identity, err := key(record)
			if err != nil {
				return nil, err
			}
			if seen[identity] {
				return nil, fmt.Errorf("provider metadata snapshot repeats a record identity")
			}
			seen[identity] = true
			records = append(records, record)
		}
		if page.Complete {
			return records, nil
		}
	}
}

func nilMetadataReader(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func logRecordKey(record dbschema.MigrationLogRecord) (string, error) {
	if record.RunID == "" || strings.ContainsRune(record.RunID, 0) || !utf8.ValidString(record.RunID) || record.Seq <= 0 || record.At.IsZero() {
		return "", fmt.Errorf("provider migration log has an invalid event identity or timestamp")
	}
	if record.Operation != "up" && record.Operation != "down" {
		return "", fmt.Errorf("provider migration log has an unknown operation")
	}
	switch MigrationLogState(record.State) {
	case MigrationLogStarted, MigrationLogApplied, MigrationLogFailed, MigrationLogRolledBack:
	default:
		return "", fmt.Errorf("provider migration log has an unknown state")
	}
	switch ActorSource(record.ActorSource) {
	case ActorUnknown, ActorProvided, ActorProcessUser:
	default:
		return "", fmt.Errorf("provider migration log has unknown actor provenance")
	}
	return record.RunID + "\x00" + strconv.FormatInt(record.Seq, 10), nil
}

func (m *Migrator) readProviderLog(ctx context.Context, store dbschema.MigrationLogStore, limit int) ([]MigrationLogAttempt, error) {
	reader, err := store.OpenRead(ctx, m.providerLogTable())
	if err != nil {
		return nil, err
	}
	records, err := readProviderMetadata(ctx, reader, logRecordKey)
	if err != nil {
		return nil, err
	}
	entries := make([]MigrationLogEntry, len(records))
	for index, record := range records {
		entries[index] = MigrationLogEntry{RunID: record.RunID, Seq: record.Seq, Operation: record.Operation, Version: record.Version,
			State: MigrationLogState(record.State), Actor: record.Actor, ActorSource: ActorSource(record.ActorSource), Checksum: record.Checksum, At: record.At, Error: record.Error}
	}
	slices.SortFunc(entries, func(left, right MigrationLogEntry) int {
		return cmp.Or(right.At.Compare(left.At), cmp.Compare(right.RunID, left.RunID), cmp.Compare(right.Seq, left.Seq))
	})
	attempts := pairMigrationLogEntries(entries)
	if limit > 0 && len(attempts) > limit {
		attempts = attempts[:limit]
	}
	return attempts, nil
}

func (m *Migrator) appendProviderLog(ctx context.Context, store dbschema.MigrationLogStore, entry MigrationLogEntry) error {
	return store.Append(ctx, m.providerLogTable(), dbschema.MigrationLogRecord{RunID: entry.RunID, Seq: entry.Seq, Operation: entry.Operation,
		Version: entry.Version, State: string(entry.State), Actor: entry.Actor, ActorSource: string(entry.ActorSource), Checksum: entry.Checksum, At: entry.At, Error: entry.Error})
}

func tagRecordKey(record dbschema.MigrationTagRecord) (string, error) {
	name, err := normalizeMigrationTag(record.Name)
	if err != nil || name != record.Name || record.RecordedAt.IsZero() || !utf8.ValidString(name) || strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("provider returned an invalid migration tag")
	}
	return name, nil
}

func (m *Migrator) recordProviderTag(ctx context.Context, store dbschema.MigrationTagsStore, name string, version int64) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	record := dbschema.MigrationTagRecord{Name: name, Version: version, RecordedAt: time.Now().UTC()}
	if _, err := tagRecordKey(record); err != nil {
		return err
	}
	if err := store.Prepare(ctx, m.providerTagsTable()); err != nil {
		return err
	}
	return store.Put(ctx, m.providerTagsTable(), record)
}

func (m *Migrator) resolveProviderTag(ctx context.Context, store dbschema.MigrationTagsStore, name string) (int64, error) {
	record, found, err := store.Lookup(ctx, m.providerTagsTable(), name)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("%w: %q", ErrMigrationTagNotFound, name)
	}
	key, err := tagRecordKey(record)
	if err != nil {
		return 0, err
	}
	if key != name {
		return 0, fmt.Errorf("provider migration tag lookup changed its identity")
	}
	return record.Version, nil
}

func (m *Migrator) readProviderTags(ctx context.Context, store dbschema.MigrationTagsStore) ([]MigrationTag, error) {
	reader, err := store.OpenRead(ctx, m.providerTagsTable())
	if err != nil {
		return nil, err
	}
	records, err := readProviderMetadata(ctx, reader, tagRecordKey)
	if err != nil {
		return nil, err
	}
	tags := make([]MigrationTag, len(records))
	for index, record := range records {
		tags[index] = MigrationTag{Tag: record.Name, Version: record.Version, RecordedAt: record.RecordedAt}
	}
	slices.SortFunc(tags, func(left, right MigrationTag) int { return cmp.Compare(left.Tag, right.Tag) })
	return tags, nil
}

func (m *Migrator) deleteProviderTag(ctx context.Context, store dbschema.MigrationTagsStore, name string) error {
	if m.conn.Writer().IsDryRun() {
		_, err := m.resolveProviderTag(ctx, store, name)
		return err
	}
	deleted, err := store.Delete(ctx, m.providerTagsTable(), name)
	if err != nil {
		return err
	}
	if !deleted {
		return fmt.Errorf("%w: %q", ErrMigrationTagNotFound, name)
	}
	return nil
}
