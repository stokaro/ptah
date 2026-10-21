package providerhost

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	providerruntime "github.com/stokaro/ptah/provider-sdk/runtime"

	"ptah.run/dbschema"
)

type metadataReader[T any] struct {
	mu                       sync.Mutex
	store                    metadataStore
	scope                    *protocol.MetadataScope
	id                       string
	page                     uint64
	exists, complete, closed bool
	failure, closeErr        error
	seen                     map[string]bool
	decode                   func(*protocol.MetadataPage) []T
	identity                 func(T) string
}

func (m migrationLogStore) OpenRead(ctx context.Context, table dbschema.MigrationMetadataTable) (dbschema.MigrationMetadataReader[dbschema.MigrationLogRecord], error) {
	return openMetadataReader(ctx, m.metadataStore, table, metadataLogs, func(row dbschema.MigrationLogRecord) string {
		return row.RunID + "\x00" + strconv.FormatInt(row.Seq, 10)
	})
}
func (m migrationTagsStore) OpenRead(ctx context.Context, table dbschema.MigrationMetadataTable) (dbschema.MigrationMetadataReader[dbschema.MigrationTagRecord], error) {
	return openMetadataReader(ctx, m.metadataStore, table, metadataTags, func(row dbschema.MigrationTagRecord) string { return row.Name })
}
func metadataLogs(page *protocol.MetadataPage) []dbschema.MigrationLogRecord {
	records := make([]dbschema.MigrationLogRecord, len(page.GetLogs()))
	for i, row := range page.GetLogs() {
		records[i] = metadataLog(row)
	}
	return records
}
func metadataTags(page *protocol.MetadataPage) []dbschema.MigrationTagRecord {
	records := make([]dbschema.MigrationTagRecord, len(page.GetTags()))
	for i, row := range page.GetTags() {
		records[i] = metadataTag(row)
	}
	return records
}
func openMetadataReader[T any](ctx context.Context, store metadataStore, table dbschema.MigrationMetadataTable, decode func(*protocol.MetadataPage) []T, identity func(T) string) (dbschema.MigrationMetadataReader[T], error) {
	scope, err := store.scope(table)
	if err != nil {
		return nil, err
	}
	reader := &metadataReader[T]{store: store, scope: scope, id: rand.Text(), seen: make(map[string]bool), decode: decode, identity: identity}
	err = store.use(ctx, func() error {
		_, err := store.session.target.host.client.OpenMetadataRead(ctx, reader.request())
		return err
	})
	if err != nil {
		return nil, err
	}
	// A late successful open still owns a resource. Cancellation does not excuse
	// losing that handle when the caller can no longer use the result.
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, reader.cleanup(ctx))
	}
	return reader, nil
}
func (r *metadataReader[T]) request() *protocol.MetadataReadRequest {
	return &protocol.MetadataReadRequest{Scope: r.scope, ReaderId: r.id, RequestId: rand.Text()}
}
func (r *metadataReader[T]) Next(ctx context.Context, maxRecords uint32) (dbschema.MigrationMetadataPage[T], error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return dbschema.MigrationMetadataPage[T]{}, errors.Join(fmt.Errorf("provider metadata reader is closed"), r.failure, r.closeErr)
	}
	if r.complete {
		return dbschema.MigrationMetadataPage[T]{}, fmt.Errorf("provider metadata reader is complete")
	}
	request := &protocol.ReadMetadataPageRequest{Reader: r.request(), Page: r.page, MaxRecords: maxRecords}
	if err := provider.ValidateReadMetadataPageRequest(request); err != nil {
		return dbschema.MigrationMetadataPage[T]{}, err
	}
	var page dbschema.MigrationMetadataPage[T]
	err := r.store.use(ctx, func() error {
		result, err := r.store.session.target.host.client.ReadMetadataPage(ctx, request)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.page != 0 && r.exists != result.GetExists() {
			return fmt.Errorf("provider metadata snapshot changed its existence")
		}
		records := r.decode(result)
		for _, row := range records {
			key := r.identity(row)
			if r.seen[key] {
				return fmt.Errorf("provider metadata snapshot repeats a record identity")
			}
			r.seen[key] = true
		}
		page = dbschema.MigrationMetadataPage[T]{Exists: result.GetExists(), Complete: result.GetComplete(), Records: records}
		return nil
	})
	if err != nil {
		r.failure = err
		return dbschema.MigrationMetadataPage[T]{}, errors.Join(err, r.cleanup(ctx))
	}
	r.exists, r.complete = page.Exists, page.Complete
	r.page++
	return page, nil
}
func (r *metadataReader[T]) cleanup(ctx context.Context) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), providerruntime.CallTimeout)
	defer cancel()
	return r.close(cleanup)
}
func (r *metadataReader[T]) Close(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.close(ctx)
}
func (r *metadataReader[T]) close(ctx context.Context) error {
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	r.seen = nil
	// Cleanup must remain available after failed execution or transaction finish.
	// A completed session close already released its readers; synchronize with
	// that close before consuming its retained outcome.
	if r.store.session.closed.Load() {
		if err := r.store.session.Close(); err != nil {
			r.closeErr = nativeOutcomeError(errors.Join(provider.ErrOutcomeUnknown, err))
		}
		return r.closeErr
	}
	_, err := r.store.session.target.host.client.CloseMetadataRead(ctx, r.request())
	r.closeErr = nativeOutcomeError(err)
	return r.closeErr
}
