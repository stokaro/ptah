package ydb

import (
	"context"
	"errors"
	"fmt"
	"time"

	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
)

// dropListedTable drops a row table the teardown found in a scheme listing.
// YDB can list a replica before its SQL service can drop it. Retry only the
// definitive SCHEME_ERROR refusal, for the same budget the reader gives a
// listed table to become describable. A persistent refusal stays an error;
// neither a missing path nor an access failure is treated as a successful drop.
func (w *Writer) dropListedTable(ctx context.Context, statement string) error {
	err := w.ExecuteSQL(ctx, statement)
	ctx, cancel := context.WithTimeout(ctx, tableSettleBudget)
	defer cancel()
	wait := 50 * time.Millisecond
	for errors.Is(err, ErrPathNotVisible) || ydbsdk.IsOperationErrorSchemeError(err) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w; stopped waiting to drop the listed table: %w", err, ctx.Err())
		case <-time.After(wait):
		}
		err = w.ExecuteSQL(ctx, statement)
		wait = min(2*wait, time.Second)
	}
	return err
}
