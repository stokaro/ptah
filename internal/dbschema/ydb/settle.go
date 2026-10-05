package ydb

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
)

// tableSettleBudget is how long a read waits for a row table whose directory
// lists it and the table service does not describe, before it gives up.
//
// The listing and the table service each answer from a view of the scheme
// that follows a change in a moment of its own, so a table another operation
// is creating or dropping can be listed while DescribeTable answers
// SCHEME_ERROR -- which 25.1.4.7 sends with no issue text for a path that does
// not exist. An async replication creates its replica tables after CREATE
// ASYNC REPLICATION returns, and DROP ASYNC REPLICATION ... CASCADE drops them,
// so a read run next to either meets that state: on GitHub's runners against
// local-ydb 25.1.4.7, a replica listed and not described failed the read
// (`describe YDB table /local/ptah_ydb_repl/rep: SCHEME_ERROR: no issue
// text`). Neither the listing nor DescribePath carries a state that says a
// path is being created. Measured against 25.1.4.7 on another host, in 50
// create and drop cycles at full speed and with the server held to 0.4 of a
// CPU, the listing named the replica 90 to 150 ms after DescribeTable first
// described it and dropped it as the drop returned, so the wait is short where
// the state occurs at all.
const tableSettleBudget = 10 * time.Second

// errTableGone reports a row table its directory listed that a later listing
// no longer holds: another operation dropped it while the read ran.
var errTableGone = errors.New("the table is no longer there")

// describeListedTable describes the row table name in the directory schema,
// which a listing of the directory named.
//
// Where the table service answers SCHEME_ERROR, it asks again, listing the
// directory each time, until the table is described, the listing no longer
// holds it, or [tableSettleBudget] runs out: a table being created is then
// described as it is once created, and one being dropped is reported as
// errTableGone, which the caller reads as a table the database no longer
// holds. A table still listed and still not described when the budget runs
// out fails the read with the last answer. The context ends the wait early.
func describeListedTable(
	ctx context.Context,
	source Source,
	directory, name string,
) (*Ydb_Table.DescribeTableResult, error) {
	absolute := path.Join(directory, name)
	deadline := time.Now().Add(tableSettleBudget)
	wait := 50 * time.Millisecond
	for {
		described, err := source.DescribeTable(ctx, absolute)
		if !isSchemeError(err) {
			return described, err
		}
		listed, listErr := listsTable(ctx, source, directory, name)
		switch {
		case listErr != nil:
			return nil, listErr
		case !listed:
			return nil, errTableGone
		case time.Now().After(deadline):
			return nil, fmt.Errorf("%w; the directory has listed the table for %s and YDB has not described it, "+
				"so another operation may be creating or dropping it", err, tableSettleBudget)
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w; the read stopped waiting for the table YDB lists and does not describe: %w",
				err, ctx.Err())
		case <-time.After(wait):
		}
		wait = min(2*wait, time.Second)
	}
}

// listsTable reports whether the directory lists a row table name. A
// directory that is gone holds nothing.
func listsTable(ctx context.Context, source Source, directory, name string) (bool, error) {
	_, entries, err := source.ListDirectory(ctx, directory)
	if isSchemeError(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(entries, func(entry *Ydb_Scheme.Entry) bool {
		return entry.GetName() == name && entry.GetType() == Ydb_Scheme.Entry_TABLE
	}), nil
}
