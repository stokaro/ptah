// Package repeatables keeps the fixture's record of repeatable migrations on
// SQLite, the service contract 25.44 adds. It lives apart from package
// backend, which keeps to contract 25.0, so a provider built from that
// contract's SDK still compiles the backend.
package repeatables

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// Provider is the transactional fixture provider with the record of
// repeatable migrations.
type Provider struct {
	backend.TransactionalProvider
}

func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// repeatableDDL is the only layout the fixture keeps the record of repeatable
// migrations in. The columns are ordinary, so a test reads the stored facts
// back with SQL. state holds the protocol's enum number.
func repeatableDDL(table string) string {
	return "CREATE TABLE " + table + ` ("identity" TEXT NOT NULL PRIMARY KEY, "state" INTEGER NOT NULL, "checksum" TEXT NOT NULL, ` +
		`"attempt_checksum" TEXT NOT NULL, "attempt_order" INTEGER NOT NULL, "transactional" INTEGER NOT NULL, "run_id" TEXT NOT NULL, ` +
		`"attempted_at_seconds" INTEGER NOT NULL, "attempted_at_nanos" INTEGER NOT NULL, "applied" INTEGER NOT NULL, ` +
		`"applied_at_seconds" INTEGER NOT NULL, "applied_at_nanos" INTEGER NOT NULL, "error" TEXT NOT NULL, "body" BLOB)`
}

// repeatableColumns are the columns a snapshot reads, in the order
// scanRepeatableRow scans them.
const repeatableColumns = `"identity", "state", "checksum", "attempt_checksum", "attempt_order", "transactional", "run_id", ` +
	`"attempted_at_seconds", "attempted_at_nanos", "applied", "applied_at_seconds", "applied_at_nanos", "error", "body" IS NOT NULL`

func repeatableTable(scope *protocol.RepeatableScope) (string, error) {
	if scope.GetSchema() != "" && scope.GetSchema() != "main" {
		return "", fmt.Errorf("fixture repeatable migration record supports storage in main only")
	}
	return quote(scope.GetTable()), nil
}

// checkRepeatableTable reports whether the record exists, and refuses one in
// another layout or with triggers.
func checkRepeatableTable(ctx context.Context, runner backend.Querier, scope *protocol.RepeatableScope) (bool, error) {
	table, err := repeatableTable(scope)
	if err != nil {
		return false, err
	}
	var kind, definition string
	err = runner.QueryRowContext(ctx, "SELECT type, sql FROM main.sqlite_schema WHERE name = ? COLLATE NOCASE", scope.GetTable()).Scan(&kind, &definition)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if kind != "table" || definition != repeatableDDL(table) {
		return false, fmt.Errorf("fixture repeatable migration record has an incompatible layout")
	}
	var triggers int
	query := "SELECT COUNT(*) FROM (SELECT type, tbl_name FROM main.sqlite_schema UNION ALL SELECT type, tbl_name FROM temp.sqlite_schema) " +
		"WHERE type = 'trigger' AND tbl_name = ? COLLATE NOCASE"
	if err := runner.QueryRowContext(ctx, query, scope.GetTable()).Scan(&triggers); err != nil {
		return false, err
	}
	if triggers != 0 {
		return false, fmt.Errorf("fixture repeatable migration record has triggers")
	}
	return true, nil
}

func repeatableResult(scope *protocol.RepeatableScope, id string, outcome protocol.RepeatableResult_Outcome) *protocol.RepeatableResult {
	return &protocol.RepeatableResult{Scope: proto.CloneOf(scope), RequestId: id, Outcome: outcome}
}

// PrepareRepeatables creates a missing record. An existing one must have the
// fixture's layout and no triggers; another layout is never upgraded.
func (p *Provider) PrepareRepeatables(ctx context.Context, request *protocol.PrepareRepeatablesRequest) (*protocol.RepeatableResult, error) {
	if err := provider.ValidatePrepareRepeatablesRequest(request); err != nil {
		return nil, err
	}
	table, err := repeatableTable(request.GetScope())
	if err != nil {
		return nil, err
	}
	outcome := protocol.RepeatableResult_REFUSED
	err = p.WithSession(ctx, request.GetScope().GetSessionId(), "", func(access backend.Access) error {
		exists, err := checkRepeatableTable(ctx, access.Querier, request.GetScope())
		if err != nil {
			return nil
		}
		if !exists {
			ddl := strings.Replace(repeatableDDL(table), "CREATE TABLE ", "CREATE TABLE IF NOT EXISTS main.", 1)
			if _, err := access.Querier.ExecContext(ctx, ddl); err != nil {
				return err
			}
		}
		if exists, err := checkRepeatableTable(ctx, access.Querier, request.GetScope()); err == nil && exists {
			outcome = protocol.RepeatableResult_APPLIED
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return repeatableResult(request.GetScope(), request.GetRequestId(), outcome), nil
}

// ReadRepeatables reads every row in one query. A missing record stays
// distinguishable from an empty one, and reading creates nothing.
func (p *Provider) ReadRepeatables(ctx context.Context, request *protocol.ReadRepeatablesRequest) (*protocol.RepeatableSnapshot, error) {
	if err := provider.ValidateReadRepeatablesRequest(request); err != nil {
		return nil, err
	}
	table, err := repeatableTable(request.GetScope())
	if err != nil {
		return nil, err
	}
	result := &protocol.RepeatableSnapshot{Scope: proto.CloneOf(request.GetScope()), Complete: true}
	err = p.WithSession(ctx, request.GetScope().GetSessionId(), "", func(access backend.Access) error {
		exists, err := checkRepeatableTable(ctx, access.Querier, request.GetScope())
		if err != nil || !exists {
			return err
		}
		result.Exists = true
		result.Rows, err = readRows(ctx, access.Querier, table, request.GetMaxRows())
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := provider.ValidateRepeatableSnapshotReply(request, result); err != nil {
		return nil, err
	}
	return result, nil
}

// readRows reads one row past limit, so a record over it fails the reply's
// validation rather than reading as complete.
func readRows(ctx context.Context, runner backend.Querier, table string, limit uint32) ([]*protocol.RepeatableRow, error) {
	rows, err := runner.QueryContext(ctx, "SELECT "+repeatableColumns+" FROM main."+table+" ORDER BY \"identity\" LIMIT ?", int64(limit)+1)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []*protocol.RepeatableRow
	for rows.Next() {
		row, err := scanRepeatableRow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func scanRepeatableRow(rows *sql.Rows) (*protocol.RepeatableRow, error) {
	row := &protocol.RepeatableRow{}
	var state int32
	if err := rows.Scan(&row.Identity, &state, &row.Checksum, &row.AttemptChecksum, &row.AttemptOrder, &row.Transactional, &row.RunId,
		&row.AttemptedAtSeconds, &row.AttemptedAtNanos, &row.Applied, &row.AppliedAtSeconds, &row.AppliedAtNanos, &row.Error, &row.BodyKept); err != nil {
		return nil, err
	}
	row.State = protocol.RepeatableRow_State(state)
	return row, nil
}

// ReadRepeatableBody reads the bytes kept for one identity. An empty body is
// kept and differs from none.
func (p *Provider) ReadRepeatableBody(ctx context.Context, request *protocol.ReadRepeatableBodyRequest) (*protocol.RepeatableBody, error) {
	if err := provider.ValidateReadRepeatableBodyRequest(request); err != nil {
		return nil, err
	}
	table, err := repeatableTable(request.GetScope())
	if err != nil {
		return nil, err
	}
	result := &protocol.RepeatableBody{Scope: proto.CloneOf(request.GetScope()), Identity: request.GetIdentity()}
	err = p.WithSession(ctx, request.GetScope().GetSessionId(), "", func(access backend.Access) error {
		if exists, err := checkRepeatableTable(ctx, access.Querier, request.GetScope()); err != nil || !exists {
			return err
		}
		var body []byte
		var kept bool
		err := access.Querier.QueryRowContext(ctx, `SELECT "body", "body" IS NOT NULL FROM main.`+table+` WHERE "identity" = ?`, request.GetIdentity()).Scan(&body, &kept)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		result.Found, result.Kept = true, kept
		if kept {
			result.Body = body
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// MutateRepeatable writes one row in one statement, so of two claims against
// one precondition exactly one changes the row, across independent sessions
// too. A write that changed nothing reads the row back to name its run. Only
// a completion runs in the transaction its scope names.
func (p *Provider) MutateRepeatable(ctx context.Context, request *protocol.MutateRepeatableRequest) (*protocol.RepeatableResult, error) {
	if err := provider.ValidateMutateRepeatableRequest(request); err != nil {
		return nil, err
	}
	table, err := repeatableTable(request.GetScope())
	if err != nil {
		return nil, err
	}
	result := repeatableResult(request.GetScope(), request.GetRequestId(), protocol.RepeatableResult_APPLIED)
	err = p.WithSession(ctx, request.GetScope().GetSessionId(), request.GetScope().GetTransactionId(), func(access backend.Access) error {
		if exists, err := checkRepeatableTable(ctx, access.Querier, request.GetScope()); err != nil || !exists {
			result.Outcome = protocol.RepeatableResult_REFUSED
			return nil
		}
		query, args, identity := repeatableMutation("main."+table, request)
		changed, err := execRows(ctx, access.Querier, query, args...)
		if err != nil {
			return err
		}
		if access.Fault == "lost-repeatable-claim" && request.GetClaim() != nil {
			return fmt.Errorf("fixture lost repeatable migration claim acknowledgment")
		}
		if remove := request.GetRemove(); remove != nil {
			result.Removed = changed == 1
			return nil
		}
		if changed == 1 {
			return nil
		}
		return unappliedRepeatable(ctx, access.Querier, table, identity, request, result)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// unappliedRepeatable answers a claim, completion or failure that changed no
// row. A row another run holds, or none, is a CONFLICT; a completion whose
// run holds the row with another digest is REFUSED.
func unappliedRepeatable(ctx context.Context, runner backend.Querier, table, identity string,
	request *protocol.MutateRepeatableRequest, result *protocol.RepeatableResult,
) error {
	var holder string
	err := runner.QueryRowContext(ctx, `SELECT "run_id" FROM main.`+table+` WHERE "identity" = ?`, identity).Scan(&holder)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	result.Outcome = protocol.RepeatableResult_CONFLICT
	if complete := request.GetComplete(); complete != nil && holder == complete.GetRunId() {
		result.Outcome = protocol.RepeatableResult_REFUSED
		return nil
	}
	result.HolderRunId = holder
	return nil
}

// repeatableMutation is the one statement of a write, its values, and the
// identity it writes.
func repeatableMutation(table string, request *protocol.MutateRepeatableRequest) (string, []any, string) {
	switch {
	case request.GetClaim() != nil:
		claim := request.GetClaim()
		if claim.GetInsert() {
			return "INSERT INTO " + table + ` ("identity", "state", "checksum", "attempt_checksum", "attempt_order", "transactional", ` +
					`"run_id", "attempted_at_seconds", "attempted_at_nanos", "applied", "applied_at_seconds", "applied_at_nanos", "error") ` +
					`VALUES (?, ?, '', ?, ?, ?, ?, ?, ?, 0, 0, 0, '') ON CONFLICT("identity") DO NOTHING`,
				[]any{claim.GetIdentity(), int32(protocol.RepeatableRow_RUNNING), claim.GetAttemptChecksum(), claim.GetAttemptOrder(),
					claim.GetTransactional(), claim.GetRunId(), claim.GetAttemptedAtSeconds(), claim.GetAttemptedAtNanos()},
				claim.GetIdentity()
		}
		return "UPDATE " + table + ` SET "state" = ?, "attempt_checksum" = ?, "attempt_order" = ?, "transactional" = ?, "run_id" = ?, ` +
				`"attempted_at_seconds" = ?, "attempted_at_nanos" = ?, "error" = '' WHERE "identity" = ? AND "run_id" = ?`,
			[]any{int32(protocol.RepeatableRow_RUNNING), claim.GetAttemptChecksum(), claim.GetAttemptOrder(), claim.GetTransactional(),
				claim.GetRunId(), claim.GetAttemptedAtSeconds(), claim.GetAttemptedAtNanos(), claim.GetIdentity(), claim.GetExpectedRunId()},
			claim.GetIdentity()
	case request.GetComplete() != nil:
		complete := request.GetComplete()
		body := complete.GetBody()
		if body == nil {
			body = make([]byte, 0)
		}
		return "UPDATE " + table + ` SET "state" = ?, "checksum" = ?, "body" = ?, "applied" = 1, "applied_at_seconds" = ?, ` +
				`"applied_at_nanos" = ?, "error" = '' WHERE "identity" = ? AND "run_id" = ? AND "attempt_checksum" = ?`,
			[]any{int32(protocol.RepeatableRow_APPLIED), complete.GetChecksum(), body, complete.GetAppliedAtSeconds(), complete.GetAppliedAtNanos(),
				complete.GetIdentity(), complete.GetRunId(), complete.GetChecksum()},
			complete.GetIdentity()
	case request.GetFail() != nil:
		fail := request.GetFail()
		return "UPDATE " + table + ` SET "state" = ?, "error" = ? WHERE "identity" = ? AND "run_id" = ?`,
			[]any{int32(protocol.RepeatableRow_FAILED), fail.GetError(), fail.GetIdentity(), fail.GetRunId()},
			fail.GetIdentity()
	default:
		remove := request.GetRemove()
		return "DELETE FROM " + table + ` WHERE "identity" = ?`, []any{remove.GetIdentity()}, remove.GetIdentity()
	}
}

// execRows runs one write and returns how many rows it changed, which is at
// most one: every statement addresses one identity.
func execRows(ctx context.Context, runner backend.Querier, query string, args ...any) (int64, error) {
	result, err := runner.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if changed < 0 || changed > 1 {
		return 0, fmt.Errorf("fixture repeatable migration write changed an unexpected number of rows")
	}
	return changed, nil
}
