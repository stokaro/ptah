package backend

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/url"
	"path/filepath"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/runtime"
)

// lock is one lock ID of a session. Ptah takes a lock before it applies a
// plan, so two runs never change one database at once.
//
// The tutorial engine holds a lock as a write transaction on a sidecar file
// beside the database, one file per lock name. SQLite lets one connection at
// a time hold a write transaction on a file, in any process, so a second
// attempt is BUSY. The transaction never commits: rolling it back releases
// the lock, and so does the end of the provider process.
type lock struct {
	name    string
	outcome protocol.LockResult_Outcome
	db      *sql.DB
	tx      *sql.Tx
}

// TryLock makes one attempt to take a lock and does not wait. A lock ID is
// used once: a second request with the same ID gets the answer of the first.
func (p *Provider) TryLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	current, err := p.lookup(request.GetScope().GetSessionId())
	if err != nil {
		return nil, err
	}
	current.mu.Lock()
	defer current.mu.Unlock()
	record, fresh, err := current.reserve(request.GetScope())
	if err != nil {
		return nil, err
	}
	switch {
	case current.lost:
		return receipt(request, protocol.LockResult_LOST), nil
	case !fresh:
		current.verify(ctx, record)
		return receipt(request, record.outcome), nil
	}
	if err := record.acquire(ctx, current.path); err != nil {
		return nil, err
	}
	return receipt(request, record.outcome), nil
}

// CheckLock proves that the session still holds the lock now, rather than
// repeating an earlier answer.
func (p *Provider) CheckLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	current, err := p.lookup(request.GetScope().GetSessionId())
	if err != nil {
		return nil, err
	}
	current.mu.Lock()
	defer current.mu.Unlock()
	record, _, err := current.reserve(request.GetScope())
	if err != nil {
		return nil, err
	}
	current.verify(ctx, record)
	if record.outcome == protocol.LockResult_BUSY {
		return receipt(request, protocol.LockResult_NOT_ACQUIRED), nil
	}
	return receipt(request, record.outcome), nil
}

// ReleaseLock gives a lock back. Releasing an ID that never took a lock
// fences it, so a late TryLock with that ID acquires nothing.
func (p *Provider) ReleaseLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	current, err := p.lookup(request.GetScope().GetSessionId())
	if err != nil {
		return nil, err
	}
	current.mu.Lock()
	defer current.mu.Unlock()
	record, _, err := current.reserve(request.GetScope())
	if err != nil {
		return nil, err
	}
	current.verify(ctx, record)
	if err := record.release(); err != nil {
		record.outcome, current.lost = protocol.LockResult_LOST, true
		return nil, runtime.ErrorWithReason(codes.Internal, "the lock could not be released")
	}
	switch record.outcome {
	case protocol.LockResult_ACQUIRED:
		record.outcome = protocol.LockResult_RELEASED
	case protocol.LockResult_BUSY:
		record.outcome = protocol.LockResult_NOT_ACQUIRED
	}
	return receipt(request, record.outcome), nil
}

// reserve returns the record of a lock ID and whether this call created it.
// The caller holds the session's mutex. The SDK has already refused a request
// without a session, a lock ID or a name.
func (s *session) reserve(scope *protocol.LockScope) (*lock, bool, error) {
	if s.locks == nil {
		s.locks = make(map[string]*lock)
	}
	if record := s.locks[scope.GetLockId()]; record != nil {
		if record.name != scope.GetName() {
			return nil, false, runtime.ErrorWithReason(codes.InvalidArgument, "a lock ID names one lock")
		}
		return record, false, nil
	}
	record := &lock{name: scope.GetName(), outcome: protocol.LockResult_NOT_ACQUIRED}
	s.locks[scope.GetLockId()] = record
	return record, true, nil
}

// acquire opens the lock's sidecar file and starts the write transaction
// that holds it.
func (l *lock) acquire(ctx context.Context, database string) error {
	path, err := filepath.EvalSymlinks(database)
	if err != nil {
		return runtime.ErrorWithReason(codes.Unavailable, "the database file cannot be found")
	}
	digest := sha256.Sum256([]byte(l.name))
	sidecar := url.URL{
		Scheme:   "file",
		Path:     "/" + strings.TrimPrefix(filepath.ToSlash(path), "/") + ".lock-" + hex.EncodeToString(digest[:]),
		RawQuery: url.Values{"_txlock": {"immediate"}, "_pragma": {"busy_timeout(0)"}}.Encode(),
	}
	if l.db, err = sql.Open("sqlite", sidecar.String()); err != nil {
		return runtime.ErrorWithReason(codes.Unavailable, "the lock file cannot be opened")
	}
	l.db.SetMaxOpenConns(1)
	// The transaction outlives this call: database/sql would roll it back
	// when the call's context ends.
	l.tx, err = l.db.BeginTx(context.WithoutCancel(ctx), nil)
	if err == nil {
		_, err = l.tx.ExecContext(ctx, "CREATE TABLE ownership (held INTEGER NOT NULL)")
	}
	if err == nil {
		_, err = l.tx.ExecContext(ctx, "INSERT INTO ownership VALUES (1)")
	}
	if err == nil {
		l.outcome = protocol.LockResult_ACQUIRED
		return nil
	}
	if cleanup := l.release(); cleanup != nil {
		return runtime.ErrorWithReason(codes.Internal, "a failed lock attempt could not be cleaned up")
	}
	var refused *sqlite.Error
	if errors.As(err, &refused) && refused.Code()&0xff == sqlite3.SQLITE_BUSY {
		l.outcome = protocol.LockResult_BUSY
		return nil
	}
	return runtime.ErrorWithReason(codes.Unavailable, "the lock file cannot be written")
}

// verify reads the row only the lock's open transaction can see. When the
// read fails, the session has lost the lock, and it stays fenced.
func (s *session) verify(ctx context.Context, record *lock) {
	if record.outcome != protocol.LockResult_ACQUIRED {
		return
	}
	var held int
	if s.lost || record.tx == nil || record.tx.QueryRowContext(ctx, "SELECT held FROM ownership").Scan(&held) != nil || held != 1 {
		record.outcome, s.lost = protocol.LockResult_LOST, true
	}
}

// guard verifies every lock the session holds. Execute calls it before each
// unit, because a statement must not run after the lock that protects it is
// gone.
func (s *session) guard(ctx context.Context) bool {
	for _, record := range s.locks {
		s.verify(ctx, record)
	}
	return !s.lost
}

// release ends the lock's transaction and closes its sidecar file.
func (l *lock) release() error {
	var failures []error
	if l.tx != nil {
		if err := l.tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			failures = append(failures, err)
		}
		l.tx = nil
	}
	if l.db != nil {
		failures = append(failures, l.db.Close())
		l.db = nil
	}
	return errors.Join(failures...)
}

// receipt answers a lock request with its own scope and request ID.
func receipt(request *protocol.LockRequest, outcome protocol.LockResult_Outcome) *protocol.LockResult {
	return &protocol.LockResult{Scope: proto.CloneOf(request.GetScope()), RequestId: request.GetRequestId(), Outcome: outcome}
}
