package backend

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// A private sidecar write transaction provides real process exclusion without
// blocking the schema database. It never commits. Checking its uncommitted row
// detects rollback even when database/sql has not observed that rollback yet.
// This fixture requires one canonical database pathname, without hard-link aliases.
type sessionLock struct {
	name    string
	outcome protocol.LockResult_Outcome
	db      *sql.DB
	tx      *sql.Tx
}

func (s *session) reserveLock(scope *protocol.LockScope) (*sessionLock, bool, error) {
	if s.closed {
		return nil, false, fmt.Errorf("fixture session is closed")
	}
	if s.locks == nil {
		s.locks = make(map[string]*sessionLock)
	}
	if record := s.locks[scope.GetLockId()]; record != nil {
		if record.name != scope.GetName() {
			return nil, false, fmt.Errorf("fixture lock handle changed its name")
		}
		return record, false, nil
	}
	record := &sessionLock{name: scope.GetName(), outcome: protocol.LockResult_NOT_ACQUIRED}
	s.locks[scope.GetLockId()] = record
	return record, true, nil
}

func lockResult(request *protocol.LockRequest, outcome protocol.LockResult_Outcome) *protocol.LockResult {
	return &protocol.LockResult{Scope: proto.CloneOf(request.GetScope()), RequestId: request.GetRequestId(), Outcome: outcome}
}

// TryLock reserves an immutable handle and attempts one database acquisition.
func (p *Provider) TryLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	if err := provider.ValidateLockRequest(request); err != nil {
		return nil, err
	}
	s, err := p.lookup(request.GetScope().GetSessionId())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, fresh, err := s.reserveLock(request.GetScope())
	if err != nil {
		return nil, err
	}
	if s.lockLost {
		return lockResult(request, protocol.LockResult_LOST), nil
	}
	if !fresh {
		if record.outcome == protocol.LockResult_ACQUIRED {
			s.checkLock(ctx, record)
		}
		return lockResult(request, record.outcome), nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := record.acquire(ctx, s.path); err != nil {
		if record.outcome == protocol.LockResult_BUSY {
			return lockResult(request, record.outcome), nil
		}
		record.outcome, s.lockLost = protocol.LockResult_LOST, true
		return nil, err
	}
	if s.fault == "lost-lock-reply" {
		return nil, fmt.Errorf("fixture lost acquisition reply")
	}
	return lockResult(request, record.outcome), nil
}

func (lock *sessionLock) acquire(ctx context.Context, database string) error {
	path, err := filepath.EvalSymlinks(database)
	if err != nil {
		return err
	}
	dsn := &url.URL{Scheme: "file", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/") + fmt.Sprintf(".lock-%x", sha256.Sum256([]byte(lock.name))),
		RawQuery: url.Values{"_txlock": {"immediate"}, "_pragma": {"busy_timeout(0)"}}.Encode()}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	lock.db = db
	// The acquisition RPC ends before ownership does. The transaction belongs
	// to the session, while statements still use the caller's cancellation.
	lock.tx, err = db.BeginTx(context.WithoutCancel(ctx), nil)
	if err == nil {
		_, err = lock.tx.ExecContext(ctx, "CREATE TABLE ownership (held INTEGER NOT NULL)")
	}
	if err == nil {
		_, err = lock.tx.ExecContext(ctx, "INSERT INTO ownership VALUES (1)")
	}
	if err != nil {
		cleanupErr := lock.release()
		var databaseError *sqlite.Error
		if cleanupErr == nil && errors.As(err, &databaseError) && databaseError.Code()&0xff == sqlite3.SQLITE_BUSY {
			lock.outcome = protocol.LockResult_BUSY
		}
		return errors.Join(err, cleanupErr)
	}
	lock.outcome = protocol.LockResult_ACQUIRED
	return nil
}

// CheckLock verifies the owning transaction rather than a cached receipt.
func (p *Provider) CheckLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	if err := provider.ValidateLockRequest(request); err != nil {
		return nil, err
	}
	s, err := p.lookup(request.GetScope().GetSessionId())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, _, err := s.reserveLock(request.GetScope())
	if err != nil {
		return nil, err
	}
	if s.fault == "lose-lock-on-check" && record.tx != nil {
		_ = record.tx.Rollback()
	}
	if record.outcome == protocol.LockResult_ACQUIRED {
		s.checkLock(ctx, record)
	}
	outcome := record.outcome
	if outcome == protocol.LockResult_BUSY {
		outcome = protocol.LockResult_NOT_ACQUIRED
	}
	return lockResult(request, outcome), nil
}

func (s *session) checkLock(ctx context.Context, record *sessionLock) {
	var held int
	if s.lockLost || record.tx == nil || record.tx.QueryRowContext(ctx, "SELECT held FROM ownership").Scan(&held) != nil || held != 1 {
		record.outcome = protocol.LockResult_LOST
		s.lockLost = true
	}
}

// ReleaseLock fences unused handles and preserves loss even after cleanup.
func (p *Provider) ReleaseLock(ctx context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := provider.ValidateLockRequest(request); err != nil {
		return nil, err
	}
	s, err := p.lookup(request.GetScope().GetSessionId())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, _, err := s.reserveLock(request.GetScope())
	if err != nil {
		return nil, err
	}
	if record.outcome == protocol.LockResult_ACQUIRED {
		s.checkLock(ctx, record)
	}
	if err := record.release(); err != nil {
		record.outcome, s.lockLost = protocol.LockResult_LOST, true
		return nil, err
	}
	switch record.outcome {
	case protocol.LockResult_ACQUIRED:
		record.outcome = protocol.LockResult_RELEASED
	case protocol.LockResult_BUSY:
		record.outcome = protocol.LockResult_NOT_ACQUIRED
	}
	return lockResult(request, record.outcome), nil
}

func (lock *sessionLock) release() error {
	var failures []error
	if lock.tx != nil {
		if err := lock.tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			failures = append(failures, err)
		}
		lock.tx = nil
	}
	if lock.db != nil {
		failures = append(failures, lock.db.Close())
		lock.db = nil
	}
	return errors.Join(failures...)
}

// guardLocks runs under the session mutex before every write or commit. A lost
// lock poisons the session even after its handle is released or replaced.
func (s *session) guardLocks(ctx context.Context) error {
	for _, record := range s.locks {
		if record.outcome == protocol.LockResult_ACQUIRED {
			if s.fault == "lose-lock-before-write" && record.tx != nil {
				_ = record.tx.Rollback()
			}
			s.checkLock(ctx, record)
		}
	}
	if s.lockLost {
		return provider.ErrLockLost
	}
	return nil
}

// interruptLocks injects database ownership loss without updating cached state.
func (s *session) interruptLocks() {
	for _, lock := range s.locks {
		if lock.tx != nil {
			_ = lock.tx.Rollback()
		}
	}
}
