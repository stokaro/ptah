package dbschema

import (
	"context"
	"errors"
	"fmt"

	"ptah.run/core/ptaherr"
)

// ErrSessionLockBusy means another owner prevented a confirmed acquisition.
var ErrSessionLockBusy = errors.New("provider session lock is busy")

// ErrSessionLockLost means ownership is lost or cannot be established anymore.
var ErrSessionLockLost = errors.New("provider session lock ownership was lost")

// SessionLock is exclusion owned by the session that acquired it. Check must
// establish current ownership rather than replay an acquisition receipt.
// Release is idempotent and preserves any earlier loss or uncertain outcome.
// Session close releases the underlying resources even after a failed release.
type SessionLock interface {
	Check(context.Context) error
	Release(context.Context) error
}

// SessionLocker attempts one nonblocking acquisition with a fresh handle.
// Only ErrSessionLockBusy permits another attempt. An error returns no usable
// lock and cleans up partial acquisition. An uncertain outcome must match
// ptaherr.ErrOutcomeUnknown, including errors that also match a busy result.
// Locks guard this session's writes and commits; they never belong to an
// unrelated connection. Lost ownership permanently fences further writes.
type SessionLocker interface {
	TryLock(context.Context, string) (SessionLock, error)
}

// SessionLockProvider exposes the lock service of the implementing session.
// An unsupported service returns an error, never a no-op implementation.
type SessionLockProvider interface {
	SessionLocker() (SessionLocker, error)
}

// SessionLocker returns the provider's lock service on this execution session.
// SQL connections return nil without error and retain their SQL lock path.
// Native connections without a service refuse rather than selecting that path.
// Acquire locks before beginning a transaction, on the parent session.
func (dc *DatabaseConnection) SessionLocker() (SessionLocker, error) {
	if dc == nil {
		return nil, fmt.Errorf("session locking requires a database connection")
	}
	if dc.native == nil {
		return nil, nil
	}
	if dc.executor != nil {
		return nil, fmt.Errorf("%w: acquire provider locks before beginning a transaction", ptaherr.ErrUnsupportedFeature)
	}
	owner, ok := dc.native.(SessionLockProvider)
	if !ok || nilNativeService(owner) {
		return nil, fmt.Errorf("%w: the provider session has no lock service", ptaherr.ErrUnsupportedFeature)
	}
	service, err := owner.SessionLocker()
	if err != nil {
		return nil, err
	}
	if nilNativeService(service) {
		return nil, fmt.Errorf("native provider returned a nil lock service")
	}
	return service, nil
}
