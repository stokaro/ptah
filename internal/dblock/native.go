package dblock

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
)

const nativeLockRetryInterval = 50 * time.Millisecond

// nativeLock checks the selected session before consulting built-in dialect
// services. Missing native support must never become a built-in or no-op lock.
func nativeLock(ctx context.Context, conn Connection, name string, timeout time.Duration) (*Lock, bool, error) {
	service, handled, err := nativeLockService(conn)
	if !handled || err != nil {
		return nil, handled, err
	}
	held, err := waitForNativeLock(ctx, service, conn.Info().Dialect, name, timeout)
	if err != nil {
		return nil, true, err
	}
	hold := newSessionHold(held.CheckOwnership, held.Release, time.Now)
	return &Lock{name: name, dialect: conn.Info().Dialect, holding: hold, release: func(ctx context.Context) error {
		err := hold.release(ctx)
		if errors.Is(err, errSessionEnded) {
			// A lost provider lock can still own resources. Keep the loss
			// while asking the original handle to release those resources.
			err = errors.Join(err, held.Release(ctx))
		}
		if errors.Is(err, dbschema.ErrSessionLockLost) || errors.Is(err, errSessionEnded) {
			return errors.Join(&LostError{Dialect: conn.Info().Dialect, Name: name}, err)
		}
		return err
	}}, true, nil
}

func nativeLockService(conn Connection) (dbschema.SessionLocker, bool, error) {
	owner, ok := conn.(dbschema.SessionLockProvider)
	if !ok {
		return nil, false, nil
	}
	service, err := owner.SessionLocker()
	if err != nil {
		return nil, true, err
	}
	return service, service != nil, nil
}

// nativePreviewLock answers a run that writes nothing on a provider-served
// target. The provider protocol has no acquisition that is known to write
// nothing, so the preview takes no lock and gets a no-op one, the way a
// built-in lock whose service would first have to create its own state does
// (see [WithPreviewLockSession]). A provider that offers no lock service is
// still refused, as it is for a run that writes.
func nativePreviewLock(conn Connection, dialect, name string) (*Lock, bool, error) {
	_, handled, err := nativeLockService(conn)
	if !handled || err != nil {
		return nil, handled, err
	}
	return &Lock{name: name, dialect: dialect}, true, nil
}

func waitForNativeLock(ctx context.Context, service dbschema.SessionLocker, dialect, name string, timeout time.Duration) (dbschema.SessionLock, error) {
	waiting := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		waiting, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	for {
		if err := waiting.Err(); err != nil {
			return nil, nativeWaitError(ctx, err, dialect, name, timeout)
		}
		held, err := service.TryLock(waiting, name)
		if err == nil {
			if held == nil || reflect.ValueOf(held).Kind() == reflect.Pointer && reflect.ValueOf(held).IsNil() {
				return nil, fmt.Errorf("%w: provider returned a nil acquired lock", ptaherr.ErrOutcomeUnknown)
			}
			if waitErr := waiting.Err(); waitErr != nil {
				cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), DefaultReleaseTimeout)
				releaseErr := held.Release(cleanup)
				cancel()
				return nil, errors.Join(nativeWaitError(ctx, waitErr, dialect, name, timeout), releaseErr)
			}
			return held, nil
		}
		if !errors.Is(err, dbschema.ErrSessionLockBusy) || errors.Is(err, ptaherr.ErrOutcomeUnknown) {
			return nil, err
		}
		if timeout < 0 {
			return nil, &TimeoutError{Dialect: dialect, Name: name, Timeout: timeout}
		}
		timer := time.NewTimer(nativeLockRetryInterval)
		select {
		case <-waiting.Done():
			timer.Stop()
			return nil, nativeWaitError(ctx, waiting.Err(), dialect, name, timeout)
		case <-timer.C:
		}
	}
}

func nativeWaitError(parent context.Context, err error, dialect, name string, timeout time.Duration) error {
	if parent.Err() != nil {
		return parent.Err()
	}
	if timeout > 0 && errors.Is(err, context.DeadlineExceeded) {
		return &TimeoutError{Dialect: dialect, Name: name, Timeout: timeout}
	}
	return err
}
