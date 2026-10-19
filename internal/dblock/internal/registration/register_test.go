// Package registration_test registers an engine Ptah does not ship, with a
// lock service of its own, and takes its lock through internal/dblock.
//
// The registrations last for the rest of the process, so the directory holds
// only this test and they stay in a test binary of their own. Beside
// internal/dblock they would be visible to every other test there, such as the
// ones that list SupportedDialects.
package registration_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dblockext"
	"ptah.run/internal/dbschema/dbtest"
)

// builtinLocks are the dialects whose locks Ptah ships, in registry order.
var builtinLocks = []string{"postgres", "mysql", "mariadb", "sqlserver", "yugabytedb", "ydb"}

func described(name string, aliases ...string) descriptor.Dialect {
	return descriptor.Dialect{
		Name:          name,
		Aliases:       aliases,
		DefaultSchema: "app",
		Identifiers:   descriptor.Identifiers{IndexNamespace: "schema", IndexNames: "exact", TableNames: "ascii_case_insensitive", ColumnNames: "exact"},
		Placeholder:   descriptor.PlaceholderDollar,
	}
}

// connection is a pool of the dialect it names, the part of a
// *dbschema.DatabaseConnection that dblock.Acquire reads. No connector serves
// an engine Ptah does not ship, so no *dbschema.DatabaseConnection reaches
// one. It counts the sessions it hands out.
type connection struct {
	db       *sql.DB
	dialect  string
	sessions atomic.Int64
}

func (c *connection) Info() catalog.ServerInfo { return catalog.ServerInfo{Dialect: c.dialect} }

func (c *connection) Conn(ctx context.Context) (*sql.Conn, error) {
	c.sessions.Add(1)
	return c.db.Conn(ctx)
}

func open(c *qt.C, dialect string) *connection {
	c.Helper()
	db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{}, nil
	})
	return &connection{db: db.SQL, dialect: dialect}
}

// call is one call a fake lock service received.
type call struct {
	op      string
	session dblockext.Session
	request dblockext.Request
}

// sessionLock is the session lock of the engine: it records its calls, and
// answers an acquisition with request.Held when held says another session has
// the lock.
type sessionLock struct {
	held bool

	mu    sync.Mutex
	calls []call
}

func (l *sessionLock) record(op string, session dblockext.Session, request dblockext.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, call{op: op, session: session, request: request})
}

func (l *sessionLock) Acquire(_ context.Context, session dblockext.Session, request dblockext.Request) error {
	l.record("acquire", session, request)
	if l.held {
		return request.Held
	}
	return nil
}

func (l *sessionLock) Release(_ context.Context, session dblockext.Session, request dblockext.Request) error {
	l.record("release", session, request)
	return nil
}

// separateLock is a lock held by a session of the service's own.
type separateLock struct {
	hold     *hold
	requests []dblockext.Request
}

func (l *separateLock) Acquire(_ context.Context, _ dblockext.Pool, request dblockext.Request) (dblockext.Hold, error) {
	l.requests = append(l.requests, request)
	return l.hold, nil
}

type hold struct {
	lostCh   chan struct{}
	lost     atomic.Bool
	releases atomic.Int64
}

func (h *hold) Lost() <-chan struct{}         { return h.lostCh }
func (h *hold) Held() bool                    { return !h.lost.Load() }
func (h *hold) Release(context.Context) error { h.releases.Add(1); return nil }

func (h *hold) lose() {
	h.lost.Store(true)
	close(h.lostCh)
}

// A registered engine's lock is reported by Supported and SupportedDialects,
// and Acquire takes it through the engine's service on a database session of
// its own, which the release gives the lock back on.
func TestRegister_SessionLock(t *testing.T) {
	c := qt.New(t)
	c.Assert(dblock.Supported("acme"), qt.IsFalse)
	lock := &sessionLock{}
	c.Assert(platform.Register(described("acme", "acmedb")), qt.IsNil)

	err := dblock.Register(dblockext.Services{Dialects: []string{"acme"}, Session: lock})

	c.Assert(err, qt.IsNil)
	c.Assert(dblock.Supported("acme"), qt.IsTrue)
	c.Assert(dblock.Supported(" ACMEDB "), qt.IsTrue)
	c.Assert(dblock.SupportedDialects()[:len(builtinLocks)], qt.DeepEquals, builtinLocks)
	c.Assert(dblock.SupportedDialects(), qt.Contains, "acme")

	conn := open(c, "acmedb")
	held, err := dblock.Acquire(c.Context(), conn, " acme_lock ", 2*time.Second)
	c.Assert(err, qt.IsNil)
	c.Assert(held.Supported(), qt.IsTrue)
	c.Assert(held.Name(), qt.Equals, "acme_lock")
	c.Assert(conn.sessions.Load(), qt.Equals, int64(1))
	// The lock lives on a session of its own, which dblock watches.
	c.Assert(held.Done(), qt.IsNotNil)
	c.Assert(held.Err(), qt.IsNil)
	c.Assert(held.Release(c.Context()), qt.IsNil)

	c.Assert(lock.calls, qt.HasLen, 2)
	c.Assert(lock.calls[0].op, qt.Equals, "acquire")
	c.Assert(lock.calls[1].op, qt.Equals, "release")
	c.Assert(lock.calls[0].session, qt.IsNotNil)
	c.Assert(lock.calls[1].session, qt.Equals, lock.calls[0].session)
	c.Assert(lock.calls[0].request.Dialect, qt.Equals, "acme")
	c.Assert(lock.calls[0].request.Name, qt.Equals, "acme_lock")
	c.Assert(lock.calls[0].request.Timeout, qt.Equals, 2*time.Second)
	c.Assert(lock.calls[0].request.Preview, qt.IsFalse)
	c.Assert(lock.calls[0].request.Held, qt.DeepEquals,
		error(&dblock.TimeoutError{Dialect: "acme", Name: "acme_lock", Timeout: 2 * time.Second}))
}

// A service that answers request.Held is a timeout dblock reports, under the
// engine's name.
func TestRegister_SessionLockHeldByAnotherSession(t *testing.T) {
	c := qt.New(t)
	c.Assert(platform.Register(described("acmeheld")), qt.IsNil)
	c.Assert(dblock.Register(dblockext.Services{Dialects: []string{"acmeheld"}, Session: &sessionLock{held: true}}), qt.IsNil)

	held, err := dblock.Acquire(c.Context(), open(c, "acmeheld"), "acme_lock", dblock.NoWait)

	c.Assert(err, qt.ErrorMatches, `advisory lock "acme_lock" on acmeheld is held by another session`)
	c.Assert(dblock.IsTimeout(err), qt.IsTrue)
	c.Assert(held, qt.IsNil)
}

// A lock a session of the service's own holds takes no database session from
// the pool, and its loss is the lock's.
func TestRegister_SeparateLock(t *testing.T) {
	c := qt.New(t)
	lock := &separateLock{hold: &hold{lostCh: make(chan struct{})}}
	c.Assert(platform.Register(described("acmesep")), qt.IsNil)
	c.Assert(dblock.Register(dblockext.Services{Dialects: []string{"acmesep"}, Separate: lock}), qt.IsNil)
	conn := open(c, "acmesep")

	held, err := dblock.Acquire(c.Context(), conn, "acme_lock", 0)

	c.Assert(err, qt.IsNil)
	c.Assert(held.Supported(), qt.IsTrue)
	c.Assert(conn.sessions.Load(), qt.Equals, int64(0))
	c.Assert(lock.requests, qt.HasLen, 1)
	c.Assert(lock.requests[0].Dialect, qt.Equals, "acmesep")
	c.Assert(lock.requests[0].Timeout, qt.Equals, time.Duration(0))
	c.Assert(held.Err(), qt.IsNil)

	lock.hold.lose()

	c.Assert(held.Err(), qt.ErrorMatches, `advisory lock "acme_lock" on acmesep was lost while it was held: .*`)
	c.Assert(dblock.IsLost(held.Err()), qt.IsTrue)
	c.Assert(held.Release(c.Context()), qt.IsNil)
	c.Assert(lock.hold.releases.Load(), qt.Equals, int64(1))
}

// An engine described without a lock service takes the no-op lock.
func TestRegister_AnEngineWithoutALock(t *testing.T) {
	c := qt.New(t)
	c.Assert(platform.Register(described("acmenolock")), qt.IsNil)
	conn := open(c, "acmenolock")

	held, err := dblock.Acquire(c.Context(), conn, "acme_lock", time.Second)

	c.Assert(err, qt.IsNil)
	c.Assert(held.Supported(), qt.IsFalse)
	c.Assert(held.Name(), qt.Equals, "acme_lock")
	c.Assert(conn.sessions.Load(), qt.Equals, int64(0))
	c.Assert(dblock.Supported("acmenolock"), qt.IsFalse)
	c.Assert(dblock.SupportedDialects(), qt.Not(qt.Contains), "acmenolock")
}

func TestRegister_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(platform.Register(described("refused", "refusedalias")), qt.IsNil)
	session := &sessionLock{}
	separate := &separateLock{}
	tests := []struct {
		name     string
		services []dblockext.Services
		wantErr  string
	}{
		{
			name:     "a dialect the registry does not describe",
			services: []dblockext.Services{{Dialects: []string{"nonesuch"}, Session: session}},
			wantErr:  `invalid lock service registration: "nonesuch" is not the canonical name of a described dialect`,
		},
		{
			name:     "an alias",
			services: []dblockext.Services{{Dialects: []string{"refusedalias"}, Session: session}},
			wantErr:  `invalid lock service registration: "refusedalias" is not the canonical name of a described dialect`,
		},
		{
			name:     "a dialect Ptah ships a lock for",
			services: []dblockext.Services{{Dialects: []string{"refused", "postgres"}, Session: session}},
			wantErr:  `invalid lock service registration: dialect "postgres" already has a lock service`,
		},
		{
			name:     "a dialect named twice",
			services: []dblockext.Services{{Dialects: []string{"refused"}, Session: session}, {Dialects: []string{"refused"}, Session: session}},
			wantErr:  `invalid lock service registration: dialect "refused" already has a lock service`,
		},
		{
			name:     "no dialect",
			services: []dblockext.Services{{Session: session}},
			wantErr:  `invalid lock service registration: the services name no dialect`,
		},
		{
			name:     "no lock",
			services: []dblockext.Services{{Dialects: []string{"refused"}}},
			wantErr:  `invalid lock service registration: the services of \["refused"\] take no lock`,
		},
		{
			name:     "a session lock and a separate one",
			services: []dblockext.Services{{Dialects: []string{"refused"}, Session: session, Separate: separate}},
			wantErr:  `invalid lock service registration: the services of \["refused"\] take a session lock and a separate one`,
		},
		{
			name:     "an exclusion check without a session lock",
			services: []dblockext.Services{{Dialects: []string{"refused"}, Separate: separate, Exclusion: exclusion{}}},
			wantErr:  `invalid lock service registration: the services of \["refused"\] check the exclusion of a lock no session holds`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := dblock.Register(test.services...)

			c.Assert(err, qt.ErrorIs, dblock.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			// Nothing of a refused call is added, not even the services
			// before the refused one.
			c.Assert(dblock.Supported("refused"), qt.IsFalse)
		})
	}
}

// A dialect keeps the lock service it was first registered with.
func TestRegister_Twice(t *testing.T) {
	c := qt.New(t)
	c.Assert(platform.Register(described("acmetwice")), qt.IsNil)
	first := &sessionLock{}
	c.Assert(dblock.Register(dblockext.Services{Dialects: []string{"acmetwice"}, Session: first}), qt.IsNil)

	err := dblock.Register(dblockext.Services{Dialects: []string{"acmetwice"}, Session: &sessionLock{held: true}})

	c.Assert(err, qt.ErrorIs, dblock.ErrInvalidRegistration)
	held, acquireErr := dblock.Acquire(c.Context(), open(c, "acmetwice"), "acme_lock", time.Second)
	c.Assert(acquireErr, qt.IsNil)
	c.Assert(held.Release(c.Context()), qt.IsNil)
	c.Assert(first.calls, qt.HasLen, 2)
}

// Register and the readers share no unsynchronized state. Run under -race, the
// test is the check; without it, it checks that no registration is lost.
func TestRegister_Concurrently(t *testing.T) {
	c := qt.New(t)
	names := []string{"acmec0", "acmec1", "acmec2", "acmec3", "acmec4", "acmec5", "acmec6", "acmec7"}
	for _, name := range names {
		c.Assert(platform.Register(described(name)), qt.IsNil)
	}
	var group sync.WaitGroup
	errs := make([]error, len(names))
	for i, name := range names {
		group.Go(func() {
			errs[i] = dblock.Register(dblockext.Services{Dialects: []string{name}, Session: &sessionLock{}})
		})
		group.Go(func() {
			_ = dblock.Supported(name)
			_ = dblock.SupportedDialects()
		})
	}

	group.Wait()

	c.Assert(errs, qt.DeepEquals, make([]error, len(names)))
	for _, name := range names {
		c.Assert(dblock.Supported(name), qt.IsTrue)
	}
}

// exclusion is an exclusion check nothing runs: the registration that names
// it is refused.
type exclusion struct{}

func (exclusion) Excludes(context.Context, dblockext.Pool, dblockext.Request) error {
	return nil
}
