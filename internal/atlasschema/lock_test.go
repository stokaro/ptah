package atlasschema_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/dblock"
)

func TestParseApplyLockTimeout_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "empty waits indefinitely", value: "", want: 0},
		{name: "whitespace waits indefinitely", value: "  ", want: 0},
		{name: "seconds", value: "10s", want: 10 * time.Second},
		{name: "minutes", value: "2m", want: 2 * time.Minute},
		{name: "subsecond", value: "250ms", want: 250 * time.Millisecond},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasschema.ParseApplyLockTimeout(test.value)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestParseApplyLockTimeout_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "zero rejected", value: "0s", wantErr: "invalid --lock-timeout: must be greater than zero"},
		{name: "negative rejected", value: "-1s", wantErr: "invalid --lock-timeout: must be greater than zero"},
		{name: "not a duration", value: "soon", wantErr: `invalid --lock-timeout: time: invalid duration "soon"`},
		{name: "bare number", value: "10", wantErr: `invalid --lock-timeout: time: missing unit in duration "10"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasschema.ParseApplyLockTimeout(test.value)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, time.Duration(0))
		})
	}
}

func TestAcquireApplyLock_HappyPath(t *testing.T) {
	t.Run("sqlite acquires an explicit no-op lock", func(t *testing.T) {
		c := qt.New(t)
		conn := connectSQLite(c, filepath.Join(c.TB.TempDir(), "lock.db"))
		defer dbschema.CloseAndWarn(conn)

		lock, err := atlasschema.AcquireApplyLock(c.Context(), conn, "", time.Second)
		c.Assert(err, qt.IsNil)
		c.Assert(lock.Supported(), qt.IsFalse)
		c.Assert(lock.Release(), qt.IsNil)
	})

	t.Run("release is idempotent", func(t *testing.T) {
		c := qt.New(t)
		conn := connectSQLite(c, filepath.Join(c.TB.TempDir(), "lock-idem.db"))
		defer dbschema.CloseAndWarn(conn)

		lock, err := atlasschema.AcquireApplyLock(c.Context(), conn, "", 0)
		c.Assert(err, qt.IsNil)
		c.Assert(lock.Release(), qt.IsNil)
		c.Assert(lock.Release(), qt.IsNil)
	})

	t.Run("nil lock releases without error", func(t *testing.T) {
		c := qt.New(t)
		var lock *atlasschema.ApplyLock
		c.Assert(lock.Release(), qt.IsNil)
		c.Assert(lock.Supported(), qt.IsFalse)
	})
}

func TestAcquireApplyLock_FailurePath(t *testing.T) {
	t.Run("nil connection", func(t *testing.T) {
		c := qt.New(t)
		lock, err := atlasschema.AcquireApplyLock(c.Context(), nil, "", time.Second)
		c.Assert(err, qt.ErrorMatches, "schema apply locking requires database connection")
		c.Assert(lock, qt.IsNil)
	})
}

func TestIsLockTimeout(t *testing.T) {
	c := qt.New(t)

	wrapped := fmt.Errorf("acquire schema apply lock: %w", &dblock.TimeoutError{
		Dialect: "postgres",
		Name:    atlasschema.ApplyLockName,
		Timeout: time.Second,
	})
	c.Assert(atlasschema.IsLockTimeout(wrapped), qt.IsTrue)
	c.Assert(atlasschema.IsLockTimeout(fmt.Errorf("other error")), qt.IsFalse)
	c.Assert(atlasschema.IsLockTimeout(nil), qt.IsFalse)
}
