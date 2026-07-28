package dblock_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/dblock"
)

func TestSupported(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		want    bool
	}{
		{name: "postgres", dialect: "postgres", want: true},
		{name: "postgresql alias", dialect: "postgresql", want: true},
		{name: "mysql", dialect: "mysql", want: true},
		{name: "mariadb", dialect: "mariadb", want: true},
		{name: "sqlserver", dialect: "sqlserver", want: true},
		{name: "sqlite", dialect: "sqlite", want: false},
		{name: "clickhouse", dialect: "clickhouse", want: false},
		{name: "cockroachdb", dialect: "cockroachdb", want: false},
		{name: "spanner", dialect: "spanner", want: false},
		{name: "unknown", dialect: "not-a-dialect", want: false},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(dblock.Supported(test.dialect), qt.Equals, test.want)
		})
	}
}

func TestAcquire_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Run("sqlite acquires a no-op lock", func(c *qt.C) {
		conn := openSQLite(c)

		lock, err := dblock.Acquire(c.Context(), conn, "ptah_test_lock", time.Second)
		c.Assert(err, qt.IsNil)
		c.Assert(lock.Supported(), qt.IsFalse)
		c.Assert(lock.Release(c.Context()), qt.IsNil)
	})

	c.Run("no-op lock release is idempotent", func(c *qt.C) {
		conn := openSQLite(c)

		lock, err := dblock.Acquire(c.Context(), conn, "ptah_test_lock", 0)
		c.Assert(err, qt.IsNil)
		c.Assert(lock.Release(c.Context()), qt.IsNil)
		c.Assert(lock.Release(c.Context()), qt.IsNil)
	})

	c.Run("nil and zero locks release without error", func(c *qt.C) {
		var lock *dblock.Lock
		c.Assert(lock.Release(c.Context()), qt.IsNil)
		c.Assert(lock.Supported(), qt.IsFalse)
		c.Assert(new(dblock.Lock).Release(c.Context()), qt.IsNil)
	})
}

func TestAcquire_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("empty name rejected", func(c *qt.C) {
		conn := openSQLite(c)

		lock, err := dblock.Acquire(c.Context(), conn, "  ", time.Second)
		c.Assert(err, qt.ErrorMatches, `advisory lock name must not be empty`)
		c.Assert(lock, qt.IsNil)
	})
}

func TestTimeoutError(t *testing.T) {
	c := qt.New(t)

	err := &dblock.TimeoutError{Dialect: "postgres", Name: "ptah_schema_apply", Timeout: 250 * time.Millisecond}
	c.Assert(err.Error(), qt.Equals, `timed out acquiring advisory lock "ptah_schema_apply" on postgres after 250ms`)
}

func TestIsTimeout(t *testing.T) {
	c := qt.New(t)

	wrapped := fmt.Errorf("acquire schema apply lock: %w", &dblock.TimeoutError{
		Dialect: "postgres",
		Name:    "ptah_schema_apply",
		Timeout: time.Second,
	})
	c.Assert(dblock.IsTimeout(wrapped), qt.IsTrue)
	c.Assert(dblock.IsTimeout(fmt.Errorf("other error")), qt.IsFalse)
	c.Assert(dblock.IsTimeout(nil), qt.IsFalse)
}

func TestPostgresKey(t *testing.T) {
	c := qt.New(t)

	// The key must stay stable across releases: concurrent runners of
	// different Ptah versions must contend on the same pg_advisory_lock key.
	c.Assert(dblock.PostgresKey("ptah_migrate"), qt.Equals, int64(2705505214))
	c.Assert(dblock.PostgresKey("ptah_schema_apply"), qt.Not(qt.Equals), dblock.PostgresKey("ptah_migrate"))
	c.Assert(dblock.PostgresKey("custom-lock"), qt.Equals, dblock.PostgresKey("custom-lock"))
}

func openSQLite(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TB.TempDir(), "dblock.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}
