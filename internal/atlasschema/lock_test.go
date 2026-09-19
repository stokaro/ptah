package atlasschema_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dblock"
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

func TestWithApplyLockSession_UsesPinnedCallbackConnection(t *testing.T) {
	c := qt.New(t)
	conn := connectSQLite(c, filepath.Join(c.TB.TempDir(), "lock-session.db"))
	defer dbschema.CloseAndWarn(conn)
	var callbackConnection *dbschema.DatabaseConnection
	var rootReadErr error

	runErr, releaseErr := atlasschema.WithApplyLockSession(
		c.Context(),
		conn,
		"",
		time.Second,
		func(session *dbschema.DatabaseConnection, lock *atlasschema.ApplyLock) error {
			callbackConnection = session
			c.Assert(session, qt.Not(qt.Equals), conn)
			c.Assert(lock.Supported(), qt.IsFalse)
			_, err := session.ExecContext(c.Context(),
				"CREATE TEMPORARY TABLE apply_lock_session_marker (id INTEGER)")
			c.Assert(err, qt.IsNil)

			var count int
			rootReadErr = conn.QueryRowContext(
				c.Context(),
				"SELECT COUNT(*) FROM apply_lock_session_marker",
			).Scan(&count)
			return nil
		},
	)

	c.Assert(runErr, qt.IsNil)
	c.Assert(releaseErr, qt.IsNil)
	c.Assert(callbackConnection, qt.Not(qt.Equals), conn)
	c.Assert(rootReadErr, qt.ErrorMatches, ".*no such table: apply_lock_session_marker.*")
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

func TestEnsureApplyLockSupported_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: "postgres"},
		{name: "postgres alias", dialect: "postgresql"},
		{name: "yugabytedb", dialect: "yugabytedb"},
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
		{name: "sqlserver", dialect: "sqlserver"},
		{name: "sqlserver alias", dialect: "mssql"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasschema.EnsureApplyLockSupported("--lock-timeout", test.dialect), qt.IsNil)
		})
	}
}

func TestEnsureApplyLockSupported_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request string
		dialect string
		wantErr string
	}{
		{
			name: "sqlite", request: "--lock-timeout", dialect: "sqlite",
			wantErr: `--lock-timeout requested a schema apply lock, and dialect "sqlite" has none: ` +
				`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
				`Remove --lock-timeout to apply without a lock`,
		},
		{
			name: "clickhouse", request: "--lock-timeout", dialect: "clickhouse",
			wantErr: `--lock-timeout requested a schema apply lock, and dialect "clickhouse" has none: ` +
				`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
				`Remove --lock-timeout to apply without a lock`,
		},
		{
			name: "cockroachdb", request: "--lock-timeout", dialect: "cockroachdb",
			wantErr: `--lock-timeout requested a schema apply lock, and dialect "cockroachdb" has none: ` +
				`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
				`Remove --lock-timeout to apply without a lock`,
		},
		{
			name: "spanner", request: "--lock-timeout", dialect: "spanner",
			wantErr: `--lock-timeout requested a schema apply lock, and dialect "spanner" has none: ` +
				`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
				`Remove --lock-timeout to apply without a lock`,
		},
		{
			name: "oracle", request: "--lock-timeout", dialect: "oracle",
			wantErr: `--lock-timeout requested a schema apply lock, and dialect "oracle" has none: ` +
				`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
				`Remove --lock-timeout to apply without a lock`,
		},
		{
			name: "environment variable names itself", request: "PTAH_LOCK_TIMEOUT", dialect: "sqlite",
			wantErr: `PTAH_LOCK_TIMEOUT requested a schema apply lock, and dialect "sqlite" has none: ` +
				`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
				`Remove PTAH_LOCK_TIMEOUT to apply without a lock`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := atlasschema.EnsureApplyLockSupported(test.request, test.dialect)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var unsupported *atlasschema.UnsupportedApplyLockError
			c.Assert(err, qt.ErrorAs, &unsupported)
			c.Assert(unsupported.Dialect, qt.Equals, test.dialect)
			c.Assert(unsupported.Request, qt.Equals, test.request)
		})
	}
}

// TestApplyLockSupported_HappyPath names the dialects whose servers give Ptah a
// session advisory lock, through the predicate a caller asks before connecting.
func TestApplyLockSupported_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: "postgres"},
		{name: "postgres alias", dialect: "postgresql"},
		{name: "yugabytedb", dialect: "yugabytedb"},
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
		{name: "sqlserver", dialect: "sqlserver"},
		{name: "sqlserver alias", dialect: "mssql"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasschema.ApplyLockSupported(test.dialect), qt.IsTrue)
		})
	}
}

// TestApplyLockSupported_FailurePath covers every connectable dialect outside
// that set, Oracle included: the refusal and the note both key on this answer,
// so a dialect missing from here would be told it has a lock.
func TestApplyLockSupported_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "sqlite", dialect: "sqlite"},
		{name: "clickhouse", dialect: "clickhouse"},
		{name: "cockroachdb", dialect: "cockroachdb"},
		{name: "spanner", dialect: "spanner"},
		{name: "oracle", dialect: "oracle"},
		{name: "unknown", dialect: "frobnicate"},
		{name: "empty", dialect: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasschema.ApplyLockSupported(test.dialect), qt.IsFalse)
		})
	}
}
