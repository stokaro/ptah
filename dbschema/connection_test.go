package dbschema_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
)

func TestFormatDatabaseURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "PostgreSQL URL with password",
			input:    "postgres://user:secret123@localhost:5432/mydb",
			expected: "postgres://user:***@localhost:5432/mydb",
		},
		{
			name:     "PostgreSQL URL with query password",
			input:    "postgres://user:secret123@localhost:5432/mydb?sslmode=disable&sslpassword=querysecret",
			expected: "postgres://user:***@localhost:5432/mydb?sslmode=disable&sslpassword=redacted",
		},
		{
			name:     "PostgreSQL URL with query password and no user password",
			input:    "postgres://user@localhost:5432/mydb?password=querysecret",
			expected: "postgres://user@localhost:5432/mydb?password=redacted",
		},
		{
			name:     "PostgreSQL URL preserves fragment while redacting query",
			input:    "postgres://user:secret123@localhost:5432/mydb?password=querysecret#frag",
			expected: "postgres://user:***@localhost:5432/mydb?password=redacted#frag",
		},
		{
			name:     "PostgreSQL URL without password",
			input:    "postgres://user@localhost:5432/mydb",
			expected: "postgres://user@localhost:5432/mydb",
		},
		{
			name:     "Invalid URL",
			input:    "not-a-url",
			expected: "not-a-url",
		},
		{
			name:     "MySQL URL with password",
			input:    "mysql://root:password@localhost:3306/testdb",
			expected: "mysql://root:***@localhost:3306/testdb",
		},
		{
			name:     "MySQL tcp URL with query secret",
			input:    "mysql://root:password@tcp(localhost:3306)/testdb?parseTime=true&sslpassword=querysecret",
			expected: "mysql://root:***@tcp(localhost:3306)/testdb?parseTime=true&sslpassword=redacted",
		},
		{
			name:     "MySQL tcp URL does not redact embedded query URL credentials",
			input:    "mysql://root@tcp(localhost:3306)/testdb?callback=https%3A%2F%2Fx%3Ay%40example.test&password=querysecret",
			expected: "mysql://root@tcp(localhost:3306)/testdb?callback=https%3A%2F%2Fx%3Ay%40example.test&password=redacted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			result := dbschema.FormatDatabaseURL(tt.input)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

func TestConnectToDatabase_InvalidURL(t *testing.T) {
	tests := []struct {
		name   string
		dbURL  string
		errMsg string
	}{
		{
			name:   "Invalid URL format",
			dbURL:  "not-a-url",
			errMsg: "invalid database URL: missing scheme",
		},
		{
			name:   "Empty URL",
			dbURL:  "",
			errMsg: "invalid database URL: missing scheme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(context.Background(), tt.dbURL)
			c.Assert(err, qt.ErrorMatches, ".*"+tt.errMsg+".*")
			c.Assert(conn, qt.IsNil)
		})
	}
}

// TestPostgreSQLConnection tests PostgreSQL connection (will fail if no server running)
func TestPostgreSQLConnection_NoServer(t *testing.T) {
	c := qt.New(t)

	// This test expects to fail since we don't have a PostgreSQL server running
	// It's mainly to test that the connection logic works correctly
	conn, err := dbschema.ConnectToDatabase(context.Background(), "postgres://user:pass@localhost:5432/testdb")

	// We expect an error because no PostgreSQL server is running
	c.Assert(err, qt.IsNotNil)
	c.Assert(conn, qt.IsNil)

	// The error should be about connection failure, not about invalid URL or unsupported dialect
	c.Assert(err.Error(), qt.Not(qt.Contains), "unsupported database dialect")
	c.Assert(err.Error(), qt.Not(qt.Contains), "invalid database URL")
}

func TestConnectToDatabase_SQLiteMemory(t *testing.T) {
	c := qt.New(t)

	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	info := conn.Info()
	c.Assert(info.Dialect, qt.Equals, "sqlite")
	c.Assert(info.Schema, qt.Equals, "main")
	c.Assert(info.Version, qt.Not(qt.Equals), "")

	var foreignKeys int
	err = conn.QueryRowContext(context.Background(), "PRAGMA foreign_keys").Scan(&foreignKeys)
	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeys, qt.Equals, 1)
}

func TestConnectToDatabase_SQLiteFile(t *testing.T) {
	c := qt.New(t)

	dbPath := filepath.Join(t.TempDir(), "ptah.sqlite")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)")
	c.Assert(err, qt.IsNil)
	c.Assert(conn.Close(), qt.IsNil)

	reopened, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(reopened)

	schema, err := reopened.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 1)
	c.Assert(schema.Tables[0].Name, qt.Equals, "users")
}

// stuckPostgresURL spins up a local TCP listener that completes the TCP
// handshake but never reads from the socket, so the PostgreSQL protocol
// handshake stalls indefinitely. This is the most portable way to simulate
// "host accepts but never answers" without depending on routing tables or
// reserved IP blocks (those behave differently on offline/restricted hosts).
//
// The listener and a sentinel "drainer" goroutine are wired to a cancellable
// context so the test can guarantee the goroutine exits even if the call
// under test races ahead.
func stuckPostgresURL(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	stop := make(chan struct{})
	go func() {
		// Accept and hold connections until the test ends. Holding the conn
		// stops the kernel from sending an RST, so the Postgres handshake
		// hangs waiting for server bytes instead of failing fast.
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				<-stop
				_ = c.Close()
			}(conn)
		}
	}()

	t.Cleanup(func() {
		close(stop)
		_ = ln.Close()
	})

	return fmt.Sprintf("postgres://user:pass@%s/db", ln.Addr())
}

// TestConnectToDatabase_CancelledContext is the acceptance test for issue #139.
// A pre-cancelled context must short-circuit the call so that
// ConnectToDatabase returns promptly with context.Canceled.
func TestConnectToDatabase_CancelledContext(t *testing.T) {
	c := qt.New(t)

	dbURL := stuckPostgresURL(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before the call to guarantee the ping never starts.

	start := time.Now()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	elapsed := time.Since(start)

	c.Assert(conn, qt.IsNil)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err, qt.ErrorIs, context.Canceled,
		qt.Commentf("expected context.Canceled in error chain, got: %v", err))

	// "Promptly" is intentionally loose to avoid flakiness on slow CI runners,
	// but well below the multi-second TCP timeout the bug describes.
	c.Assert(elapsed < 2*time.Second, qt.IsTrue,
		qt.Commentf("ConnectToDatabase took %s with a cancelled context, want <2s", elapsed))
}

// TestConnectToDatabase_DeadlineExceeded covers the typical --connect-timeout
// case: a context that expires while the connection attempt is pending must
// surface context.DeadlineExceeded instead of hanging.
func TestConnectToDatabase_DeadlineExceeded(t *testing.T) {
	c := qt.New(t)

	dbURL := stuckPostgresURL(t)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	elapsed := time.Since(start)

	c.Assert(conn, qt.IsNil)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded,
		qt.Commentf("expected context.DeadlineExceeded in error chain, got: %v", err))

	// Generous upper bound — the deadline is 200ms; allow for handshake +
	// scheduling slack without flaking on slow CI runners.
	c.Assert(elapsed < 5*time.Second, qt.IsTrue,
		qt.Commentf("ConnectToDatabase took %s with a 200ms deadline, want <5s", elapsed))
}
