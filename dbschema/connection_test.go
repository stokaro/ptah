package dbschema_test

import (
	"context"
	"fmt"
	"net"
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
			name:   "Unsupported dialect",
			dbURL:  "sqlite://test.db",
			errMsg: "unsupported database dialect: sqlite",
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

func TestConnectToDatabase_UnsupportedDialects(t *testing.T) {
	tests := []struct {
		name     string
		dbURL    string
		expected string
	}{
		{
			name:     "SQLite not supported",
			dbURL:    "sqlite://test.db",
			expected: "unsupported database dialect: sqlite",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(context.Background(), tt.dbURL)
			c.Assert(err, qt.ErrorMatches, ".*"+tt.expected+".*")
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
