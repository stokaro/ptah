package dbschema_test

import (
	"context"
	"errors"
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

// TestConnectToDatabase_CancelledContext is the acceptance test for issue #139.
// It points ConnectToDatabase at a routable but quiet TCP endpoint, which would
// otherwise block on the initial Ping indefinitely. A pre-cancelled context
// must short-circuit the call so that ConnectToDatabase returns promptly with
// context.Canceled.
func TestConnectToDatabase_CancelledContext(t *testing.T) {
	c := qt.New(t)

	// The TEST-NET-1 block (192.0.2.0/24) is reserved for documentation and
	// never routes anywhere — connections sit until the TCP timeout. Pairing
	// it with a port the kernel will silently drop makes for a reliable "slow
	// host" surrogate that doesn't depend on the local environment.
	const stuckURL = "postgres://user:pass@192.0.2.1:65535/db"

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before the call to guarantee the ping never starts.

	start := time.Now()
	conn, err := dbschema.ConnectToDatabase(ctx, stuckURL)
	elapsed := time.Since(start)

	c.Assert(conn, qt.IsNil)
	c.Assert(err, qt.IsNotNil)
	c.Assert(errors.Is(err, context.Canceled), qt.IsTrue,
		qt.Commentf("expected context.Canceled in error chain, got: %v", err))

	// "Promptly" is intentionally loose to avoid flakiness on slow CI runners,
	// but well below the 30-60s TCP timeout that the bug describes.
	c.Assert(elapsed < 2*time.Second, qt.IsTrue,
		qt.Commentf("ConnectToDatabase took %s with a cancelled context, want <2s", elapsed))
}

// TestConnectToDatabase_DeadlineExceeded covers the typical --connect-timeout
// case: a context that expires while the connection attempt is pending must
// surface context.DeadlineExceeded instead of hanging.
func TestConnectToDatabase_DeadlineExceeded(t *testing.T) {
	c := qt.New(t)

	const stuckURL = "postgres://user:pass@192.0.2.1:65535/db"

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	conn, err := dbschema.ConnectToDatabase(ctx, stuckURL)
	elapsed := time.Since(start)

	c.Assert(conn, qt.IsNil)
	c.Assert(err, qt.IsNotNil)
	c.Assert(errors.Is(err, context.DeadlineExceeded), qt.IsTrue,
		qt.Commentf("expected context.DeadlineExceeded in error chain, got: %v", err))

	c.Assert(elapsed < 5*time.Second, qt.IsTrue,
		qt.Commentf("ConnectToDatabase took %s with a 100ms deadline, want <5s", elapsed))
}
