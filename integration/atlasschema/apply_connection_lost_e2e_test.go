//go:build integration

package atlasschema_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/rolepassword"
)

// connectionBreaker forwards PostgreSQL connections to a server and ends the
// connection that sends the first client message the trigger matches. It stands
// where a network or a failover would, so the command under test meets the error
// a lost connection gives it and the server meets the end of a real session.
type connectionBreaker struct {
	listener net.Listener
	upstream string
	trigger  func(message []byte) bool
	// forward sends the matching message to the server before ending the
	// connection, so the server may act on it and the client hears nothing.
	forward bool
	// broken reports that the trigger matched.
	broken atomic.Bool
	wg     sync.WaitGroup
}

func startConnectionBreaker(c *qt.C, upstream string, forward bool, trigger func([]byte) bool) *connectionBreaker {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	b := &connectionBreaker{listener: listener, upstream: upstream, forward: forward, trigger: trigger}
	b.wg.Go(b.serve)
	c.Cleanup(func() {
		c.Check(listener.Close(), qt.IsNil)
		b.wg.Wait()
	})
	return b
}

func (b *connectionBreaker) serve() {
	for {
		client, err := b.listener.Accept()
		if err != nil {
			return
		}
		b.wg.Go(func() { b.relay(client) })
	}
}

func (b *connectionBreaker) relay(client net.Conn) {
	defer func() { _ = client.Close() }()
	server, err := net.Dial("tcp", b.upstream)
	if err != nil {
		return
	}
	defer func() { _ = server.Close() }()
	go func() {
		// The reply direction stops when the deferred closes end either side.
		_, _ = io.Copy(client, server)
		_ = client.Close()
	}()
	buffer := make([]byte, 64*1024)
	for {
		n, err := client.Read(buffer)
		if n > 0 {
			matched := !b.broken.Load() && b.trigger(buffer[:n])
			if !matched || b.forward {
				if _, writeErr := server.Write(buffer[:n]); writeErr != nil {
					return
				}
			}
			if matched {
				b.broken.Store(true)
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// beginMessage matches the query that opens the apply's transaction, which the
// driver sends alone, as a simple query. The first one in a run is the apply's:
// the row reads the error that names it, so a read that began first would fail
// the row rather than pass it for another reason.
func beginMessage(message []byte) bool {
	return bytes.Contains(message, []byte("begin\x00"))
}

// commitMessage matches the query that ends the apply's transaction.
func commitMessage(message []byte) bool {
	return bytes.Contains(message, []byte("commit\x00"))
}

// createOrdersMessage matches the plan's only statement.
func createOrdersMessage(message []byte) bool {
	return bytes.Contains(message, []byte("CREATE TABLE")) && bytes.Contains(message, []byte("orders"))
}

// connectionBreakerDatabase creates a database of its own for one row. It
// returns a direct handle, which reads the database back with nothing of Ptah's
// in between, and the URL the command reaches it through the breaker with.
func connectionBreakerDatabase(c *qt.C, forward bool, trigger func([]byte) bool) (*sql.DB, string, *connectionBreaker) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_apply_lost_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), `CREATE DATABASE "`+name+`"`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.WithoutCancel(c.Context()), `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`)
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	query := parsed.Query()
	// The breaker reads the client's messages, which TLS would hide.
	query.Set("sslmode", "disable")
	parsed.RawQuery = query.Encode()
	direct, err := sql.Open("pgx", parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(direct.Close(), qt.IsNil) })
	breaker := startConnectionBreaker(c, parsed.Host, forward, trigger)
	parsed.Host = breaker.listener.Addr().String()
	return direct, parsed.String(), breaker
}

// runSchemaCommand runs one `ptah schema` verb through the command tree the binary
// assembles, and returns what it wrote and its exit code.
func runSchemaCommand(c *qt.C, verb string, args ...string) (stdout, stderr string, exitCode int) {
	c.Helper()
	cmd := root.NewRootCommand()
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	cmd.SetIn(strings.NewReader(""))
	code := root.RunContext(c.Context(), cmd, append([]string{"schema", verb}, args...)...)
	return out.String(), diagnostics.String(), code
}

// ordersTableWithin reads whether the plan's table is on the server, and waits up
// to patience for it to appear: the client knows the connection ended before the
// server has finished with the statement it delivered.
func ordersTableWithin(c *qt.C, db *sql.DB, patience time.Duration) bool {
	c.Helper()
	deadline := time.Now().Add(patience)
	for {
		var name sql.NullString
		c.Assert(db.QueryRowContext(c.Context(), `SELECT to_regclass('public.orders')::text`).Scan(&name), qt.IsNil)
		if name.Valid || !time.Now().Before(deadline) {
			return name.Valid
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestSchemaApplyLostConnectionE2E loses the connection at the two moments that
// differ on a live server. During BEGIN no statement of the plan is on the wire,
// so the report says nothing reached the database and the table is not there.
// During the statement the report cannot know: without a transaction the table
// is there, in one it is not, and both are unknown.
func TestSchemaApplyLostConnectionE2E(t *testing.T) {
	tests := []struct {
		name        string
		mode        string
		forward     bool
		trigger     func([]byte) bool
		wantOutcome atlasschema.ApplyOutcome
		wantError   string
		// patience is how long the row waits for the table to appear. A row that
		// expects no table has nothing to wait for.
		patience  time.Duration
		wantTable bool
		// wantFailure is where the record says the run stopped. The server gave no
		// answer, so it carries no code.
		wantFailure *atlasschema.ApplyFailure
	}{
		{
			name: "during BEGIN", mode: "file", trigger: beginMessage,
			wantOutcome: atlasschema.ApplyOutcomeFailed, wantError: "begin schema apply transaction: ",
		},
		{
			name: "during a statement in a transaction", mode: "file", forward: true, trigger: createOrdersMessage,
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantError: "failed to execute SQL statement",
			wantFailure: &atlasschema.ApplyFailure{Statement: 1, Statements: 1},
		},
		{
			name: "during a statement without a transaction", mode: "none", forward: true, trigger: createOrdersMessage,
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantError: "failed to execute SQL statement",
			patience: 15 * time.Second, wantTable: true,
			wantFailure: &atlasschema.ApplyFailure{Statement: 1, Statements: 1},
		},
		{
			name: "during the commit", mode: "file", forward: true, trigger: commitMessage,
			wantOutcome: atlasschema.ApplyOutcomeUnknown, wantError: "commit schema apply transaction: ",
			patience: 15 * time.Second, wantTable: true,
			wantFailure: &atlasschema.ApplyFailure{Statements: 1, Commit: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			direct, address, breaker := connectionBreakerDatabase(c, test.forward, test.trigger)
			schema := filepath.Join(c.TempDir(), "schema.yaml")
			c.Assert(os.WriteFile(schema, []byte("tables:\n  orders:\n    columns:\n      id: {type: INTEGER, not_null: true}\n"), 0o600), qt.IsNil)

			out, diagnostics, code := runSchemaCommand(c, "apply", "--db-url", address, "--schema-file", schema,
				"--tx-mode", test.mode, "--auto-approve", "--json")

			c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
			c.Assert(breaker.broken.Load(), qt.IsTrue)
			var report atlasschema.ApplyReport
			c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s", out))
			c.Assert(report.Outcome, qt.Equals, test.wantOutcome, qt.Commentf("%s", diagnostics))
			c.Assert(report.Refusal, qt.IsNil)
			c.Assert(report.Error.Message, qt.Contains, test.wantError)
			c.Assert(report.Failure, qt.DeepEquals, test.wantFailure)
			c.Assert(report.Units, qt.HasLen, 1)
			c.Assert(ordersTableWithin(c, direct, test.patience), qt.Equals, test.wantTable)
		})
	}
}

// TestSchemaApplyUnsetRolePasswordE2E applies a saved plan whose first statement
// names a role password in a variable nobody set. The writer refuses it before
// sending it, so the report says nothing reached the database, and the role is
// not on the server. With the variable set, the same plan applies.
func TestSchemaApplyUnsetRolePasswordE2E(t *testing.T) {
	for _, mode := range []string{"file", "none"} {
		t.Run(mode, func(t *testing.T) {
			c := qt.New(t)
			direct, address, _ := connectionBreakerDatabase(c, false, func([]byte) bool { return false })
			role := fmt.Sprintf("ptah_apply_role_%d", time.Now().UnixNano())
			variable := rolepassword.Variable(role)
			c.Cleanup(func() {
				_, err := direct.ExecContext(context.WithoutCancel(c.Context()), `DROP ROLE IF EXISTS "`+role+`"`)
				c.Check(err, qt.IsNil)
			})
			dir := c.TempDir()
			schema := filepath.Join(dir, "schema.sql")
			plan := filepath.Join(dir, "change.plan.json")
			c.Assert(os.WriteFile(schema, []byte(`CREATE ROLE "`+role+`" WITH LOGIN PASSWORD 'declared';`+"\n"), 0o600), qt.IsNil)
			_, diagnostics, code := runSchemaCommand(c, "plan", "--db-url", address, "--schema-file", schema, "--output", plan)
			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			roleExists := func() bool {
				var count int
				c.Assert(direct.QueryRowContext(c.Context(), `SELECT count(*) FROM pg_roles WHERE rolname = $1`, role).Scan(&count), qt.IsNil)
				return count == 1
			}
			apply := []string{"--db-url", address, "--plan", plan, "--tx-mode", mode, "--auto-approve", "--json"}

			out, diagnostics, code := runSchemaCommand(c, "apply", apply...)

			c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
			var report atlasschema.ApplyReport
			c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s", out))
			c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
			c.Assert(report.Error.Message, qt.Contains, "environment variable "+variable+", which is not set")
			c.Assert(roleExists(), qt.IsFalse)

			c.Setenv(variable, "from-the-environment")
			out, diagnostics, code = runSchemaCommand(c, "apply", apply...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s", out))
			c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
			c.Assert(roleExists(), qt.IsTrue)
		})
	}
}
