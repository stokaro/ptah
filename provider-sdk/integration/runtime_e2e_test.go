//go:build integration

package integration_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/internal/testprocess"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
)

func TestIndependentProviderProcess(t *testing.T) {
	c := qt.New(t)
	binary := buildFixture(c, "provider")
	bytes, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(bytes)
	options := transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture", RequiredFeatures: []string{provider.InspectFeature, provider.RenderFeature, provider.ExecuteFeature}}

	t.Run("inspect render execute verify", func(t *testing.T) {
		c := qt.New(t)
		client := start(c, options)
		session := open(c, client, "")
		before, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.Id})
		c.Assert(err, qt.IsNil)
		c.Assert(before.GetTables(), qt.HasLen, 0)
		rendered, err := client.Render(c.Context(), &protocol.RenderRequest{Engine: "fixture", Changes: []*protocol.Change{{Id: "create", Object: &protocol.Reference{Kind: "table", Name: "items"}, Operation: &protocol.Change_CreateTable{CreateTable: &protocol.Table{Columns: []*protocol.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}, {Name: "value", Type: "TEXT", Nullable: true}}}}}}})
		c.Assert(err, qt.IsNil)
		result, err := client.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: session.Id, RequestId: "apply", Units: rendered.GetUnits()})
		c.Assert(err, qt.IsNil)
		c.Assert(result.GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
		after, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.Id})
		c.Assert(err, qt.IsNil)
		c.Assert(after.GetTables(), qt.HasLen, 1)
		c.Assert(after.Tables[0].Identity.Name, qt.Equals, "items")
		c.Assert(after.Tables[0].Columns, qt.HasLen, 2)
		c.Assert(after.Tables[0].Columns[0].PrimaryKey, qt.IsTrue)
		c.Assert(client.CloseSession(c.Context(), session.Id), qt.IsNil)
		c.Assert(client.CloseSession(c.Context(), session.Id), qt.IsNil)
		_, err = client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.Id})
		c.Assert(err, qt.IsNotNil)
		client.Close()
		client.Close()
	})

	t.Run("crash after committed write is unknown", func(t *testing.T) {
		c := qt.New(t)
		client := start(c, options)
		session := open(c, client, "crash-after")
		_, err := client.Execute(c.Context(), execution(session.Id))
		c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
		fresh := start(c, options)
		reopened, err := fresh.Open(c.Context(), &protocol.OpenRequest{Engine: "fixture", Address: (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(session.Database), "/")}).String()})
		c.Assert(err, qt.IsNil)
		after, err := fresh.Inspect(c.Context(), &protocol.InspectRequest{SessionId: reopened.Id})
		c.Assert(err, qt.IsNil)
		c.Assert(after.GetTables(), qt.HasLen, 1)
		c.Assert(after.Tables[0].Identity.Name, qt.Equals, "first")
	})

	t.Run("partial result preserves confirmed prefix", func(t *testing.T) {
		c := qt.New(t)
		client := start(c, options)
		session := open(c, client, "partial")
		result, err := client.Execute(c.Context(), execution(session.Id))
		c.Assert(err, qt.ErrorIs, provider.ErrExecution)
		c.Assert(result.GetAppliedCount(), qt.Equals, uint32(1))
		after, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.Id})
		c.Assert(err, qt.IsNil)
		c.Assert(after.GetTables(), qt.HasLen, 1)
	})

	t.Run("incomplete inspection refuses comparison input", func(t *testing.T) {
		c := qt.New(t)
		client := start(c, options)
		session := open(c, client, "incomplete")
		schema, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.Id})
		c.Assert(err, qt.IsNotNil)
		c.Assert(schema, qt.IsNil)
	})

	t.Run("cancellation leaves connection usable", func(t *testing.T) {
		c := qt.New(t)
		client := start(c, options)
		slow := open(c, client, "slow-inspect")
		ctx, cancel := context.WithTimeout(c.Context(), 100*time.Millisecond)
		defer cancel()
		_, err := client.Inspect(ctx, &protocol.InspectRequest{SessionId: slow.Id})
		c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
		fast := open(c, client, "")
		_, err = client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: fast.Id})
		c.Assert(err, qt.IsNil)
	})

	t.Run("cancellation after write is unknown", func(t *testing.T) {
		c := qt.New(t)
		client := start(c, options)
		session := open(c, client, "delay-after")
		ctx, cancel := context.WithCancel(c.Context())
		defer cancel()
		completed := make(chan error, 1)
		go func() { _, err := client.Execute(ctx, execution(session.Id)); completed <- err }()
		c.Assert(await(c.Context(), func() bool { _, err := os.Stat(session.Database + ".applied"); return err == nil }), qt.IsNil)
		cancel()
		c.Assert(<-completed, qt.ErrorIs, provider.ErrOutcomeUnknown)
		after, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.Id})
		c.Assert(err, qt.IsNil)
		c.Assert(after.GetTables(), qt.HasLen, 1)
	})

	t.Run("environment is explicitly selected", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_FIXTURE_HOST_SECRET", "secret")
		client := start(c, options)
		session := open(c, client, "")
		c.Assert(session.GetId(), qt.Not(qt.Equals), "")
		selected := options
		selected.Environment = []string{"PTAH_FIXTURE_HOST_SECRET=secret"}
		other := start(c, selected)
		_, err := other.Open(c.Context(), &protocol.OpenRequest{Engine: "fixture", Address: "secret-address"})
		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Not(qt.Contains), "secret")
	})

	t.Run("message size is bounded", func(t *testing.T) {
		c := qt.New(t)
		client := start(c, options)
		result, err := client.Render(c.Context(), &protocol.RenderRequest{Engine: "fixture", TargetFeatures: []string{strings.Repeat("x", transport.MaxMessageBytes)}})
		c.Assert(err, qt.IsNotNil)
		c.Assert(result, qt.IsNil)
	})

	t.Run("provider ends when host is killed", func(t *testing.T) {
		c := qt.New(t)
		hostBinary := buildFixture(c, "host")
		pidFile := filepath.Join(c.TempDir(), "provider.pid")
		command := exec.CommandContext(c.Context(), hostBinary, binary, pidFile)
		input, err := command.StdinPipe()
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { _ = input.Close() })
		output, err := command.StdoutPipe()
		c.Assert(err, qt.IsNil)
		c.Assert(command.Start(), qt.IsNil)
		c.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
		ready, err := bufio.NewReader(output).ReadString('\n')
		c.Assert(err, qt.IsNil)
		c.Assert(ready, qt.Equals, "ready\n")
		pidBytes, err := os.ReadFile(pidFile)
		c.Assert(err, qt.IsNil)
		pid, err := strconv.Atoi(string(pidBytes))
		c.Assert(err, qt.IsNil)
		c.Assert(testprocess.Running(pid), qt.IsTrue)
		c.Cleanup(func() { stopProcess(pid) })
		c.Assert(command.Process.Kill(), qt.IsNil)
		c.Assert(command.Wait(), qt.IsNotNil)
		c.Assert(await(c.Context(), func() bool { return !testprocess.Running(pid) }), qt.IsNil)
	})

	t.Run("unauthenticated bootstrap is refused", func(t *testing.T) {
		c := qt.New(t)
		command := exec.CommandContext(c.Context(), binary)
		command.Env = []string{"PTAH_PROVIDER_BOOTSTRAP=ptah-provider-experimental-1"}
		output, err := command.CombinedOutput()
		c.Assert(err, qt.IsNotNil)
		c.Assert(string(output), qt.Contains, "bootstrap requires authenticated transport")
	})

	t.Run("invalid checksum prevents startup", func(t *testing.T) {
		c := qt.New(t)
		wrong := options
		wrong.SHA256 = strings.Repeat("0", 64)
		client, err := transport.Start(c.Context(), wrong)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(client, qt.IsNil)
	})

	t.Run("identity mismatch prevents sessions", func(t *testing.T) {
		c := qt.New(t)
		wrong := options
		wrong.ProviderID = "example.org/another-provider"
		client, err := transport.Start(c.Context(), wrong)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(client, qt.IsNil)
	})
}

func buildFixture(c *qt.C, program string) string {
	c.Helper()
	_, source, _, ok := runtime.Caller(0)
	c.Assert(ok, qt.IsTrue)
	dir := filepath.Join(filepath.Dir(source), "..", "testdata", "fixture")
	binary := filepath.Join(c.TempDir(), program+".exe")
	command := exec.CommandContext(c.Context(), "go", "build", "-o", binary, "./cmd/"+program)
	command.Dir = dir
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("fixture build: %s", output))
	return binary
}

func start(c *qt.C, options transport.Options) *transport.Client {
	c.Helper()
	client, err := transport.Start(c.Context(), options)
	c.Assert(err, qt.IsNil)
	c.Cleanup(client.Close)
	return client
}

func open(c *qt.C, client *transport.Client, fault string) *protocol.Session {
	c.Helper()
	address := &url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(c.TempDir(), "database.db")), "/"), RawQuery: url.Values{"fault": {fault}}.Encode()}
	// TempDir registers its cleanup above. Terminate the process before that
	// cleanup runs, because Windows refuses to remove an open database file.
	c.Cleanup(client.Close)
	session, err := client.Open(c.Context(), &protocol.OpenRequest{Engine: "fixture", Address: address.String()})
	c.Assert(err, qt.IsNil)
	return session
}

func execution(session string) *protocol.ExecuteRequest {
	return &protocol.ExecuteRequest{SessionId: session, RequestId: "request", Units: []*protocol.Unit{
		{ChangeId: "first", Body: &protocol.Unit_Sql{Sql: `CREATE TABLE "first" ("id" INTEGER NOT NULL);`}},
		{ChangeId: "second", Body: &protocol.Unit_Sql{Sql: `CREATE TABLE "second" ("id" INTEGER NOT NULL);`}},
	}}
}

func await(ctx context.Context, ready func() bool) error {
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ready() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for process state")
		case <-ticker.C:
		}
	}
}

func stopProcess(pid int) {
	process, err := os.FindProcess(pid)
	if err == nil {
		_ = process.Kill()
	}
}
