//go:build integration

package root_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/root"
	"ptah.run/internal/providerhost"
)

func TestNativeCLIWithIndependentlyBuiltProvider(t *testing.T) {
	c := qt.New(t)
	binary := independentCLIProvider(c)
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	spec := providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"}
	t.Run("inspect plan apply verify", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		c.Setenv("PTAH_FIXTURE_HOST_SECRET", "must-not-reach-provider")
		before := f.success(c, "inspect", "--format", "json")
		c.Assert(before, qt.Not(qt.Contains), "items")
		planned := f.plan(c)
		c.Assert(json.Valid([]byte(planned)), qt.IsTrue)
		c.Assert(planned, qt.Contains, `"outcome": "changes"`)
		c.Assert(planned, qt.Contains, `"provider"`)
		applied := f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve", "--json")
		c.Assert(json.Valid([]byte(applied)), qt.IsTrue)
		c.Assert(applied, qt.Contains, `"outcome": "applied"`)
		after := f.success(c, "inspect", "--format", "json")
		c.Assert(after, qt.Contains, `"name":"items"`)
		c.Assert(after, qt.Contains, `"name":"second"`)
		c.Assert(f.plan(c), qt.Contains, `"outcome": "no-changes"`)
	})

	t.Run("unsupported transaction mode writes nothing", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		f.plan(c)
		_, diagnostics, code := f.run(c, c.Context(), "apply", "--plan", f.planPath, "--auto-approve", "--json")
		c.Assert(code, qt.Equals, 2)
		c.Assert(diagnostics, qt.Contains, "transactions")
		c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), "items")
	})
	t.Run("explicit schema reaches the owner unchanged", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		c.Assert(os.WriteFile(f.schemaPath, []byte("tables:\n  items:\n    schema: main\n    columns:\n      id:\n        type: INTEGER\n"), 0o600), qt.IsNil)
		_, diagnostics, code := f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")
		c.Assert(code, qt.Equals, 2)
		c.Assert(diagnostics, qt.Contains, "provider render failed")
		c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), "items")
	})
	t.Run("incomplete snapshot is an error", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		f.address += "?fault=incomplete"
		out, diagnostics, code := f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")
		c.Assert(code, qt.Equals, 2)
		c.Assert(out, qt.Contains, `"outcome": "failed"`)
		c.Assert(diagnostics, qt.Contains, "provider inspect failed")
		_, err := os.Stat(f.planPath)
		c.Assert(os.IsNotExist(err), qt.IsTrue)
	})
	t.Run("provider binding mismatch refuses before write", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		f.plan(c)
		data, err := os.ReadFile(f.planPath)
		c.Assert(err, qt.IsNil)
		var plan map[string]any
		c.Assert(json.Unmarshal(data, &plan), qt.IsNil)
		binding := plan["provider"].(map[string]any)["identity"].(map[string]any)
		binding["artifact_sha256"] = strings.Repeat("a", 64)
		changed, err := json.Marshal(plan)
		c.Assert(err, qt.IsNil)
		c.Assert(os.WriteFile(f.planPath, changed, 0o600), qt.IsNil)
		_, diagnostics, code := f.run(c, c.Context(), "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve", "--json")
		c.Assert(code, qt.Equals, 2)
		c.Assert(diagnostics, qt.Contains, "provider")
		c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), "items")
	})
	t.Run("crash after write is unknown and never replayed", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		f.plan(c)
		f.address += "?fault=crash-after"
		out, diagnostics, code := f.run(c, c.Context(), "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve", "--json")
		c.Assert(code, qt.Equals, 2)
		c.Assert(out, qt.Contains, `"outcome": "unknown"`)
		c.Assert(diagnostics, qt.Contains, "UNKNOWN")
		f.address = strings.Split(f.address, "?")[0]
		after := f.success(c, "inspect", "--format", "json")
		c.Assert(after, qt.Contains, `"name":"items"`)
		c.Assert(after, qt.Not(qt.Contains), `"name":"second"`)
		_, _, code = f.run(c, c.Context(), "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve", "--json")
		c.Assert(code, qt.Equals, 2)
		c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), `"name":"second"`)
	})
	t.Run("cancellation after write preserves uncertainty", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		f.plan(c)
		f.address += "?fault=delay-after"
		ctx, cancel := context.WithCancel(c.Context())
		defer cancel()
		type result struct {
			output, diagnostics string
			code                int
		}
		done := make(chan result, 1)
		go func() {
			out, diagnostics, code := executeProviderCLI(ctx, f.arguments("apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve", "--json"))
			done <- result{out, diagnostics, code}
		}()
		c.Assert(awaitProviderCLI(c.Context(), func() bool { _, err := os.Stat(f.databasePath + ".applied"); return err == nil }), qt.IsNil)
		cancel()
		reply := <-done
		c.Assert(reply.code, qt.Equals, 2)
		c.Assert(reply.output, qt.Contains, `"outcome": "unknown"`)
		f.assertStopped(c)
		f.address = strings.Split(f.address, "?")[0]
		after := f.success(c, "inspect", "--format", "json")
		c.Assert(after, qt.Contains, `"name":"items"`)
		c.Assert(after, qt.Not(qt.Contains), `"name":"second"`)
	})
	t.Run("checksum refusal starts no process", func(t *testing.T) {
		c := qt.New(t)
		bad := spec
		bad.SHA256 = strings.Repeat("0", 64)
		f := newProviderCLI(c, bad)
		_, diagnostics, code := executeProviderCLI(c.Context(), f.arguments("inspect", "--format", "json"))
		c.Assert(code, qt.Equals, 2)
		c.Assert(diagnostics, qt.Contains, "checksum")
		_, err := os.Stat(f.pidPath)
		c.Assert(os.IsNotExist(err), qt.IsTrue)
		_, err = os.Stat(f.databasePath)
		c.Assert(os.IsNotExist(err), qt.IsTrue)
	})
}

type providerCLI struct{ manifestPath, pidPath, address, databasePath, schemaPath, planPath string }

func newProviderCLI(c *qt.C, spec providerhost.Spec) providerCLI {
	c.Helper()
	dir := c.TempDir()
	f := providerCLI{manifestPath: filepath.Join(dir, "providers.json"), pidPath: filepath.Join(dir, "provider.pid"), databasePath: filepath.Join(dir, "database.db"), schemaPath: filepath.Join(dir, "schema.yaml"), planPath: filepath.Join(dir, "change.plan.json")}
	f.address = (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(f.databasePath), "/")}).String()
	spec.Environment = []string{"PTAH_FIXTURE_PID_FILE=" + f.pidPath}
	manifest, err := json.Marshal(map[string]any{"format_version": 1, "providers": []providerhost.Spec{spec}})
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(f.manifestPath, manifest, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(f.schemaPath, []byte("tables:\n  items:\n    columns:\n      id:\n        type: INTEGER\n        not_null: true\n  second:\n    columns:\n      value:\n        type: TEXT\n"), 0o600), qt.IsNil)
	return f
}
func (f providerCLI) arguments(verb string, args ...string) []string {
	return append([]string{"schema", verb, "--provider-config", f.manifestPath, "--db-url", f.address}, args...)
}
func executeProviderCLI(ctx context.Context, args []string) (stdout, stderr string, exitCode int) {
	cmd := root.NewRootCommand()
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	cmd.SetIn(strings.NewReader(""))
	code := root.RunContext(ctx, cmd, args...)
	return out.String(), diagnostics.String(), code
}
func (f providerCLI) run(c *qt.C, ctx context.Context, verb string, args ...string) (stdout, stderr string, exitCode int) {
	c.Helper()
	out, diagnostics, code := executeProviderCLI(ctx, f.arguments(verb, args...))
	f.assertStopped(c)
	return out, diagnostics, code
}
func (f providerCLI) success(c *qt.C, verb string, args ...string) string {
	c.Helper()
	out, diagnostics, code := f.run(c, c.Context(), verb, args...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	return out
}
func (f providerCLI) plan(c *qt.C) string {
	c.Helper()
	return f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")
}
func (f providerCLI) assertStopped(c *qt.C) {
	c.Helper()
	data, err := os.ReadFile(f.pidPath)
	c.Assert(err, qt.IsNil)
	pid, err := strconv.Atoi(string(data))
	c.Assert(err, qt.IsNil)
	c.Assert(awaitProviderCLI(c.Context(), func() bool { return !providerProcessRunning(pid) }), qt.IsNil)
}
func awaitProviderCLI(ctx context.Context, ready func() bool) error {
	wait, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ready() {
			return nil
		}
		select {
		case <-wait.Done():
			return wait.Err()
		case <-ticker.C:
		}
	}
}

// Build outside the checkout against the host's published SDK dependency.
// No local replacement or workspace can satisfy the independent build.
func independentCLIProvider(c *qt.C) string {
	c.Helper()
	repo, err := filepath.Abs("../../..")
	c.Assert(err, qt.IsNil)
	dir := c.TempDir()
	copyRoot, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(copyRoot.Close(), qt.IsNil) })
	const prefix = "provider-sdk/testdata/fixture/"
	paths := strings.Split(strings.TrimSpace(string(providerBuildCommand(c, repo, "git", "ls-files", prefix))), "\n")
	c.Assert(len(paths) >= 5, qt.IsTrue)
	for _, path := range paths {
		relative, ok := strings.CutPrefix(path, prefix)
		c.Assert(ok, qt.IsTrue)
		data, err := os.ReadFile(filepath.Join(repo, path))
		c.Assert(err, qt.IsNil)
		c.Assert(copyRoot.MkdirAll(filepath.Dir(relative), 0o700), qt.IsNil)
		c.Assert(copyRoot.WriteFile(relative, data, 0o600), qt.IsNil)
	}
	const sdk = "github.com/stokaro/ptah/provider-sdk"
	version := strings.TrimSpace(string(providerBuildCommand(c, repo, "go", "list", "-m", "-f", "{{.Version}}", sdk)))
	c.Assert(version, qt.Not(qt.Equals), "")
	providerBuildCommand(c, dir, "go", "mod", "edit", "-dropreplace="+sdk, "-require="+sdk+"@"+version)
	providerBuildCommand(c, dir, "go", "mod", "tidy")
	dependencies := string(providerBuildCommand(c, dir, "go", "list", "-deps", "-f", "{{.ImportPath}}", "./cmd/provider"))
	c.Assert(dependencies, qt.Not(qt.Contains), "ptah.run/")
	binary := filepath.Join(c.TempDir(), "provider.exe")
	providerBuildCommand(c, dir, "go", "build", "-o", binary, "./cmd/provider")
	return binary
}
func providerBuildCommand(c *qt.C, dir, program string, args ...string) []byte {
	c.Helper()
	command := exec.CommandContext(c.Context(), program, args...)
	command.Dir = dir
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s %v: %s", program, args, output))
	return output
}
