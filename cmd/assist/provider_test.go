package assist_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/assist"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// execute runs the command with the configuration directory pointed at a
// temporary tree, and returns what it printed.
//
// XDG_CONFIG_HOME is set through the process environment because that is what
// the command reads: the point of driving the command rather than the package
// is to measure the wiring, and a test that injected the configuration would
// skip exactly the part that could be wrong.
func execute(c *qt.C, configPath string, args ...string) (string, error) {
	out, _, err := executeStreams(c, configPath, args...)
	return out, err
}

// executeStreams keeps the two streams apart, because the contract this tree
// documents is that machine-readable output goes to stdout and diagnostics go
// to stderr. A helper that merged them would let a command print its JSON into
// the middle of a progress line and still pass.
func executeStreams(c *qt.C, configPath string, args ...string) (stdout, stderr string, _ error) {
	c.Helper()
	clearEnv(c,
		"PTAH_ASSIST_PROFILE", "PTAH_ASSIST_MODEL",
		"OPENAI_API_KEY", "OPENAI_BASE_URL", "ANTHROPIC_API_KEY", "OLLAMA_HOST")
	// The file is named directly rather than through the home directory: a test
	// that moved HOME would be testing the operator's shell rather than the
	// command, and one that wrote into the real home would be worse.
	c.Setenv("PTAH_ASSIST_CONFIG", configPath)
	// The provider commands write nothing, and `sessions` and `explain` reach
	// this helper too: moving out of the package directory keeps any of them
	// from leaving a file in the repository.
	c.Chdir(c.TempDir())

	cmd := assist.NewCommand()
	out := &bytes.Buffer{}
	errOut := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errOut)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), errOut.String(), err
}

// clearEnv unsets variables for the length of one test, and puts back what was
// there.
//
// Unset rather than emptied, because those are different states and this
// package treats them differently: an exported empty key is a configuration
// error the operator is told about, and blanking the variables here would test
// that path instead of the one each test names.
func clearEnv(c *qt.C, names ...string) {
	c.Helper()
	for _, name := range names {
		previous, was := os.LookupEnv(name)
		c.Assert(os.Unsetenv(name), qt.IsNil)
		c.Cleanup(func() {
			if was {
				_ = os.Setenv(name, previous)
				return
			}
			_ = os.Unsetenv(name)
		})
	}
}

// configHome writes a configuration file and returns its path.
func configHome(c *qt.C, content string) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), ".ptah")
	c.Assert(os.MkdirAll(dir, 0o700), qt.IsNil)
	path := filepath.Join(dir, "assist.yaml")
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}

// endpoint starts a stub model server and returns its base URL.
func endpoint(c *qt.C, chat http.HandlerFunc) string {
	c.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"data":[{"id":"a-model"}]}`))
	})
	mux.HandleFunc("/v1/chat/completions", chat)
	server := httptest.NewServer(mux)
	c.Cleanup(server.Close)
	return server.URL + "/v1"
}

// answersWithAToolCall is a model that can do what Assist needs.
func answersWithAToolCall(writer http.ResponseWriter, _ *http.Request) {
	_, _ = writer.Write([]byte(`{"id":"c1","model":"a-model","choices":[{"finish_reason":"tool_calls",
		"message":{"role":"assistant","tool_calls":[{"id":"t1","type":"function",
		"function":{"name":"ptah_connectivity_check","arguments":"{\"ok\":true}"}}]}}]}`))
}

// answersWithProse is a model that cannot.
func answersWithProse(writer http.ResponseWriter, _ *http.Request) {
	_, _ = writer.Write([]byte(`{"id":"c1","model":"a-model","choices":[{"finish_reason":"stop",
		"message":{"role":"assistant","content":"I am unable to call tools."}}]}`))
}

// profileFor renders a configuration naming one endpoint.
func profileFor(base string) string {
	return "default: local\n\nprofiles:\n  local:\n    type: openai-compatible\n    base_url: " +
		base + "\n    model: a-model\n"
}

func TestProviderList_ReportsWhereEachProfileCameFrom(t *testing.T) {
	c := qt.New(t)
	home := configHome(c, profileFor("https://example.invalid/v1"))

	out, err := execute(c, home, "provider", "list")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "local (openai-compatible, from the configuration file)")
	c.Assert(out, qt.Contains, "https://example.invalid/v1")
	c.Assert(out, qt.Contains, "* is the default")
}

func TestProviderList_SaysWhatToDoWhenNothingIsConfigured(t *testing.T) {
	// The first thing an operator sees. A bare "no profiles" would leave them
	// to find the documentation; naming the variables they may already have
	// exported is the answer most of them need.
	c := qt.New(t)

	out, err := execute(c, filepath.Join(c.TempDir(), "absent.yaml"), "provider", "list")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "No provider profiles are configured")
	c.Assert(out, qt.Contains, "OPENAI_API_KEY")
	c.Assert(out, qt.Contains, "OLLAMA_HOST")
}

func TestProviderList_NeverReadsACredential(t *testing.T) {
	// Listing profiles must not require the keys they name: an operator with
	// three profiles and one key present should still see all three.
	c := qt.New(t)
	home := configHome(c, "profiles:\n  work:\n    type: anthropic\n    model: m\n"+
		"    credential: env:A_VARIABLE_THAT_IS_NOT_SET\n")

	out, err := execute(c, home, "provider", "list")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "work (anthropic")
	c.Assert(out, qt.Contains, "credential: env:A_VARIABLE_THAT_IS_NOT_SET")
}

func TestProviderTest_HappyPath(t *testing.T) {
	c := qt.New(t)
	home := configHome(c, profileFor(endpoint(c, answersWithAToolCall)))

	out, err := execute(c, home, "provider", "test")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "reachable:     yes")
	c.Assert(out, qt.Contains, "credential:    accepted")
	c.Assert(out, qt.Contains, "model listed:  yes")
	c.Assert(out, qt.Contains, "tool calling:  yes")
}

func TestProviderTest_ReportsAModelThatCannotCallTools(t *testing.T) {
	// #1488's rule: a model without tool calling is refused with a capability
	// error rather than accepted into a mode that invents SQL and claims it was
	// validated. The exit code is the machine-readable half of that.
	c := qt.New(t)
	home := configHome(c, profileFor(endpoint(c, answersWithProse)))

	out, err := execute(c, home, "provider", "test")

	c.Assert(out, qt.Contains, "tool calling:  no")
	c.Assert(err, qt.ErrorMatches, `(?s)profile "local": the selected model does not provide the tool-calling.*`)

	c.Assert(exitcode.Code(err, 2), qt.Equals, 1,
		qt.Commentf("an unusable profile is an expected negative, not a usage error"))
}

func TestProviderTest_JSONCarriesTheDocumentEvenWhenTheProfileFails(t *testing.T) {
	// The stream contract: the machine-readable document goes to stdout in
	// every case, and the outcome is the exit code. A caller that got no
	// document on failure would have to parse the error text.
	c := qt.New(t)
	home := configHome(c, profileFor(endpoint(c, answersWithProse)))

	out, err := execute(c, home, "provider", "test", "--format", "json")

	c.Assert(err, qt.IsNotNil)
	report := make(map[string]any)
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil)
	c.Assert(report["usable"], qt.IsFalse)
	c.Assert(report["reachable"], qt.IsTrue)
	c.Assert(report["tool_calling"], qt.IsFalse)
	c.Assert(report["profile"], qt.Equals, "local")
}

func TestProviderTest_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "a profile that does not exist",
			args:    []string{"provider", "test", "--provider-profile", "nope"},
			wantErr: `unknown provider profile "nope".*`,
		},
		{
			name:    "an unknown output format",
			args:    []string{"provider", "test", "--format", "yaml"},
			wantErr: `--format: unknown format "yaml"; want text or json`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			home := configHome(c, profileFor("https://example.invalid/v1"))

			_, err := execute(c, home, test.args...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestProviderList_AnExportedEmptyKeyIsAConfigurationErrorRatherThanSilence(t *testing.T) {
	// os.Getenv answers the same empty string for an absent variable and for
	// one exported empty, which is how a typo in a CI environment file becomes
	// a silent default. This package distinguishes them, the same way
	// internal/envbool does for boolean variables: the profile is derived, and
	// using it reports the variable by name.
	c := qt.New(t)
	clearEnv(c, "PTAH_ASSIST_PROFILE", "PTAH_ASSIST_MODEL",
		"OPENAI_BASE_URL", "ANTHROPIC_API_KEY", "OLLAMA_HOST")
	c.Setenv("PTAH_ASSIST_CONFIG", filepath.Join(c.TempDir(), "absent.yaml"))
	c.Setenv("OPENAI_API_KEY", "")

	cmd := assist.NewCommand()
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetArgs([]string{"provider", "test", "--model", "a-model"})
	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `(?s).*OPENAI_API_KEY is set to an empty value.*`)
}

func TestProviderTest_SendsNoProjectContent(t *testing.T) {
	// An operator testing a provider has not agreed to send it anything about
	// their schema. The check is a fixed prompt, and this is what says so.
	c := qt.New(t)
	bodies := make([]string, 0, 1)
	base := endpoint(c, func(writer http.ResponseWriter, request *http.Request) {
		payload := make([]byte, request.ContentLength)
		_, _ = request.Body.Read(payload)
		bodies = append(bodies, string(payload))
		answersWithAToolCall(writer, request)
	})
	home := configHome(c, profileFor(base))

	_, err := execute(c, home, "provider", "test")

	c.Assert(err, qt.IsNil)
	c.Assert(bodies, qt.HasLen, 1)
	c.Assert(bodies[0], qt.Contains, "ptah_connectivity_check")
	c.Assert(bodies[0], qt.Not(qt.Contains), "migration")
	c.Assert(bodies[0], qt.Not(qt.Contains), "schema")
}
