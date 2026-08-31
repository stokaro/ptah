package inference_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/inference"
)

// TestEveryVerbTakingASpecificationRefusesACredentialInTheEndpoint is
// stokaro/ptah#2644 asserted where the operator meets it.
//
// The refusal lives in one place — the document resolution both `--spec` and
// `--release` go through — and this is what says so. Measured on the shipped
// binary before it: `probe` reported "no credential was sent" on the request
// that carried the credential, `plan` printed the whole URL, `backfill` sent
// the corpus under the same header, and `plan --publish-evidence` wrote the
// document into the release artifact. A fix applied verb by verb would leave
// whichever verb nobody thought of still sending it.
//
// The verbs are walked rather than listed. A list here would be a claim that
// was true when it was written, and the verb added next month is exactly the
// one that would carry the credential.
func TestEveryVerbTakingASpecificationRefusesACredentialInTheEndpoint(t *testing.T) {
	c := qt.New(t)
	path := writePoisonedSpec(c)

	corpus := writeEmptyCorpus(c)
	verbs := verbsTakingASpecification(inference.NewCommand())
	c.Assert(len(verbs) > 5, qt.IsTrue,
		qt.Commentf("the walk found %d verbs; a walk that finds none passes vacuously", len(verbs)))

	for _, verb := range verbs {
		t.Run(verb.Name(), func(t *testing.T) {
			c := qt.New(t)

			_, err := runVerb(c, verb.Name(), argumentsFor(verb, path, corpus))

			c.Assert(err, qt.ErrorMatches,
				`(?s).*model\.endpoint carries a credential in its userinfo.*`)
		})
	}
}

// TestTheRefusalReachesTheOperatorBeforeAnyRequest is the property that makes
// the refusal a fix rather than a report.
//
// The endpoint in the fixture is a port nothing listens on, and the database
// URL names one too. A refusal that arrived after either was contacted would
// answer with a connection error instead — so an assertion that the endpoint
// message is what comes back is also an assertion that nothing was dialed.
func TestTheRefusalReachesTheOperatorBeforeAnyRequest(t *testing.T) {
	c := qt.New(t)

	output, err := runVerb(c, "probe", []string{"--spec", writePoisonedSpec(c)})

	c.Assert(err, qt.ErrorMatches, `(?s).*model\.endpoint carries a credential.*`)
	c.Assert(output+err.Error(), qt.Not(qt.Contains), "s3cr3t")
	c.Assert(output+err.Error(), qt.Not(qt.Contains), "connection refused")
}

// verbsTakingASpecification is every leaf command registering --spec.
func verbsTakingASpecification(root *cobra.Command) []*cobra.Command {
	var found []*cobra.Command
	for _, command := range root.Commands() {
		found = append(found, verbsTakingASpecification(command)...)
	}
	return appendIfTakesSpec(found, root)
}

// appendIfTakesSpec adds root when it is a leaf that registers --spec.
func appendIfTakesSpec(found []*cobra.Command, root *cobra.Command) []*cobra.Command {
	for range onlyWhen(!root.HasSubCommands() && root.Flags().Lookup("spec") != nil) {
		found = append(found, root)
	}
	return found
}

// argumentsFor supplies --spec plus a value for each flag below that this verb
// actually registers.
//
// It is keyed by FLAG rather than by verb, so a verb added later is given what
// it needs without being named here. The six are the ones some verb validates
// before it resolves the specification: without them `evaluate` answers
// "--corpus is required", `pause` "--reason is required", `rollback`
// "--to is required" and `retire` "--generation is required", and a row
// asserting a refusal would pass on the wrong refusal.
func argumentsFor(verb *cobra.Command, path, corpus string) []string {
	args := []string{"--spec", path}
	supplied := [][2]string{
		{"db-url", "postgres://u:p@127.0.0.1:9/x?sslmode=disable"},
		{"run-id", "credential-refusal"},
		{"corpus", corpus},
		{"reason", "asserting the endpoint refusal"},
		{"to", strings.Repeat("0", 64)},
		{"generation", strings.Repeat("0", 64)},
	}
	for _, flag := range supplied {
		for range onlyWhen(verb.Flags().Lookup(flag[0]) != nil) {
			args = append(args, "--"+flag[0], flag[1])
		}
	}
	return args
}

// writeEmptyCorpus is a file `evaluate` reads far enough to get past its own
// argument checks, so that verb reaches the specification rather than stopping
// on a missing or unreadable one.
func writeEmptyCorpus(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TB.(*testing.T).TempDir(), "corpus.jsonl")
	c.Assert(os.WriteFile(path, []byte("version: 1\nname: refusal\ndefault_k: 1\n"+
		"cases:\n  - id: one\n    query: anything\n    required: [a]\n"), 0o600), qt.IsNil)
	return path
}

// onlyWhen yields once when yes, so a caller can branch without an if. The
// test-style rule forbids conditionals in test functions; the branch belongs in
// a helper either way, because it is argument construction rather than an
// assertion.
func onlyWhen(yes bool) []struct{} {
	return map[bool][]struct{}{true: {{}}, false: nil}[yes]
}

// writePoisonedSpec is the measured document: the describe fixture with a
// service account and a token moved into the endpoint's userinfo.
func writePoisonedSpec(c *qt.C) string {
	c.Helper()
	document := strings.Replace(
		describeSpecDocument("test-embed", "embedding", "1"),
		"endpoint: http://127.0.0.1:9/v1",
		"endpoint: http://svc:s3cr3t-token@127.0.0.1:9/v1", 1)
	c.Assert(document, qt.Contains, "s3cr3t-token",
		qt.Commentf("the substitution must land, or every row asserts a refusal of nothing"))
	path := filepath.Join(c.TB.(*testing.T).TempDir(), "spec.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// runVerb executes one inference verb and returns everything it wrote.
func runVerb(c *qt.C, verb string, args []string) (string, error) {
	c.Helper()
	cmd := inference.NewCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs(append([]string{verb}, args...))
	return output.String(), cmd.ExecuteContext(context.Background())
}
