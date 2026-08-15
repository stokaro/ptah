package schema_test

import (
	"bytes"
	"net"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/schema"
)

// This file pins the DECISION behind stokaro/ptah#928 item 2, not merely its
// outcome.
//
// The item is that `ptah schema inspect --schema-file oci://…` was refused
// where six sibling commands accept it. The refusal was not an oversight:
// inspect does not parse its source. It hands the value to
// internal/atlasschema.InspectSource, which materializes it on a destructively
// reset --dev-url database and introspects the result — which is the point of
// the verb — and that path classifies the value through
// internal/atlassource.Classify, which names no oci source kind.
//
// Classify is SHARED with the Atlas-compatible surface: thirteen non-test call
// sites live under cmd/atlas and internal/atlasschema, and
// cmd/atlas/compat_url_diagnostic.go re-words exactly its unsupported-scheme
// verdict — its own comment says it does so "so a scheme added to
// atlassource.Classify later is recognized here without this file being
// edited". Teaching Classify an oci branch would therefore have stopped
// `ptah-compat schema inspect --url oci://…` refusing, and the pinned community
// binary refuses that reference at exit 1. That is a compatibility-policy (a)
// violation handed to a surface this issue never mentioned.
//
// So the scheme is resolved on the native verb, before classification. The two
// tests below are the two halves of that decision, and they are in one file on
// purpose: the second is what makes the first's justification checkable rather
// than merely asserted. A future change that took the easy route — one branch
// in Classify — would pass the first and fail the second.

// closedRegistryReference returns an oci:// reference whose host is a port
// nothing is listening on.
//
// A closed port is what lets both halves be measured with no registry at all:
// a command that RESOLVES the scheme gets far enough to dial and fails with
// `connection refused`, and one that REFUSES the scheme never dials and says so
// in different words. The two dispositions are distinguishable in the message,
// which is the whole discrimination this file needs.
func closedRegistryReference(tb testing.TB) string {
	c := qt.New(tb)
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	addr := listener.Addr().String()
	c.Assert(listener.Close(), qt.IsNil)
	return "oci://" + addr + "/demo/schema:v1"
}

func runCommand(cmd *cobra.Command, args ...string) (string, error) {
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// TestSchemaInspect_ResolvesAnOCISchemaFile is item 2's outcome.
//
// The assertion is about WHICH failure occurs, not that the command succeeds:
// with no registry behind the reference the pull cannot complete, and that is
// the point — the pull was attempted. Before this change the same invocation
// never reached a socket and answered `unsupported desired-state URL scheme
// "oci"`, which is the string asserted absent.
func TestSchemaInspect_ResolvesAnOCISchemaFile(t *testing.T) {
	c := qt.New(t)
	reference := closedRegistryReference(c.TB)

	out, err := runCommand(schema.NewSchemaCommand(),
		"inspect", "--schema-file", reference, "--dev-url", "sqlite://:memory:", "--plain-http")
	combined := out + errorText(err)

	c.Assert(err, qt.IsNotNil, qt.Commentf("output:\n%s", combined))
	c.Check(combined, qt.Not(qt.Contains), `unsupported desired-state URL scheme "oci"`)
	c.Check(combined, qt.Not(qt.Contains), "unknown flag: --plain-http")
	c.Check(combined, qt.Contains, "connection refused", qt.Commentf("output:\n%s", combined))
	// --plain-http reached the OCI client rather than being parsed and
	// dropped: the dial that failed was plain HTTP.
	c.Check(combined, qt.Contains, "http://", qt.Commentf("output:\n%s", combined))
	c.Check(combined, qt.Not(qt.Contains), "https://")
}

// TestSchemaInspect_WithoutPlainHTTPUsesTLS is the companion direction. A build
// where the flag changed nothing would dial HTTPS with it and HTTPS without it;
// a build where TLS stopped being the default would dial plain HTTP in both.
func TestSchemaInspect_WithoutPlainHTTPUsesTLS(t *testing.T) {
	c := qt.New(t)
	reference := closedRegistryReference(c.TB)

	out, err := runCommand(schema.NewSchemaCommand(),
		"inspect", "--schema-file", reference, "--dev-url", "sqlite://:memory:")
	combined := out + errorText(err)

	c.Assert(err, qt.IsNotNil, qt.Commentf("output:\n%s", combined))
	c.Check(combined, qt.Contains, "https://", qt.Commentf("output:\n%s", combined))
}

// TestSchemaInspect_AnswersLocalArgumentErrorsBeforeReachingTheRegistry is the
// review finding on stokaro/ptah#1496, pinned.
//
// Resolving the artifact before the source can be classified put the pull in
// front of every local argument check, so `--format garbage` and a missing
// `--dev-url` were answered with a registry dial failure instead of the message
// the identical local-file invocation receives. On a slow or unreachable
// registry the operator waited for a timeout to be told about a typo, and on a
// reachable one an authenticated round trip happened for a command that could
// never have run.
//
// The rows use a closed port, so a probe that DID reach the registry reports
// `connection refused` — which is what each row asserts absent. The local
// control alongside each is the message the same mistake produces without an
// oci:// source, so the rows pin agreement between the two spellings rather
// than merely pinning some error.
func TestSchemaInspect_AnswersLocalArgumentErrorsBeforeReachingTheRegistry(t *testing.T) {
	tests := []struct {
		name string
		// extra are the arguments that make the invocation locally wrong.
		extra []string
		want  string
	}{
		{
			name:  "unsupported --format",
			extra: []string{"--dev-url", "sqlite://:memory:", "--format", "garbage"},
			want:  `unsupported --format "garbage": expected hcl, sql, or json`,
		},
		{
			name:  "absent --dev-url",
			extra: nil,
			want:  "--dev-url cannot be empty",
		},
		{
			name:  "unsupported --include selector",
			extra: []string{"--dev-url", "sqlite://:memory:", "--include", "[type=column]"},
			want:  `unsupported Atlas include selector "[type=column]"`,
		},
		{
			// Parsed after the pull until stokaro/ptah#1496, so an
			// unparseable duration was reported as a registry dial failure.
			name:  "unparseable --connect-timeout",
			extra: []string{"--dev-url", "sqlite://:memory:", "--connect-timeout", "nonsense"},
			want:  `invalid --connect-timeout value "nonsense"`,
		},
		{
			// The preflight judged the raw flag while the code that enforces
			// the same requirement trimmed first, so three spaces read as a
			// value here and as empty there. The two are one value now.
			name:  "whitespace-only --dev-url",
			extra: []string{"--dev-url", "   "},
			want:  "--dev-url cannot be empty",
		},
		{
			// The same disagreement on the other dev-URL verdict: a leading
			// space stopped the docker:// form being recognized.
			//
			// The value this row carries changed with stokaro/ptah#1468, which
			// made `docker://` a dev database this verb PROVISIONS rather than
			// one it refuses. `docker://postgres/16/dev` is therefore no longer
			// a local error at all, and the row would have been pinning the
			// absence of a feature. What is still local is the URL's FORM, so
			// the row names an engine no image table has.
			//
			// The leading space is kept, but it no longer discriminates on its
			// own and the comment above must not claim it does: measured by
			// removing the command's TrimSpace, this row stays GREEN, because
			// atlasurl and devdocker both normalize a docker URL themselves.
			// The row above is the one that reddens, and it is what pins the
			// command's single normalization. What the space still proves here
			// is the other direction -- that the docker preflight is reached
			// for a value carrying one, so it does not have to trim again.
			name:  "whitespace-prefixed docker --dev-url naming no engine",
			extra: []string{"--dev-url", " docker://nosuchengine/16/dev"},
			want:  `unsupported docker --dev-url engine "nosuchengine"`,
		},
		{
			// A different function answers this one. The engine is real and
			// the dialect pins, so only devdocker.Parse can refuse it: a
			// preflight that ran the dialect check alone would pass the row
			// above and still dial the registry for this.
			name:  "docker --dev-url path with too many segments",
			extra: []string{"--dev-url", "docker://postgres/16/a/b/c"},
			want:  `docker --dev-url path "/16/a/b/c" has more than <tag>/<database>`,
		},
		{
			// The refusal most worth reaching without a network. The parameter
			// would point the connection away from the container the URL
			// provisions, at a database this run then destructively resets, so
			// answering it after an authenticated registry round trip is the
			// worst place to answer it.
			name:  "docker --dev-url rerouting parameter",
			extra: []string{"--dev-url", "docker://postgres/16/dev?host=prod.example"},
			want:  `docker --dev-url parameter "host" would point the connection away from the container this URL provisions`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			reference := closedRegistryReference(c.TB)

			out, err := runCommand(schema.NewSchemaCommand(), append([]string{
				"inspect", "--schema-file", reference, "--plain-http",
			}, tt.extra...)...)
			combined := out + errorText(err)

			c.Assert(err, qt.IsNotNil, qt.Commentf("output:\n%s", combined))
			c.Check(combined, qt.Contains, tt.want, qt.Commentf("output:\n%s", combined))
			// Nothing was dialed: the local error came first.
			c.Check(combined, qt.Not(qt.Contains), "connection refused")
			c.Check(combined, qt.Not(qt.Contains), "fetch OCI manifest")
		})
	}
}

// TestSchemaInspect_ProvisionableDockerDevURLReachesTheRegistry is the other
// half of the docker rows above, and the half that stops them being satisfied
// by deleting a capability.
//
// The rows answer the forms that are wrong in the URL text. A `docker://` value
// that is NOT wrong is a dev database stokaro/ptah#1468 provisions, so it has to
// travel on to the pull. Without this test the cheapest way to make those rows
// green is the check this file used to carry -- refuse every `docker://` -- and
// that would pass all three while silently removing the feature the branch
// exists to add.
//
// It also pins the order of the two remote steps, which is a decision and not an
// accident. The artifact is pulled BEFORE the container is started, so a
// registry that cannot be reached leaves nothing behind to clean up. The pinned
// community binary v1.3.0 orders them the other way: measured with an unreadable
// source and `--dev-url docker://postgres/16/dev`, it creates, starts and
// destroys a postgres:16 container and only then reports that the file does not
// exist. Matching that would trade a wasted network read for a container
// lifecycle on every failed run, which compatibility rule (b) does not ask for.
// Reversing the two here would answer this invocation with a Docker diagnostic,
// or start a real database inside a unit test, instead of the dial failure
// below.
func TestSchemaInspect_ProvisionableDockerDevURLReachesTheRegistry(t *testing.T) {
	c := qt.New(t)
	reference := closedRegistryReference(c.TB)

	out, err := runCommand(schema.NewSchemaCommand(),
		"inspect", "--schema-file", reference, "--plain-http",
		"--dev-url", "docker://postgres/16/dev")
	combined := out + errorText(err)

	c.Assert(err, qt.IsNotNil, qt.Commentf("output:\n%s", combined))
	c.Check(combined, qt.Contains, "connection refused", qt.Commentf("output:\n%s", combined))
	// The value was carried, not refused: none of the dev-URL verdicts fired.
	c.Check(combined, qt.Not(qt.Contains), "directly connectable dev database URL")
	c.Check(combined, qt.Not(qt.Contains), "unsupported docker")
	// Nothing reached the Docker daemon, because the pull came first. A build
	// that provisioned before pulling could not produce an output free of every
	// spelling of the word.
	c.Check(combined, qt.Not(qt.Contains), "docker")
	c.Check(combined, qt.Not(qt.Contains), "Docker")
}

// TestCompatSchemaInspect_StillRefusesOCI is the guard that makes the decision
// above load-bearing rather than a preference.
//
// The pinned community binary answers `oci://` on this surface with
// `sql/sqlclient: unknown driver "oci"` at exit 1, and compatibility policy (a)
// forbids ptah-compat exiting 0 where it exits 1. This surface must therefore
// go on refusing it whatever the native verb learns.
//
// It reddens under exactly the change this file argues against: adding an oci
// branch to atlassource.Classify makes atlasDesiredStateURLDiagnostic stop
// recognizing the unsupported-scheme verdict, and the refusal disappears.
func TestCompatSchemaInspect_StillRefusesOCI(t *testing.T) {
	c := qt.New(t)
	reference := closedRegistryReference(c.TB)

	out, err := runCommand(atlas.NewCompatCommand("atlas"),
		"schema", "inspect", "--url", reference)
	combined := out + errorText(err)

	c.Assert(err, qt.IsNotNil, qt.Commentf("output:\n%s", combined))
	c.Check(combined, qt.Contains, `sql/sqlclient: unknown driver "oci"`,
		qt.Commentf("output:\n%s", combined))
	// It refused rather than dialing: no socket was opened for a scheme this
	// surface does not implement.
	c.Check(combined, qt.Not(qt.Contains), "connection refused")
}

// TestCompatSchemaInspect_RegistersNoPlainHTTP pins the other half of the
// boundary.
//
// The conformance `cli-surface` tier asserts flag parity against the pinned
// binary, which registers no --plain-http anywhere. The native verb gaining the
// flag must not leak it onto the compat spelling of the same verb.
func TestCompatSchemaInspect_RegistersNoPlainHTTP(t *testing.T) {
	c := qt.New(t)

	out, err := runCommand(atlas.NewCompatCommand("atlas"),
		"schema", "inspect", "--url", "sqlite://:memory:", "--plain-http")
	combined := out + errorText(err)

	c.Assert(err, qt.IsNotNil, qt.Commentf("output:\n%s", combined))
	c.Check(combined, qt.Contains, "unknown flag: --plain-http")
}
