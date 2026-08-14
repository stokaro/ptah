package root_test

import (
	"bytes"
	"net"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/cmd/root"
)

// This file is the LIVE ENUMERATION of Ptah's OCI command surface.
//
// stokaro/ptah#928 items 1, 2 and 4 were each a hand-counted census that went
// stale: `--plain-http` was missing from four commands when the issue was
// filed, from six by the time it was implemented, and `--verify-sum` was on two
// verbs out of the set that could use it. A census is a snapshot; the defect it
// records comes back the next time a command is added. So the set is derived
// here by walking the built command tree rather than written down, and every
// command the walk finds must be accounted for by a row.
//
// Two properties are gated, and they fail for different reasons:
//
//   - REGISTRATION: a command that resolves an oci:// source registers
//     --plain-http. Without it there is no spelling that reaches a registry
//     serving plain HTTP.
//   - REACHABILITY: the flag is not merely parsed. Each row drives the real
//     command at an oci:// reference on a closed port and requires the failure
//     to be the OCI client's dial failure. A flag that parses and is dropped on
//     the floor produces a different error, and that is the defect shape this
//     repository has been closing all week.
//
// The reachability probe needs no registry: a port nothing listens on answers
// `connection refused` from the same client a real pull would use, and the
// three dispositions a command can have — resolves it, refuses the scheme,
// treats it as a path — are distinguishable in the message.

// closedRegistryHost returns host:port for a port nothing is listening on.
//
// It binds a port, reads it back and closes the listener, so the number is one
// the kernel actually handed out rather than a guess that might collide with a
// registry another test — or another agent on the same machine — is running.
func closedRegistryHost(c *qt.C) string {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	addr := listener.Addr().String()
	c.Assert(listener.Close(), qt.IsNil)
	return addr
}

// runNative executes one native command path against the real root command.
func runNative(args ...string) (string, error) {
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// ociSourceVerb is one command that takes an oci:// source through a flag.
//
// args carries the invocation as a func field because the repository's test
// style forbids branching inside a test body; the table is the branch. Each
// verb needs different companion flags, so the wiring is per row.
type ociSourceVerb struct {
	// verb is the space-joined command path, matching what the walk prints.
	verb string
	// args builds a full invocation whose only unreachable thing is the
	// registry, so the error under test is the dial and not a missing
	// companion flag.
	args func(reference string) []string
}

// ociSchemaSourceVerbs enumerates the commands that resolve an oci:// SCHEMA
// artifact through --schema-file.
//
// Six of these registered no --plain-http before stokaro/ptah#928 item 1. The
// issue named four; `schema export` and `schema inspect` were found by this
// walk, which is the reason the walk exists rather than a list.
func ociSchemaSourceVerbs() []ociSourceVerb {
	return []ociSourceVerb{
		{
			verb: "schema render",
			args: func(reference string) []string {
				return []string{"schema", "render", "--schema-file", reference, "--dialect", "sqlite", "--plain-http"}
			},
		},
		{
			verb: "schema export",
			args: func(reference string) []string {
				return []string{"schema", "export", "--schema-file", reference, "--to", "openapi-v3", "--plain-http"}
			},
		},
		{
			verb: "schema compare",
			args: func(reference string) []string {
				return []string{"schema", "compare", "--schema-file", reference, "--db-url", "sqlite://:memory:", "--plain-http"}
			},
		},
		{
			verb: "schema drift",
			args: func(reference string) []string {
				return []string{"schema", "drift", "--schema-file", reference, "--db-url", "sqlite://:memory:", "--plain-http"}
			},
		},
		{
			verb: "schema plan",
			args: func(reference string) []string {
				return []string{"schema", "plan", "--schema-file", reference, "--db-url", "sqlite://:memory:", "--dry-run", "--plain-http"}
			},
		},
		{
			verb: "schema apply",
			args: func(reference string) []string {
				return []string{"schema", "apply", "--schema-file", reference, "--db-url", "sqlite://:memory:", "--dry-run", "--plain-http"}
			},
		},
		{
			verb: "schema push",
			args: func(reference string) []string {
				return []string{"schema", "push", "oci://127.0.0.1:1/demo/out:v1", "--schema-file", reference, "--plain-http"}
			},
		},
		{
			verb: "schema inspect",
			args: func(reference string) []string {
				return []string{"schema", "inspect", "--schema-file", reference, "--dev-url", "sqlite://:memory:", "--plain-http"}
			},
		},
		{
			verb: "migrations plan",
			args: func(reference string) []string {
				return []string{"migrations", "plan", "--schema-file", reference, "--db-url", "sqlite://:memory:", "--plain-http"}
			},
		},
		{
			verb: "migrations generate",
			args: func(reference string) []string {
				return []string{
					"migrations", "generate", "--schema-file", reference,
					"--db-url", "sqlite://:memory:", "--migrations-dir", "./does-not-matter", "--plain-http",
				}
			},
		},
	}
}

// schemaFileVerbsThatDoNotResolveOCI names the commands registering
// --schema-file whose value is NOT dispatched to the OCI loader, so the
// coverage check above has somewhere to put them.
//
// It is empty today, and that is a measured result rather than a stub: every
// command in the built tree registering --schema-file resolves oci:// and is
// driven by a row above. The function stays because the alternative to an empty
// accounted-for list is a coverage check with no way to record a deliberate
// exception, which is how such checks come to be weakened when the first
// exception appears. An entry added here needs its reason written beside it.
//
// `schema test --root-dir` is the near miss worth naming: it also reaches
// schemaload, but through --root-dir rather than --schema-file, and an oci://
// value there is stat'ed as a path and left to the runner rather than pulled.
// It is outside this walk because it registers no --schema-file.
func schemaFileVerbsThatDoNotResolveOCI() []string {
	return nil
}

// TestOCISchemaSourceVerbs_RegisterPlainHTTP is the registration gate.
//
// It walks the built tree for --schema-file rather than reading the rows, so a
// command added later cannot land without appearing here.
func TestOCISchemaSourceVerbs_RegisterPlainHTTP(t *testing.T) {
	c := qt.New(t)

	registered := nativeVerbsRegisteringFlag(root.NewRootCommand(), "schema-file")
	c.Assert(len(registered) > 0, qt.IsTrue,
		qt.Commentf("the walk found no --schema-file at all, so it is measuring nothing"))

	accounted := slices.Clone(schemaFileVerbsThatDoNotResolveOCI())
	for _, row := range ociSchemaSourceVerbs() {
		accounted = append(accounted, row.verb)
	}
	slices.Sort(accounted)
	accounted = slices.Compact(accounted)

	for _, verb := range registered {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(accounted, qt.Contains, verb,
				qt.Commentf("%q registers --schema-file but no row states whether it resolves oci://", verb))
		})
	}

	// The converse: a name that no longer registers --schema-file must not sit
	// here claiming coverage it cannot have.
	for _, verb := range accounted {
		t.Run("still registered: "+verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(registered, qt.Contains, verb,
				qt.Commentf("%q is accounted for but no longer registers --schema-file", verb))
		})
	}

	// Every verb that resolves oci:// must register --plain-http. This is the
	// property stokaro/ptah#928 item 1 is about.
	for _, row := range ociSchemaSourceVerbs() {
		t.Run("plain-http on "+row.verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(nativeVerbsRegisteringFlag(root.NewRootCommand(), "plain-http"), qt.Contains, row.verb)
		})
	}
}

// TestOCISchemaSourceVerbs_ReachTheRegistryWithPlainHTTP is the reachability
// gate, and it is the one that matters.
//
// Registration is not acceptance. A --plain-http that parses and is never
// passed to the OCI client leaves the command failing exactly as it did before
// the flag existed, and the registration test above would still pass. Each row
// therefore drives the real command at a port nothing is listening on and
// requires the failure to be the dial — which can only be produced by a value
// that reached the client.
func TestOCISchemaSourceVerbs_ReachTheRegistryWithPlainHTTP(t *testing.T) {
	c := qt.New(t)
	reference := "oci://" + closedRegistryHost(c) + "/demo/schema:v1"

	for _, row := range ociSchemaSourceVerbs() {
		t.Run(row.verb, func(t *testing.T) {
			c := qt.New(t)
			out, err := runNative(row.args(reference)...)
			combined := out + errorText(err)

			// The flag parsed.
			c.Check(combined, qt.Not(qt.Contains), "unknown flag: --plain-http")
			// The value reached the OCI client, which dialed and failed.
			c.Check(combined, qt.Contains, "connection refused",
				qt.Commentf("output:\n%s", combined))
			// It dialed plain HTTP, not HTTPS: --plain-http did the work
			// rather than being accepted and dropped.
			c.Check(combined, qt.Contains, "http://",
				qt.Commentf("output:\n%s", combined))
			c.Check(combined, qt.Not(qt.Contains), "https://")
		})
	}
}

// TestOCISchemaSourceVerbs_WithoutPlainHTTPUseTLS is the other direction, and
// it is what stops the whole suite passing on a build where TLS quietly
// stopped being the default.
//
// Without the flag every one of these must attempt HTTPS. A row that dialed
// plain HTTP here would mean the flag changed nothing and the commands had been
// unencrypted all along.
func TestOCISchemaSourceVerbs_WithoutPlainHTTPUseTLS(t *testing.T) {
	c := qt.New(t)
	reference := "oci://" + closedRegistryHost(c) + "/demo/schema:v1"

	for _, row := range ociSchemaSourceVerbs() {
		t.Run(row.verb, func(t *testing.T) {
			c := qt.New(t)
			args := slices.DeleteFunc(row.args(reference), func(arg string) bool {
				return arg == "--plain-http"
			})
			out, err := runNative(args...)
			combined := out + errorText(err)

			c.Check(combined, qt.Contains, "https://",
				qt.Commentf("output:\n%s", combined))
		})
	}
}

// verifySumVerb is one command registering --verify-sum, with the reason.
type verifySumVerb struct {
	verb string
	// why records what the flag ADDS on this verb, so a reader can see that
	// the four registrations are not one contract repeated.
	why string
}

// verifySumVerbs enumerates the commands that offer the stricter integrity
// contract.
//
// The always-on gate stokaro/ptah#1450 installed refuses a hashed directory
// that does not match its sum, on every verb that executes the directory's SQL.
// --verify-sum is the DIFFERENT question: it refuses a directory that carries
// no sum at all. Measured against an unhashed artifact published to a registry:
// `up` without the flag exits 0, `up --verify-sum` exits 2 `ptah.sum not
// found`. Nothing else on the native surface asks that question — `migrations
// validate` does, but it cannot read an oci:// directory, so for a registry
// source the flag is the only spelling there is.
func verifySumVerbs() []verifySumVerb {
	return []verifySumVerb{
		{
			verb: "migrations push",
			why:  "the local directory being published must be covered by a sum before it becomes an artifact",
		},
		{
			verb: "migrations up",
			why:  "the pulled directory must be covered before anything is applied",
		},
		{
			verb: "migrations down",
			why:  "the pulled directory must be covered before rollback SQL executes; stokaro/ptah#928 item 4",
		},
		{
			verb: "migrations status",
			why:  "status runs no gate by default, so this is the only way its report is an integrity claim",
		},
	}
}

// TestVerifySum_IsRegisteredExactlyWhereItIsAccountedFor walks the tree for
// --verify-sum and requires the registered set and the accounted set to be
// equal in both directions.
func TestVerifySum_IsRegisteredExactlyWhereItIsAccountedFor(t *testing.T) {
	c := qt.New(t)

	registered := nativeVerbsRegisteringFlag(root.NewRootCommand(), "verify-sum")
	c.Assert(len(registered) > 0, qt.IsTrue,
		qt.Commentf("the walk found no --verify-sum at all, so it is measuring nothing"))

	accounted := make([]string, 0, len(verifySumVerbs()))
	for _, row := range verifySumVerbs() {
		accounted = append(accounted, row.verb)
	}
	slices.Sort(accounted)

	for _, verb := range registered {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(accounted, qt.Contains, verb,
				qt.Commentf("%q registers --verify-sum but no row states what the flag adds there", verb))
		})
	}
	for _, verb := range accounted {
		t.Run("still registered: "+verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(registered, qt.Contains, verb,
				qt.Commentf("%q is accounted for but no longer registers --verify-sum", verb))
		})
	}
}

// TestVerifySum_EveryHelpCarriesTheQualifier is stokaro/ptah#928 item 5, gated
// rather than fixed once.
//
// The issue's own reproduction is that rewriting a migration AND re-running
// `ptah migrations hash` produces an artifact that passes --verify-sum at exit
// 0 and installs whatever the rewrite said. So no registration of this flag may
// read as tamper detection. `migrations up` was reworded for that in
// stokaro/ptah#1093; `migrations push` was not, and went on saying "Require the
// migration directory to match ptah.sum or atlas.sum before pushing" with no
// qualifier at all. One surface keeping the old wording is exactly what a
// per-command fix cannot prevent from happening again.
func TestVerifySum_EveryHelpCarriesTheQualifier(t *testing.T) {
	c := qt.New(t)

	usages := nativeFlagUsages(root.NewRootCommand(), "verify-sum")
	c.Assert(len(usages) > 0, qt.IsTrue,
		qt.Commentf("the walk found no --verify-sum at all, so it is measuring nothing"))

	// Contains rather than HasSuffix: cmdflags.InstallEnvBinding appends
	// " [env: PTAH_VERIFY_SUM]" to every registered usage string, so the
	// qualifier is the last PROSE but never the last bytes. Asserting the
	// suffix would be asserting the env-binding machinery, not the wording.
	for verb, usage := range usages {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(usage, qt.Contains, migrationsource.VerifySumQualifier,
				qt.Commentf("--verify-sum help on %q does not carry the shared qualifier", verb))
		})
	}
}

// TestPlainHTTP_EveryRegistrationSharesOneHelpString pins the other half of the
// registrar.
//
// Before stokaro/ptah#928 the twelve registrations carried five different help
// strings — "Use plain HTTP for OCI registry access", "…for an explicitly
// trusted local registry", "…local OCI registry", "Allow an unencrypted HTTP
// connection to a local OCI registry" — so an operator reading two commands'
// help could not tell whether they meant the same thing. They do.
func TestPlainHTTP_EveryRegistrationSharesOneHelpString(t *testing.T) {
	c := qt.New(t)

	usages := nativeFlagUsages(root.NewRootCommand(), "plain-http")
	c.Assert(len(usages) > 0, qt.IsTrue,
		qt.Commentf("the walk found no --plain-http at all, so it is measuring nothing"))

	var distinct []string
	for _, usage := range usages {
		distinct = append(distinct, usage)
	}
	slices.Sort(distinct)
	distinct = slices.Compact(distinct)

	c.Assert(distinct, qt.HasLen, 1,
		qt.Commentf("--plain-http is registered with %d different help strings: %q", len(distinct), distinct))
}

// nativeVerbsRegisteringFlag returns the space-joined path of every runnable
// command below root that registers the named flag, sorted.
func nativeVerbsRegisteringFlag(tree *cobra.Command, flag string) []string {
	usages := nativeFlagUsages(tree, flag)
	found := make([]string, 0, len(usages))
	for verb := range usages {
		found = append(found, verb)
	}
	slices.Sort(found)
	return found
}

// nativeFlagUsages maps every runnable command path below root that registers
// the named flag to that flag's help string.
//
// "Runnable" means a leaf: a command with children is a namespace, and cobra
// prints its children's help rather than accepting flags of its own. The two
// gates above read this one walk so they cannot drift into disagreeing about
// what a command is.
func nativeFlagUsages(tree *cobra.Command, flag string) map[string]string {
	usages := make(map[string]string)
	var walk func(cmd *cobra.Command, path []string)
	walk = func(cmd *cobra.Command, path []string) {
		children := cmd.Commands()
		for _, child := range children {
			walk(child, append(slices.Clone(path), child.Name()))
		}
		if len(children) > 0 {
			return
		}
		found := cmd.Flags().Lookup(flag)
		if found == nil {
			return
		}
		usages[strings.Join(path, " ")] = found.Usage
	}
	for _, child := range tree.Commands() {
		walk(child, []string{child.Name()})
	}
	return usages
}

// errorText renders an error for a Contains assertion without branching in a
// test body: a nil error contributes the empty string, which contains nothing.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
