package root_test

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"regexp"
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

// migrationDirFlags names every spelling a command uses for the migration
// DIRECTORY it reads.
//
// Two spellings exist and neither is going away: `--dir` on the verbs that only
// ever look at a directory, `--migrations-dir` on the verbs that also take a
// schema or a database. The walk below covers both, because a census of one
// spelling is how `migrations validate` — a `--dir` verb sitting beside
// `migrations up`, a `--migrations-dir` verb — went a full release resolving no
// scheme at all while the gate above reported every row green
// (stokaro/ptah#1499). That gate walks `--schema-file`, which validate does not
// register, so the defect was outside every set anything was counting.
func migrationDirFlags() []string {
	return []string{"dir", "migrations-dir"}
}

// ociMigrationDirVerbs enumerates the commands that resolve an oci:// MIGRATION
// DIRECTORY, as opposed to an oci:// schema artifact.
//
// The rows drive the real command exactly as the schema rows above do, and for
// the same reason: registration is not acceptance.
func ociMigrationDirVerbs() []ociSourceVerb {
	return []ociSourceVerb{
		{
			verb: "migrations validate",
			args: func(reference string) []string {
				return []string{"migrations", "validate", "--dir", reference, "--plain-http"}
			},
		},
		{
			verb: "migrations lint",
			args: func(reference string) []string {
				return []string{"migrations", "lint", "--dir", reference, "--plain-http"}
			},
		},
		{
			verb: "migrations up",
			args: func(reference string) []string {
				return []string{
					"migrations", "up", "--migrations-dir", reference,
					"--db-url", "sqlite://:memory:", "--plain-http",
				}
			},
		},
		{
			verb: "migrations down",
			args: func(reference string) []string {
				return []string{
					"migrations", "down", "--migrations-dir", reference,
					"--db-url", "sqlite://:memory:", "--target", "0", "--plain-http",
				}
			},
		},
		{
			verb: "migrations status",
			args: func(reference string) []string {
				return []string{
					"migrations", "status", "--migrations-dir", reference,
					"--db-url", "sqlite://:memory:", "--plain-http",
				}
			},
		},
	}
}

// localOnlyMigrationDirVerb is one command whose migration-directory flag is
// NOT dispatched to the OCI puller, with the reason it is not.
type localOnlyMigrationDirVerb struct {
	verb string
	// why records why a registry reference is meaningless on this verb, so a
	// reader can tell a deliberate exception from a row nobody got to yet. Two
	// shapes recur: the flag names a directory the command WRITES, and an OCI
	// artifact is immutable; or it names a directory the command rewrites in
	// place after reading.
	why string
}

// localOnlyMigrationDirVerbs accounts for every remaining command registering a
// migration-directory flag.
//
// This list is long on purpose. The alternative is a walk that silently ignores
// what it does not recognise, which is the failure mode the file's opening
// comment is about: a new verb landing outside every set. A verb added here
// needs its reason written beside it, and a verb that starts resolving the
// scheme has to move to the table above, where it is driven rather than merely
// named.
func localOnlyMigrationDirVerbs() []localOnlyMigrationDirVerb {
	return []localOnlyMigrationDirVerb{
		{verb: "migrations hash", why: "it rewrites the integrity file in the directory; an artifact is immutable"},
		{verb: "migrations create", why: "it writes new migration files into the directory"},
		{verb: "migrations generate", why: "it writes generated migration files into the directory"},
		{verb: "migrations edit", why: "it rewrites a migration and re-hashes the directory in place"},
		{verb: "migrations rebase", why: "it re-timestamps a migration and re-hashes the directory in place"},
		{verb: "migrations rm", why: "it deletes a migration pair and re-hashes the directory in place"},
		{verb: "migrations checkpoint", why: "it squashes history into a new file written to the directory"},
		{verb: "migrations import", why: "the flag names the destination the converted files are written to"},
		{verb: "migrations data", why: "it writes a generated data migration into the directory"},
		{verb: "migrations push", why: "the flag names the LOCAL directory being published; the registry is the positional argument"},
		{verb: "migrations baseline", why: "it reads the directory to record revisions, and is not wired to the puller"},
		{verb: "migrations repair", why: "it reads the directory to rewrite revision metadata, and is not wired to the puller"},
		{verb: "migrations set", why: "it reads the directory to move the revision boundary, and is not wired to the puller"},
		{verb: "migrations test", why: "it runs migration test files from a local working tree, and is not wired to the puller"},
		{verb: "schema test", why: "it runs schema test files from a local working tree, and is not wired to the puller"},
		{verb: "schema inspect", why: "the flag supplies a local migration directory to replay, and is not wired to the puller"},
	}
}

// TestOCIMigrationDirVerbs_EveryDirectoryFlagIsAccountedFor is the census.
//
// It walks the built tree for both migration-directory spellings and requires
// every leaf that registers either to appear in exactly one of the two tables,
// in both directions.
func TestOCIMigrationDirVerbs_EveryDirectoryFlagIsAccountedFor(t *testing.T) {
	c := qt.New(t)

	var registered []string
	for _, flag := range migrationDirFlags() {
		found := nativeVerbsRegisteringFlag(root.NewRootCommand(), flag)
		c.Assert(len(found) > 0, qt.IsTrue,
			qt.Commentf("the walk found no --%s at all, so it is measuring nothing", flag))
		registered = append(registered, found...)
	}
	slices.Sort(registered)
	registered = slices.Compact(registered)

	var accounted []string
	for _, row := range ociMigrationDirVerbs() {
		accounted = append(accounted, row.verb)
	}
	for _, row := range localOnlyMigrationDirVerbs() {
		c.Assert(row.why, qt.Not(qt.Equals), "",
			qt.Commentf("%q is excluded with no reason written beside it", row.verb))
		accounted = append(accounted, row.verb)
	}
	slices.Sort(accounted)
	c.Assert(slices.Compact(slices.Clone(accounted)), qt.DeepEquals, accounted,
		qt.Commentf("a verb is listed twice: %q", accounted))

	for _, verb := range registered {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(accounted, qt.Contains, verb,
				qt.Commentf("%q registers a migration-directory flag but no row states whether it resolves oci://", verb))
		})
	}
	for _, verb := range accounted {
		t.Run("still registered: "+verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(registered, qt.Contains, verb,
				qt.Commentf("%q is accounted for but no longer registers a migration-directory flag", verb))
		})
	}

	for _, row := range ociMigrationDirVerbs() {
		t.Run("plain-http on "+row.verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(nativeVerbsRegisteringFlag(root.NewRootCommand(), "plain-http"), qt.Contains, row.verb)
		})
	}
}

// TestOCIMigrationDirVerbs_ReachTheRegistryWithPlainHTTP is the reachability
// gate for migration directories, and it is the one that would have caught
// stokaro/ptah#1499.
//
// `migrations validate --dir oci://…` did not fail at the dial: it failed at
// `stat oci://…: no such file or directory`, because the reference never
// reached a registry client at all. Requiring the dial distinguishes the three
// dispositions — resolves it, refuses the scheme, treats it as a path — without
// a registry being involved.
func TestOCIMigrationDirVerbs_ReachTheRegistryWithPlainHTTP(t *testing.T) {
	c := qt.New(t)
	reference := "oci://" + closedRegistryHost(c) + "/demo/migrations:v1"

	for _, row := range ociMigrationDirVerbs() {
		t.Run(row.verb, func(t *testing.T) {
			c := qt.New(t)
			out, err := runNative(row.args(reference)...)
			combined := out + errorText(err)

			c.Check(combined, qt.Not(qt.Contains), "unknown flag: --plain-http")
			// The path-shaped failure stokaro/ptah#1499 reported. A command
			// that stats the reference never dials, so this and the dial
			// assertion below cannot both hold.
			c.Check(combined, qt.Not(qt.Contains), "no such file or directory")
			c.Check(combined, qt.Contains, "connection refused",
				qt.Commentf("output:\n%s", combined))
			c.Check(combined, qt.Contains, "http://",
				qt.Commentf("output:\n%s", combined))
			c.Check(combined, qt.Not(qt.Contains), "https://")
		})
	}
}

// TestOCIMigrationDirVerbs_WithoutPlainHTTPUseTLS is the other direction: the
// flag must be what selected plain HTTP, not a default that was never
// encrypted.
func TestOCIMigrationDirVerbs_WithoutPlainHTTPUseTLS(t *testing.T) {
	c := qt.New(t)
	reference := "oci://" + closedRegistryHost(c) + "/demo/migrations:v1"

	for _, row := range ociMigrationDirVerbs() {
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

// ociSourceReferenceHeading names the section of the native command reference
// that carries the census of commands resolving an `oci://` source.
const ociSourceReferenceHeading = "## OCI transport behavior"

// ociReferenceRow matches one row of that section's table: the native command
// and the flag its `oci://` value arrives on.
var ociReferenceRow = regexp.MustCompile("^\\| `ptah ([^`]+)` \\| `(--[a-z-]+)` \\|")

// ociSourceReferencePath is the reference page, relative to this package.
func ociSourceReferencePath() string {
	return filepath.Join(
		"..", "..", "docs", "site", "src", "content", "docs", "reference", "native-commands.md",
	)
}

// ociSourceFlag returns the flag a row's `oci://` reference is passed on.
//
// It is derived from the invocation the row already builds rather than restated
// beside it, so the census below cannot claim a pairing the driven probe does
// not exercise.
func ociSourceFlag(c *qt.C, row ociSourceVerb) string {
	c.Helper()
	const reference = "oci://registry.invalid/demo/artifact:v1"
	args := row.args(reference)
	index := slices.Index(args, reference)
	c.Assert(index > 0, qt.IsTrue,
		qt.Commentf("%q builds no invocation passing the reference as a flag value", row.verb))
	return args[index-1]
}

// ociSourceCensus returns `<verb> <flag>` for every command the driven tables
// above establish as resolving an `oci://` source, sorted.
func ociSourceCensus(c *qt.C) []string {
	c.Helper()
	rows := slices.Concat(ociSchemaSourceVerbs(), ociMigrationDirVerbs())
	census := make([]string, 0, len(rows))
	for _, row := range rows {
		census = append(census, row.verb+" "+ociSourceFlag(c, row))
	}
	slices.Sort(census)
	return census
}

// ociReferenceCensus returns `<verb> <flag>` for every row of the reference
// page's table, sorted.
func ociReferenceCensus(c *qt.C) []string {
	c.Helper()
	path := ociSourceReferencePath()
	raw, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	_, section, found := strings.Cut(string(raw), ociSourceReferenceHeading+"\n")
	c.Assert(found, qt.IsTrue,
		qt.Commentf("%q is gone from %s, so this gate would measure nothing", ociSourceReferenceHeading, path))
	section, _, _ = strings.Cut(section, "\n## ")

	census := make([]string, 0)
	for line := range strings.SplitSeq(section, "\n") {
		census = appendOCIReferenceRow(census, ociReferenceRow.FindStringSubmatch(line))
	}
	slices.Sort(census)
	return census
}

// appendOCIReferenceRow appends one parsed table row and ignores a line that is
// not one. The branch lives here rather than in the loop above because this
// repository's test style keeps branching out of test bodies.
func appendOCIReferenceRow(census []string, match []string) []string {
	if match == nil {
		return census
	}
	return append(census, match[1]+" "+match[2])
}

// TestOCISourceVerbs_MatchTheCommandReference is the same census, read from the
// documentation instead of from the command tree.
//
// The reference page said `migrations validate` "takes a local --dir only, so
// an artifact must be pulled before it can be validated on its own". That was
// accurate until stokaro/ptah#1499 wired the verb to the puller, and it survived
// the commit that did: the walk above gates what the binary does, and nothing
// gated what the page says about it, so every row stayed green while the command
// reference told a reader the workflow did not exist and sent them to do a pull
// the verb no longer needs.
//
// The comparison is set equality in both directions and it pairs each command
// with its flag, so neither a verb that starts resolving the scheme nor one that
// stops, nor a row that moves to a different flag, can leave the page behind.
func TestOCISourceVerbs_MatchTheCommandReference(t *testing.T) {
	c := qt.New(t)

	census := ociSourceCensus(c)
	c.Assert(len(census) > 0, qt.IsTrue,
		qt.Commentf("the driven tables are empty, so this gate is measuring nothing"))

	c.Assert(ociReferenceCensus(c), qt.DeepEquals, census)
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
// found`.
//
// `migrations validate` asks the same question, and since stokaro/ptah#1499 it
// asks it of an oci:// reference too. The flag survives that because the two
// are not interchangeable: validate resolves the reference in its own process,
// so a movable tag can select different bytes before the executing verb
// resolves it again. --verify-sum verifies the artifact the same invocation is
// about to execute, which is the window a separate call cannot close.
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
