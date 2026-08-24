// Package agentflags is the one place `ptah mcp` and `ptah assist` resolve what
// an AI-driven session may reach.
//
// Both surfaces answer the same questions -- which project, which artifact
// directories, which target dialect, what may be written, and where the audit
// record goes -- and #1483 requires them to answer identically. Two copies of
// this resolution would agree the day the second was written and stop agreeing
// the first time one grew a flag, which is the failure AGENTS.md describes as
// recognition that spans two functions.
package agentflags

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdflags"
	"go.5x5.cz/ptah/cmd/internal/serverversion"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/agentaudit"
	"go.5x5.cz/ptah/internal/agentgate"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agentworkspace"
	"go.5x5.cz/ptah/internal/buildinfo"
	"go.5x5.cz/ptah/internal/mcpserver"
	"go.5x5.cz/ptah/internal/servertarget"
)

// Flag names, in one place because two commands, their help text, and the
// documentation all quote them.
const (
	WorkspaceFlag     = "workspace"
	MigrationsDirFlag = "migrations-dir"
	SchemaDirFlag     = "schema-dir"
	TestsDirFlag      = "tests-dir"
	DialectFlag       = "dialect"
	AllowWriteFlag    = "allow-write"
	AutoApproveFlag   = "auto-approve"
	AuditLogFlag      = "audit-log"
)

// projectPolicyFile is the repository-carried policy this server reads.
//
// It may only narrow. The file lives in the tree the model is reading and
// proposing patches to, so treating it as a grant would let repository content
// decide what the next tool call is allowed to do -- which is the loop the
// layering in internal/agentpolicy exists to break.
const projectPolicyFile = ".ptah/agent-policy"

// Options is everything the flags collect.
type Options struct {
	Workspace     string
	MigrationsDir string
	SchemaDir     string
	TestsDir      string
	Dialect       string
	ServerVersion string
	AllowWrite    []string
	AutoApprove   bool
	AuditLog      string
}

// Build turns flags into an agent session, refusing anything ambiguous before a
// client can connect.
//
// Every refusal happens before the transport opens, which is deliberate: a
// misconfiguration discovered at the first tool call is one the operator reads
// as an agent failure rather than as their own typo.
//
// A nil session with a nil error is the read-only case: no workspace was named,
// so there are no artifact operations and nothing to configure for them.
func Build(cmd *cobra.Command, opts *Options, surface agentaudit.Surface) (*agentapi.Session, func(), error) {
	return build(cmd, opts, func(root, version string) (agentaudit.Recorder, func(), error) {
		return openAudit(cmd, opts, root, version, surface)
	})
}

// BuildInert builds a session for a command that runs no tool and makes no
// decision, so nothing is recorded and no audit file is left behind.
//
// `assist context` reports what a question would send and sends nothing. A
// command making that claim while dropping an empty file into the project it
// is describing would be contradicting itself in the directory a person was
// just told to go and look at.
func BuildInert(cmd *cobra.Command, opts *Options) (*agentapi.Session, func(), error) {
	return build(cmd, opts, func(string, string) (agentaudit.Recorder, func(), error) {
		return agentaudit.Discard{}, func() {}, nil
	})
}

// auditOpener supplies the recorder a session writes its decisions to.
type auditOpener func(root, version string) (agentaudit.Recorder, func(), error)

func build(cmd *cobra.Command, opts *Options, openRecorder auditOpener) (*agentapi.Session, func(), error) {
	version := buildinfo.Resolve().Version
	noop := func() {}
	if opts.Workspace == "" {
		if err := refuseWorkspaceFlagsWithoutAWorkspace(opts); err != nil {
			return nil, noop, err
		}
		return nil, noop, nil
	}

	classes, err := resolveClasses(opts)
	if err != nil {
		return nil, noop, err
	}
	dialect, err := resolveDialect(opts.Dialect)
	if err != nil {
		return nil, noop, err
	}

	workspace, err := agentworkspace.Open(agentworkspace.Config{
		Root:    opts.Workspace,
		Classes: classes,
		Dialect: dialect,
	})
	if err != nil {
		return nil, noop, err
	}
	closeWorkspace := func() { _ = workspace.Close() }

	policy, err := resolvePolicy(opts, workspace.Root())
	if err != nil {
		closeWorkspace()
		return nil, noop, err
	}
	target, err := servertarget.Resolve(dialect, opts.ServerVersion)
	if err != nil {
		closeWorkspace()
		return nil, noop, fmt.Errorf("invalid --%s: %w", serverversion.FlagName, err)
	}
	if target.Note != "" {
		// The note says the run resolved to something other than the version
		// named. Silence there is how a session against an unmodeled server
		// reads as a session against the server the operator asked for.
		fmt.Fprintf(cmd.ErrOrStderr(), "ptah: %s\n", target.Note)
	}
	gates, err := agentgate.New(agentgate.Options{
		Dialect:      dialect,
		Version:      opts.ServerVersion,
		Capabilities: target.Capabilities,
	})
	if err != nil {
		closeWorkspace()
		return nil, noop, err
	}
	audit, closeAudit, err := openRecorder(workspace.Root(), version)
	if err != nil {
		closeWorkspace()
		return nil, noop, err
	}

	session, err := agentapi.NewSession(agentapi.SessionConfig{
		Workspace: workspace,
		Broker:    agentpolicy.NewBroker(policy, brokerOptions(opts, audit)...),
		Gates:     gates,
		Audit:     audit,
	})
	if err != nil {
		closeAudit()
		closeWorkspace()
		return nil, noop, err
	}
	return session, func() {
		closeAudit()
		closeWorkspace()
	}, nil
}

// brokerOptions wires the approver and the audit recorder.
//
// Without --auto-approve the approver is the protocol's elicitation, so a
// policy that says ask reaches the person driving the client. With it, no
// approver is installed and the policy resolves those capabilities to allow
// instead -- the flag changes the policy rather than silencing the question,
// because a broker that asked nobody and proceeded would be the promotion this
// design refuses to have.
func brokerOptions(opts *Options, audit agentaudit.Recorder) []agentpolicy.BrokerOption {
	options := []agentpolicy.BrokerOption{
		agentpolicy.WithRecorder(func(outcome agentpolicy.Outcome) {
			_ = audit.Record(brokerEvent(outcome))
		}),
	}
	if !opts.AutoApprove {
		options = append(options, agentpolicy.WithApprover(mcpserver.Approver{}))
	}
	return options
}

// brokerEvent turns a broker outcome into an audit record.
func brokerEvent(outcome agentpolicy.Outcome) agentaudit.Event {
	event := agentaudit.Event{
		Operation:  "authorize",
		Capability: outcome.Request.String(),
		Verdict:    outcome.Decision.Verdict.String(),
		DecidedBy:  outcome.Decision.Layer.String(),
		Approved:   outcome.Approved,
		Artifact:   string(outcome.Request.Artifact),
		Paths:      outcome.Request.Paths,
		Outcome:    agentaudit.OutcomeDenied,
	}
	if outcome.Permitted {
		event.Outcome = agentaudit.OutcomePermitted
		if outcome.Approved {
			event.Outcome = agentaudit.OutcomeApproved
			event.ApprovalScope = outcome.GrantScope.String()
		}
	}
	if outcome.Err != nil {
		event.Reason = outcome.Err.Error()
	}
	return event
}

// refuseWorkspaceFlagsWithoutAWorkspace names the flag that does nothing.
//
// Silently ignoring --allow-write because --workspace was forgotten is how an
// operator concludes the write surface is broken, and how one concludes it is
// enabled when it is not.
func refuseWorkspaceFlagsWithoutAWorkspace(opts *Options) error {
	dependent := []struct {
		name  string
		given bool
	}{
		{MigrationsDirFlag, opts.MigrationsDir != ""},
		{SchemaDirFlag, opts.SchemaDir != ""},
		{TestsDirFlag, opts.TestsDir != ""},
		{serverversion.FlagName, opts.ServerVersion != ""},
		{AllowWriteFlag, len(opts.AllowWrite) > 0},
		{AutoApproveFlag, opts.AutoApprove},
		{AuditLogFlag, opts.AuditLog != ""},
	}
	for _, flag := range dependent {
		if flag.given {
			return fmt.Errorf("--%s needs --%s: without a workspace only the reading tools are served",
				flag.name, WorkspaceFlag)
		}
	}
	return nil
}

// resolveClasses maps the directory flags onto artifact classes.
func resolveClasses(opts *Options) (map[agentpolicy.ArtifactClass]agentworkspace.ClassConfig, error) {
	writable, err := writableClasses(opts.AllowWrite)
	if err != nil {
		return nil, err
	}
	configured := map[agentpolicy.ArtifactClass]string{
		agentpolicy.ClassMigrations: opts.MigrationsDir,
		agentpolicy.ClassSchema:     opts.SchemaDir,
		agentpolicy.ClassTests:      opts.TestsDir,
	}
	classes := make(map[agentpolicy.ArtifactClass]agentworkspace.ClassConfig)
	for class, dir := range configured {
		if dir == "" {
			continue
		}
		classes[class] = agentworkspace.ClassConfig{
			Dir:      dir,
			Writable: slices.Contains(writable, class),
		}
	}
	if len(classes) == 0 {
		return nil, fmt.Errorf(
			"--%s needs at least one of --%s, --%s or --%s: a workspace with no artifact directory has nothing to serve",
			WorkspaceFlag, MigrationsDirFlag, SchemaDirFlag, TestsDirFlag)
	}
	for _, class := range writable {
		if _, configured := classes[class]; !configured {
			return nil, fmt.Errorf(
				"--%s names %q and no directory was given for it",
				AllowWriteFlag, class)
		}
	}
	return classes, nil
}

// writableClasses parses --allow-write, refusing a class this build does not
// know rather than ignoring it.
func writableClasses(values []string) ([]agentpolicy.ArtifactClass, error) {
	classes := make([]agentpolicy.ArtifactClass, 0, len(values))
	for _, value := range values {
		class := agentpolicy.ArtifactClass(strings.TrimSpace(value))
		if !slices.Contains(agentpolicy.ArtifactClasses(), class) {
			return nil, fmt.Errorf("--%s: unknown artifact class %q; want migrations, schema or tests",
				AllowWriteFlag, value)
		}
		classes = append(classes, class)
	}
	return classes, nil
}

// resolveDialect refuses a workspace without a target.
//
// The gates run for one dialect. A workspace without one would either guess or
// check nothing, and a gate that checks nothing reports the same "ok" as one
// that checked everything.
func resolveDialect(requested string) (string, error) {
	if requested == "" {
		return "", fmt.Errorf("--%s is required with --%s: the validation gates run for one target",
			DialectFlag, WorkspaceFlag)
	}
	resolved := platform.NormalizeDialect(requested)
	if resolved == "" {
		return "", fmt.Errorf("--%s: unknown dialect %q", DialectFlag, requested)
	}
	return resolved, nil
}

// resolvePolicy assembles the invocation layer from the flags and the project
// layer from the repository, in that order of authority.
func resolvePolicy(opts *Options, root string) (*agentpolicy.Policy, error) {
	writable, err := writableClasses(opts.AllowWrite)
	if err != nil {
		return nil, err
	}
	granted := agentpolicy.VerdictAsk
	if opts.AutoApprove {
		granted = agentpolicy.VerdictAllow
	}

	rules := make([]agentpolicy.Rule, 0, len(agentpolicy.ArtifactClasses()))
	for _, class := range agentpolicy.ArtifactClasses() {
		verdict := agentpolicy.VerdictDeny
		if slices.Contains(writable, class) {
			verdict = granted
		}
		rules = append(rules, agentpolicy.Rule{
			Capability: agentpolicy.ArtifactWrite,
			Artifact:   class,
			Verdict:    verdict,
		})
	}

	layers := []agentpolicy.LayerRules{{
		Layer:  agentpolicy.LayerInvocation,
		Source: "ptah mcp flags",
		Rules:  rules,
	}}
	project, err := readProjectPolicy(root)
	if err != nil {
		return nil, err
	}
	if project != nil {
		layers = append(layers, *project)
	}
	return agentpolicy.Assemble(layers...)
}

// readProjectPolicy reads the repository's own policy file, if it has one.
//
// A missing file is not an error: most projects will not carry one, and the
// file's only power is to take permissions away.
func readProjectPolicy(root string) (*agentpolicy.LayerRules, error) {
	path := filepath.Join(root, filepath.FromSlash(projectPolicyFile))
	// #nosec G304 -- the path is this process's own workspace root, resolved by
	// pathguard before the server started, plus a fixed relative name.
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil //nolint:nilnil // "no file" is not a layer, and it is not a failure either
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", projectPolicyFile, err)
	}

	rules := make([]agentpolicy.Rule, 0)
	for number, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		rule, parseErr := agentpolicy.ParseRule(trimmed)
		if parseErr != nil {
			return nil, fmt.Errorf("%s line %d: %w", projectPolicyFile, number+1, parseErr)
		}
		rules = append(rules, rule)
	}
	return &agentpolicy.LayerRules{
		Layer:  agentpolicy.LayerProject,
		Source: projectPolicyFile,
		Rules:  rules,
	}, nil
}

// openAudit starts the provenance record.
func openAudit(
	cmd *cobra.Command,
	opts *Options,
	root, version string,
	surface agentaudit.Surface,
) (agentaudit.Recorder, func(), error) {
	path := opts.AuditLog
	if path == "" {
		path = agentaudit.DefaultPath(root)
	}
	writer, err := agentaudit.OpenFile(path, agentaudit.Options{
		SessionID:   newSessionID(),
		Surface:     surface,
		PtahVersion: version,
	})
	if err != nil {
		return nil, nil, err
	}
	// The path goes to stderr rather than stdout: stdout is the protocol's.
	fmt.Fprintf(cmd.ErrOrStderr(), "ptah: recording agent decisions to %s\n", path)
	return writer, func() { _ = writer.Close() }, nil
}

// newSessionID groups one server run's records.
func newSessionID() string {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		// A session identifier that cannot be random is still better than none:
		// the records of one run have to be groupable, and the identifier is not
		// a secret.
		return "session-unavailable"
	}
	return hex.EncodeToString(raw)
}

// Register declares what the operator decides at startup and returns the values
// the flags fill in.
//
// Everything an agent may reach is here rather than in a tool argument, and
// that is the design rather than a convenience: ADR 0003 records that the model
// chooses the arguments, so a server taking its root or its permissions from a
// tool call would let the untrusted party choose its own confinement.
func Register(cmd *cobra.Command) *Options {
	opts := &Options{}
	flags := cmd.Flags()
	flags.StringVar(&opts.Workspace, WorkspaceFlag, "",
		"Project root the artifact tools work within; without it only the reading tools are served")
	flags.StringVar(&opts.MigrationsDir, MigrationsDirFlag, "",
		"Migration directory, inside the workspace")
	flags.StringVar(&opts.SchemaDir, SchemaDirFlag, "",
		"Declared-schema directory, inside the workspace")
	flags.StringVar(&opts.TestsDir, TestsDirFlag, "",
		"Ptah test directory, inside the workspace")
	flags.StringVar(&opts.Dialect, DialectFlag, "",
		"Target dialect the validation and lint gates run for; required with --workspace")
	serverversion.Register(flags, &opts.ServerVersion)
	flags.StringSliceVar(&opts.AllowWrite, AllowWriteFlag, nil,
		"Artifact classes an agent may propose writes to: migrations, schema, tests")
	flags.BoolVar(&opts.AutoApprove, AutoApproveFlag, false,
		"Apply patches without asking for approval through the client")
	flags.StringVar(&opts.AuditLog, AuditLogFlag, "",
		"Where to append the agent audit record (default <workspace>/.ptah/agent-audit.jsonl)")

	// --auto-approve carries no environment binding, for the reason
	// `db drop-all --auto-approve` carries none: a variable exported once in a
	// shell profile is not a decision somebody made about this session.
	_ = cmdflags.DisableEnvBinding(flags, AutoApproveFlag)
	return opts
}
