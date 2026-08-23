package mcp

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

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
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

// projectPolicyFile is the repository-carried policy this server reads.
//
// It may only narrow. The file lives in the tree the model is reading and
// proposing patches to, so treating it as a grant would let repository content
// decide what the next tool call is allowed to do -- which is the loop the
// layering in internal/agentpolicy exists to break.
const projectPolicyFile = ".ptah/agent-policy"

// options is everything the flags collect.
type options struct {
	workspace     string
	migrationsDir string
	schemaDir     string
	testsDir      string
	dialect       string
	serverVersion string
	allowWrite    []string
	autoApprove   bool
	auditLog      string
}

// run resolves the operator's configuration and serves until the session ends.
func run(cmd *cobra.Command, opts *options) error {
	cfg, cleanup, err := build(cmd, opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer cleanup()
	return mcpserver.Run(cmd.Context(), cfg)
}

// build turns flags into a server configuration, refusing anything ambiguous
// before a client can connect.
//
// Every refusal here happens before the transport opens, which is deliberate:
// a misconfiguration discovered at the first tool call is one the operator sees
// as an agent failure rather than as their own typo.
func build(cmd *cobra.Command, opts *options) (mcpserver.Config, func(), error) {
	version := buildinfo.Resolve().Version
	noop := func() {}
	if opts.workspace == "" {
		if err := refuseWorkspaceFlagsWithoutAWorkspace(opts); err != nil {
			return mcpserver.Config{}, noop, err
		}
		return mcpserver.Config{Version: version}, noop, nil
	}

	classes, err := resolveClasses(opts)
	if err != nil {
		return mcpserver.Config{}, noop, err
	}
	dialect, err := resolveDialect(opts.dialect)
	if err != nil {
		return mcpserver.Config{}, noop, err
	}

	workspace, err := agentworkspace.Open(agentworkspace.Config{
		Root:    opts.workspace,
		Classes: classes,
		Dialect: dialect,
	})
	if err != nil {
		return mcpserver.Config{}, noop, err
	}
	closeWorkspace := func() { _ = workspace.Close() }

	policy, err := resolvePolicy(opts, workspace.Root())
	if err != nil {
		closeWorkspace()
		return mcpserver.Config{}, noop, err
	}
	target, err := servertarget.Resolve(dialect, opts.serverVersion)
	if err != nil {
		closeWorkspace()
		return mcpserver.Config{}, noop, fmt.Errorf("invalid --%s: %w", serverversion.FlagName, err)
	}
	if target.Note != "" {
		// The note says the run resolved to something other than the version
		// named. Silence there is how a session against an unmodeled server
		// reads as a session against the server the operator asked for.
		fmt.Fprintf(cmd.ErrOrStderr(), "ptah mcp: %s\n", target.Note)
	}
	gates, err := agentgate.New(agentgate.Options{
		Dialect:      dialect,
		Version:      opts.serverVersion,
		Capabilities: target.Capabilities,
	})
	if err != nil {
		closeWorkspace()
		return mcpserver.Config{}, noop, err
	}
	audit, closeAudit, err := openAudit(cmd, opts, workspace.Root(), version)
	if err != nil {
		closeWorkspace()
		return mcpserver.Config{}, noop, err
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
		return mcpserver.Config{}, noop, err
	}
	return mcpserver.Config{Version: version, Session: session}, func() {
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
func brokerOptions(opts *options, audit agentaudit.Recorder) []agentpolicy.BrokerOption {
	options := []agentpolicy.BrokerOption{
		agentpolicy.WithRecorder(func(outcome agentpolicy.Outcome) {
			_ = audit.Record(brokerEvent(outcome))
		}),
	}
	if !opts.autoApprove {
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
func refuseWorkspaceFlagsWithoutAWorkspace(opts *options) error {
	dependent := []struct {
		name  string
		given bool
	}{
		{migrationsDirFlag, opts.migrationsDir != ""},
		{schemaDirFlag, opts.schemaDir != ""},
		{testsDirFlag, opts.testsDir != ""},
		{serverversion.FlagName, opts.serverVersion != ""},
		{allowWriteFlag, len(opts.allowWrite) > 0},
		{autoApproveFlag, opts.autoApprove},
		{auditLogFlag, opts.auditLog != ""},
	}
	for _, flag := range dependent {
		if flag.given {
			return fmt.Errorf("--%s needs --%s: without a workspace only the reading tools are served",
				flag.name, workspaceFlag)
		}
	}
	return nil
}

// resolveClasses maps the directory flags onto artifact classes.
func resolveClasses(opts *options) (map[agentpolicy.ArtifactClass]agentworkspace.ClassConfig, error) {
	writable, err := writableClasses(opts.allowWrite)
	if err != nil {
		return nil, err
	}
	configured := map[agentpolicy.ArtifactClass]string{
		agentpolicy.ClassMigrations: opts.migrationsDir,
		agentpolicy.ClassSchema:     opts.schemaDir,
		agentpolicy.ClassTests:      opts.testsDir,
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
			workspaceFlag, migrationsDirFlag, schemaDirFlag, testsDirFlag)
	}
	for _, class := range writable {
		if _, configured := classes[class]; !configured {
			return nil, fmt.Errorf(
				"--%s names %q and no directory was given for it",
				allowWriteFlag, class)
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
				allowWriteFlag, value)
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
			dialectFlag, workspaceFlag)
	}
	dialect := platform.NormalizeDialect(requested)
	if dialect == "" {
		return "", fmt.Errorf("--%s: unknown dialect %q", dialectFlag, requested)
	}
	return dialect, nil
}

// resolvePolicy assembles the invocation layer from the flags and the project
// layer from the repository, in that order of authority.
func resolvePolicy(opts *options, root string) (*agentpolicy.Policy, error) {
	writable, err := writableClasses(opts.allowWrite)
	if err != nil {
		return nil, err
	}
	granted := agentpolicy.VerdictAsk
	if opts.autoApprove {
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
	opts *options,
	root, version string,
) (agentaudit.Recorder, func(), error) {
	path := opts.auditLog
	if path == "" {
		path = agentaudit.DefaultPath(root)
	}
	writer, err := agentaudit.OpenFile(path, agentaudit.Options{
		SessionID:   newSessionID(),
		Surface:     agentaudit.SurfaceMCP,
		PtahVersion: version,
	})
	if err != nil {
		return nil, nil, err
	}
	// The path goes to stderr rather than stdout: stdout is the protocol's.
	fmt.Fprintf(cmd.ErrOrStderr(), "ptah mcp: recording agent decisions to %s\n", path)
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
