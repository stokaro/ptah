// Package viz contains the native schema visualization command.
package viz

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/serverversion"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/schemasecurity"
	"go.5x5.cz/ptah/internal/schemaviz"
	"go.5x5.cz/ptah/internal/servertarget"
	"go.5x5.cz/ptah/migration/risk"
)

const (
	rootDirFlag        = "root-dir"
	formatFlag         = "format"
	includeColumnsFlag = "include-columns"
	excludeTablesFlag  = "exclude-tables"
	themeFlag          = "theme"
	securityFlag       = "security"
	dialectFlag        = "dialect"
	formatSVG          = "svg"
)

type options struct {
	rootDir        string
	format         string
	includeColumns bool
	excludeTables  string
	theme          string
	security       bool
	dialect        string
	serverVersion  string
}

// NewCommand returns the native schema visualization command.
func NewCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "viz",
		Short: "Render desired schema diagrams",
		Long: `Render desired schema diagrams.

The command scans Go annotations and writes Graphviz DOT, Mermaid erDiagram, or
SVG output to stdout:

  ptah viz --root-dir ./models --format mermaid --include-columns

--security runs the schema security rules over the same schema and marks the
tables they attach to, so the diagram shows where the findings are rather than
sending the reader to a separate report:

  ptah viz --root-dir ./models --format dot --security`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.rootDir, rootDirFlag, ".", "Root directory to scan for Go annotations")
	flags.StringVar(&opts.format, formatFlag, schemaviz.FormatMermaid, "Output format: dot, mermaid, or svg")
	flags.BoolVar(&opts.includeColumns, includeColumnsFlag, false, "Include table columns in the diagram")
	flags.StringVar(&opts.excludeTables, excludeTablesFlag, "", "Comma-separated table names to omit from the diagram")
	flags.StringVar(&opts.theme, themeFlag, schemaviz.ThemeLight, "Diagram theme: light or dark")
	flags.BoolVar(&opts.security, securityFlag, false,
		"Mark tables with the security findings that attach to them")
	flags.StringVar(&opts.dialect, dialectFlag, "postgres",
		"Dialect the schema is read for, which decides which security rules can run")
	serverversion.Register(flags, &opts.serverVersion)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func run(cmd *cobra.Command, opts options) error {
	rootDir, err := pathguard.ResolveCLIPath(opts.rootDir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("invalid root directory: %w", err))
	}
	if err := cmdutil.StatDir(rootDir); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	db, err := goschema.ParseDir(rootDir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("parse Go annotations: %w", err))
	}
	format := strings.ToLower(strings.TrimSpace(opts.format))
	renderFormat := format
	if renderFormat == formatSVG {
		renderFormat = schemaviz.FormatDOT
	}
	annotations, unattached, err := securityAnnotations(db, opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	rendered, err := schemaviz.Render(db, schemaviz.Options{
		Format:         renderFormat,
		IncludeColumns: opts.includeColumns,
		ExcludeTables:  splitCSV(opts.excludeTables),
		Theme:          opts.theme,
		Annotations:    annotations,
		Unattached:     unattached,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if format == formatSVG {
		rendered, err = renderDOTToSVG(cmd.Context(), rendered, defaultGraphvizBudget)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	_, err = cmd.OutOrStdout().Write(rendered)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("write visualization: %w", err))
	}
	return nil
}

// securityAnnotations runs the security rules over the schema being drawn and
// sorts their findings into the ones a node can carry and the ones it cannot.
//
// A finding about a routine or a schema has no node in an entity diagram. Those
// are returned separately and emitted as a comment, because a diagram showing
// three of five findings without saying so is worse than one that shows three
// and names the other two (stokaro/ptah#1035).
//
// A rule that could not run here is reported the same way, for the same reason
// it is reported by `schema security`: a rule that did not run is
// indistinguishable from one that found nothing.
func securityAnnotations(
	db *schemamodel.Database,
	opts options,
) (map[string]schemaviz.Annotation, []string, error) {
	if !opts.security {
		return nil, nil, nil
	}
	// The named server is resolved rather than ignored, and a version naming no
	// server is refused rather than planned under the dialect default.
	//
	// Measured on this tree, row_level_security -- the only capability a rule
	// reads today -- varies by dialect and not within any release ladder, so
	// --server-version changes no current answer. It is read anyway because the
	// set the rules see should be the one the operator named, and because a
	// rule gated on a key that does vary would otherwise be planned against the
	// dialect default without saying so.
	target, err := servertarget.Resolve(opts.dialect, opts.serverVersion)
	if err != nil {
		return nil, nil, err
	}
	// No memberships: this diagram is drawn from Go annotations, which model no
	// role graph. The rules that need one report themselves skipped, and those
	// lines are drawn as comments like every other rule that could not run.
	report := schemasecurity.Analyze(db, schemasecurity.Options{Capabilities: target.Capabilities})
	annotations := make(map[string]schemaviz.Annotation, len(report.Findings))
	unattached := make([]string, 0, len(report.SkippedRules))
	for _, finding := range report.Findings {
		if finding.Subject.Kind != "table" {
			unattached = append(unattached,
				fmt.Sprintf("%s %s: %s %s", finding.Subject.Kind, finding.Subject.Name,
					finding.Severity, finding.Code))
			continue
		}
		annotation := annotations[finding.Subject.Name]
		annotation.Severity = higherSeverity(annotation.Severity, string(finding.Severity))
		annotation.Labels = append(annotation.Labels, finding.Code)
		annotations[finding.Subject.Name] = annotation
	}
	for _, skipped := range report.SkippedRules {
		unattached = append(unattached, fmt.Sprintf("%s not checked here: %s", skipped.Code, skipped.Reason))
	}
	return annotations, unattached, nil
}

// higherSeverity keeps the worst of two severities, so a node marked by three
// rules is drawn in the color of the one that matters most.
func higherSeverity(current, candidate string) string {
	// The empty string is "nothing seen yet" rather than a severity, and it has
	// to lose to every real one: risk.Rank scores it and `info` alike at 0, so
	// comparing ranks alone would leave the first info finding unnamed.
	if current == "" {
		return candidate
	}
	if risk.Rank(risk.Severity(candidate)) > risk.Rank(risk.Severity(current)) {
		return candidate
	}
	return current
}

// defaultGraphvizBudget bounds a `dot` invocation for a caller that named no
// deadline of its own. Long enough for an ordinary schema, short enough that a
// wedged process does not hold a terminal.
const defaultGraphvizBudget = 10 * time.Second

// renderDOTToSVG pipes dot through Graphviz.
//
// budget is taken as an argument rather than read from the constant, because
// the property worth testing is which of two deadlines governs, and a test that
// had to outlast the real one would take ten seconds to say so.
func renderDOTToSVG(ctx context.Context, dot []byte, budget time.Duration) ([]byte, error) {
	if _, err := exec.LookPath("dot"); err != nil {
		return nil, fmt.Errorf("Graphviz dot is required for --format svg; install graphviz or use --format dot: %w", err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// The budget is a floor for a caller who named none, not a ceiling on one
	// who did. Applying it unconditionally capped every caller at ten seconds:
	// a large model's render, and a test whose assertion is about the
	// diagnostic rather than the clock, both lost to a deadline nobody asked
	// for and nothing could raise.
	if _, named := ctx.Deadline(); !named {
		timed, cancel := context.WithTimeout(ctx, budget)
		defer cancel()
		ctx = timed
	}

	cmd := exec.CommandContext(ctx, "dot", "-Tsvg")
	cmd.Stdin = bytes.NewReader(dot)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		// A render the deadline stopped reports the deadline. CommandContext
		// kills the process, so cmd.Run answers `signal: killed` -- which names
		// the symptom, tells an operator nothing about the budget they cannot
		// see, and carries nothing a caller can branch on.
		if deadline := ctx.Err(); deadline != nil {
			return nil, fmt.Errorf(
				"render SVG with Graphviz dot: %w, and dot was still running", deadline)
		}
		if message != "" {
			return nil, fmt.Errorf("render SVG with Graphviz dot: %w: %s", err, message)
		}
		return nil, fmt.Errorf("render SVG with Graphviz dot: %w", err)
	}
	return stdout.Bytes(), nil
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
