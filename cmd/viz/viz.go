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

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/internal/schemaviz"
)

const (
	rootDirFlag        = "root-dir"
	formatFlag         = "format"
	includeColumnsFlag = "include-columns"
	excludeTablesFlag  = "exclude-tables"
	themeFlag          = "theme"
	formatSVG          = "svg"
)

type options struct {
	rootDir        string
	format         string
	includeColumns bool
	excludeTables  string
	theme          string
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

  ptah viz --root-dir ./models --format mermaid --include-columns`,
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
	rendered, err := schemaviz.Render(db, schemaviz.Options{
		Format:         renderFormat,
		IncludeColumns: opts.includeColumns,
		ExcludeTables:  splitCSV(opts.excludeTables),
		Theme:          opts.theme,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if format == formatSVG {
		rendered, err = renderDOTToSVG(cmd.Context(), rendered)
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

func renderDOTToSVG(ctx context.Context, dot []byte) ([]byte, error) {
	if _, err := exec.LookPath("dot"); err != nil {
		return nil, fmt.Errorf("Graphviz dot is required for --format svg; install graphviz or use --format dot: %w", err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "dot", "-Tsvg")
	cmd.Stdin = bytes.NewReader(dot)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
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
