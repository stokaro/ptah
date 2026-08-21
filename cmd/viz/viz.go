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
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/lineage"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/schemaviz"
)

const (
	rootDirFlag        = "root-dir"
	formatFlag         = "format"
	includeColumnsFlag = "include-columns"
	excludeTablesFlag  = "exclude-tables"
	themeFlag          = "theme"
	lineageFlag        = "lineage"
	formatSVG          = "svg"
)

type options struct {
	rootDir        string
	format         string
	includeColumns bool
	excludeTables  string
	theme          string
	lineage        bool
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

--lineage draws column-level dependencies instead of the entity diagram: which
base column each view column reads. A column whose source cannot be established
is drawn with the reason on it rather than left out, so the picture does not
read as more settled than it is:

  ptah viz --root-dir ./models --lineage --format dot`,
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
	flags.BoolVar(&opts.lineage, lineageFlag, false, "Draw column-level lineage instead of the entity diagram")
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
	rendered, err := renderGraph(db, opts, renderFormat)
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

// renderGraph draws whichever of the two graphs was asked for.
//
// They are separate graphs rather than one with extra edges: an entity diagram
// answers "how do these tables relate" and a lineage graph answers "where does
// this column come from", and drawing both at once produces a picture that
// answers neither (stokaro/ptah#1712).
func renderGraph(db *goschema.Database, opts options, renderFormat string) ([]byte, error) {
	if opts.lineage {
		return lineage.Render(lineage.Derive(db), renderFormat)
	}
	return schemaviz.Render(db, schemaviz.Options{
		Format:         renderFormat,
		IncludeColumns: opts.includeColumns,
		ExcludeTables:  splitCSV(opts.excludeTables),
		Theme:          opts.theme,
	})
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
