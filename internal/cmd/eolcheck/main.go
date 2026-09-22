// Command eolcheck asks endoflife.date whether the release lines this
// repository declares are still supported by their vendors, and lowers one
// declaration when asked to.
//
//	eolcheck report                    # print what the calendars say
//	eolcheck report --json             # the same report, for a workflow to read
//	eolcheck demote <id> --reason ...  # lower that line's declared promise
//
// Nothing here removes a release line or refuses a server. Upstream end of
// life lowers what Ptah promises about a line and nothing else, so `demote`
// drops the declared support level to best-effort and stops a probe job being
// spent on the line, while the capability preset, the image and the dialect
// stay where they are. What a best-effort line loses is the guarantee: it may
// break as the code around it moves, which is what the level says.
//
// The two verbs are separate because the finding is not the decision. A line
// past end of life may be kept measured on purpose -- PostgreSQL 13 is, as a
// regression sentinel (stokaro/ptah#341) -- so `report` never rewrites
// anything, and `demote` names one cell and changes only that.
//
// `report` exits 0 whether or not there are findings. A daily job reads the
// JSON and decides; a non-zero exit would make the schedule red every day from
// the first end-of-life line until somebody acted on it, which is a red that
// stops meaning anything. A calendar that cannot be read is the other case and
// does exit non-zero, because an empty finding list then says nothing.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/internal/capabilityprobe"
	"ptah.run/internal/eolcheck"
)

func main() {
	if err := newRoot().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "eolcheck: %v\n", err)
		os.Exit(1)
	}
}

func newRoot() *cobra.Command {
	root := &cobra.Command{
		Use:           "eolcheck",
		Short:         "Report and retire release lines their vendor no longer supports",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newReport(), newDemote())
	return root
}

func newReport() *cobra.Command {
	var (
		asJSON bool
		on     string
	)
	cmd := &cobra.Command{
		Use:   "report",
		Short: "Compare the declared release lines against the vendor calendars",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			day, err := resolveDay(on)
			if err != nil {
				return err
			}
			client := &http.Client{Timeout: 30 * time.Second}
			ctx, cancel := context.WithTimeout(cmd.Context(), 3*time.Minute)
			defer cancel()
			report, err := eolcheck.Check(
				ctx, capabilityprobe.Cells, day, eolcheck.HTTPFetcher(client))
			if err != nil {
				return err
			}
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), report)
			}
			return writeText(cmd.OutOrStdout(), report)
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "write the report as JSON")
	cmd.Flags().StringVar(&on, "on", "", "compare against this date rather than today (YYYY-MM-DD)")
	return cmd
}

func newDemote() *cobra.Command {
	var (
		root   string
		reason string
	)
	cmd := &cobra.Command{
		Use:   "demote <cell-id>",
		Short: "Lower one release line to best-effort and stop probing it",
		Long: "Rewrites one cell in " + eolcheck.CellsFile + ": the declared support level\n" +
			"drops to best-effort, and --reason joins the cell as Unprobed, which is what\n" +
			"stops a probe job being spent on the line.\n\n" +
			"It changes that file and nothing else. Whether the line's capabilityline\n" +
			"constant, capability preset, integration service or Compose service still\n" +
			"has another user is a judgement, so those stay for a reviewer to decide.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := filepath.Join(root, filepath.FromSlash(eolcheck.CellsFile))
			source, err := os.ReadFile(path) // #nosec G304 -- the path is the fixed declarations file under --root
			if err != nil {
				return err
			}
			demoted, err := eolcheck.DemoteCell(source, args[0], reason)
			if err != nil {
				return err
			}
			// #nosec G703 -- the path is the fixed declarations file joined onto --root, which is this run's own checkout
			if err := os.WriteFile(path, demoted, 0o600); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(),
				"eolcheck: %s now declares best-effort and is no longer probed, in %s\n",
				args[0], eolcheck.CellsFile)
			return nil
		},
	}
	cmd.Flags().StringVar(&root, "root", ".", "the repository root to rewrite the declarations in")
	cmd.Flags().StringVar(&reason, "reason", "", "why the line is no longer probed")
	return cmd
}

func resolveDay(on string) (time.Time, error) {
	if on == "" {
		return time.Now().UTC(), nil
	}
	parsed, err := time.Parse("2006-01-02", on)
	if err != nil {
		return time.Time{}, fmt.Errorf("--on must be a date: %w", err)
	}
	return parsed, nil
}

// row is one finding in the JSON a workflow reads. It carries the identifiers
// a branch, a pull request and a search are composed from, so the workflow
// builds strings rather than parsing prose.
type row struct {
	ID      string `json:"id"`
	Dialect string `json:"dialect"`
	Line    string `json:"line"`
	Label   string `json:"label,omitempty"`
	Support string `json:"support"`
	Preset  string `json:"preset,omitempty"`
	Image   string `json:"image,omitempty"`
	Product string `json:"product"`
	EOL     string `json:"eol"`
	// Overstated is what a workflow acts on. A line already declared
	// best-effort or legacy-tested is listed and asks for no change.
	Overstated bool   `json:"overstated"`
	Reason     string `json:"reason"`
}

type unanswered struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

type payload struct {
	On         string       `json:"on"`
	Asked      int          `json:"asked"`
	Findings   []row        `json:"findings"`
	Unanswered []unanswered `json:"unanswered"`
}

func writeJSON(out io.Writer, report eolcheck.Report) error {
	body := payload{
		On:         report.On.Format("2006-01-02"),
		Asked:      report.Asked,
		Findings:   make([]row, 0, len(report.Findings)),
		Unanswered: make([]unanswered, 0, len(report.Unanswered)),
	}
	for _, finding := range report.Findings {
		body.Findings = append(body.Findings, row{
			ID:      finding.ID(),
			Dialect: finding.Cell.Dialect,
			Line:    finding.Cell.Line,
			Label:   finding.Cell.Label,
			Support: string(finding.Cell.Support),
			Preset:  finding.Cell.PresetName,
			Image:   finding.Cell.Image,
			Product: finding.Product,
			EOL:     finding.Ended(),

			Overstated: finding.Overstated(),
			Reason:     finding.UnprobedReason(),
		})
	}
	for _, line := range report.Unanswered {
		body.Unanswered = append(body.Unanswered, unanswered{
			ID: capabilityprobe.CellID(line.Cell), Reason: line.Reason,
		})
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(body)
}

func writeText(out io.Writer, report eolcheck.Report) error {
	fmt.Fprintf(out, "eolcheck: %d declared lines answered by a vendor calendar on %s\n",
		report.Asked, report.On.Format("2006-01-02"))
	for _, finding := range report.Findings {
		fmt.Fprintf(out, "  past end of life: %s (%s %s, %s) ended %s, declared %s -- overstated: %v\n",
			finding.ID(), finding.Cell.Dialect, finding.Cell.Line,
			finding.Product, finding.Ended(), finding.Cell.Support, finding.Overstated())
	}
	for _, line := range report.Unanswered {
		fmt.Fprintf(out, "  unanswered: %s -- %s\n",
			capabilityprobe.CellID(line.Cell), line.Reason)
	}
	fmt.Fprintf(out, "  %d declared lines are past end of life, %d of them declaring more than that allows\n",
		len(report.Findings), len(overstated(report)))
	return nil
}

// overstated is the subset a workflow opens a pull request for.
func overstated(report eolcheck.Report) []eolcheck.Finding {
	out := make([]eolcheck.Finding, 0, len(report.Findings))
	for _, finding := range report.Findings {
		if finding.Overstated() {
			out = append(out, finding)
		}
	}
	return out
}
