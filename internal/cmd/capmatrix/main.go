// Command capmatrix is the driver for Ptah's tiered capability-matrix
// pipeline (stokaro/ptah#1341).
//
// It has one job in each tier and one job outside them. `matrix` turns the
// release lines declared in internal/capabilityprobe into the fan-out both
// tiers use, so the two tiers cannot disagree about which versions exist.
// `probe` runs one cell. `record` folds a tier 3 cell's integration-suite
// result into the cell it was probed on. `report` aggregates the cells back
// into one verdict and fails when a declared cell produced no result at all.
// `markdown` prints the documentation matrix, which the same declaration
// generates so a third list cannot drift from the other two.
//
//	capmatrix matrix
//	capmatrix probe --cell postgres-17 --result results/postgres-17.json
//	capmatrix record --cell postgres-17 --probe-result results/postgres-17.json \
//	    --suite-exit 0 --suite-report-dir reports --result results/postgres-17.json
//	capmatrix report --tier 2 --results results
//	capmatrix markdown
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/internal/capabilityprobe"
	"go.5x5.cz/ptah/internal/capmatrix"
)

func main() {
	if err := newCommand().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "capmatrix: %v\n", err)
		os.Exit(1)
	}
}

func newCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           "capmatrix",
		Short:         "Drive the tiered capability-matrix pipeline",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(
		newMatrixCommand(), newMarkdownCommand(), newPresetsCommand(),
		newDockerArgsCommand(), newSuiteEnvCommand(),
		newProbeCommand(), newRecordCommand(), newReportCommand(),
	)
	return root
}

// newPresetsCommand fails when a declared release line has no measured
// capability preset. It is its own job rather than part of `matrix` because
// `matrix` feeds the fan-out: a matrix step that failed on a preset gap would
// skip every cell, and a tier that produced no cells reads as a tier with no
// failures.
func newPresetsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "presets",
		Short: "Check that every declared release line names a capability preset",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			gaps := capabilityprobe.PresetGaps()
			fmt.Fprintf(cmd.OutOrStdout(), "checked %d declared release lines, %d name no preset\n",
				len(capabilityprobe.Cells), len(gaps))
			for _, gap := range gaps {
				fmt.Fprintf(cmd.OutOrStdout(), "  %s\n", gap)
			}
			return errors.Join(gaps...)
		},
	}
}

// newMatrixCommand prints the fan-out both tiers consume.
//
// --ids exists because of a measured refusal. The first run of this pipeline
// passed the whole cell objects through a job output, and the runner answered
// "Skip output 'cells' since it may contain secret" — the throwaway container
// credentials in each cell's URL are enough to trip it. The output was silently
// empty, the strategy produced zero jobs, and only the reporting job's census
// caught it. So what crosses a job boundary is now a list of ids, and anything
// carrying a credential is asked for by id inside the job that needs it.
func newMatrixCommand() *cobra.Command {
	var ids bool
	cmd := &cobra.Command{
		Use:   "matrix",
		Short: "Print the CI matrix derived from the declared release lines",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			matrix := capabilityprobe.CIMatrix()
			if err := matrix.Validate(); err != nil {
				return err
			}
			encoder := json.NewEncoder(cmd.OutOrStdout())
			if ids {
				return encoder.Encode(matrix.IDs())
			}
			encoder.SetIndent("", "  ")
			return encoder.Encode(matrix)
		},
	}
	cmd.Flags().BoolVar(&ids, "ids", false, "print only the runnable cell ids, as the JSON array a matrix strategy consumes")
	return cmd
}

// newDockerArgsCommand prints how to start one cell's server, one argument per
// line, for a shell to read into an array.
func newDockerArgsCommand() *cobra.Command {
	var cellID string
	cmd := &cobra.Command{
		Use:   "docker-args",
		Short: "Print the docker run arguments that start one cell's server",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cell, err := runnableCell(cellID)
			if err != nil {
				return err
			}
			for _, argument := range cell.DockerRun {
				fmt.Fprintln(cmd.OutOrStdout(), argument)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&cellID, "cell", "", "matrix cell id")
	return cmd
}

// newSuiteEnvCommand prints the environment the integration runner needs for
// one cell, in the KEY=VALUE form GITHUB_ENV accepts.
func newSuiteEnvCommand() *cobra.Command {
	var cellID string
	cmd := &cobra.Command{
		Use:   "suite-env",
		Short: "Print the integration runner's environment for one cell",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cell, err := runnableCell(cellID)
			if err != nil {
				return err
			}
			if cell.SuiteDatabase == "" || cell.SuiteURLEnv == "" {
				return fmt.Errorf("cell %s has no integration-runner target", cell.ID)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "CAPMATRIX_SUITE_DATABASE=%s\n", cell.SuiteDatabase)
			fmt.Fprintf(cmd.OutOrStdout(), "%s=%s\n", cell.SuiteURLEnv, cell.URL)
			return nil
		},
	}
	cmd.Flags().StringVar(&cellID, "cell", "", "matrix cell id")
	return cmd
}

func newMarkdownCommand() *cobra.Command {
	var compact bool
	cmd := &cobra.Command{
		Use:   "markdown",
		Short: "Print the documentation version matrix",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if compact {
				capabilityprobe.WriteMatrixSummary(cmd.OutOrStdout())
				return nil
			}
			capabilityprobe.WriteMatrixMarkdown(cmd.OutOrStdout())
			return nil
		},
	}
	cmd.Flags().BoolVar(&compact, "compact", false,
		"print the narrow rendering the documentation site uses, which has to fit a phone-width reading column")
	return cmd
}

func newProbeCommand() *cobra.Command {
	var (
		cellID string
		result string
		tier   int
		wait   time.Duration
	)
	cmd := &cobra.Command{
		Use:   "probe",
		Short: "Probe one matrix cell against the server started for it",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cell, err := runnableCell(cellID)
			if err != nil {
				return err
			}
			outcome := capmatrix.RunProbe(cmd.Context(), cmd.OutOrStdout(), cell, tier, wait)
			return finish(cmd, outcome, result)
		},
	}
	cmd.Flags().StringVar(&cellID, "cell", "", "matrix cell id, as printed by `capmatrix matrix`")
	cmd.Flags().StringVar(&result, "result", "", "path to write the cell result JSON to")
	cmd.Flags().IntVar(&tier, "tier", 2, "which tier is running this cell")
	cmd.Flags().DurationVar(&wait, "wait", 5*time.Minute, "how long to wait for the server to answer")
	return cmd
}

func newRecordCommand() *cobra.Command {
	var (
		cellID       string
		probeResult  string
		result       string
		suiteExit    int
		suiteReports string
	)
	cmd := &cobra.Command{
		Use:   "record",
		Short: "Fold a tier 3 integration-suite result into a probed cell",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			probed, err := capmatrix.ReadResult(probeResult)
			if err != nil {
				return err
			}
			if probed.Cell != cellID {
				return fmt.Errorf("%s holds the result for cell %q, not %q", probeResult, probed.Cell, cellID)
			}
			probed.Tier = 3
			recorded, err := capmatrix.RecordSuite(probed, suiteExit, suiteReports)
			if err != nil {
				return err
			}
			return finish(cmd, recorded, result)
		},
	}
	cmd.Flags().StringVar(&cellID, "cell", "", "matrix cell id the result belongs to")
	cmd.Flags().StringVar(&probeResult, "probe-result", "", "path of the result JSON `capmatrix probe` wrote")
	cmd.Flags().StringVar(&result, "result", "", "path to write the merged cell result JSON to")
	cmd.Flags().IntVar(&suiteExit, "suite-exit", 0, "exit code the integration runner returned")
	cmd.Flags().StringVar(&suiteReports, "suite-report-dir", "", "directory the integration runner wrote its JSON report to")
	return cmd
}

func newReportCommand() *cobra.Command {
	var (
		tier    int
		results string
	)
	cmd := &cobra.Command{
		Use:   "report",
		Short: "Aggregate one tier's cell results into a verdict",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cells, err := capmatrix.ReadResults(results)
			if err != nil {
				return err
			}
			aggregate := capmatrix.Aggregate{Tier: tier, Matrix: capabilityprobe.CIMatrix(), Results: cells}
			capmatrix.WriteAggregate(cmd.OutOrStdout(), aggregate)
			return aggregate.Err()
		},
	}
	cmd.Flags().IntVar(&tier, "tier", 0, "which tier is being reported")
	cmd.Flags().StringVar(&results, "results", "", "directory holding the per-cell result JSON files")
	return cmd
}

// runnableCell resolves a cell id against the matrix and refuses anything the
// matrix does not declare runnable, naming why when the line exists but cannot
// be executed.
func runnableCell(id string) (capabilityprobe.CICell, error) {
	matrix := capabilityprobe.CIMatrix()
	if err := matrix.Validate(); err != nil {
		return capabilityprobe.CICell{}, err
	}
	if cell, found := matrix.Find(id); found {
		return cell, nil
	}
	for _, skipped := range matrix.Skipped {
		if skipped.ID == id {
			return capabilityprobe.CICell{}, fmt.Errorf("matrix cell %q is declared but cannot be run: %s", id, skipped.Skip)
		}
	}
	return capabilityprobe.CICell{}, fmt.Errorf("no matrix cell is named %q; `capmatrix matrix` lists the ids", id)
}

// finish writes the result file, if one was asked for, and turns the cell's
// verdict into this process's exit status.
func finish(cmd *cobra.Command, result capmatrix.CellResult, path string) error {
	if path != "" {
		if err := capmatrix.WriteResult(path, result); err != nil {
			return err
		}
	}
	fmt.Fprintf(cmd.OutOrStdout(), "\ncell %s: %s\n", result.Cell, result.Verdict())
	if result.Verdict() == capmatrix.Passed {
		return nil
	}
	for _, reason := range result.Reasons() {
		fmt.Fprintf(cmd.OutOrStdout(), "  %s\n", reason)
	}
	return fmt.Errorf("cell %s: %s", result.Cell, result.Verdict())
}
