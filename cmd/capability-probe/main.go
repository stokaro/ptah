// Command capability-probe measures a live database server against the
// capability preset Ptah hands out for it.
//
// It answers one question per capability: does the server actually behave the
// way core/platform/capability says it does? A disagreement exits non-zero. So
// does a run that decided nothing — a probe that skipped every row must not
// read as a probe that passed every row.
//
//	capability-probe --db-url postgres://user:pw@localhost:5432/db
//	capability-probe --db-url mysql://root:pw@localhost:3306/db --evidence
//	capability-probe --list-cells
//
// Wiring this into CI is stokaro/ptah#1341 and is deliberately not done here.
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/internal/capabilityprobe"
)

func main() {
	if err := newCommand().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "capability-probe: %v\n", err)
		os.Exit(1)
	}
}

func newCommand() *cobra.Command {
	var (
		dbURL     string
		listCells bool
		evidence  bool
	)
	cmd := &cobra.Command{
		Use:           "capability-probe",
		Short:         "Measure a live server against the capability preset Ptah gives it",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if listCells {
				capabilityprobe.WriteCells(cmd.OutOrStdout())
				return nil
			}
			if dbURL == "" {
				return fmt.Errorf("--db-url is required (or --list-cells)")
			}
			var extra []sectionWriter
			if evidence {
				extra = append(extra, capabilityprobe.WriteEvidence)
			}
			return probe(cmd.Context(), cmd.OutOrStdout(), dbURL, extra)
		},
	}
	cmd.Flags().StringVar(&dbURL, "db-url", "", "URL of the live server to probe")
	cmd.Flags().BoolVar(&listCells, "list-cells", false, "print the matrix cell list and exit")
	cmd.Flags().BoolVar(&evidence, "evidence", false, "print every statement executed and the server's answer")
	return cmd
}

// sectionWriter renders one optional section of the report.
type sectionWriter func(io.Writer, *capabilityprobe.Report)

func probe(ctx context.Context, out io.Writer, dbURL string, extra []sectionWriter) error {
	if ctx == nil {
		ctx = context.Background()
	}
	report, err := capabilityprobe.Run(ctx, dbURL)
	if err != nil {
		return err
	}
	capabilityprobe.WriteReport(out, report)
	for _, section := range extra {
		section(out, report)
	}
	return report.Err()
}
