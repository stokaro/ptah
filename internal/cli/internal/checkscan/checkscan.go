// Package checkscan declares --skip-check-scan, the flag a native command that
// plans schema changes takes to add a CHECK constraint without scanning the
// rows the table already holds.
//
// ClickHouse checks only the rows written after ADD CONSTRAINT, so a plan for
// it reads the table for a held row the CHECK refuses, before the change and
// again after the ADD, and stops on one. Each read scans the whole table. The
// flag leaves both reads out, and the plan says above each ADD CONSTRAINT that
// the rows the table holds were not checked. The flag reads
// PTAH_SKIP_CHECK_SCAN, unlike --allow-table-rebuild: the scans cost the same
// on every plan of a large table, and a pipeline that accepts the risk states
// it once.
package checkscan

import (
	"github.com/spf13/cobra"
)

// FlagName is the flag's name, without the dashes.
const FlagName = "skip-check-scan"

const usage = "Add a CHECK constraint without the SELECTs that scan the rows the table holds for one it refuses " +
	"(on ClickHouse, whose ADD CONSTRAINT checks only the rows written after it). Each scan reads the whole table; " +
	"without them a held row the CHECK refuses stays"

// Register adds the flag to cmd. Its environment twin is PTAH_SKIP_CHECK_SCAN.
func Register(cmd *cobra.Command) {
	cmd.Flags().Bool(FlagName, false, usage)
}

// Requested reports whether the operator asked cmd to plan without the scans.
func Requested(cmd *cobra.Command) (bool, error) {
	return cmd.Flags().GetBool(FlagName)
}
