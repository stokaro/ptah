package capabilityprobe_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/capabilityprobe"
)

// A row the cell declares understated is counted on the summary line and
// printed with the declaration's reason, so the report says why Ptah claims
// less than the server does instead of only that it does. The YDB header also
// names the pragma every statement was sent after, which is what lets a reader
// run one again by hand.
func TestWriteReport_NamesTheUnderstatedRowsAndTheStatementPrefix(t *testing.T) {
	c := qt.New(t)
	report := &capabilityprobe.Report{
		Dialect:         platform.YDB,
		Namespace:       "/local/ptah_capprobe_00",
		StatementPrefix: "PRAGMA TablePathPrefix('/local/ptah_capprobe_00');",
		Control:         capabilityprobe.Attempt{Statement: "CREATE NONSENSE x", ServerErr: "parse error"},
		Rows: []capabilityprobe.Row{
			{
				Capability: capability.Views, ServerDoes: true, Observed: true,
				Outcome: capabilityprobe.Conservative, Reason: "YDB creates and reads a view",
			},
			{Capability: capability.ForeignKeys, Observed: true, Outcome: capabilityprobe.Agrees},
		},
	}
	var out strings.Builder

	capabilityprobe.WriteReport(&out, report)

	c.Assert(out.String(), qt.Contains,
		"  prefix       every statement below was sent after PRAGMA TablePathPrefix('/local/ptah_capprobe_00');\n")
	c.Assert(out.String(), qt.Contains,
		"summary: 2 rows — 1 AGREES, 0 DISAGREES, 1 CONSERVATIVE, 0 UNDECIDABLE; decided 2, floor 1\n")
	c.Assert(out.String(), qt.Contains, "\nunderstated on purpose:\n  views\n      YDB creates and reads a view\n")
}
