package projectconfig_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
)

// parsedIgnoredConstructs lists what a parse recorded as accepted-and-inert,
// which is what the command reports to the operator on stderr. It reads the
// names through the package's existing helper rather than walking the field a
// second time.
func parsedIgnoredConstructs(c *qt.C, raw string) []string {
	c.Helper()
	cfg, err := projectconfig.ParseAtlas([]byte(raw), "atlas.hcl", "local")
	c.Assert(err, qt.IsNil)
	return ignoredConstructNames(cfg)
}

// envLintProject wraps one lint block body in the env scope every --env project
// uses.
func envLintProject(body string) string {
	return fmt.Sprintf("env \"local\" {\n  url = \"sqlite://app.db\"\n\n  lint {\n%s\n  }\n}\n", body)
}

// TestParseAtlas_HonoredLintBlocksAreNotReportedInert is the guard the defect
// asked for (stokaro/ptah#3117).
//
// Two answers to "is this a known lint block" live apart: the parser routes the
// block in a switch, and the structure walk carries its own list of known
// children. Nothing compared them, so `naming` was honored by the first and
// unknown to the second -- and a run whose naming policy had just failed the
// lint told the operator the block "is ignored for Atlas compatibility and has
// no effect".
//
// The rows are the bodies, one per honored block, and the assertion is the same
// for all of them: a block Ptah acts on is never reported inert. A block added
// to the parser without a row here is not covered, which is the residue of two
// lists this table narrows rather than removes.
func TestParseAtlas_HonoredLintBlocksAreNotReportedInert(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "naming", body: "    naming {\n      match = \"^[a-z_]+$\"\n    }"},
		{
			name: "naming with per-kind patterns",
			body: "    naming {\n      match = \"^[a-z_]+$\"\n" +
				"      table { match = \"^t_\" }\n" +
				"      index { match = \"_idx$\" }\n" +
				"      foreign_key { match = \"^fk_\" }\n    }",
		},
		{name: "destructive", body: "    destructive {\n      error = false\n    }"},
		{name: "concurrent_index", body: "    concurrent_index {\n      error = true\n    }"},
		{name: "condrop", body: "    condrop {\n      error = true\n    }"},
		{name: "data_depend", body: "    data_depend {\n      error = true\n    }"},
		{name: "incompatible", body: "    incompatible {\n      error = true\n    }"},
		{name: "nestedtx", body: "    nestedtx {\n      error = true\n    }"},
		{name: "git", body: "    git {\n      base = \"main\"\n    }"},
		{name: "ptah rule", body: "    rule \"NM102\" {\n      match = \"^[a-z_]+$\"\n    }"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			ignored := parsedIgnoredConstructs(c, envLintProject(test.body))

			c.Assert(ignored, qt.HasLen, 0)
		})
	}
}

// TestParseAtlas_InertLintBlocksAreStillReported is the control. The stderr line
// exists to name a construct that really does nothing, and widening the known
// list must not have silenced it.
func TestParseAtlas_InertLintBlocksAreStillReported(t *testing.T) {
	c := qt.New(t)

	ignored := parsedIgnoredConstructs(c, envLintProject("    review {\n      error = true\n    }"))

	c.Assert(ignored, qt.DeepEquals, []string{"review"})
}
