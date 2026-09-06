package embedguard_test

import (
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedguard"
)

// moduleRoot is where the scan starts, from this package's directory.
const moduleRoot = "../.."

// TestScan_TheInferenceVerticalMakesEveryDecisionItDeclares is the gate.
//
// Twice in one epic a function carried a rule, carried its own tests, and had
// no caller outside them: the row-level write rules, and the phase machine.
// Both shipped green -- `unused` counts a test as a use, so full coverage of
// behavior that is not in effect looks exactly like coverage of behavior that
// is.
//
// A finding here means one of two things, and the second is the common one: the
// declaration is dead and should go, or the behavior it describes is missing
// and something should call it. Neither is a reason to add it to Exempt without
// saying which.
func TestScan_TheInferenceVerticalMakesEveryDecisionItDeclares(t *testing.T) {
	c := qt.New(t)

	findings, err := embedguard.Scan(moduleRoot)

	c.Assert(err, qt.IsNil)
	for _, finding := range findings {
		c.Assert(finding.Name, qt.Equals, "",
			qt.Commentf("%s:%d declares %s and no non-test file calls it. Either wire it "+
				"up, delete it, or name it in embedguard.Exempt with the reason",
				finding.File, finding.Line, finding.Name))
	}
}

// TestScan_FindsSomethingToScan is the control every corpus gate needs.
//
// An implementation that walked the wrong directory, or matched no file, would
// report zero findings and read exactly like a clean tree. This asserts the
// scan reached a corpus large enough to be the one intended.
func TestScan_FindsSomethingToScan(t *testing.T) {
	c := qt.New(t)

	declared, err := embedguard.Declarations(moduleRoot)

	c.Assert(err, qt.IsNil)
	c.Assert(len(declared) > 100, qt.IsTrue,
		qt.Commentf("only %d exported declarations under internal/embed..., which is fewer "+
			"than the vertical has; the scan is looking in the wrong place", len(declared)))
	for _, declaration := range declared {
		c.Assert(filepath.ToSlash(declaration.File), qt.Contains, "internal/embed",
			qt.Commentf("%s is not under internal/embed...", declaration.File))
	}
}

// TestExempt_EveryEntryNamesSomethingDeclared is the other direction.
//
// An exemption for a declaration that has been renamed or removed covers
// nothing, and it makes the gate pass for the wrong reason: the entry is inert,
// and whatever took its name is unguarded.
func TestExempt_EveryEntryNamesSomethingDeclared(t *testing.T) {
	c := qt.New(t)

	declared, err := embedguard.Declarations(moduleRoot)
	c.Assert(err, qt.IsNil)

	names := make(map[string]bool, len(declared))
	for _, declaration := range declared {
		names[declaration.Name] = true
	}
	for name := range embedguard.Exempt {
		c.Assert(names[name], qt.IsTrue,
			qt.Commentf("Exempt names %s, which internal/embed... does not declare", name))
	}
}

// TestExempt_EveryReasonSaysSomething refuses a bare entry.
//
// The failure this package exists for was a rule nobody was applying. An
// exemption with no reason is the same thing one level up.
func TestExempt_EveryReasonSaysSomething(t *testing.T) {
	c := qt.New(t)

	c.Assert(embedguard.Exempt, qt.Not(qt.HasLen), 0)
	for name, reason := range embedguard.Exempt {
		c.Assert(len(reason) > 30, qt.IsTrue,
			qt.Commentf("%s is exempt for %q, which is not a reason", name, reason))
	}
}

// TestScan_StillReportsWhatNothingCalls is the control the call side did not
// have, and the reason it needs one is that every other assertion here expects
// ZERO findings.
//
// A `calledNames` that answered "called" for everything would satisfy them all
// and report nothing ever again. The declaration side has
// TestScan_FindsSomethingToScan; this is its opposite number.
//
// Exempt is exactly the set of declarations known to have no caller, so
// emptying it must produce exactly those findings. That pins both directions at
// once: the scan can still find an uncalled declaration, and Exempt suppresses
// those and no others.
func TestScan_StillReportsWhatNothingCalls(t *testing.T) {
	c := qt.New(t)
	exempt := embedguard.Exempt
	embedguard.Exempt = make(map[string]string)
	defer func() { embedguard.Exempt = exempt }()

	findings, err := embedguard.Scan(moduleRoot)

	c.Assert(err, qt.IsNil)
	reported := make([]string, 0, len(findings))
	for _, finding := range findings {
		reported = append(reported, finding.Name)
	}
	slices.Sort(reported)
	suppressed := make([]string, 0, len(exempt))
	for name := range exempt {
		suppressed = append(suppressed, name)
	}
	slices.Sort(suppressed)
	c.Assert(reported, qt.DeepEquals, suppressed)
}
