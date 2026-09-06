package main

// White-box testing required: expectedMatrix is unexported and the property
// under test is the census identity it hands to Matrix.Validate, which no
// caller of the command can observe except as an exit code.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/capabilityprobe"
)

// TestExpectedMatrix_APartialRequestStillValidates is the defect.
//
// The census identity is declared == runnable + skipped, and it is what stops a
// declared line leaving the pipeline by being forgotten. Narrowing Cells to the
// requested subject while leaving Declared at the full inventory makes that
// identity unsatisfiable, so `/capability-matrix postgres` reported
//
//	the census does not add up: 30 declared, 6 runnable, 4 skipped
//
// and failed a run that did exactly what it was asked to do -- the same failure
// stokaro/ptah#2185 removed from MISSING, one assertion further in.
func TestExpectedMatrix_APartialRequestStillValidates(t *testing.T) {
	c := qt.New(t)

	matrix, err := expectedMatrix("postgres-18,postgres-17")

	c.Assert(err, qt.IsNil)
	c.Assert(matrix.Validate(), qt.IsNil)
	c.Assert(matrix.Cells, qt.HasLen, 2)
	c.Assert(matrix.Declared, qt.Equals, 2)
}

// TestExpectedMatrix_TheWholeMatrixIsUnchanged is the control.
//
// Without it, a narrowing that simply zeroed the census would pass the test
// above while destroying the guarantee for the full run, which is the one that
// gates every push.
func TestExpectedMatrix_TheWholeMatrixIsUnchanged(t *testing.T) {
	c := qt.New(t)
	full := capabilityprobe.CIMatrix()

	matrix, err := expectedMatrix("")

	c.Assert(err, qt.IsNil)
	c.Assert(matrix.Validate(), qt.IsNil)
	c.Assert(matrix.Declared, qt.Equals, full.Declared)
	c.Assert(matrix.Cells, qt.HasLen, len(full.Cells))
	c.Assert(matrix.Skipped, qt.HasLen, len(full.Skipped))
}

// TestExpectedMatrix_ADeclaredButUnrunnableCellIsDeclared pins the other half
// of the narrowing.
//
// A line the tier cannot execute is still declared and carries its own reason.
// Counting it as runnable would claim it was probed; refusing the name outright
// would call a declared line undeclared.
func TestExpectedMatrix_ADeclaredButUnrunnableCellIsDeclared(t *testing.T) {
	c := qt.New(t)
	skipped := capabilityprobe.CIMatrix().Skipped
	c.Assert(skipped, qt.Not(qt.HasLen), 0,
		qt.Commentf("the matrix declares no unrunnable line, so this test measures nothing"))

	matrix, err := expectedMatrix("postgres-18," + skipped[0].ID)

	c.Assert(err, qt.IsNil)
	c.Assert(matrix.Validate(), qt.IsNil)
	c.Assert(matrix.Cells, qt.HasLen, 1)
	c.Assert(matrix.Skipped, qt.HasLen, 1)
	c.Assert(matrix.Declared, qt.Equals, 2)
}

// TestExpectedMatrix_AnUndeclaredNameIsStillACallerError keeps the narrowing
// from becoming a way to silently ask for nothing.
func TestExpectedMatrix_AnUndeclaredNameIsStillACallerError(t *testing.T) {
	c := qt.New(t)

	_, err := expectedMatrix("postgres-18,nosuch-cell")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "nosuch-cell")
}
