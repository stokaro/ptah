package apiguard_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// baselineFixture is a package with an exported concrete type (with an exported
// field and two exported methods) and an exported interface. It is the input
// the guard's per-package generation runs against.
const baselineFixture = `// Package fixture is the baseline input for the public API snapshot guard self-test.
package fixture

// Widget is a concrete named type whose exported fields and methods must be
// recorded by the snapshot guard.
type Widget struct {
	// Name identifies the widget.
	Name string

	// Weight is an exported field.
	Weight int
}

// Describe is a concrete method retained by every fixture.
func (w Widget) Describe() string { return w.Name }

// Rank is a concrete method.
func (w Widget) Rank() int { return w.Weight }

// Sink is an interface, kept to confirm interface coverage did not regress.
type Sink interface {
	// Drain consumes a widget.
	Drain(w Widget) error
}
`

// fieldRemovedFixture is baselineFixture with the exported Weight field dropped.
const fieldRemovedFixture = `// Package fixture is the baseline input for the public API snapshot guard self-test.
package fixture

// Widget is a concrete named type whose exported fields and methods must be
// recorded by the snapshot guard.
type Widget struct {
	// Name identifies the widget.
	Name string
}

// Describe is a concrete method retained by every fixture.
func (w Widget) Describe() string { return w.Name }

// Rank is a concrete method.
func (w Widget) Rank() int { return 0 }

// Sink is an interface, kept to confirm interface coverage did not regress.
type Sink interface {
	// Drain consumes a widget.
	Drain(w Widget) error
}
`

// methodRemovedFixture is baselineFixture with the exported Rank method dropped.
const methodRemovedFixture = `// Package fixture is the baseline input for the public API snapshot guard self-test.
package fixture

// Widget is a concrete named type whose exported fields and methods must be
// recorded by the snapshot guard.
type Widget struct {
	// Name identifies the widget.
	Name string

	// Weight is an exported field.
	Weight int
}

// Describe is a concrete method retained by every fixture.
func (w Widget) Describe() string { return w.Name }

// Sink is an interface, kept to confirm interface coverage did not regress.
type Sink interface {
	// Drain consumes a widget.
	Drain(w Widget) error
}
`

// TestSnapshotCapturesStructFields proves that removing an exported struct field
// changes the generated snapshot fragment.
func TestSnapshotCapturesStructFields(t *testing.T) {
	c := qt.New(t)

	base := emitFragment(t, baselineFixture)
	noField := emitFragment(t, fieldRemovedFixture)

	c.Assert(base, qt.Contains, "Weight int")
	c.Assert(noField, qt.Not(qt.Contains), "Weight int")
	c.Assert(base, qt.Not(qt.Equals), noField)
}

// TestSnapshotCapturesConcreteMethods proves that removing an exported method on
// a concrete named type changes the generated snapshot fragment. This is the
// coverage the pre-#784 interface-only guard lacked.
func TestSnapshotCapturesConcreteMethods(t *testing.T) {
	c := qt.New(t)

	base := emitFragment(t, baselineFixture)
	noMethod := emitFragment(t, methodRemovedFixture)

	c.Assert(base, qt.Contains, "func (w Widget) Rank() int")
	c.Assert(noMethod, qt.Not(qt.Contains), "func (w Widget) Rank() int")
	c.Assert(base, qt.Not(qt.Equals), noMethod)
}

// TestSnapshotRetainsInterfaceAndMethodCoverage guards against regressing the
// interface coverage that predates #784 while proving concrete-method coverage
// is present in the same fragment.
func TestSnapshotRetainsInterfaceAndMethodCoverage(t *testing.T) {
	c := qt.New(t)

	base := emitFragment(t, baselineFixture)

	c.Assert(base, qt.Contains, "Drain(w Widget) error")
	c.Assert(base, qt.Contains, "func (w Widget) Describe() string")
}

// emitFragment writes source as the sole file of a throwaway temp module and
// runs the guard's per-package generation (`--emit-package .`) against it,
// returning the emitted snapshot fragment. The tests therefore exercise exactly
// the logic the real snapshot is built from rather than a Go reimplementation.
func emitFragment(t *testing.T, source string) string {
	t.Helper()
	c := qt.New(t)

	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module apiguardfixture\n\ngo 1.21\n"), 0o600)
	c.Assert(err, qt.IsNil)
	err = os.WriteFile(filepath.Join(dir, "fixture.go"), []byte(source), 0o600)
	c.Assert(err, qt.IsNil)

	script := filepath.Join(moduleRoot(t), "scripts", "check-public-api-snapshot.sh")
	cmd := exec.Command("bash", script, "--emit-package", ".")
	cmd.Dir = dir

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	c.Assert(err, qt.IsNil, qt.Commentf("emit failed: %v\nstderr:\n%s", err, stderr.String()))

	return string(out)
}

// moduleRoot returns the repository root. This package lives at a fixed depth
// (internal/apiguard) and go test runs with the working directory set to the
// package source directory, so the root is two directories up.
func moduleRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	qt.New(t).Assert(err, qt.IsNil)

	return filepath.Dir(filepath.Dir(wd))
}
