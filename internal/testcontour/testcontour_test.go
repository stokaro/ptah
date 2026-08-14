package testcontour_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/testcontour"
)

func TestRun_HappyPath(t *testing.T) {
	c := qt.New(t)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./testdata/pass",
		Tags:    []string{"testcontour_fixture"},
		Timeout: time.Minute,
		Race:    true,
		Stdout:  &stdout,
		Stderr:  &stderr,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(stdout.String(), qt.Contains, "--- PASS: TestTaggedPass")
	c.Assert(stdout.String(), qt.Contains, "--- PASS: TestTaggedSubtestPass")
	c.Assert(stdout.String(), qt.Contains, "--- PASS: TestIntegrationPackagePass")
	c.Assert(stdout.String(), qt.Not(qt.Contains), "TestOrdinaryMustNotRun")
	c.Assert(stderr.String(), qt.Equals, "")
}

func TestRun_FailurePathRejectsTopLevelSkip(t *testing.T) {
	c := qt.New(t)
	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./testdata/topskip",
		Tags:    []string{"testcontour_fixture"},
		Timeout: time.Minute,
	})
	c.Assert(err, qt.ErrorMatches, `test contour ./testdata/topskip skipped go\.5x5\.cz/ptah/internal/testcontour/testdata/topskip:TestTaggedTopLevelSkip`)
}

func TestRun_FailurePathRejectsSubtestSkip(t *testing.T) {
	c := qt.New(t)
	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./testdata/subskip",
		Tags:    []string{"testcontour_fixture"},
		Timeout: time.Minute,
	})
	c.Assert(err, qt.ErrorMatches, `test contour ./testdata/subskip skipped go\.5x5\.cz/ptah/internal/testcontour/testdata/subskip:TestTaggedSubtestSkip/skipped`)
}

func TestRun_FailurePathPreservesTestFailure(t *testing.T) {
	c := qt.New(t)
	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./testdata/fail",
		Tags:    []string{"testcontour_fixture"},
		Timeout: time.Minute,
	})
	c.Assert(err, qt.ErrorMatches, `test contour ./testdata/fail failed: exit status 1`)
}

func TestRun_FailurePathRejectsEmptyContour(t *testing.T) {
	c := qt.New(t)
	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./testdata/empty",
		Tags:    []string{"testcontour_fixture"},
		Timeout: time.Minute,
	})
	c.Assert(err, qt.ErrorMatches, `test contour ./testdata/empty ran no tests`)
}

func TestRun_FailurePathRejectsIncompletePackageInRecursiveContour(t *testing.T) {
	c := qt.New(t)
	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./testdata/recursive/...",
		Tags:    []string{"testcontour_fixture"},
		Timeout: time.Minute,
	})
	c.Assert(
		err,
		qt.ErrorMatches,
		`test contour ./testdata/recursive/\.\.\. produced no complete result for go\.5x5\.cz/ptah/internal/testcontour/testdata/recursive/incomplete:TestNeverRuns`,
	)
}

func TestRun_FailurePathRejectsWhiteBoxIntegrationTest(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(exec.Command("git", "init", dir).Run(), qt.IsNil)
	writeIntegrationFixture(c, dir, "fixture", "fixture", "integration")
	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./integration/...",
		Tags:    []string{"integration"},
		Timeout: time.Minute,
		Dir:     dir,
	})
	c.Assert(
		err,
		qt.ErrorMatches,
		`test file integration/fixture/contour_test\.go under an integration tree uses white-box package fixture; package name must end in _test`,
	)
}

func TestRun_FailurePathRejectsUntaggedIntegrationTest(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(exec.Command("git", "init", dir).Run(), qt.IsNil)
	writeIntegrationFixture(c, dir, "fixture", "fixture_test", "!windows")
	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./integration/...",
		Tags:    []string{"integration"},
		Timeout: time.Minute,
		Dir:     dir,
	})
	c.Assert(
		err,
		qt.ErrorMatches,
		`test file integration/fixture/contour_test\.go under an integration tree must require //go:build integration or !integration`,
	)
}

func TestRun_FailurePathRejectsLeakingIntegrationConstraint(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(exec.Command("git", "init", dir).Run(), qt.IsNil)
	writeIntegrationFixture(c, dir, "fixture", "fixture_test", "integration || linux")
	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./integration/...",
		Tags:    []string{"integration"},
		Timeout: time.Minute,
		Dir:     dir,
	})
	c.Assert(err, qt.ErrorMatches, `integration test file integration/fixture/contour_test\.go has a build constraint that can select it without integration`)
}

func TestRun_FailurePathRejectsIntegrationTestOutsideDedicatedPackage(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(exec.Command("git", "init", dir).Run(), qt.IsNil)
	c.Assert(os.MkdirAll(filepath.Join(dir, "product"), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "product", "live_test.go"), []byte(`//go:build integration

package product_test

import "testing"

func TestLive(_ *testing.T) {}
`), 0o600), qt.IsNil)

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./integration/...",
		Tags:    []string{"integration"},
		Timeout: time.Minute,
		Dir:     dir,
	})
	c.Assert(err, qt.ErrorMatches, `integration test file product/live_test\.go must live under integration/ or testkit/integration/`)
}

func TestRun_FailurePathRejectsPlatformExcludedWhiteBoxIntegrationTest(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(exec.Command("git", "init", dir).Run(), qt.IsNil)
	c.Assert(os.MkdirAll(filepath.Join(dir, "integration", "fixture"), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "integration", "fixture", "contour_windows_test.go"), []byte(`//go:build integration

package fixture

import "testing"

func TestLive(_ *testing.T) {}
`), 0o600), qt.IsNil)

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./integration/...",
		Tags:    []string{"integration"},
		Timeout: time.Minute,
		Dir:     dir,
	})
	c.Assert(
		err,
		qt.ErrorMatches,
		`test file integration/fixture/contour_windows_test\.go under an integration tree uses white-box package fixture; package name must end in _test`,
	)
}

func TestRun_FailurePathRejectsPlatformExcludedTestWithoutContourTag(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(exec.Command("git", "init", dir).Run(), qt.IsNil)
	c.Assert(os.MkdirAll(filepath.Join(dir, "integration", "fixture"), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/fixture\n\ngo 1.26\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "integration", "fixture", "contour_windows_test.go"),
		[]byte(`//go:build windows

package fixture_test

import "testing"

func TestLive(_ *testing.T) {}
`),
		0o600,
	), qt.IsNil)

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./integration/...",
		Tags:    []string{"integration"},
		Timeout: time.Minute,
		Dir:     dir,
	})
	c.Assert(
		err,
		qt.ErrorMatches,
		`test file integration/fixture/contour_windows_test\.go under an integration tree must require //go:build integration or !integration`,
	)
}

func TestRun_FailurePathRejectsPlatformExcludedIntegrationTest(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(exec.Command("git", "init", dir).Run(), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/fixture\n\ngo 1.26\n"), 0o600), qt.IsNil)
	c.Assert(os.MkdirAll(filepath.Join(dir, "integration", "fixture"), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "integration", "fixture", "doc.go"),
		[]byte("package fixture\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "integration", "fixture", "contour_windows_test.go"),
		[]byte(`//go:build integration

package fixture_test

import "testing"

func TestLive(_ *testing.T) {}
`),
		0o600,
	), qt.IsNil)

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./integration/...",
		Tags:    []string{"integration"},
		Timeout: time.Minute,
		Dir:     dir,
	})
	c.Assert(
		err,
		qt.ErrorMatches,
		`complete integration contour did not select .*/integration/fixture/contour_windows_test\.go on this platform`,
	)
}

func TestRun_HappyPathAcceptsGoBuildWhitespaceAndExcludedUnitTests(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(exec.Command("git", "init", dir).Run(), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/fixture\n\ngo 1.26\n"), 0o600), qt.IsNil)
	fixtureDir := filepath.Join(dir, "integration", "fixture")
	c.Assert(os.MkdirAll(fixtureDir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(fixtureDir, "doc.go"), []byte("package fixture\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(fixtureDir, "contour_test.go"), []byte(`//go:build	integration

package fixture_test

import "testing"

func TestLive(_ *testing.T) {}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(fixtureDir, "unit_windows_test.go"), []byte(`//go:build !integration && windows

package fixture_test

import "testing"

func TestUnit(_ *testing.T) {}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(fixtureDir, "logical_test.go"), []byte(`//go:build (!windows || integration) && (windows || integration)

package fixture_test

import "testing"

func TestLogicalRequirement(_ *testing.T) {}
`), 0o600), qt.IsNil)

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./integration/...",
		Tags:    []string{"integration"},
		Timeout: time.Minute,
		Dir:     dir,
	})
	c.Assert(err, qt.IsNil)
}

func TestRun_FailurePathRejectsExcludedWhiteBoxTestInIntegrationTree(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(exec.Command("git", "init", dir).Run(), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/fixture\n\ngo 1.26\n"), 0o600), qt.IsNil)
	fixtureDir := filepath.Join(dir, "integration", "fixture")
	c.Assert(os.MkdirAll(fixtureDir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(fixtureDir, "doc.go"), []byte("package fixture\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(fixtureDir, "contour_test.go"), []byte(`//go:build integration

package fixture_test

import "testing"

func TestLive(_ *testing.T) {}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(fixtureDir, "unit_test.go"), []byte(`//go:build !integration

package fixture

import "testing"

func TestUnit(_ *testing.T) {}
`), 0o600), qt.IsNil)

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./integration/...",
		Tags:    []string{"integration"},
		Timeout: time.Minute,
		Dir:     dir,
	})
	c.Assert(
		err,
		qt.ErrorMatches,
		`test file integration/fixture/unit_test\.go under an integration tree uses white-box package fixture; package name must end in _test`,
	)
}

func writeIntegrationFixture(c *qt.C, root, packageDir, packageName, buildConstraint string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/fixture\n\ngo 1.26\n"), 0o600), qt.IsNil)
	dir := filepath.Join(root, "integration", packageDir)
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	c.Assert(
		os.WriteFile(filepath.Join(dir, "doc.go"), []byte("package "+strings.TrimSuffix(packageName, "_test")+"\n"), 0o600),
		qt.IsNil,
	)
	source := fmt.Sprintf(`//go:build %s

package %s

import "testing"

func TestLive(_ *testing.T) {}
`, buildConstraint, packageName)
	c.Assert(os.WriteFile(filepath.Join(dir, "contour_test.go"), []byte(source), 0o600), qt.IsNil)
}

func TestRun_FailurePathValidatesConfiguration(t *testing.T) {
	t.Run("package", func(t *testing.T) {
		c := qt.New(t)
		err := testcontour.Run(context.Background(), testcontour.Config{
			Tags:    []string{"integration"},
			Timeout: time.Minute,
		})
		c.Assert(err, qt.ErrorMatches, "test contour package is required")
	})
	t.Run("tags", func(t *testing.T) {
		c := qt.New(t)
		err := testcontour.Run(context.Background(), testcontour.Config{
			Package: "./testdata/pass",
			Timeout: time.Minute,
		})
		c.Assert(err, qt.ErrorMatches, "test contour requires at least one build tag")
	})
	t.Run("timeout", func(t *testing.T) {
		c := qt.New(t)
		err := testcontour.Run(context.Background(), testcontour.Config{
			Package: "./testdata/pass",
			Tags:    []string{"integration"},
		})
		c.Assert(err, qt.ErrorMatches, "test contour timeout must be positive")
	})
	t.Run("invalid tag", func(t *testing.T) {
		c := qt.New(t)
		err := testcontour.Run(context.Background(), testcontour.Config{
			Package: "./testdata/pass",
			Tags:    []string{"integration,unix"},
			Timeout: time.Minute,
		})
		c.Assert(err, qt.ErrorMatches, `build tag "integration,unix" is not valid`)
	})
	t.Run("narrow integration package", func(t *testing.T) {
		c := qt.New(t)
		err := testcontour.Run(context.Background(), testcontour.Config{
			Package: "./integration/fixture",
			Tags:    []string{"integration"},
			Timeout: time.Minute,
		})
		c.Assert(
			err,
			qt.ErrorMatches,
			`integration contour package must be \./integration/\.\.\., got \./integration/fixture`,
		)
	})
}
