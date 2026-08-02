package oci_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/oci"
)

func TestCommandTreeAndFlags(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()
	referrers, _, err := cmd.Find([]string{"referrers"})

	c.Assert(err, qt.IsNil)
	c.Assert(referrers.CommandPath(), qt.Equals, "oci referrers")
	c.Assert(referrers.Flag("type"), qt.IsNotNil)
	c.Assert(referrers.Flag("format"), qt.IsNotNil)
	c.Assert(referrers.Flag("plain-http"), qt.IsNotNil)
}

func TestReferrers_RejectsUnsupportedTypeBeforeNetworkAccess(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()
	cmd.SetArgs([]string{
		"referrers",
		"oci://registry.invalid/acme/schema:latest",
		"--type", "schema",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unsupported referrer type "schema": expected all, lint, plan, or deployment`)
}

func TestReferrers_RejectsUnsupportedFormatBeforeNetworkAccess(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()
	cmd.SetArgs([]string{
		"referrers",
		"oci://registry.invalid/acme/schema:latest",
		"--format", "yaml",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unsupported output format "yaml": expected text or json`)
}
