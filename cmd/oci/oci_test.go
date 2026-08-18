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

func TestCommandTree_RegistersTheLifecycleVerbs(t *testing.T) {
	for _, tc := range []struct {
		verb  string
		flags []string
	}{
		{verb: "resolve", flags: []string{"format", "plain-http"}},
		{verb: "inspect", flags: []string{"format", "no-referrers", "plain-http"}},
		{verb: "fetch", flags: []string{"type", "digest", "file", "output", "plain-http"}},
	} {
		t.Run(tc.verb, func(t *testing.T) {
			c := qt.New(t)
			cmd := oci.NewCommand()

			found, _, err := cmd.Find([]string{tc.verb})

			c.Assert(err, qt.IsNil)
			c.Assert(found.CommandPath(), qt.Equals, "oci "+tc.verb)
			for _, flag := range tc.flags {
				c.Assert(found.Flag(flag), qt.IsNotNil, qt.Commentf("--%s is not registered on %s", flag, tc.verb))
			}
		})
	}
}

func TestResolve_RejectsUnsupportedFormatBeforeNetworkAccess(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()
	cmd.SetArgs([]string{"resolve", "oci://registry.invalid/acme/schema:latest", "--format", "yaml"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unsupported output format "yaml": expected text or json`)
}

func TestInspect_RejectsUnsupportedFormatBeforeNetworkAccess(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()
	cmd.SetArgs([]string{"inspect", "oci://registry.invalid/acme/schema:latest", "--format", "yaml"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unsupported output format "yaml": expected text or json`)
}

func TestFetch_RejectsUnsupportedTypeBeforeNetworkAccess(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()
	cmd.SetArgs([]string{"fetch", "oci://registry.invalid/acme/schema:latest", "--type", "schema"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unsupported referrer type "schema": expected all, lint, plan, or deployment`)
}

func TestCommandTree_RegistersThePromotionVerbs(t *testing.T) {
	for _, tc := range []struct {
		verb  string
		flags []string
	}{
		{verb: "tags", flags: []string{"format", "plain-http"}},
		{verb: "tag", flags: []string{"plain-http"}},
		{verb: "copy", flags: []string{"recursive", "tag", "plain-http"}},
		{verb: "capabilities", flags: []string{"format", "plain-http"}},
	} {
		t.Run(tc.verb, func(t *testing.T) {
			c := qt.New(t)
			cmd := oci.NewCommand()

			found, _, err := cmd.Find([]string{tc.verb})

			c.Assert(err, qt.IsNil)
			c.Assert(found.CommandPath(), qt.Equals, "oci "+tc.verb)
			for _, flag := range tc.flags {
				c.Assert(found.Flag(flag), qt.IsNotNil, qt.Commentf("--%s is not registered on %s", flag, tc.verb))
			}
		})
	}
}

func TestTag_RequiresTheAliasToMove(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()
	cmd.SetArgs([]string{"tag", "oci://registry.invalid/acme/db:latest"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil, qt.Commentf("a tag with nothing to move must not reach the registry"))
}

func TestCapabilities_RejectsUnsupportedFormatBeforeNetworkAccess(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()
	cmd.SetArgs([]string{"capabilities", "oci://registry.invalid/acme/db:latest", "--format", "yaml"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unsupported output format "yaml": expected text or json`)
}

func TestTags_RejectsUnsupportedFormatBeforeNetworkAccess(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()
	cmd.SetArgs([]string{"tags", "oci://registry.invalid/acme/db:latest", "--format", "yaml"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unsupported output format "yaml": expected text or json`)
}

func TestCommandTree_RegistersReindex(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()

	found, _, err := cmd.Find([]string{"reindex"})

	c.Assert(err, qt.IsNil)
	c.Assert(found.CommandPath(), qt.Equals, "oci reindex")
	c.Assert(found.Flag("format"), qt.IsNotNil)
	c.Assert(found.Flag("plain-http"), qt.IsNotNil)
}

func TestReindex_RejectsUnsupportedFormatBeforeNetworkAccess(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()
	cmd.SetArgs([]string{"reindex", "oci://registry.invalid/acme/db:latest", "--format", "yaml"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unsupported output format "yaml": expected text or json`)
}

func TestCommandTree_RegistersVerify(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()

	found, _, err := cmd.Find([]string{"verify"})

	c.Assert(err, qt.IsNil)
	c.Assert(found.CommandPath(), qt.Equals, "oci verify")
	c.Assert(found.Flag("policy"), qt.IsNotNil)
	c.Assert(found.Flag("format"), qt.IsNotNil)
	c.Assert(found.Flag("plain-http"), qt.IsNotNil)
}

// TestVerify_RequiresAPolicy keeps the verb from reporting success for a run
// that checked nothing.
func TestVerify_RequiresAPolicy(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()
	cmd.SetArgs([]string{"verify", "oci://registry.invalid/acme/db:latest"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `--policy is required: verification with no policy would check nothing`)
}
