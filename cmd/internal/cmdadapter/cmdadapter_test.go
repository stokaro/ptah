package cmdadapter

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
)

func TestForwardCommandWithTargetHelpShowsTargetFlags(t *testing.T) {
	c := qt.New(t)

	target := newTestTargetCommand(nil)
	cmd := NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
		return target
	})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "--value")
}

func TestForwardCommandWithTargetHelpUsesAdapterUsage(t *testing.T) {
	c := qt.New(t)

	target := newTestTargetCommand(nil)
	cmd := NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
		return target
	})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Usage:\n  atlas [flags]")
	c.Assert(out.String(), qt.Not(qt.Contains), "Usage:\n  target")
}

func TestForwardCommandWithTargetHelpUsesAdapterUsageForPrefixedChild(t *testing.T) {
	c := qt.New(t)

	target := &cobra.Command{Use: "target"}
	child := newTestTargetCommand(nil)
	child.Use = "child NAME"
	target.AddCommand(child)
	cmd := NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target child", func() *cobra.Command {
		return target
	}, "child")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Usage:\n  atlas NAME [flags]")
	c.Assert(out.String(), qt.Contains, "--value")
	c.Assert(out.String(), qt.Not(qt.Contains), "target child")
}

func TestForwardCommandResetsTargetFlagsAndIO(t *testing.T) {
	c := qt.New(t)

	var values []string
	target := newTestTargetCommand(func(value string) {
		values = append(values, value)
	})
	cmd := NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
		return target
	})
	var adapterOut bytes.Buffer
	cmd.SetOut(&adapterOut)
	cmd.SetErr(&adapterOut)
	cmd.SetArgs([]string{"--value", "changed"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(adapterOut.String(), qt.Equals, "changed\n")
	c.Assert(values, qt.DeepEquals, []string{"changed"})

	var directOut bytes.Buffer
	target.SetOut(&directOut)
	target.SetErr(&directOut)
	target.SetArgs(nil)
	err = target.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(directOut.String(), qt.Equals, "default\n")
	c.Assert(values, qt.DeepEquals, []string{"changed", "default"})
}

func newTestTargetCommand(onRun func(string)) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "target",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			value, err := cmd.Flags().GetString("value")
			if err != nil {
				return err
			}
			if onRun != nil {
				onRun(value)
			}
			_, err = cmd.OutOrStdout().Write([]byte(value + "\n"))
			return err
		},
	}
	cmd.Flags().String("value", "default", "Value to print")
	return cmd
}
