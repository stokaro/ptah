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

func TestForwardCommandResetsStringArrayEmptyDefault(t *testing.T) {
	c := qt.New(t)

	target := newStringArrayTargetCommand(nil, nil)
	cmd := NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
		return target
	})
	var adapterOut bytes.Buffer
	cmd.SetOut(&adapterOut)
	cmd.SetErr(&adapterOut)
	cmd.SetArgs([]string{"--value", "DS101", "--value", "MY"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	values, err := target.Flags().GetStringArray("value")
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 0)
	c.Assert(target.Flags().Lookup("value").Value.String(), qt.Equals, "[]")
	c.Assert(target.Flags().Lookup("value").Changed, qt.IsFalse)
}

func TestForwardCommandRepeatedStringArrayRunsReplaceDefault(t *testing.T) {
	c := qt.New(t)

	var runs [][]string
	target := newStringArrayTargetCommand([]string{"prod", "production"}, func(values []string) {
		runs = append(runs, values)
	})
	cmd := NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
		return target
	})
	var adapterOut bytes.Buffer
	cmd.SetOut(&adapterOut)
	cmd.SetErr(&adapterOut)
	cmd.SetArgs([]string{"--value", "staging"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	cmd.SetArgs([]string{"--value", "qa", "--value", "dev"})
	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(runs, qt.DeepEquals, [][]string{{"staging"}, {"qa", "dev"}})
	values, err := target.Flags().GetStringArray("value")
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, []string{"prod", "production"})
	c.Assert(target.Flags().Lookup("value").Changed, qt.IsFalse)
}

func TestResetCommandFlagsStringArrayUsesDeclaredDefault(t *testing.T) {
	c := qt.New(t)

	target := newStringArrayTargetCommand([]string{"prod", "production"}, nil)
	c.Assert(target.Flags().Set("value", "staging"), qt.IsNil)
	values, err := target.Flags().GetStringArray("value")
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, []string{"staging"})

	resetCommandFlags(target)

	values, err = target.Flags().GetStringArray("value")
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, []string{"prod", "production"})
	c.Assert(target.Flags().Lookup("value").Changed, qt.IsFalse)
}

func TestForwardCommandRepeatedStringSliceRunsReplaceDefault(t *testing.T) {
	c := qt.New(t)

	var runs [][]string
	target := newStringSliceTargetCommand([]string{"prod"}, func(values []string) {
		runs = append(runs, values)
	})
	cmd := NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
		return target
	})
	var adapterOut bytes.Buffer
	cmd.SetOut(&adapterOut)
	cmd.SetErr(&adapterOut)
	cmd.SetArgs([]string{"--value", "staging"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	cmd.SetArgs([]string{"--value", "qa,dev"})
	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(runs, qt.DeepEquals, [][]string{{"staging"}, {"qa", "dev"}})
	values, err := target.Flags().GetStringSlice("value")
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, []string{"prod"})
	c.Assert(target.Flags().Lookup("value").Changed, qt.IsFalse)
}

func TestForwardCommandStringArrayCLIOverridesEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_VALUE", "prod")

	var runs [][]string
	target := newStringArrayTargetCommand(nil, func(values []string) {
		runs = append(runs, values)
	})
	cmd := NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
		return target
	})
	var adapterOut bytes.Buffer
	cmd.SetOut(&adapterOut)
	cmd.SetErr(&adapterOut)
	cmd.SetArgs([]string{"--value", "qa", "--value", "dev"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(runs, qt.DeepEquals, [][]string{{"qa", "dev"}})
}

func TestForwardCommandStringSliceCLIOverridesEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_VALUE", "prod,production")

	var runs [][]string
	target := newStringSliceTargetCommand(nil, func(values []string) {
		runs = append(runs, values)
	})
	cmd := NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
		return target
	})
	var adapterOut bytes.Buffer
	cmd.SetOut(&adapterOut)
	cmd.SetErr(&adapterOut)
	cmd.SetArgs([]string{"--value", "qa,dev"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(runs, qt.DeepEquals, [][]string{{"qa", "dev"}})
}

func TestExplicitFlagNamesIncludesShorthandClusters(t *testing.T) {
	c := qt.New(t)

	names := explicitFlagNames([]string{"-ab", "--value=qa", "--", "--ignored"})

	c.Assert(names, qt.DeepEquals, map[string]struct{}{
		"a":     {},
		"b":     {},
		"value": {},
	})
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

func newStringArrayTargetCommand(defaults []string, onRun func([]string)) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "target",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			values, err := cmd.Flags().GetStringArray("value")
			if err != nil {
				return err
			}
			if onRun != nil {
				onRun(append([]string(nil), values...))
			}
			return nil
		},
	}
	cmd.Flags().StringArray("value", defaults, "Values to collect")
	return cmd
}

func newStringSliceTargetCommand(defaults []string, onRun func([]string)) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "target",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			values, err := cmd.Flags().GetStringSlice("value")
			if err != nil {
				return err
			}
			if onRun != nil {
				onRun(append([]string(nil), values...))
			}
			return nil
		},
	}
	cmd.Flags().StringSlice("value", defaults, "Values to collect")
	return cmd
}
