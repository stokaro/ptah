package cmdadapter_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdadapter"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/testutils"
)

type testContextKey struct{}

type middlewareContextKey struct{}

var errAdapterCanceled = errors.New("adapter canceled")
var errMapperFailed = errors.New("mapper failed")
var errTargetFailed = errors.New("target failed")
var errCleanupFailed = errors.New("cleanup failed")

func TestForwardCommandWithTargetHelpShowsTargetFlags(t *testing.T) {
	c := qt.New(t)

	target := newTestTargetCommand(nil)
	cmd := cmdadapter.NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
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
	cmd := cmdadapter.NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
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
	cmd := cmdadapter.NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target child", func() *cobra.Command {
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
	cmd := cmdadapter.NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
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
	c.Assert(target.Context(), qt.IsNil)

	var directOut bytes.Buffer
	target.SetOut(&directOut)
	target.SetErr(&directOut)
	target.SetArgs(nil)
	err = target.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(directOut.String(), qt.Equals, "default\n")
	c.Assert(values, qt.DeepEquals, []string{"changed", "default"})
}

func TestForwardCommandCarriesAndRestoresContext(t *testing.T) {
	c := qt.New(t)
	targetContext := context.WithValue(t.Context(), testContextKey{}, "target")
	var receivedValue string
	var receivedCause error
	var receivedDeadline time.Time
	var receivedDeadlineSet bool
	target := &cobra.Command{
		Use: "target",
		RunE: func(cmd *cobra.Command, _ []string) error {
			receivedValue, _ = cmd.Context().Value(testContextKey{}).(string)
			receivedCause = context.Cause(cmd.Context())
			receivedDeadline, receivedDeadlineSet = cmd.Context().Deadline()
			return nil
		},
	}
	target.SetContext(targetContext)
	cmd := cmdadapter.NewForwardCommand("atlas", "Atlas adapter command", "target", func() *cobra.Command {
		return target
	})
	deadline := time.Now().Add(time.Hour)
	deadlineContext, cancelDeadline := context.WithDeadline(t.Context(), deadline)
	defer cancelDeadline()
	adapterContext, cancel := context.WithCancelCause(
		context.WithValue(deadlineContext, testContextKey{}, "adapter"),
	)
	cancel(errAdapterCanceled)
	cmd.SetContext(adapterContext)

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(receivedValue, qt.Equals, "adapter")
	c.Assert(receivedCause, qt.ErrorIs, errAdapterCanceled)
	c.Assert(receivedDeadlineSet, qt.IsTrue)
	c.Assert(receivedDeadline, qt.Equals, deadline)
	c.Assert(target.Context().Value(testContextKey{}), qt.Equals, "target")
	c.Assert(context.Cause(target.Context()), qt.IsNil)
}

func TestForwardCommandReplacesContextAcrossReusedTargetTree(t *testing.T) {
	c := qt.New(t)
	var receivedValues []string
	var receivedCauses []error
	target := &cobra.Command{Use: "target"}
	target.AddCommand(&cobra.Command{
		Use: "child",
		RunE: func(cmd *cobra.Command, _ []string) error {
			receivedValues = append(
				receivedValues,
				cmd.Context().Value(testContextKey{}).(string),
			)
			receivedCauses = append(receivedCauses, context.Cause(cmd.Context()))
			return nil
		},
	})
	cmd := cmdadapter.NewForwardCommandWithArgs(
		"atlas",
		"Atlas adapter command",
		"target child",
		func() *cobra.Command {
			return target
		},
		"child",
	)
	cmd.SetArgs([]string{})
	firstContext, cancelFirst := context.WithCancelCause(
		context.WithValue(t.Context(), testContextKey{}, "first"),
	)
	cancelFirst(errAdapterCanceled)

	err := cmd.ExecuteContext(firstContext)

	c.Assert(err, qt.IsNil)
	secondContext := context.WithValue(t.Context(), testContextKey{}, "second")
	err = cmd.ExecuteContext(secondContext)

	c.Assert(err, qt.IsNil)
	c.Assert(receivedValues, qt.DeepEquals, []string{"first", "second"})
	c.Assert(receivedCauses[0], qt.ErrorIs, errAdapterCanceled)
	c.Assert(receivedCauses[1], qt.IsNil)
	c.Assert(target.Context(), qt.IsNil)
	c.Assert(target.Commands()[0].Context(), qt.IsNil)
}

func TestForwardCommandCarriesFreshExecutingContextAcrossRootReuse(t *testing.T) {
	c := qt.New(t)
	var receivedValues []string
	target := &cobra.Command{
		Use: "target",
		RunE: func(cmd *cobra.Command, _ []string) error {
			receivedValues = append(
				receivedValues,
				fmt.Sprint(cmd.Context().Value(middlewareContextKey{})),
			)
			return nil
		},
	}
	adapter := cmdadapter.NewForwardCommand(
		"adapter",
		"Adapter command",
		"target",
		func() *cobra.Command {
			return target
		},
	)
	root := &cobra.Command{
		Use: "root",
		PersistentPreRun: func(cmd *cobra.Command, _ []string) {
			cmd.SetContext(context.WithValue(
				cmd.Context(),
				middlewareContextKey{},
				cmd.Context().Value(testContextKey{}),
			))
		},
	}
	root.AddCommand(adapter)
	root.SetArgs([]string{"adapter"})

	err := root.ExecuteContext(context.WithValue(t.Context(), testContextKey{}, "first"))

	c.Assert(err, qt.IsNil)
	root.SetArgs([]string{"adapter"})
	err = root.ExecuteContext(context.WithValue(t.Context(), testContextKey{}, "second"))

	c.Assert(err, qt.IsNil)
	c.Assert(receivedValues, qt.DeepEquals, []string{"first", "second"})
	c.Assert(target.Context(), qt.IsNil)
}

func TestForwardCommandPassesProjectConfigSnapshotWithoutReloading(t *testing.T) {
	c := qt.New(t)
	snapshot := projectconfig.Config{
		Migration: projectconfig.MigrationConfig{
			PreDownHook: testutils.FailingHookCommand("snapshot-hook", 8),
		},
	}
	var loaded projectconfig.Config
	target := &cobra.Command{
		Use: "target",
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			loaded, err = dbcli.LoadProjectConfig(
				cmd,
				filepath.Join(t.TempDir(), "missing.yaml"),
			)
			return err
		},
	}
	mapper := func(
		cmd *cobra.Command,
		args []string,
		_ *cmdadapter.CleanupScope,
	) ([]string, context.Context, error) {
		ctx := dbcli.WithProjectConfig(cmd.Root().Context(), snapshot)
		return args, ctx, nil
	}
	cmd := cmdadapter.NewForwardCommandWithArgsMapper(
		"atlas",
		"Atlas adapter command",
		"target",
		func() *cobra.Command {
			return target
		},
		mapper,
	)
	cmd.SetArgs([]string{})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(loaded.Migration.PreDownHook, qt.Equals, testutils.FailingHookCommand("snapshot-hook", 8))
	snapshot.Migration.PreDownHook = testutils.FailingHookCommand("second-snapshot-hook", 9)

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(loaded.Migration.PreDownHook, qt.Equals, testutils.FailingHookCommand("second-snapshot-hook", 9))
	c.Assert(target.Context(), qt.IsNil)
}

func TestForwardCommandRunsMapperCleanupAfterEachSuccessfulExecution(t *testing.T) {
	c := qt.New(t)
	var events []string
	mapper := func(
		cmd *cobra.Command,
		args []string,
		cleanup *cmdadapter.CleanupScope,
	) ([]string, context.Context, error) {
		events = append(events, "mapper")
		cleanup.Add(func() error {
			events = append(events, "cleanup")
			return nil
		})
		return args, cmd.Context(), nil
	}
	target := &cobra.Command{
		Use: "target",
		RunE: func(_ *cobra.Command, _ []string) error {
			events = append(events, "target")
			return nil
		},
	}
	cmd := cmdadapter.NewForwardCommandWithArgsMapper(
		"atlas",
		"Atlas adapter command",
		"target",
		func() *cobra.Command {
			return target
		},
		mapper,
	)

	err := cmd.Execute()
	c.Assert(err, qt.IsNil)
	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(events, qt.DeepEquals, []string{
		"mapper", "target", "cleanup",
		"mapper", "target", "cleanup",
	})
}

func TestForwardCommandRunsMapperCleanupAfterMapperFailure(t *testing.T) {
	c := qt.New(t)
	cleanupCalls := 0
	mapper := func(
		cmd *cobra.Command,
		args []string,
		cleanup *cmdadapter.CleanupScope,
	) ([]string, context.Context, error) {
		cleanup.Add(func() error {
			cleanupCalls++
			return nil
		})
		return args, cmd.Context(), errMapperFailed
	}
	cmd := cmdadapter.NewForwardCommandWithArgsMapper(
		"atlas",
		"Atlas adapter command",
		"target",
		func() *cobra.Command {
			return &cobra.Command{Use: "target"}
		},
		mapper,
	)

	err := cmd.Execute()

	c.Assert(err, qt.ErrorIs, errMapperFailed)
	c.Assert(cleanupCalls, qt.Equals, 1)
}

func TestForwardCommandJoinsTargetAndCleanupFailures(t *testing.T) {
	c := qt.New(t)
	cleanupCalls := 0
	mapper := func(
		cmd *cobra.Command,
		args []string,
		cleanup *cmdadapter.CleanupScope,
	) ([]string, context.Context, error) {
		cleanup.Add(func() error {
			cleanupCalls++
			return errCleanupFailed
		})
		return args, cmd.Context(), nil
	}
	cmd := cmdadapter.NewForwardCommandWithArgsMapper(
		"atlas",
		"Atlas adapter command",
		"target",
		func() *cobra.Command {
			return &cobra.Command{
				Use: "target",
				RunE: func(_ *cobra.Command, _ []string) error {
					return errTargetFailed
				},
			}
		},
		mapper,
	)

	err := cmd.Execute()

	c.Assert(err, qt.ErrorIs, errTargetFailed)
	c.Assert(err, qt.ErrorIs, errCleanupFailed)
	c.Assert(cleanupCalls, qt.Equals, 1)
}

func TestCleanupScopeClosesInReverseOrderAndOnlyOnce(t *testing.T) {
	c := qt.New(t)
	var order []string
	cleanup := &cmdadapter.CleanupScope{}
	cleanup.Add(func() error {
		order = append(order, "first")
		return nil
	})
	cleanup.Add(func() error {
		order = append(order, "second")
		return nil
	})

	err := cleanup.Close()
	c.Assert(err, qt.IsNil)
	err = cleanup.Close()

	c.Assert(err, qt.IsNil)
	c.Assert(order, qt.DeepEquals, []string{"second", "first"})
}

func TestForwardCommandResetsStringArrayEmptyDefault(t *testing.T) {
	c := qt.New(t)

	target := newStringArrayTargetCommand(nil, nil)
	cmd := cmdadapter.NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
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
	cmd := cmdadapter.NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
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

func TestForwardCommandRepeatedStringSliceRunsReplaceDefault(t *testing.T) {
	c := qt.New(t)

	var runs [][]string
	target := newStringSliceTargetCommand([]string{"prod"}, func(values []string) {
		runs = append(runs, values)
	})
	cmd := cmdadapter.NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
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
	cmd := cmdadapter.NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
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
	cmd := cmdadapter.NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
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

func TestForwardCommandMalformedEnvironmentFailsBeforeTargetRun(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DRY_RUN", "notabool")

	var runs int
	target := &cobra.Command{
		Use: "target",
		Run: func(_ *cobra.Command, _ []string) {
			runs++
		},
	}
	target.Flags().Bool("dry-run", false, "Preview changes")
	cmd := cmdadapter.NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
		return target
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "notabool" for PTAH_DRY_RUN`)
	c.Assert(runs, qt.Equals, 0)
}

func TestForwardCommandExplicitCLIWinsOverMalformedEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DRY_RUN", "notabool")

	var dryRun bool
	var seenDryRun bool
	var runs int
	target := &cobra.Command{
		Use: "target",
		Run: func(_ *cobra.Command, _ []string) {
			seenDryRun = dryRun
			runs++
		},
	}
	target.Flags().BoolVar(&dryRun, "dry-run", false, "Preview changes")
	cmd := cmdadapter.NewForwardCommandWithTargetHelp("atlas", "Atlas adapter command", "target", func() *cobra.Command {
		return target
	})
	cmd.SetArgs([]string{"--dry-run"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(seenDryRun, qt.IsTrue)
	c.Assert(runs, qt.Equals, 1)
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
