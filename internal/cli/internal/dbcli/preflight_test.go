package dbcli_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/migration/migrator"
)

func TestReportMigrationInputsNamesTheApprovedStatement(t *testing.T) {
	c := qt.New(t)
	var output strings.Builder
	dbcli.ReportMigrationInputs(func(format string, args ...any) { fmt.Fprintf(&output, format, args...) }, migrator.MigrationPlan{Inputs: []migrator.MigrationInput{
		{Version: 42, VersionKey: "20261026_rotation", StatementIndex: 3, Inputs: []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "APP_TOKEN"}}},
		{Version: 43, StatementIndex: 1, Inputs: []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "DB_PASSWORD"}}},
	}})
	c.Assert(output.String(), qt.Equals, "Execution input for migration 20261026_rotation statement 3: environment APP_TOKEN\nExecution input for migration 43 statement 1: environment DB_PASSWORD\n")
}

func TestCombineMigrationHooks_NoHooks(t *testing.T) {
	c := qt.New(t)

	hook := dbcli.CombineMigrationHooks(nil)

	c.Assert(hook, qt.IsNil)
}

func TestCombineMigrationHooks_RunsNonNilHooksInOrder(t *testing.T) {
	c := qt.New(t)
	var calls []string
	plan := migrator.MigrationPlan{Versions: []int64{1, 2}}
	first := func(context.Context, migrator.MigrationPlan) error {
		calls = append(calls, "first")
		return nil
	}
	second := func(context.Context, migrator.MigrationPlan) error {
		calls = append(calls, "second")
		return nil
	}

	hook := dbcli.CombineMigrationHooks(first, nil, second)
	err := hook(context.Background(), plan)

	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.DeepEquals, []string{"first", "second"})
}

func TestCombineMigrationHooks_StopsAtFirstError(t *testing.T) {
	c := qt.New(t)
	var calls []string
	wantErr := errors.New("stop")
	first := func(context.Context, migrator.MigrationPlan) error {
		calls = append(calls, "first")
		return wantErr
	}
	second := func(context.Context, migrator.MigrationPlan) error {
		calls = append(calls, "second")
		return nil
	}

	hook := dbcli.CombineMigrationHooks(first, second)
	err := hook(context.Background(), migrator.MigrationPlan{})

	c.Assert(err, qt.ErrorIs, wantErr)
	c.Assert(calls, qt.DeepEquals, []string{"first"})
}
