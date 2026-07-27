package dbcli_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/migration/migrator"
)

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
