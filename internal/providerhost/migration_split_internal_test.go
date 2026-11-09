package providerhost

// White-box testing required: which provider answers the host keeps, and the
// translation of the abort flags, sit below the session; transport and the
// provider's own answers are covered by the providercli process tests.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationsplit"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// The split is offered only when both sides listed the feature and the engine
// declares it.
func TestMigrationSplitsRequireTheNegotiatedFeature(t *testing.T) {
	for _, row := range []struct {
		name           string
		global, engine []string
		want           bool
	}{
		{"absent", nil, nil, false},
		{"provider only", []string{provider.MigrationSplitFeature}, nil, false},
		{"engine only", nil, []string{provider.MigrationSplitFeature}, false},
		{"negotiated", []string{provider.MigrationSplitFeature}, []string{provider.MigrationSplitFeature}, true},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			target := &target{host: &Host{description: &protocol.Description{Features: row.global}}, engine: &protocol.Engine{Features: row.engine}}
			c.Assert(target.splitsMigrations(), qt.Equals, row.want)
		})
	}
}

// An answer is kept by engine, the body's split apart from each assertion's
// verdict, so a later question about the same body, or the same body with
// fewer assertions, is answered without asking again.
func TestMigrationSplitsKeepTheProvidersAnswers(t *testing.T) {
	c := qt.New(t)
	var splits migrationSplits
	answer := migrationsplit.Split{
		Units:    []migrationsplit.Unit{{SQL: "CREATE TABLE t (id INTEGER)", Source: "CREATE TABLE t (id INTEGER);", Placement: migrationsplit.Outside}},
		Verdicts: []migrationsplit.Verdict{{ReadOnly: true}, {Reason: "it writes"}},
	}
	splits.store("fixture", "body", []string{"SELECT 1", "DELETE FROM t"}, answer)

	for _, row := range []struct {
		name       string
		engine     string
		body       string
		assertions []string
		want       migrationsplit.Split
		found      bool
	}{
		{name: "the same question", engine: "fixture", body: "body", assertions: []string{"SELECT 1", "DELETE FROM t"}, want: answer, found: true},
		{name: "the body alone", engine: "fixture", body: "body", want: migrationsplit.Split{Units: answer.Units}, found: true},
		{name: "one assertion", engine: "fixture", body: "body", assertions: []string{"DELETE FROM t"},
			want: migrationsplit.Split{Units: answer.Units, Verdicts: answer.Verdicts[1:]}, found: true},
		{name: "an assertion never asked about", engine: "fixture", body: "body", assertions: []string{"SELECT 2"}},
		{name: "another body", engine: "fixture", body: "other"},
		{name: "another engine", engine: "other", body: "body"},
		{name: "an empty body", engine: "fixture", body: "", found: true},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			got, found := splits.lookup(row.engine, row.body, row.assertions)
			c.Assert(found, qt.Equals, row.found)
			c.Assert(got, qt.DeepEquals, row.want)
		})
	}
	c.Assert(splits.bodies, qt.HasLen, 1)
}

// A unit transaction carries the one unit its checkpoint names, and refuses
// a second before reaching its provider.
func TestUnitTransactionRunsOneUnit(t *testing.T) {
	c := qt.New(t)
	unit := &unitTransaction{executed: true}

	err := unit.ExecuteSQL(context.Background(), "INSERT INTO t VALUES (2)")

	c.Assert(err, qt.ErrorMatches, "a unit transaction runs one unit")
}

// An abort keeps the failure it marks, and marks no success.
func TestAbortedErrorMarksTheFailure(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("provider execution: NOT_APPLIED")

	err := abortedError(failure)

	c.Assert(err, qt.ErrorIs, migrationsplit.ErrTransactionAborted)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(abortedError(nil), qt.IsNil)
}
