package atlasschema_test

import (
	"errors"
	"fmt"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/atlasschema"
)

// refusedAuditLogs is the refusal of the selector unmatchedAuditLogs returns.
const refusedAuditLogs = `the exclude selection from ptah.yaml matched no objects: "audit_logs"; ` +
	`migrations down --plan refuses a selection that protects nothing, ` +
	`set PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1 to keep the permissive behavior`

// errCleanup stands for a failure the planning reports beside the refusal.
var errCleanup = errors.New("the dev database could not be emptied")

// unmatchedAuditLogs is the error a comparison returns for one selector from
// ptah.yaml that matched nothing.
func unmatchedAuditLogs() *atlasfilter.UnmatchedExcludeError {
	return &atlasfilter.UnmatchedExcludeError{
		Selectors: []string{"audit_logs"},
		Sources:   atlasfilter.ExcludeSources{"audit_logs": "ptah.yaml"},
	}
}

// TestUnmatchedExcludeRefusal_HappyPath leaves an error that carries no
// unmatched selector as it is.
func TestUnmatchedExcludeRefusal_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "no error"},
		{name: "another failure", err: fmt.Errorf("replay migrations: %w", errCleanup)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasschema.UnmatchedExcludeRefusal("migrations down --plan", test.err)

			c.Assert(got, qt.Equals, test.err)
		})
	}
}

// TestUnmatchedExcludeRefusal_FailurePath words the refusal whether the
// comparison returned it as it is or wrapped it in the planner's context.
func TestUnmatchedExcludeRefusal_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "the refusal alone", err: unmatchedAuditLogs()},
		{name: "a wrapped refusal", err: fmt.Errorf("dynamic rollback planning failed: compare the schemas: %w", unmatchedAuditLogs())},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasschema.UnmatchedExcludeRefusal("migrations down --plan", test.err)

			c.Assert(got, qt.ErrorMatches, regexp.QuoteMeta(refusedAuditLogs))
			c.Assert(got, qt.ErrorIs, atlasfilter.ErrUnmatchedExclude)
		})
	}
}

// errReplay stands for a second failure the planning reports beside the
// refusal.
var errReplay = errors.New("the replay left a scratch table")

// TestUnmatchedExcludeRefusalKeepsAFailedCleanup_FailurePath words a refusal
// the planner returned beside other failures, however the errors were put
// together: a join, a wrapper with two %w verbs, and a join inside a join.
// The refusal comes first, each other failure follows on its own line, and
// every one matches errors.Is. A wrapper whose chain leads to the refusal
// only frames it, so its text is not repeated.
func TestUnmatchedExcludeRefusalKeepsAFailedCleanup_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		planned error
		wantErr string
	}{
		{
			name: "a join",
			planned: errors.Join(
				fmt.Errorf("dynamic rollback planning failed: compare the schemas: %w", unmatchedAuditLogs()),
				fmt.Errorf("dynamic rollback planning failed: %w", errCleanup),
			),
			wantErr: refusedAuditLogs + "\n" + "dynamic rollback planning failed: the dev database could not be emptied",
		},
		{
			name: "a wrapper with two %w verbs",
			planned: fmt.Errorf("%w: %w", unmatchedAuditLogs(),
				fmt.Errorf("dynamic rollback planning failed: %w", errCleanup)),
			wantErr: refusedAuditLogs + "\n" + "dynamic rollback planning failed: the dev database could not be emptied",
		},
		{
			name: "a join inside a join",
			planned: errors.Join(
				errors.Join(fmt.Errorf("compare the schemas: %w", unmatchedAuditLogs()), errReplay),
				errCleanup,
			),
			wantErr: refusedAuditLogs + "\n" + "the replay left a scratch table" + "\n" + "the dev database could not be emptied",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasschema.UnmatchedExcludeRefusal("migrations down --plan", test.planned)

			c.Assert(got, qt.ErrorIs, atlasfilter.ErrUnmatchedExclude)
			c.Assert(got, qt.ErrorIs, errCleanup)
			c.Assert(got, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
		})
	}
}
