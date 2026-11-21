package policyprobe_test

import (
	"bytes"
	"context"
	"database/sql"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgpolicy/policyprobe"
	"ptah.run/internal/probetx"
	"ptah.run/internal/rundiag"
)

// textOnlySession runs the probe body with no transaction. A probe that
// sent a statement would panic on the nil transaction.
type textOnlySession struct{}

func (textOnlySession) WithRolledBackTransaction(ctx context.Context, _ string, body func(context.Context, *sql.Tx) error) (bool, error) {
	return true, body(ctx, nil)
}

// restartingSession is [textOnlySession] in a transaction the server names 17
// when it begins and 18 every time after, as it names one a probe ended and
// began again.
type restartingSession struct{}

func (restartingSession) WithRolledBackTransaction(ctx context.Context, label string, body func(context.Context, *sql.Tx) error) (bool, error) {
	calls := 0
	watched, err := probetx.Watch(ctx, func(context.Context) (string, error) {
		calls++
		return strconv.Itoa(16 + min(calls, 2)), nil
	})
	if err != nil {
		return false, err
	}
	return textOnlySession{}.WithRolledBackTransaction(watched, label, body)
}

// escapingClause closes the parentheses the probe writes it in and commits a
// table of its own.
const escapingClause = "owner_name = current_user)) ; COMMIT ; CREATE TABLE public.made_by_plan (x int) ; BEGIN ; SELECT ((1"

// escapingRequest declares policy p on public.docs, against a database
// holding the policy.
func escapingRequest(session schemaext.ProbeSession, declared *pgpolicy.DesiredPolicy) schemaext.NormalizationRequest {
	ref := pgpolicy.PolicyRef("public", "docs", "p")
	return schemaext.NormalizationRequest{
		Target:  "postgres",
		Session: session,
		Desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: declared}))},
		Current: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: observedUsing("owner_name = CURRENT_USER")}))},
	}
}

// TestNormalizeObjects_DoesNotProbeAClauseThatLeavesItsParentheses leaves the
// policy to compare as written and says so, without a statement, for either
// clause, the other being harmless.
func TestNormalizeObjects_DoesNotProbeAClauseThatLeavesItsParentheses(t *testing.T) {
	for _, test := range []struct {
		name     string
		declared *pgpolicy.DesiredPolicy
	}{
		{name: "USING", declared: &pgpolicy.DesiredPolicy{Using: new(escapingClause), WithCheck: new("owner_name = current_user")}},
		{name: "WITH CHECK", declared: &pgpolicy.DesiredPolicy{Using: new("owner_name = current_user"), WithCheck: new(escapingClause)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var stream bytes.Buffer
			ctx := rundiag.With(context.Background(), rundiag.New(rundiag.Options{Stream: &stream, Listing: rundiag.ListedAtDebugLevel}))

			result, err := policyprobe.Service{}.NormalizeObjects(ctx, escapingRequest(textOnlySession{}, test.declared))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(stream.String(), qt.Equals, "warning: policy public.docs.p is compared as written, not in the form the server stores: "+
				"the probe that spells it does not run, because its text closes a parenthesis it does not open\n")
		})
	}
}

// TestNormalizeObjects_StopsWhenAProbeEndsTheTransaction fails the
// normalization, naming the policy, when the transaction is not the one the
// probe began in.
func TestNormalizeObjects_StopsWhenAProbeEndsTheTransaction(t *testing.T) {
	c := qt.New(t)

	result, err := policyprobe.Service{}.NormalizeObjects(context.Background(), escapingRequest(restartingSession{}, &pgpolicy.DesiredPolicy{Using: new(escapingClause)}))

	c.Assert(err, qt.ErrorIs, probetx.ErrTransactionChanged)
	c.Assert(err, qt.ErrorMatches, `resolve row-security policies: public\.docs\.p: a probe ended the transaction .*`)
	c.Assert(result, qt.DeepEquals, schemaext.NormalizationResult{})
}
