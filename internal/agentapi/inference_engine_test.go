package agentapi_test

import (
	"context"
	"net/url"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/agentapi"
	"ptah.run/internal/agentdiag"
	"ptah.run/internal/agentpolicy"
	"ptah.run/internal/agenttarget"
	"ptah.run/internal/ydbgap"
)

// An inference tool given a YDB target refuses before it dials, in the words
// `ptah inference` uses for the same URL, rather than handing the URL to the
// PostgreSQL driver to fail on. The target is allowed, so the refusal is not
// the policy's: it is measured at the socket, and the code tells the caller to
// name another target.
func TestInferenceStatus_FailurePath_YDBTarget(t *testing.T) {
	c := qt.New(t)
	postgresURL, dialed := countingDatabase(c)
	listener, err := url.Parse(postgresURL)
	c.Assert(err, qt.IsNil)
	session := sessionOptions{
		targets: []agenttarget.Config{{
			Name: "events", URL: "ydb://" + listener.Host + "/local", Class: agentpolicy.ClassEphemeral,
		}},
	}.build(c)

	response, err := session.InferenceStatus(context.Background(), agentapi.InferenceStatusRequest{RunID: "run-1"})

	c.Assert(err, qt.ErrorMatches,
		`target events: "ydb://" names a YDB database: `+regexp.QuoteMeta(ydbgap.Inference.Message()))
	code, coded := agentdiag.CodeOf(err)
	c.Assert(coded, qt.IsTrue)
	c.Assert(code, qt.Equals, agentdiag.CodeInvalidRequest)
	c.Assert(response, qt.IsNil)
	c.Assert(dialed.Load(), qt.Equals, int64(0))
}

// The control: a PostgreSQL target on the same listener is dialed, so the test
// above measures the refusal and not a listener nothing could reach.
func TestInferenceStatus_PostgreSQLTargetIsDialed(t *testing.T) {
	c := qt.New(t)
	postgresURL, dialed := countingDatabase(c)
	session := sessionOptions{
		targets: []agenttarget.Config{{Name: "events", URL: postgresURL, Class: agentpolicy.ClassEphemeral}},
	}.build(c)

	_, err := session.InferenceStatus(context.Background(), agentapi.InferenceStatusRequest{RunID: "run-1"})

	c.Assert(err, qt.Not(qt.ErrorMatches), `.*YDB.*`)
	c.Assert(dialed.Load() > 0, qt.IsTrue)
}
