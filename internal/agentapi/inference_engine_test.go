package agentapi_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/agentapi"
	"ptah.run/internal/agentdiag"
	"ptah.run/internal/agentpolicy"
	"ptah.run/internal/agenttarget"
)

// An inference tool given a target of an engine inference does not run against
// refuses before it dials, in the words `ptah inference` uses for the same URL,
// rather than handing the URL to a driver to fail on. The target is allowed, so
// the refusal is not the policy's: it is measured at the socket, and the code
// tells the caller to name another target.
func TestInferenceStatus_FailurePath_AnotherEngineTarget(t *testing.T) {
	c := qt.New(t)
	postgresURL, dialed := countingDatabase(c)
	listener, err := url.Parse(postgresURL)
	c.Assert(err, qt.IsNil)
	session := sessionOptions{
		targets: []agenttarget.Config{{
			Name: "events", URL: "mysql://" + listener.Host + "/app", Class: agentpolicy.ClassEphemeral,
		}},
	}.build(c)

	response, err := session.InferenceStatus(context.Background(), agentapi.InferenceStatusRequest{RunID: "run-1"})

	c.Assert(err, qt.ErrorMatches,
		`target events: ptah inference works against PostgreSQL with pgvector, against YDB, and against a provider `+
			`engine with an embedding store, and "mysql://" `+
			`names another engine: .* nothing here to run against mysql`)
	code, coded := agentdiag.CodeOf(err)
	c.Assert(coded, qt.IsTrue)
	c.Assert(code, qt.Equals, agentdiag.CodeInvalidRequest)
	c.Assert(response, qt.IsNil)
	c.Assert(dialed.Load(), qt.Equals, int64(0))
}

// The controls: a PostgreSQL target and a YDB target on the same listener are
// both dialed, so the test above measures the refusal and not a listener
// nothing could reach, and YDB is an engine the tools run against rather than
// one they refuse.
func TestInferenceStatus_SupportedEngineTargetIsDialed(t *testing.T) {
	tests := []struct {
		name   string
		target func(postgresURL string) string
	}{
		{name: "postgresql", target: func(postgresURL string) string { return postgresURL }},
		{name: "ydb", target: func(postgresURL string) string {
			listener, _ := url.Parse(postgresURL)
			return "ydb://" + listener.Host + "/local"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			postgresURL, dialed := countingDatabase(c)
			session := sessionOptions{
				targets: []agenttarget.Config{{
					Name: "events", URL: test.target(postgresURL), Class: agentpolicy.ClassEphemeral,
				}},
			}.build(c)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			_, err := session.InferenceStatus(ctx, agentapi.InferenceStatusRequest{RunID: "run-1"})

			c.Assert(err, qt.ErrorMatches, `connect: .*`)
			c.Assert(dialed.Load() > 0, qt.IsTrue)
		})
	}
}
