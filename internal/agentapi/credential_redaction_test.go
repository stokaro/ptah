package agentapi_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/agentapi"
	"ptah.run/internal/agentpolicy"
	"ptah.run/internal/agenttarget"
)

// agentProbePassword is the secret the URLs below carry. It is a word that
// appears nowhere else in this tree, so a match is the password rather than a
// coincidence.
const agentProbePassword = "hunter2correcthorse"

// TestReadDatabase_ErrorDoesNotCarryThePassword holds a property ADR 0003
// measured and recorded as a test rather than as a note.
//
// A tool result is read by a model, and the model is untrusted and its context
// is shared. A connection error that echoed the operator's password would put
// that password into the conversation, where every later turn can see it.
//
// The property is real today and it is not Ptah's doing: the driver builds the
// message and omits the password. That is exactly why it is a test. A driver
// change is how it would be lost, and it would be lost quietly -- the same shape
// as the driver defect stokaro/ptah#1875 found, which passed every build, vet
// and unit test in the tree (stokaro/ptah#1484).
func TestReadDatabase_ErrorDoesNotCarryThePassword(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{
			// Port 1 is refused rather than filtered, so the error arrives
			// immediately and is a connection error rather than a timeout.
			name: "postgres",
			url:  "postgres://ptah_user:" + agentProbePassword + "@127.0.0.1:1/nope?sslmode=disable",
		},
		{
			name: "mysql",
			url:  "mysql://ptah_user:" + agentProbePassword + "@tcp(127.0.0.1:1)/nope",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			// The URL is the operator's, so the target is where it goes. The
			// class is ephemeral so the read is allowed and actually attempts
			// the connection: a refusal would make the assertion below vacuous.
			session := sessionOptions{
				targets: []agenttarget.Config{
					{Name: "probe", URL: test.url, Class: agentpolicy.ClassEphemeral},
				},
			}.build(c)

			_, err := session.ReadDatabase(c.Context(), agentapi.ReadDatabaseRequest{})

			// The call must fail, or the assertion below is about nothing.
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Not(qt.Contains), agentProbePassword)
		})
	}
}
