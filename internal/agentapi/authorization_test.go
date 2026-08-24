package agentapi_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/agentaudit"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agenttarget"
)

// probeUser is the userinfo the URLs below carry. It is assembled rather than
// written inline so that no literal in this file looks like a credential.
const probeUser = "u" + ":" + "p"

// countingDatabase listens on a local port and counts connections, returning a
// URL that points at it.
//
// A refusal has to be measured at the socket. Asserting only on the error text
// would pass for an implementation that dialed the database, failed, and
// reported a refusal afterwards -- which is the defect this whole boundary
// exists to close.
func countingDatabase(c *qt.C) (string, *atomic.Int64) {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = listener.Close() })

	dialed := &atomic.Int64{}
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			dialed.Add(1)
			_ = conn.Close()
		}
	}()
	return "postgres://" + probeUser + "@" + listener.Addr().String() + "/app?sslmode=disable", dialed
}

// answering is an approver that gives one fixed answer and records what it was
// shown.
type answering struct {
	grant    agentpolicy.Grant
	requests []agentpolicy.Request
	subjects []agentpolicy.Subject
}

func (a *answering) Approve(
	_ context.Context,
	req agentpolicy.Request,
	subject agentpolicy.Subject,
) (agentpolicy.Grant, error) {
	a.requests = append(a.requests, req)
	a.subjects = append(a.subjects, subject)
	return a.grant, nil
}

// sessionOptions is how a test says what a session is configured with.
type sessionOptions struct {
	rules    []agentpolicy.Rule
	roots    []string
	targets  []agenttarget.Config
	approver agentpolicy.Approver
	audit    agentaudit.Recorder
}

// build assembles a session from the options.
func (o sessionOptions) build(c *qt.C) *agentapi.Session {
	c.Helper()
	layers := make([]agentpolicy.LayerRules, 0, 1)
	if len(o.rules) > 0 {
		layers = append(layers, agentpolicy.LayerRules{
			Layer: agentpolicy.LayerInvocation, Source: "test", Rules: o.rules,
		})
	}
	policy, err := agentpolicy.Assemble(layers...)
	c.Assert(err, qt.IsNil)

	brokerOptions := make([]agentpolicy.BrokerOption, 0, 1)
	if o.approver != nil {
		brokerOptions = append(brokerOptions, agentpolicy.WithApprover(o.approver))
	}

	targets := make([]*agenttarget.Target, 0, len(o.targets))
	for _, cfg := range o.targets {
		target, targetErr := agenttarget.New(cfg)
		c.Assert(targetErr, qt.IsNil)
		targets = append(targets, target)
	}
	set, err := agenttarget.NewSet(targets...)
	c.Assert(err, qt.IsNil)

	session, err := agentapi.NewSession(agentapi.SessionConfig{
		Broker:      agentpolicy.NewBroker(policy, brokerOptions...),
		Targets:     set,
		SourceRoots: o.roots,
		Audit:       o.audit,
	})
	c.Assert(err, qt.IsNil)
	return session
}

// openSession is a session with the builtin policy that may read the roots.
func openSession(c *qt.C, roots ...string) *agentapi.Session {
	c.Helper()
	return sessionOptions{roots: roots}.build(c)
}

func TestReadDatabase_DeniedNeverReachesTheDatabase(t *testing.T) {
	// The property the capability table was claiming and the code was not
	// keeping.
	c := qt.New(t)
	databaseURL, dialed := countingDatabase(c)
	session := sessionOptions{
		targets: []agenttarget.Config{{Name: "app", URL: databaseURL}},
	}.build(c)

	_, err := session.ReadDatabase(context.Background(), agentapi.ReadDatabaseRequest{})

	c.Assert(err, qt.ErrorAs, new(*agentpolicy.DeniedError))
	c.Assert(dialed.Load(), qt.Equals, int64(0),
		qt.Commentf("a denied capability must not open the connection"))
}

func TestReadDatabase_AllowedReachesTheDatabase(t *testing.T) {
	// The other half: a grant has to actually grant, or the capability is
	// decorative in the opposite direction.
	c := qt.New(t)
	databaseURL, dialed := countingDatabase(c)
	session := sessionOptions{
		targets: []agenttarget.Config{{Name: "app", URL: databaseURL, Class: agentpolicy.ClassEphemeral}},
	}.build(c)

	_, err := session.ReadDatabase(context.Background(), agentapi.ReadDatabaseRequest{})

	// The listener is not a database, so the read fails -- after connecting,
	// which is what this measures.
	c.Assert(err, qt.Not(qt.ErrorAs), new(*agentpolicy.DeniedError))
	c.Assert(dialed.Load() > 0, qt.IsTrue,
		qt.Commentf("an allowed capability must reach the database"))
}

func TestReadDatabase_ClassComesFromTheOperatorNotTheName(t *testing.T) {
	// A name is a label. Reading trust out of it would let whoever chose the
	// label choose the verdict, and the builtin table grants an ephemeral
	// database outright.
	tests := []struct {
		name   string
		target string
	}{
		{name: "named like a throwaway", target: "ephemeral"},
		{name: "named like a dev box", target: "dev"},
		{name: "named like production", target: "production"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			databaseURL, dialed := countingDatabase(c)
			session := sessionOptions{
				targets: []agenttarget.Config{{Name: test.target, URL: databaseURL}},
			}.build(c)

			_, err := session.ReadDatabase(context.Background(), agentapi.ReadDatabaseRequest{})

			c.Assert(err, qt.ErrorAs, new(*agentpolicy.DeniedError))
			c.Assert(dialed.Load(), qt.Equals, int64(0))
		})
	}
}

func TestReadDatabase_EveryClassGetsItsBuiltinVerdict(t *testing.T) {
	// The builtin table, exercised rather than restated: what an operator gets
	// when they classify a database and say nothing else.
	tests := []struct {
		name    string
		class   agentpolicy.DatabaseClass
		reaches bool
	}{
		{name: "ephemeral is allowed", class: agentpolicy.ClassEphemeral, reaches: true},
		{name: "dev is asked about", class: agentpolicy.ClassDev, reaches: false},
		{name: "a target is asked about", class: agentpolicy.ClassTarget, reaches: false},
		{name: "production is denied", class: agentpolicy.ClassProduction, reaches: false},
		{name: "unclassified is denied", class: agentpolicy.ClassUnclassified, reaches: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			databaseURL, dialed := countingDatabase(c)
			session := sessionOptions{
				targets: []agenttarget.Config{{Name: "app", URL: databaseURL, Class: test.class}},
			}.build(c)

			_, _ = session.ReadDatabase(context.Background(), agentapi.ReadDatabaseRequest{})

			c.Assert(dialed.Load() > 0, qt.Equals, test.reaches)
		})
	}
}

func TestReadDatabase_AskWithoutAnApproverFailsClosed(t *testing.T) {
	// A dev database is asked about. With nobody to ask, that is a refusal and
	// never a promotion.
	c := qt.New(t)
	databaseURL, dialed := countingDatabase(c)
	session := sessionOptions{
		targets: []agenttarget.Config{{Name: "app", URL: databaseURL, Class: agentpolicy.ClassDev}},
	}.build(c)

	_, err := session.ReadDatabase(context.Background(), agentapi.ReadDatabaseRequest{})

	c.Assert(err, qt.ErrorIs, agentpolicy.ErrApprovalUnavailable)
	c.Assert(dialed.Load(), qt.Equals, int64(0))
}

func TestReadDatabase_AskReachesTheDatabaseOnlyAfterApproval(t *testing.T) {
	tests := []struct {
		name    string
		granted bool
		reaches bool
	}{
		{name: "approved", granted: true, reaches: true},
		{name: "refused", granted: false, reaches: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			databaseURL, dialed := countingDatabase(c)
			approver := &answering{grant: agentpolicy.Grant{Granted: test.granted}}
			session := sessionOptions{
				targets:  []agenttarget.Config{{Name: "app", URL: databaseURL, Class: agentpolicy.ClassDev}},
				approver: approver,
			}.build(c)

			_, _ = session.ReadDatabase(context.Background(), agentapi.ReadDatabaseRequest{})

			c.Assert(dialed.Load() > 0, qt.Equals, test.reaches)
			c.Assert(approver.requests, qt.HasLen, 1)
		})
	}
}

func TestReadDatabase_TheApprovalPromptCarriesNoCredential(t *testing.T) {
	// What a person is shown is Ptah's sentence about an operator-configured
	// database. The URL is the one thing it must never contain.
	c := qt.New(t)
	databaseURL, _ := countingDatabase(c)
	approver := &answering{grant: agentpolicy.Grant{Granted: false}}
	session := sessionOptions{
		targets:  []agenttarget.Config{{Name: "app", URL: databaseURL, Class: agentpolicy.ClassDev}},
		approver: approver,
	}.build(c)

	_, _ = session.ReadDatabase(context.Background(), agentapi.ReadDatabaseRequest{})

	c.Assert(approver.subjects, qt.HasLen, 1)
	c.Assert(approver.subjects[0].Summary, qt.Contains, "app")
	c.Assert(approver.subjects[0].Summary, qt.Contains, "dev")
	c.Assert(approver.subjects[0].Summary, qt.Not(qt.Contains), probeUser)
	c.Assert(approver.requests[0].TargetID, qt.Not(qt.Equals), "")
}

func TestReadDatabase_AnUnknownTargetIsRefusedBeforeConnecting(t *testing.T) {
	c := qt.New(t)
	databaseURL, dialed := countingDatabase(c)
	session := sessionOptions{
		targets: []agenttarget.Config{{Name: "app", URL: databaseURL, Class: agentpolicy.ClassEphemeral}},
	}.build(c)

	_, err := session.ReadDatabase(context.Background(),
		agentapi.ReadDatabaseRequest{Target: "somewhere-else"})

	c.Assert(err, qt.ErrorIs, agenttarget.ErrUnknown)
	c.Assert(dialed.Load(), qt.Equals, int64(0))
}
