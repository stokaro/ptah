package provider_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func contextDescription() *protocol.Description {
	d := description()
	d.Features = append(d.Features, provider.DatabaseContextFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.DatabaseContextFeature)
	return d
}

func TestDatabaseContextRequiresNegotiationAndRedactedRefusals(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d := contextDescription()
	request := &protocol.DatabaseContextRequest{Engine: "fixture", Address: "opaque://" + url.UserPassword("name", "never-report-this").String() + "@host/root"}
	c.Assert(provider.ValidateResolvedDatabaseContext(d, request, databaseContext()), qt.IsNil)
	unnegotiated := provider.NegotiatedDescription(d, nil)
	err := provider.ValidateDatabaseContextRequest(unnegotiated, request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err.Error(), qt.Not(qt.Contains), "never-report-this")
	request.Engine = "foreign"
	c.Assert(provider.ValidateDatabaseContextRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
	for _, value := range []*protocol.DatabaseContext{nil, {}, {Database: "/db"}, {Database: "/db", SourceDatabase: "/db\nother"}, {Database: " /db", SourceDatabase: "/db"}, {Database: "/db", SourceDatabase: "/db\u2028other"}} {
		c.Assert(provider.ValidateDatabaseContext(value), qt.ErrorIs, provider.ErrProtocol)
	}
}

func TestSessionContextMatchesPureResolutionExactly(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d := contextDescription()
	expected := databaseContext()
	session := &protocol.Session{Id: "session", Engine: "fixture", Database: expected.Database, Context: proto.CloneOf(expected), Identifiers: d.Engines[0].Identifiers}
	c.Assert(provider.ValidateSessionContext(d, "fixture", expected, session), qt.IsNil)
	for _, mutate := range []func(*protocol.Session){
		func(s *protocol.Session) { s.Database = "/other" },
		func(s *protocol.Session) { s.Context.Database = "/other" },
		func(s *protocol.Session) { s.Context.SourceDatabase = "/other" },
		func(s *protocol.Session) { s.Context = nil },
	} {
		changed := proto.CloneOf(session)
		mutate(changed)
		c.Assert(provider.ValidateSessionContext(d, "fixture", expected, changed), qt.ErrorIs, provider.ErrProtocol)
	}
	c.Assert(provider.ValidateSessionContext(d, "fixture", nil, session), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateSessionContext(provider.NegotiatedDescription(d, nil), "fixture", expected, session), qt.ErrorIs, provider.ErrProtocol)
}
