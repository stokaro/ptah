package conformance

import (
	"time"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// Canary is the text an author puts where a credential goes in
// Config.CredentialAddress. No reply, reason or message of the provider may
// quote it.
const Canary = "ptah-conformance-canary-3f9c1a"

// Config says which provider the suite checks and what it may use to check
// it. Only Target and ProviderID are required; every other field adds
// checks.
type Config struct {
	// Target is the provider under test.
	Target Target
	// ProviderID is the identity the provider must describe, as a manifest
	// names it. A host refuses an executable that describes another one.
	ProviderID string
	// RequiredFeatures are the features the host requires of every provider.
	// Ptah requires schema.inspect, schema.render, units.execute and
	// sql.analyze.
	RequiredFeatures []string
	// Engine is the engine the credential and live checks use. Empty selects
	// the provider's only engine; a provider of several engines names one.
	Engine string
	// CredentialAddress is an address of Engine that carries Canary where a
	// credential goes, such as user:<Canary>@ in front of the host. The
	// suite resolves it offline when the provider resolves database
	// contexts, and opens it in the live part. Point it at a server that is
	// not there or refuses the login: what the suite checks is that nothing
	// the provider sends back quotes the canary.
	CredentialAddress string
	// Calls are requests to the provider's offline services.
	Calls []Call
	// Models are model identities an earlier release of the provider
	// described, as ModelIdentities returns them. Each must still read: as
	// the identity of a model the provider describes, or as a predecessor
	// one of its models names.
	Models []ModelIdentity
	// Live, when set, runs the checks that need a database.
	Live *Live
	// CallTimeout bounds each call the suite makes. Zero means two minutes.
	CallTimeout time.Duration
}

// Call is one request to an offline service. The suite finds the RPC from
// the request's type: a *protocol.RenderRequest goes to Render. It sends the
// request twice to one process and once to another under a host that lists
// every two-way feature, and once under a host that lists none.
type Call struct {
	// Name identifies the call in the report. It is letters, digits, dots,
	// dashes and underscores, and unique among the calls.
	Name string
	// Request is the request the suite sends. The suite does not change it.
	Request proto.Message
	// Refused says the provider must refuse the request. A call without it
	// must succeed under a host that lists every two-way feature.
	Refused bool
}

// Live describes a database the suite may change, and what the provider
// author gives the live checks to work with.
type Live struct {
	// Address is the connection address of the database, in the form Open
	// accepts. It may carry a credential: the suite never reports it, and
	// requires that no reply quotes it.
	Address string
	// Schemas scopes every read the suite makes. Empty reads the whole
	// database, which then must not change while the suite runs.
	Schemas []string
	// LockName is the name the lock checks take. Empty means
	// "ptah-conformance".
	LockName string
	// InProcessLocks makes the lock check contend for the lock from a
	// second session of the provider process that holds it, instead of a
	// session of a second process. It is for an engine whose database one
	// process holds at a time, such as an engine linked into the provider:
	// the engine keeps other processes out of the database, and the lock
	// service keeps out the other sessions of the process that holds it. The
	// check still requires the second session to find the held lock busy.
	// Where each connection of the suite is a process of its own, as with an
	// executable or a wire target, it also requires a second process to fail
	// to open the database while the first session holds it, so the mode
	// does not pass a provider whose lock another process could contend for.
	InProcessLocks bool
	// Changes are applied and reverted, one at a time.
	Changes []Change
	// Refused are units the database refuses, such as a statement naming a
	// table that is not there. The suite runs each alone and requires
	// NOT_APPLIED with nothing applied. With a change to apply, it also runs
	// the units of the first change's Apply followed by the refused unit,
	// and requires the confirmed prefix to be what the database holds
	// afterwards: PARTIAL with every unit of the change, or NOT_APPLIED with
	// none.
	Refused []*protocol.Unit
	// Calls are requests to session-bound services.
	Calls []LiveCall
}

// Change is a change the live part applies and then reverts. The database
// read after Revert must equal the read before Apply, and the read after
// Apply must differ from it.
type Change struct {
	// Name identifies the change in the report, under the rules of
	// Call.Name.
	Name   string
	Apply  Step
	Revert Step
}

// Step is what the suite runs for one half of a change: the units Render
// returns for Render, or Units as they are. A Render request without
// capabilities takes the opened session's, as a host renders with the facts
// the session established.
type Step struct {
	Render *protocol.RenderRequest
	Units  []*protocol.Unit
}

// LiveCall is one request to a session-bound service, built for the session
// the suite opened. The suite finds the RPC from the request's type.
type LiveCall struct {
	// Name identifies the call in the report, under the rules of Call.Name.
	Name string
	// Request builds the request for an opened session.
	Request func(session *protocol.Session) proto.Message
	// Refused says the provider must refuse the request.
	Refused bool
}
