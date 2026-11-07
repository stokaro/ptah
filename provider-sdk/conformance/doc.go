// Package conformance checks a provider against the contract a Ptah host
// relies on. A provider author runs it against the provider's executable,
// started through the same bootstrap a host uses, or against the provider's
// runtime.Server served in the test process.
//
// The suite acts as a strict host. It describes the provider under several
// host profiles and checks the handshake, the description and its
// negotiation; it probes every RPC of the protocol and requires an answer
// from each service the description offers and no success from any other;
// it sends the requests the author supplies to the offline services and
// checks each reply with the validator the SDK's client runs, under every
// profile and twice, so that a reply that depends on more than its request is
// reported; and it reads every refusal the provider sends, so that a reason
// reaches only a host that negotiated errors.reasons and never quotes an
// address or a credential. Given a database, the live part opens and closes
// sessions, reads the database, takes and releases locks, begins and finishes
// transactions, and applies and reverts the changes the author supplies.
//
// The suite knows no engine. What it needs from one comes from the author:
// requests to send, changes to apply, the address of a database it may
// change. Check returns a machine-readable Report; Run reports each check as
// a subtest. The command ptah-provider-conformance in this module runs the
// suite against an executable and prints the report.
package conformance
