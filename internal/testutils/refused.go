package testutils

// RefusedConnection is the fragment shared by every supported operating
// system's message for a dial nothing was listening for.
//
// The full sentences do not agree, and a test that pinned one of them pinned
// the platform it happened to run on:
//
//	unix     dial tcp 127.0.0.1:55982: connect: connection refused
//	windows  connectex: No connection could be made because the target machine
//	         actively refused it.
//
// "refused" is what they share, and it is what the assertion is about: the
// dial reached a decision rather than hanging or resolving somewhere else.
// Matching it also strengthens the negative direction, where a test asserts a
// run never tried to connect at all.
const RefusedConnection = "refused"
