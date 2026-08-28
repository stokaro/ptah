// Package analysisboundary records which processes the analysis trees may
// start, and holds them to it.
//
// #1270 asked that "external-process execution follows Ptah security
// requirements". Measured when that was written: there is no SECURITY.md in
// this repository and AGENTS.md has no such section, so there are no
// requirements to follow -- and no analysis path runs an external process
// either. The criterion was unmet in the peculiar sense that both halves were
// absent (stokaro/ptah#2395).
//
// Writing the policy is an owner decision about what Ptah promises. Pinning the
// current state is not, and it is what makes the decision reachable: while
// nothing starts a process, no policy is needed, and the moment something does,
// this gate fails and says a policy is now owed. Without it, the first external
// analyzer would arrive with no policy and nothing would notice.
package analysisboundary
