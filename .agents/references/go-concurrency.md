# Go concurrency and resource lifetimes

Load this reference when changing goroutines, cancellation, shared state,
sessions, locks, or subprocess cleanup. Start with the actual caller and the
existing lifecycle. Do not add parallelism to an unrelated fix.

## Make ownership and termination explicit

- For every started goroutine, identify its owner, termination condition, and
  completion signal. Cancellation requests a stop; it does not wait for one.
  Do not return resources to a pool or mutate retained inputs until their users
  finish. A channel's sender or designated owner closes it.
- Bound active work and queued work according to the resource being protected.
  A semaphore inside every newly spawned goroutine can bound active database
  calls while leaving the goroutine backlog unbounded. Decide which bound the
  caller needs rather than adding concurrency by habit.
- Make waits, channel operations, and remote calls responsive to cancellation
  where their contract requires it. Ensure that early errors do not leave a
  producer blocked after its consumer exits. Inspect all exit paths before
  adding another timeout.
- Preserve ownership of locks, rows, files, processes, and provider sessions
  across success, failure, and abandonment. Close resources only after work
  using them has finished. Reuse existing lifecycle helpers.
- Give cleanup after cancellation a finite, independent deadline when it must
  still run. `internal/providerhost/session.go` closes a remotely opened session
  when local validation fails. Keep the initiating error observable if cleanup
  also fails; detaching normal work would hide cancellation instead.
- Protect each shared invariant with one coherent synchronization policy.
  Atomic access to individual fields does not make a transition involving
  several fields atomic. Do not copy values containing synchronization objects
  after use, or return an unprotected alias to mutable state.

These checks apply the Go project's guidance on
[goroutine lifetimes and copying](https://go.dev/wiki/CodeReviewComments)
to Ptah's resource boundaries.

## Exercise the affected lifecycle

Use explicit synchronization to reach the state under test. Avoid arbitrary
sleeps as proof that a goroutine started, finished, or released a lock. Select
the relevant canceled-before-start, in-flight failure, abandoned consumer, or
cleanup-failure cases. Preserve Quicktest and the repository's declarative test
style; do not introduce assertion branches for these cases.

Run a targeted race check for changed shared-memory behavior on a supported
platform. The [Go race detector](https://go.dev/doc/articles/race_detector)
detects races on executed paths; a passing run does not prove absence of leaks,
deadlocks, or database races. Test externally visible completion and cleanup
separately. Follow [validation policy](validation.md) for required CI coverage
and elapsed-time limits.
