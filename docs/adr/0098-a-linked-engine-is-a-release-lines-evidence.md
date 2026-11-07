# ADR 0098: A linked engine is a release line's evidence

Status: Proposed

Contract: 25.30

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

## Context

An engine declares its release lines and the testing behind each one. A
`CERTIFIED` or `LEGACY_TESTED` line names the container image that runs it,
and the SDK refuses such a line without one, so a provider cannot claim
testing it does not name the means of.

An engine that runs inside the provider's process has no image. The DuckDB
provider links DuckDB's library into its executable, and its tests run that
library on every build. Under the rule it can declare its lines at most
`BEST_EFFORT`, which says nothing runs against them, the opposite of what
holds.

The host keeps a rule for its own release lines: a line may claim certified
or legacy-tested only if something actually runs against it
(`TestCells_CertificationMatchesWhatContinuousIntegrationRuns`). The image is
the provider's half of that rule: it names what runs.

## Decision

### The linked version is evidence in place of an image

`ReleaseLine.linked` names the version of the engine's library the provider
executable links. Under the two-way feature `engine.linked-release-lines` a
`CERTIFIED` or `LEGACY_TESTED` line states it in place of `image`. The
provider and the engine list the feature.

The version is the line's: it equals the line or starts with the line and a
dot, so line `1.4` names `1.4.2`. One line of an engine at most names a linked
version, since an executable links one version of a library, and a line
names an image or a linked version, never both. The provider's tests run the
version it links, so a certified linked line is something the provider's CI
runs, which is the host's rule restated for an engine without a server.

A host that does not list the feature receives the line without `linked`. A
line that claimed testing by it alone reaches that host as `BEST_EFFORT`, with
a reason that names the linked version: that host can see no evidence for the
claim.

### What the SDK enforces

`ValidateDescription` refuses a line that names both an image and a linked
version, a linked version that is not of the line or holds white space, two
linked lines of one engine, the feature without a linked line, a linked line
without the feature on the engine and the provider, and a linked version on a
`BEST_EFFORT` line without a reason or on a `KNOWN_INCOMPATIBLE` line.

## Alternatives considered

**Accept any text in `image`.** A library version written as an image would
pass the check and mislead every reader of `image`, which the host reports as
a container to run.

**Allow certified lines without evidence for embedded engines.** The level
would then claim testing with nothing named, which is what the image rule
exists to stop.

**One linked line per build of several libraries.** A provider that ships one
executable per library version declares the line of each executable in that
executable's description; a description that named several linked versions
would describe executables it is not.

## Consequences

- An embedded engine's provider can certify the line it links and keep its
  other lines as best effort or incompatible.
- The linked version is the provider's statement; the SDK checks its shape and
  cannot see the library the executable links.
- A host of an earlier contract shows such a line as best effort.

## Acceptance evidence

- SDK unit tests: `TestLinkedReleaseLines_HappyPath` and
  `TestLinkedReleaseLines_FailurePath` in `provider-sdk/provider`.
- The conformance suite's memory provider certifies its line by the version
  it links, and `TestCheck_WireFailurePath` reports `description.valid` for a
  line that names an image beside it.
