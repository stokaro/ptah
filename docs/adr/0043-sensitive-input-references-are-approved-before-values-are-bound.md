# ADR 0043: Sensitive input references are approved before values are bound

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0021](0021-provider-recognition-does-not-authorize-execution.md),
[ADR 0034](0034-a-provider-renders-text-units-where-the-host-writes-text.md).

Contract: 25.5.

## Context

A YDB secret operation stores an environment variable name in its desired and
operation models. Its portable SQL form uses a placeholder in the secret value
clause. The value must be resolved at dispatch and must not enter a generated
plan, migration file or history record.

The provider subprocess has an isolated environment. Passing credentials in
process options violates the runtime contract, and the connection address is
not an execution-input channel. Native operations and SQL units both need a
bounded way to name and receive the inputs their owner understands.

## Decision

### Discover references before approval

The two-way feature `units.sensitive-inputs` enables the offline
`DescribeExecutionInputs` RPC and execution bindings. The provider and every
supporting engine list it. Discovery takes an explicit engine and the exact
ordered batch of `Unit` values, including SQL and native units. It performs no
I/O and reads no input values. Its result is complete and deterministic.

An `ExecutionInput` contains a source and name. The initial source is
`ENVIRONMENT`; names follow the ASCII environment-variable grammar. References
are unique and sorted by source and name. Empty batches return no references.
The provider's grammar decides which references its operations require; the
host does not dispatch on an engine name or recognize a secret SQL grammar.

The host discovers references before presenting a plan for approval. It includes
them in reviewable plan metadata and the approved content digest. Its operator
policy authorizes which environment references may be resolved. A provider's
request alone grants no authority to read an environment variable.

A host may discover each unit separately so a saved plan retains references
beside that unit. Dispatch then checks the exact batch against the sorted union
of approved references. A context-dependent new reference refuses the batch.

### Bind values only at dispatch

When the feature is negotiated, `ExecuteRequest.engine` selects the engine.
`expected_inputs` carries the approved references. `input_bindings` carries
exactly one `ExecutionInputBinding` per reference, with no duplicate, missing or
unrequested input. A present binding may contain empty bytes; a missing variable
is not the same state as an explicitly empty value. An engine that does not
offer discovery accepts no references or bindings.

Immediately before dispatch, the host re-describes the exact cloned units and
requires reference equality with the approved plan. It then resolves the
authorized values. The SDK server repeats discovery and equality validation
before invoking the executor. Changed references, incomplete discovery,
cancellation or invalid bindings refuse before dispatch.

Values exist only in the request for that execution. They never enter `Unit`,
retained models, plans, history, diagnostics, logs or rendered SQL artifacts.
Providers bind them through the database interface without repeating them in
an error. The SDK's validation diagnostics do not include binding values, and
the client discards remote error text. This is a transport and lifecycle rule;
it does not claim that garbage-collected memory can be securely erased.

The provider must check that the selected engine matches the session. Execution
keeps its existing confirmation and uncertainty rules: discovery succeeds
before dispatch, but it does not establish that a dispatched operation committed.

### Negotiate the complete path

A host lists the feature only after implementing reference approval, policy,
dispatch revalidation and ephemeral binding. Without negotiation, neither side
sends the new execution fields. A provider that cannot safely execute without
this path must refuse that operation or require the corresponding host contract;
it must not fall back to reading an inherited process environment.

## Alternatives considered

**Inherit the host's environment.** This exposes inputs the operation did not
name and makes a plan's behavior depend on hidden process state. Rejected.

**Put values in native payloads or SQL text.** Those artifacts are persisted and
reviewed. Rejected.

**Ask for inputs only at dispatch.** A provider could request a new environment
variable after the operator approved the plan. Rejected.

**Describe native units only.** Persisted text migrations would lose the same
operation's input contract. Rejected.

## Consequences

Hosts must carry nonsecret references through every approval and execution
surface, including persisted SQL migrations. They resolve values only after
authorization. SDK tests cover exact bindings, duplicates, empty values,
cancellation, changed discovery and redacted errors. Host and provider tests
must additionally prove that artifacts retain references and never values.
