# ADR 0071: A declared password travels as an execution input

Status: Proposed

Contract: 25.22

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0043](0043-sensitive-input-references-are-approved-before-values-are-bound.md)

## Context

A Go or YAML schema can give a role a password, and Ptah sets it when it
creates the role. The statement that sets it carries the password as a string
literal, so every output that shows the statement shows the password. Measured
with Ptah master d001c7f3a and `stokaro/ptah-ydb-plugin` master fe67ca1 on
local YDB 26.2.1.14, through the built-in YDB engine and through the provider,
with the same results on both paths:

| Output | What it holds |
| --- | --- |
| `schema render`, `schema plan`, `schema apply`, `--dry-run`, `apply --plan` | `CREATE USER ... PASSWORD '<password>'` below a warning that it is plain text |
| `schema plan --output` | the statement in `statements[].sql` |
| `migrations plan`, `migrations generate` | the statement, and the generated `.up.sql` file holds it |
| `schema compare` | the statement, and `roles_added (1): <user> <password>` |
| `schema apply --json` | the statement in `units[].sql` |
| a statement the server refuses | the statement in the error, twice on the built-in path |

The PostgreSQL renderer writes `CREATE ROLE ... PASSWORD '<password>'` into the
same outputs. Through a provider the contract carries the value:
`RoleCreation.password` and `RoleAlteration.password` travel in render
requests and in planning steps, and a provider that renders the unit puts the
value into its SQL text, which the host prints and saves.

[ADR 0043](0043-sensitive-input-references-are-approved-before-values-are-bound.md)
keeps a YDB secret's value out of every artifact: the statement names an
environment variable, the host binds its value at dispatch, and the provider
defines the value beside the statement as a named expression. A password
cannot be defined that way. YDB takes a password only as a string literal:
`$p = "x"; CREATE USER u PASSWORD $p;` answers `mismatched input '$'
expecting {NULL, STRING_VALUE}` on 26.2.1.14 and `Unexpected token '$'` on
25.1.4.7. PostgreSQL's grammar takes a string or `NULL` there too.

## Decision

### The wire carries an input, never the value

`schema.password-inputs` is a two-way feature. The provider and each engine
that takes such inputs list it, beside `schema.common-objects` and
`units.sensitive-inputs`, and the host lists it in its host features. Contract
25.22 adds one field to each role change:

```proto
message RoleCreation {
  SQLRole role = 1;
  string password = 2;
  ExecutionInput password_input = 3;
}
message RoleAlteration {
  SQLRole desired = 1;
  repeated string changed = 2;
  string password = 3;
  ExecutionInput password_input = 4;
}
```

With the feature negotiated, a change that sets a password carries
`password_input` and leaves `password` empty. It does so for a password and
for a password hash alike: the host's model holds one string, and it cannot
tell an engine's hash form from a password. Without the feature, `password`
carries the value as contract 25.3 sends it, and no input is named.

The host names the input. The provider does not derive a name, so the host and
the provider do not share a naming rule across two repositories. The source is
`ENVIRONMENT`, and the name is the one the host binds and the one a saved plan
or a migration file shows.

### The provider renders the reference and binds the value

The unit names the input and never the value, and `DescribeExecutionInputs`
reports the input for that unit, so the reference reaches plan metadata, the
approval digest and persisted SQL as ADR 0043 describes. At execution the
provider writes the bound value into the statement it sends, as a string
literal. Where its engine takes a hash in another clause, as YDB takes one with
`HASH`, the provider chooses the clause from the bound value. The statement as
written is refused by the server, so a file run without Ptah creates no user
with some other password.

### Where the host takes the value from

The host binds an input at dispatch. A plan computed from a declared schema in
the same run binds the declared password. A saved plan, a migration file and a
replay hold the reference alone, so the host reads the variable the reference
names, as it reads a secret's. A variable that is not set refuses before
execution, naming the variable and never a value.

### What the SDK enforces

Both transport boundaries refuse, in a render request and in a planning step,
an input the session did not negotiate, a value once it did, a change that
carries both, an input whose source is unknown or whose name the environment
grammar refuses, an input for a role that declares no password, and an input
for an alteration that does not change the password. Their messages never
repeat a value.

### A peer without the feature

A host refuses a declared password for a provider engine that does not
negotiate the feature, because the value would enter a unit. A provider that
lists the feature refuses a password value from a host that does not, for the
same reason. Neither side falls back to the value.

## Alternatives considered

**Redact the displayed text.** A saved plan and a migration file hold the
statement that runs. A marker there would set the password to the marker, and
the value would still travel to the provider. Rejected.

**The provider derives the variable from the role's name.** Two
implementations in two repositories would have to agree on one rule, and the
value would still travel in every render request and planning step. Rejected.

**Keep a hash as a literal.** The host cannot tell a hash from a password
without knowing the engine, and a password hash is a credential verifier that
can be attacked offline. Rejected.

**Refuse a declared password.** Ptah sets a declared password once, and
removing that would remove a capability. Rejected.

## Consequences

A migration file or a saved plan that sets a password needs the variable set on
the machine that applies it. The variable is a `PTAH_SECRET_` name, the prefix
an operator already exports a secret's value under. A provider and a host
negotiate the feature before either sets a password; a mixed pair refuses the
password instead of printing it. Host and provider tests must show that no
output, saved plan or migration file holds the value, and that the user logs in
with it.
