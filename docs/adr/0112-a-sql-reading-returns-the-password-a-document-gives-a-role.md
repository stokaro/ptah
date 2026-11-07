# ADR 0112: A SQL reading returns the password a document gives a role

Status: Proposed

Contract: 25.33

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)
(register G64)

Relates to: [ADR 0027](0027-a-provider-reads-the-sql-schema-documents-of-its-own-dialect.md),
[ADR 0071](0071-a-declared-password-travels-as-an-execution-input.md)

## Context

A Go, YAML or HCL schema can give a role a password, and the host keeps it in
its model until a plan binds it to the statement that sets it. Under
`schema.password-inputs` (ADR 0071) the statement names an execution input,
`PASSWORD $PTAH_SECRET_PASSWORD_<ROLE>`, and no output shows the value.

A SQL schema document of an engine in the DOCUMENT mode reaches the host
through `ReadSQLSchema`, whose `SQLRole` says only that a role declares a
password (`password_declared`). Its comment said why: a secret in a schema
file is not a value the host compares. The password itself had no field.

The built-in YDB engine read the password of a YQL document. Measured with
Ptah v0.18.0 on a document holding
`CREATE TABLE t (id Uint64 NOT NULL, PRIMARY KEY (id));` and
`CREATE USER app PASSWORD 's3cret-pw';`, `ptah schema render --dialect ydb`
exits 0 and writes `CREATE USER app PASSWORD '...'` with the declared
value, the plain-text output ADR 0071 later removed on every path. The YDB
provider at stokaro/ptah-ydb-plugin 5357abb2 refuses the same document,
because the reply has nowhere to put the value (`user app declares a
password, which a SQL schema reading has no home for: the host compares no
secret a schema file holds`, `internal/yqlschema/reply.go`). A user that a
YQL document creates with a password therefore cannot be declared in YQL
through the provider, while the same user declared in HCL can.

## Decision

### The reading carries the password under a two-way feature

`source.sql-role-passwords` adds `SQLRole.password` to a `ReadSQLSchema`
reply: the password as the document writes it, a literal or a reference to an
execution input such as `$PTAH_SECRET_APP_PASSWORD`. The provider lists the
two-way feature, and so does each engine whose reading returns passwords,
beside `schema.read-sql` and the common-object wire with roles. Under it a role
carries a password exactly when `password_declared` is true. The host puts the
password into its model as it puts one a Go, YAML or HCL source declares, so
the plan binds it at execution and no output prints it.

The host does not compare it. A read never reports a password, so the
comparison already treats a declared password as one it cannot see, whatever
the source.

### The password travels in that reply alone

A role a host sends never carries a password: not in declared common objects,
not in a preparation's reply, and not in a role change, which carries its
password in `RoleCreation` or `RoleAlteration`, as a value or as an input.

### What the SDK enforces

`ValidateDescription` refuses the engine's feature without the provider's,
without SQL reading or without roles on the common-object wire.
`ValidateSQLSchemaReply` refuses a password for a target that does not list
the feature, a password without `password_declared`, `password_declared`
without a password under the feature, and a password that holds a control
character or is not UTF-8. Every validator of a role a host sends, and of a
preparation's reply, refuses a password. No message repeats the value.

## Alternatives considered

**Keep refusing the password.** It keeps every secret out of the SQL wire, at
the price of the declaration: the user cannot be written in YQL at all, which
the built-in engine allowed and an HCL document still allows. The value
reaches the host from a Go, YAML or HCL source already, so the refusal protects
nothing those sources do not expose.

**Send a reference, never a value.** The reading could name an execution input
and leave the value to the environment. A document that writes a literal has
no input to name, and the host already turns a declared literal into an input
at plan time (ADR 0071). Doing it in each provider would repeat that rule per
engine, and a reading would then differ from the same declaration in HCL.

**Reuse `RoleCreation`.** A reading reports a declaration, not a change; a
role change is what a plan sends. Carrying a creation in a reading would mix
the two and make every reading look like a plan.

## Consequences

- A YQL document can declare a user with a password through the YDB provider
  once both sides list the feature, as the built-in engine read it.
- A provider that lists the feature carries a secret in a reply. The SDK keeps
  it out of its messages, and the host keeps it out of every output through
  the password-input path.
- A host of an earlier contract receives `password_declared` alone, as before.

## Acceptance evidence

- SDK unit tests: `TestSQLRolePasswords_HappyPath`,
  `TestSQLRolePasswords_FailurePath`,
  `TestSQLRolePasswordsHostMessages_FailurePath`,
  `TestSQLRolePasswordsDescription_FailurePath` and
  `TestSQLRolePasswordsNegotiation_FailurePath` in `provider-sdk/provider`.
- The host adoption reads a password from a provider's SQL reading through the
  fixture provider, and no output of render, plan or apply shows it.
