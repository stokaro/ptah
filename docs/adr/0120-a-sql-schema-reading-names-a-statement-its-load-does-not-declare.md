# ADR 0120: A SQL schema reading names a statement its load does not declare

Status: Proposed

Contract: 25.38

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)
(register G237)

## Context

Ptah reads the SQL files of one load together, so a later file can change an
object an earlier file declares. A source in another format, such as an HCL
file, is not part of that load. When a SQL file changes an object only such a
source declares, the host's own SQL reading refuses the statement with an error
that wraps `sqlext.ErrUndeclared`, and the load adds why:

```text
read SQL schema file feed.sql: the schema model has no place for this
statement: ALTER TABLE users ADD CHANGEFEED updates names a table this schema
does not declare; a ydb SQL file changes only what the SQL files of the same
load declare, not what users.hcl declares
```

That is the built-in YDB engine at 74de0b65b, and it is what the host says for
every engine it reads itself. An engine whose provider reads its SQL documents
(`schema.read-sql`, the DOCUMENT mode) refuses the same statement with a
`SQLDiagnostic` of code `UNSUPPORTED` or `INVALID`. The host cannot tell that
diagnostic from any other unsupported construct, so it prints the provider's
message without the explanation, and the reader is not told that the HCL
file, not the SQL file, is where the table has to be changed. Measured with
`ptah-compat schema diff --to file://users.hcl --to file://feed.sql`: the
built-in engine at 74de0b65b refused with the explanation above, and the YDB
provider refuses with its own message and none.

## Decision

`schema.read-sql-undeclared` adds the code `UNDECLARED` to `SQLDiagnostic`: an
error for a statement that changes an object no document of the load declares,
such as an `ALTER TABLE`, a `COMMENT ON` or a `GRANT` on a table no `CREATE
TABLE` of the load creates. The owner reports it for that statement only; a
statement it cannot read, or one it reads and cannot represent, keeps its other
codes. The host reads an `UNDECLARED` error as a statement its load has no
place for, as it reads its own refusal: the error wraps `sqlext.ErrUndeclared`,
and a load that holds a source in another format says that its SQL files change
only what its SQL files declare, naming those sources.

A two-way feature: the provider lists it, and so does each engine whose SQL
reading reports the code, beside `schema.read-sql`. A host that does not list
it receives no `UNDECLARED` diagnostic and keeps reading the owner's other
codes. The SDK refuses the code in a reply whose engine or target did not
negotiate the feature, the code as a warning, and an engine that lists the
feature without the provider's or without its own SQL reading.

## Alternatives considered

**Let the host match the provider's message.** The words are the owner's. A
host that searched them for "does not declare" would depend on a sentence no
contract fixes, and would misread an owner that words it otherwise.

**Send the host the names of the objects a statement changes.** The host could
then compare them with the other sources of the load itself. That moves the
owner's knowledge of its grammar into the host, and the owner already knows
the statement names nothing the load declares, which is all the host needs.

**Read the other sources into the provider's load.** An HCL table is not a SQL
document, and the built-in engine did not read it into the SQL load either:
the explanation is that the two formats are read apart.

## Consequences

- The YDB provider can refuse a YQL file that changes a table only an HCL file
  declares with the explanation the built-in engine gave.
- A provider that does not list the feature keeps its current codes, and the
  host keeps printing its message alone.

## Acceptance evidence

- SDK: `TestSQLUndeclared_HappyPath` and `TestSQLUndeclared_FailurePath` in
  `provider-sdk/provider`.
- Host adoption: the host lists the feature and maps an `UNDECLARED` error to
  `sqlext.ErrUndeclared`, so a load beside an HCL source adds the explanation,
  through a provider process.
- YDB adoption: the HCL and YQL load of
  `TestLoadAll_AYQLFileCannotChangeWhatAnHCLSourceDeclares` refuses with the
  explanation, as built-in 74de0b65b did.
