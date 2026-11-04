# ADR 0058: A statement argument carries the type its caller bound it at

Status: Proposed

Contract: 25.15

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0038](0038-a-provider-runs-the-operators-statements-and-returns-what-they-read.md),
[ADR 0028](0028-a-provider-owns-how-rows-are-read-written-and-seeded.md)

## Context

`RunStatementRequest.arguments` is a list of `DataValue`. A `DataValue` names
a value's case and nothing more: one signed and one unsigned integer of 64
bits, one double, and a null without a type. It has no narrower integer, no
32-bit float, no type that admits null and no list.

A Go caller binds an argument with a type, and an engine with typed
parameters, such as YDB, binds the parameter at that type and refuses a write
whose parameter type differs from the column's. The built-in YDB engine binds
an `int32` as `Int32`, a `uint` as `Uint64`, a `*uint` and a `sql.Null[uint]`
as `Optional<Uint64>`, and a `[]uint` as `List<Uint64>`. Measured on YDB
26.2.1.14 through `stokaro/ptah-ydb-plugin`, a provider host that let
`database/sql` convert the arguments sent the first four as `Int64`, and a
write of an `int32` into an `Int32` column failed with `Failed to convert
'small': Int64 to Optional<Int32>`.

The host reads each argument's Go type before `database/sql` converts it. It
sends a `uint` as `uint64_value`, and it refuses, before it sends the
statement, every argument `DataValue` cannot hold exactly: an integer narrower
than 64 bits, a `float32`, a pointer, a `database/sql` Null value and a slice
other than a byte slice. That keeps a provider from binding a value at a type
its caller did not choose. It also leaves an operator who writes an `Int32`
column, or a nullable one, through a provider with no way to say the type,
and those are the ordinary Go types for such columns. The built-in YDB engine
is being removed, so the provider is the only way left to reach YDB.

## Decision

### The argument says its type

`RunStatementRequest` gains one field, set instead of `arguments` when both
peers list `sessions.typed-arguments`, and never beside it:

```proto
message RunStatementRequest {
  // ...fields 1 to 7 unchanged...
  repeated StatementArgument typed_arguments = 8;
}

message StatementArgument {
  ArgumentType type = 1;
  // Unset for a list.
  DataValue value = 2;
  // The elements of a list, in order.
  repeated DataValue elements = 3;
}

message ArgumentType {
  enum Scalar {
    SCALAR_UNSPECIFIED = 0;
    UNTYPED = 1;
    BOOL = 2;
    INT8 = 3;
    INT16 = 4;
    INT32 = 5;
    INT64 = 6;
    UINT8 = 7;
    UINT16 = 8;
    UINT32 = 9;
    UINT64 = 10;
    FLOAT32 = 11;
    FLOAT64 = 12;
    STRING = 13;
    BYTES = 14;
    TIMESTAMP = 15;
    INTERVAL = 16;
  }
  Scalar scalar = 1;
  bool optional = 2;
  bool list = 3;
}
```

`optional` says the caller's type admits null, such as a Go pointer or a
`sql.Null` value; on a list it says each element admits null. `list` says the
caller bound a list of the scalar, such as a Go slice. `UNTYPED` is a null of
no type, such as Go's untyped `nil`, and is never optional or a list.

A value keeps the `DataValue` case it takes under `arguments`, so a provider
reads it with the code it has: `INT8` to `INT64` are `int64_value`, `UINT8` to
`UINT64` are `uint64_value`, `FLOAT32` and `FLOAT64` are `double_value`, and
`INTERVAL` is `interval_value`. The type is what the provider binds.

The scalars are the ones a Go argument type names. A decimal, a date, a JSON
value and a native value have no Go argument type the host reads, so they have
no scalar; adding one later is another negotiated addition.

### Validation

`provider.ValidateRunStatementRequest` refuses, as `ErrProtocol`:

- `arguments` and `typed_arguments` both set;
- `SCALAR_UNSPECIFIED`, an unknown scalar, or `UNTYPED` with `optional`,
  `list` or a value other than null;
- `value` set on a list, or `elements` set on an argument that is not one;
- an unset value or element;
- a null where `optional` is not set, other than for `UNTYPED`;
- a value in a case other than the one the scalar names, an integer outside
  the scalar's range, or a `FLOAT32` double that a 32-bit float does not hold
  exactly.

`provider.ValidateTypedArguments` refuses `typed_arguments` when the
negotiated description does not list the feature. The SDK runs both checks at
each transport boundary, so a typed argument the provider was not ready for
never reaches its owner.

An empty list is valid: its type is declared, so the provider binds an empty
list of the scalar.

### Negotiation and a peer without the feature

`sessions.typed-arguments` is in `TwoWayFeatures`, because it adds a field to
an existing message: a provider built before it drops the unknown field and
runs the statement with no arguments at all. The provider lists it, and it
requires the provider's `sessions.statements`.

The feature is the provider's alone. A statement request names no engine, so
the SDK could not check an engine's listing, and reading the typed form is the
transport's concern, not the engine's. A provider that lists the feature reads
typed arguments on every session it opens, and an engine whose parameters have
no type binds the value and ignores the type. `units.statement-timeout`, which
also extends a session request, is negotiated the same way.

- Both peers list it: the host sends every argument as a `StatementArgument`.
  A narrower integer, a `float32`, a pointer, a `database/sql` Null value and a
  slice reach the provider with their types.
- The provider does not list it: the host sends `arguments` and refuses,
  before sending, every argument `DataValue` cannot carry exactly.
- The host does not list it: the negotiated description removes the feature,
  so the provider receives `arguments` and keeps reading them.

The host decides from the negotiated feature, never from an engine name.

### What the host sends

The host reads the Go value before `database/sql` converts it:

| Go type | Scalar | optional | list |
| --- | --- | --- | --- |
| `nil` | `UNTYPED` | no | no |
| `bool` | `BOOL` | no | no |
| `int8` to `int64`, `int` | `INT8` to `INT64`, `INT64` | no | no |
| `uint8` to `uint64`, `uint` | `UINT8` to `UINT64`, `UINT64` | no | no |
| `float32`, `float64` | `FLOAT32`, `FLOAT64` | no | no |
| `string`, `[]byte` | `STRING`, `BYTES` | no | no |
| `time.Time`, `time.Duration` | `TIMESTAMP`, `INTERVAL` | no | no |
| `*T`, `sql.Null[T]` | the scalar of `T` | yes | no |
| `sql.NullInt32` and the other Null types | the scalar of their value | yes | no |
| `[]T` other than `[]byte` | the scalar of `T` | when `T` admits null | yes |

A named type is its underlying kind. A `driver.Valuer` other than a
`database/sql` Null type is the value its `Value` method returns. What the
table does not name stays refused: an array, a nested list, a map, a struct, a
pointer to a pointer, a pointer to a list, and a list of values whose type a
`Value` method decides.

## Alternatives considered

**New cases in `DataValue`.** An `int32_value` and the like would change the
message every row, row write and result carries, and a peer built before them
reads a case it does not know as a value with none. It still has no way to say
that a null has a type or that a value is a list. Rejected.

**The engine's type name.** The host could send `Int32` or
`Optional<Uint64>`. It would have to know each engine's type syntax, which is
the provider's knowledge ([ADR 0023](0023-engine-knowledge-moves-with-the-provider.md)),
and every engine spells a list differently or has none. Rejected.

**`DataKind` and a width.** `DataKind` names the cases of `DataValue`, so it
has no 32-bit float, and a width beside it allows pairs that mean nothing,
such as a `BOOL` of 16 bits or a `STRING` of 8. A closed set of scalars cannot
say anything the host never sends. Rejected.

**A Go type string.** `"int32"` or `"*uint"` would tie the contract to one
language's type system, and a provider in another language would have to
parse Go. Rejected.

**An engine-level feature.** An engine could list the feature as it lists
`sessions.statements`. A request names no engine, so neither transport
boundary could check it, and a provider that reads the field for one engine
reads it for all. Rejected.

**Keep refusing.** The host's refusals are correct for a provider without the
feature, and they stay for one. Leaving them as the only answer makes the
ordinary Go types for narrow and nullable columns unusable through every
provider. Rejected.

## Consequences

- Contract 25.15 adds a field, two messages and a two-way feature.
- A provider can bind every shape the built-in YDB engine binds, which the
  engine's removal needs. The YDB provider maps the scalars to `Int8` to
  `Uint64`, `Float`, `Double`, `Utf8`, `String`, `Timestamp` and `Interval`,
  `optional` to `Optional<T>`, a null of it to a typed null, `list` to
  `List<T>`, and `UNTYPED` to `Void`, as it binds an untyped null today.
- An engine without parameter types, such as SQLite, binds the value and
  ignores the type. It still has to bind a list, or refuse it.
- `DataValue` does not change, so rows, row writes and results are untouched.
- A provider that lists the feature still reads `arguments`, which a host
  without the feature sends.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.

## Acceptance evidence

- SDK tests accept each scalar at the edges of its range, an optional and an
  untyped null, a list with elements, with optional nulls and with none. They
  refuse each rule above, and typed arguments that the host, the provider or
  no description negotiated, while the same request with plain arguments
  passes.
- Both transport boundaries refuse typed arguments without the feature, and
  a typed argument its type does not hold, before the owner sees it.
- An SDK-only proof provider, started as a process, receives an `INT32`, a
  null of an optional `UINT64` and a list of `STRING` with their types from a
  host that lists the feature. A host that does not list it cannot send them.
