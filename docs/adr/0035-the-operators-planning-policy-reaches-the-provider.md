# ADR 0035: The operator's planning policy reaches the provider

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0024](0024-the-provider-owns-table-changes-through-one-lifecycle-service.md),
[ADR 0030](0030-ptah-compat-resolves-providers-through-the-native-pipeline.md)

Contract: 25.3, with ADRs 0033, 0034, 0036 and 0037.

## Context

ADR 0030 lists four YDB behaviors in shared CLI packages that block the flip,
because each needs "a provider-declared planning or session option" and no
record carries them: the table-rebuild policy input, secret rotation, the
migrate timeout and the dev snapshot realm. ADR 0024 adds that its lifecycle
service must accept the rebuild input before the embedded planner is deleted.

What the code does today:

- **The host's planner takes the operator's policy as options.**
  `planext.Options` (`core/planext/planning.go`) carries `AllowTableRebuild`
  and `TableRebuildRequest`, `OnlineAlter`, `ConcurrentIndexes` and
  `ConcurrentIndexRefs`, `ConcurrentIndexDrops` and `ConcurrentIndexDropRefs`,
  `SkipChangeKinds` and `OmitNullBackfill`. `atlasschema.DiffPolicy`
  (`internal/atlasschema/diff_policy.go`) fills them from the commands.
  `--allow-table-rebuild` comes from `internal/cli/internal/tablerebuild`, and the
  compatibility surface reads `PTAH_ALLOW_TABLE_REBUILD` instead
  (`internal/cli/atlas/table_rebuild.go`) and names it in a refusal through
  `TableRebuildRequest`.
- **The YDB planner reads the rebuild option.** `internal/planner/dialects/ydb/rebuild.go`
  plans a key, type or `NOT NULL` change as a seven-step rebuild when it is set,
  and refuses the change with the words of `TableRebuildRequest` when it is not.
- **No option reaches a provider.** `PlanFeaturesRequest` has no policy field.
  `internal/providerhost` passes a provider the capabilities of
  `planext.Options` and nothing else (`plan_context.go`).
- **Secret rotation is a comparison input, not a planning option.**
  `--rotate-secret` (`internal/cli/internal/secretrotation`) calls
  `ydbsecret.WithRotations`, which parses each path with YDB's rules
  (`ydbsecret.ParsePath`) into a `schemaext.ChangeRequest` with the action
  `rotate`, and puts it in `config.CompareOptions.FeatureRequests`. The secret's
  owner decides in its comparison whether the request is a rotation, a creation
  or undecided, and the comparison refuses a path the declaration does not hold,
  so a typo cannot read as a rotation done. The contract already carries the
  request, `CompareModelsRequest.requests` with `ModelComparison.actions`; what
  it lacks is a way to send the operator's spelling, which only the owner can
  parse.
- **The other two need no contract change.** `internal/cli/internal/migratetimeout`
  decides from the capability keys `migration_lock_timeout` and
  `migration_statement_timeout`, which a session reports, and the migrator
  bounds a statement through `ExecuteRequest.statement_timeout_nanoseconds`
  (contract 25.1). `internal/cli/internal/devsnapshot` skips its check for a
  dev database a run creates, which `devdocker.ResolvedPerRun` decides with
  `ydbrealm.Applies`; a provider engine says the same with
  `database.dev-realms` (contract 25.1), and the host already reads it in
  `devdocker.ExternalRealms`.

## Decision

### One feature for the policy

The two-way feature `planning.policy` carries the operator's policy to the
provider. It adds fields to existing messages, so the provider lists it in
`Description.features` and in each engine that honors it, and the host lists it
in `DescribeRequest.host_features`.

### The planner receives the policy

`PlanFeaturesRequest` gains `PlanningPolicy policy = 15`:

```proto
message PlanningPolicy {
  // Plan a change the engine cannot make in place as an explicit rebuild of
  // its table, rather than refuse it.
  bool allow_table_rebuild = 1;
  // How the operator asks for allow_table_rebuild, for a refusal to name:
  // "--allow-table-rebuild" on the native surface, "PTAH_ALLOW_TABLE_REBUILD=1"
  // on the compatibility one. Always set, because a refusal is what an
  // operator who did not ask reads.
  string table_rebuild_request = 2;
  // Apply without blocking the writes already running, where the engine has
  // a way to, and fail at the statement where it cannot.
  bool online_alter = 3;
  // Build every added index, or these, without blocking writes.
  bool concurrent_index_builds = 4;
  repeated ComparisonIdentity concurrent_index_build_subjects = 5;
  // Drop every removed index, or these, without blocking writes.
  bool concurrent_index_drops = 6;
  repeated ComparisonIdentity concurrent_index_drop_subjects = 7;
  // Make a column NOT NULL without filling its NULL rows first.
  bool omit_null_backfill = 8;
}
```

Each field has the meaning its `planext.Options` field has, and the host fills
it from the same options. A policy is a request, not a prediction: an engine
without the behavior plans as it would without it, as the host's own planners do.
`SkipChangeKinds` does not travel: the host drops those changes before planning,
so they never reach a provider.

A provider that does not list the feature receives no policy. It plans as if
none were set, and a change it can make only by a rebuild is refused in its own
words. The host does not refuse the flag in that case, because a host planner
without a rebuild does not refuse it either.

The lifecycle service of ADR 0024 reads `allow_table_rebuild` when it plans a
table `ALTER`, and names `table_rebuild_request` in the refusal of a change only
a rebuild can make.

### The owner resolves a request it alone can parse

A request such as `--rotate-secret ext/pg` names an object in the engine's own
spelling. `RequestedChange` gains the spelling, and `ComparedModels` says which
object each one named:

```proto
// RequestedChange gains:
//   The object as the operator wrote it, for the owner to resolve. Set
//   instead of subject.
string written = 3;
// ComparedModels gains: repeated ResolvedRequest resolved_requests = 5;
message ResolvedRequest {
  uint32 request = 1;              // index into CompareModelsRequest.requests
  ComparisonIdentity subject = 2;  // the desired object it names
}
```

The host sends a written request to every comparison service of the engine
whose `actions` include the request's action. A service that recognizes the
spelling answers it once in `resolved_requests` and acts on it as on a request
with a subject: a change, or an undecided record. The host refuses a written
request that no service resolved, or that two resolved, naming the spelling, so
a typo cannot read as a rotation done. A subject a service resolves to must be a
desired object of the kinds that service compares.

The host stops parsing secret paths. `--rotate-secret` keeps its name and its
behavior: the host sends each value as written, with the action `rotate`.

### The migrate timeout and the dev snapshot realm

These two need no contract change. The host reads the timeout capability keys
from the session before the migrator runs, and asks the selected provider's
`database.dev-realms` declaration instead of `ydbrealm.Applies` when it decides
that a dev database is one the run creates. Both are host work in the pull
request that flips `ydb://`.

### Validation

Both transport boundaries refuse:

- a `policy` or a `written` request the negotiated description does not allow;
- a policy subject that is not a valid comparison identity, or that names an
  index twice;
- a request with both `subject` and `written`, with neither, or with an empty
  spelling;
- a resolved request whose index is outside the request, that resolves a request
  with a subject, or that answers one request twice.

### Versioning

`planning.policy` is a compatible addition, part of contract 25.3. A provider
that honors it does not raise its minimum contract: an older host sends no
policy and parses no written request, which leaves the provider planning and
comparing without either.

## Alternatives considered

**Session options.** The host would set the policy on `Open`, and every call of
the session would read it. A policy belongs to one plan, while a session serves
a whole command; a command that plans twice, such as `migrate diff` with a dev
database, would need two sessions. `PlanFeatures` is offline and has no session.
Rejected.

**Engine-named options as a map.** `map<string, string> options` would carry any
setting a provider names. It adds a vocabulary the host does not check, and an
operator's typo would reach the provider as a setting it ignores. Rejected; the
fields above are the host's vocabulary, and a new one is a compatible addition.

**Rotation as a planning option.** `PlanningPolicy.rotations` would list the
secrets. The planner has no change to plan for a secret whose value compares
equal, and it does not see the desired object a rotation reads its value from.
Rejected; rotation stays in the comparison, where the host decides it today.

**Host-side path parsing behind a provider rule.** The engine would declare how a
path is spelled, and the host would build the subject. It is a second grammar
for the host to keep equal to the provider's. Rejected.

## Consequences

- `PlanFeaturesRequest`, `RequestedChange` and `ComparedModels` grow one field
  each, behind one feature.
- `ydbsecret.ParsePath` and `ydbsecret.WithRotations` leave the shared command
  packages; the YDB provider resolves secret paths.
- Of ADR 0030's four flip blockers, two are protocol additions here and two are
  host changes with no protocol change.
- A provider that ignores a policy field gives the plan it would give without it,
  which is the behavior the host's own planners have.

## Acceptance criteria

These are for the implementation. None holds today.

- The SDK carries `PlanFeaturesRequest.policy`, `RequestedChange.written` and
  `ComparedModels.resolved_requests` as contract 25.3, and both boundaries refuse
  each case under "Validation". In-process tests negotiate the feature in both
  directions, and each validator is mutation-checked.
- The fixture provider plans a change it can make only by a rebuild, refuses it
  without the policy naming the request it was given, and plans it with the
  policy. An SDK test resolves a written request and refuses an unresolved one.
- The host passes the policy from both surfaces, and a test through the fixture
  provider shows the compatibility surface's refusal naming
  `PTAH_ALLOW_TABLE_REBUILD=1`.
- The YDB provider's CI plans a key change with and without
  `--allow-table-rebuild`, and rotates a secret with `--rotate-secret`, on each
  YDB line it supports.
