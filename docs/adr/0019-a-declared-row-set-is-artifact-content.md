# ADR 0019: A declared row set is artifact content, and a checkpoint carries the rows its successors read

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#3233](https://github.com/stokaro/ptah/issues/3233)
- Answers the open questions of [stokaro/ptah-operator#41](https://github.com/stokaro/ptah-operator/issues/41) and [#3232](https://github.com/stokaro/ptah/issues/3232)

## 1. Context

The operator epic asks for three user scenarios: versioned migrations carrying
DDL and DML, declared reference data reconciled continuously, and a new database
provisioned from a checkpoint that still satisfies the migrations after it.

Six implementation questions block the subtasks. This record answers them. It
decides representation, ownership and recovery boundaries; it does not decide
controller internals.

### What the tree assumes, measured

Four facts, read at `ff5332cb6` rather than remembered.

**The rows are not in the model.** `ManagedData` names a file, not a row set:

```go
type ManagedData struct {
	StructName string
	Table      string
	Schema     string
	Keys       []string
	File       string   // Path to the YAML row-data file, verbatim, relative to SourceDir
	SourceDir  string
}
```

`LoadManagedRows` resolves `SourceDir`/`File` against a host directory and
parses it at use time. So the declaration is a pointer into the author's working
copy, and everything downstream of publication has already lost it.

**The artifact is one layer, and it refuses the pointer.** `internal/schemaartifact`
publishes a single `schema.hcl` under `application/vnd.stokaro.ptah.schema.hcl.v1`,
with an `io.stokaro.ptah.schema-format` annotation, and `Capture` begins:

```go
if len(db.ManagedData) > 0 {
	return nil, fmt.Errorf("schema artifact cannot represent managed data without loss")
}
```

The refusal is accurate, not conservative. There is no layer the rows could go
into, and the canonical-HCL round trip that `Capture` verifies has no syntax for
them.

**A checkpoint is an ordinary SQL file.** Native checkpoints are the
`0000000002_snapshot.checkpoint.up.sql` file-name pair; Atlas-format checkpoints
are marked by `-- atlas:checkpoint` on the first line, and combining that with
`-- atlas:txtar` is refused because the combination has no measured semantics.
Selection is already decided: a fresh database applies the checkpoint and the
migrations after it.

**Partial execution already has a native witness.** `MigrationRevision` records
`State`, `Applied`/`Total`, `Error`, `ErrorStatement`, `Dirty` and a checksum
that, on a dirty row with committed progress, is a `partial:h1:` cumulative
source-prefix digest "so a resume can prove which statements it may skip".
`DirtyMigrationError`, `IsDirtyMigration` and `migrations repair --resume-from`
are the path out.

## 2. Definitions

**Declared row set.** The rows one `ManagedData` declaration owns in one table,
identified by its key columns.

**Managed column.** A column the declaration writes. Every other column of a
row in a declared row set is unmanaged.

**Bootstrap data.** The declared row sets as of the version a checkpoint stands
at, as opposed to the declarations of the release that generated the checkpoint.

**Area.** The (coordination realm, target identity, schema, table set) a
Kubernetes resource claims. Two resources overlap when their areas intersect.

## 3. Decision

### 3.1 Declared rows become a second artifact layer, in canonical JSON

The schema artifact gains one layer, `managed-data.json`, under
`application/vnd.stokaro.ptah.managed-data.v1`, carrying every declared row set
of the artifact: schema, table, key columns, managed columns, and rows.

The authored form stays YAML. The published form is canonical JSON: members
sorted, no trailing zeros, no aliases, and an absent value spelled by omitting
the key while a SQL `NULL` is spelled `null`. YAML cannot carry that
distinction safely, because its scalar resolution rewrites the values a
reference table is made of: unquoted `NO` reads as false, `1.0` as a float, and
a leading-zero code as an integer. A format whose reader has to guess the type
of `NO` cannot be the wire form of a row whose column is `CHAR(2)`.

One layer, not one per table. A per-table layout makes the order of a manifest
listing part of the contract and multiplies the entries a reader has to
classify.

The layer joins the manifest, so it joins the digest, and a digest-pinned
reference pins the rows with the schema.

### 3.2 An unknown layer is a refusal, not something to skip

A reader refuses a manifest carrying a layer media type it does not know,
before it reads any layer it does know.

This is the whole of version negotiation, and it has to be stated because the
failure it prevents is silent: an old reader that fetches `schema.hcl` and
ignores an unrecognized sibling deploys a schema without the rows the author
declared and reports success. The media-type suffix is the version. A format
change that an old reader must not accept gets `.v2`; a change every reader can
ignore is not a format change.

The artifact also declares the capabilities an executor needs, so the refusal
happens before a database connection exists rather than at the first statement.

### 3.3 A checkpoint carries its bootstrap rows as SQL, inside the checkpoint

Required rows are materialized into the checkpoint's own SQL, generated at
artifact-preparation time from the declarations as of version C, against the
disposable shadow database that already generates the checkpoint.

The consequences are the point. The rows are covered by the checkpoint's
checksum. The existing selection rule needs no change, because a fresh database
already applies the checkpoint and nothing before it. There is no second
execution path, so there is no state in which the checkpoint applied and the
bootstrap did not.

A checkpoint that requires bootstrap data says so, and an executor that cannot
produce it refuses before mutating rather than deploying a schema whose
successors will fail on the rows that are not there.

The rows are the declarations of C, never the declarations of the latest
release. A migration after C may read a column later renamed, or a reference
value later retired; a bootstrap assembled from today's declarations produces a
database that no post-checkpoint migration was written against.

### 3.4 Versioned migrations get their own plan and approval kinds

`PtahMigration` does not reuse `PtahSchemaPlan` and `PtahSchemaApproval` as
kinds. It reuses their mechanics: the chunked immutable plan store, the
fingerprint discipline that binds an approval to exact bytes, and the execution
binding that names the controller, executor and runner.

The two plans are not the same object. A schema plan binds a fingerprint of the
observed schema and one of the desired schema, a dialect, a destructive flag and
a statement count. A migration plan binds a version sequence, per-migration checksums,
checkpoint boundaries and the observed history. Sharing one kind means each
field is optional and means something different depending on which controller
wrote it, which is how a stored object passes validation under a shape nobody
intended.

Shared code, not a shared CRD, is the reuse that costs nothing at read time.

### 3.5 An area is claimed before it is used, and an overlap is refused

A resource declares the area it owns. Admission refuses a second resource whose
area intersects a claimed one, `PtahSchema` against `PtahMigration` included.
By default both claim the whole target schema, so two resources over one
database conflict by default and one of them never runs.

Ownership is not the Lease. A Lease serializes access, and two controllers that
never run at the same time can still undo each other's work by taking turns. The
claim is checked before either acts, in the Kubernetes API, where a refusal costs
nothing.

Disjoint areas stay possible, and cost an explicit declaration from both sides.

### 3.6 The outcome of an execution is read from the revision row

The controller classifies an execution from the native history, never from a Job
exit code:

- **Applied**, when a clean revision row carries the planned version and its
  checksum matches the planned migration.
- **Not applied**, when neither a row nor a dirty row exists for that version
  and the execution reported a failure before its first statement.
- **Unknown**, otherwise, which includes every dirty row and every lost Job over
  a non-transactional migration.

Unknown is a terminal state for the controller. It is surfaced, not resolved:
the way out is Ptah's own resume path, which has the partial digest to prove
which statements it may skip, or a human decision. The controller never forces,
never baselines, never clears a dirty row and never edits a checksum, because
each of those turns "we do not know" into "it worked" without learning anything.

## 4. Alternatives

**Rows inside `schema.hcl`.** Atlas HCL has no row syntax, so this means an
extension that an Atlas reader cannot parse, and the round-trip check that makes
the artifact canonical would have to accept it. Rejected: it trades the one
property the layer has for the convenience of not adding a second one.

**A bootstrap layer applied after the checkpoint.** Keeps the checkpoint SQL
free of data and lets a reader see the rows without parsing SQL. Rejected: it
invents an ordering contract between a migration file and a layer, and a second
failure mode in which the checkpoint applied and the rows did not, which is
exactly the half-deployed state the epic refuses.

**One `PtahPlan` kind with a discriminated union.** Fewer kinds, one approval
flow. Rejected: a stored object would validate under either arm, and the field
set of each arm is the part a reviewer needs to see.

**Detecting an overlap at execution time.** No API surface for areas, and the
lock already exists. Rejected by the measurement in 3.5: alternating writers
never contend and still diverge.

**Marking a lost Job as not applied and retrying.** Simple and usually right.
Rejected: "usually" is doing the work there, and the case it gets wrong is a
non-idempotent `UPDATE` applied twice.

## 5. Consequences

The schema artifact format changes, and the change is breaking by design: an
artifact carrying rows is refused by every reader that predates the layer. The
compatibility statement gains the artifact format alongside the operator, Ptah
and runner versions.

`Capture`'s refusal is removed only together with the layer, the canonical
encoder, and the round-trip tests that prove a row survives publication. Until
then the refusal stays, because it is honest.

Checkpoint generation grows a data phase and stays where it is, in artifact
preparation against a shadow database. Nothing about it moves into the operator,
which never rebuilds a checkpoint against a production database.

Two new Kubernetes kinds exist, and their storage and approval code is shared
with the schema path rather than copied.

A database whose area is claimed by two resources stops being deployable without
an explicit declaration from both, which will break a user who is currently
running a `PtahSchema` and a manual migration flow over one schema. That is the
point: today the two silently overwrite each other.

An execution whose outcome is unknown blocks until a person or Ptah's resume
path resolves it. This is a real availability cost, accepted because the
alternative is a controller that guesses about DML.

### What this record does not decide

The CRD field names and the status conditions, the on-disk layout of the
migration plan chunks, the splitter between managed and unmanaged columns beyond
full ownership of a declared table, partial ownership of a table, and any
representation for arbitrary historical DML. Each belongs to the subtask that
implements it.
