---
title: Kubernetes operator
description: Reconcile a desired schema or run a versioned migration sequence from an OCI artifact with the Ptah Operator, and know what it does not cover.
type: reference
audience:
  - "platform-engineer"
readerQuestion: "How does the Ptah Operator deploy a schema change, and what does it not cover?"
goal: "Decide whether the operator fits a deployment, and reach its canonical documentation."
sourceOfTruth:
  - "https://github.com/stokaro/ptah-operator"
generated: false
searchAliases:
  - "Kubernetes operator"
  - "PtahSchema"
  - "PtahMigration"
  - "deploy to Kubernetes"
overlaps:
  - /operate/oci-registry/
disposition: keep
---

[Ptah Operator](https://github.com/stokaro/ptah-operator) is a Kubernetes
control plane that works from schema changes published as OCI artifacts. It is a
separate project with its own repository, releases, and
support matrix; this page exists so the operator is reachable from here and so
its boundary is stated where a reader decides.

## What it reconciles

`PtahSchema` names an OCI artifact and a target database. The controller
resolves the tag to a digest once, verifies the artifact, observes the
database, publishes a plan, applies the approved plan, and then observes again
rather than trusting the apply process's exit.

`PtahMigration` names an artifact holding a versioned migration sequence. The
controller reads the database's own revision table against the artifact, plans
the pending sequence, applies it once approved, and reads the history again. It
is a separate resource with its own API and state machine: one database can have
both, and the two serialize against each other through the same lock.

An executor that runs `ptah migrations up --check-drift --json` gets the drift
check's answer in the run document's `drift_check`: `clean`, `drifted` or
`unable`, with the replayed versions and each difference by object. It runs
under the migration lock before any pending migration, and exit status 1 means
the run applied nothing because the schema drifted.
[Apply migrations](../../versioned/apply/#check-the-schema-against-its-history)
says what each outcome means.

Database work runs in short-lived Jobs. The controller itself has no permission
to read database Secrets, and registry and database credentials are kept apart
from each other. Destructive plans are disabled by default, and enabling them
still requires an approval bound to the exact plan bytes.

## What it does not cover

**Every engine Ptah reaches.** The operator's contract is narrower than the Ptah
CLI's: it runs on PostgreSQL and MySQL, on the release lines its guide lists in
[Databases and privileges](https://operator.ptah.run/edge/support/databases/). For
another engine, deploy with the CLI as [Deliver a schema change](../deliver/)
describes.

**Cleanup.** Deleting or suspending a resource never runs cleanup SQL.

## Maturity

The operator publishes signed releases. Its API is `v1alpha1`, which carries no
compatibility guarantee before v1.

Ptah's version selector on this site does not select an operator version. The
operator tracks its own releases and publishes its own guide, with a selector
of its own, and that guide is the authority on what a given operator version
supports.

## Compatibility

Which Ptah build an operator version runs is the operator's claim, not this
site's, and it changes when the operator measures something new. The operator
publishes it on its own site, in
[Ptah compatibility](https://operator.ptah.run/edge/support/ptah/).

## Its documentation

The operator's guide is a site of its own, with its own versions:
[operator.ptah.run](https://operator.ptah.run/). Installation, the chart values,
the approval model, operations, the security model and the support windows all
live there, and this page deliberately does not restate any of them -- a second
copy of an install command is a second thing to get wrong.

## Watching it work

[Recorded runs](https://operator.ptah.run/demo/) are terminal sessions
captured while the operator ran against a Kubernetes cluster: applying a schema,
changing it, approving a plan, refusing a destructive change, closing drift,
failing and recovering. Each one is checked while it is recorded -- the
conditions the session claims are read off the live objects -- and published
only if every check held, so what a reader watches is a session that happened
rather than a screencast of one.

## Next steps

- [Deliver a schema change](../deliver/) places the operator in the wider path
  from a reviewed change to a verified database.
- [OCI registry artifacts](../oci-registry/) covers publishing the artifact the
  operator consumes.
- [Ptah ecosystem](../../start/ecosystem/) shows where the operator and its
  optional console sit among the other repositories.
