---
title: Kubernetes operator
description: Reconcile a desired schema from an OCI artifact with the Ptah Operator, and know what it does not deploy.
type: reference
audience:
  - "platform-engineer"
readerQuestion: "How does the Ptah Operator deploy a schema, and what does it not cover?"
goal: "Decide whether the operator fits a deployment, and reach its canonical documentation."
sourceOfTruth:
  - "https://github.com/stokaro/ptah-operator"
generated: false
searchAliases:
  - "Kubernetes operator"
  - "PtahSchema"
  - "deploy to Kubernetes"
overlaps:
  - /operate/oci-registry/
disposition: keep
sourceMode: oci-artifact-only
---

[Ptah Operator](https://github.com/stokaro/ptah-operator) is a Kubernetes
control plane that converges a database on a desired schema published as an OCI
artifact. It is a separate project with its own repository, releases, and
support matrix; this page exists so the operator is reachable from here and so
its boundary is stated where a reader decides.

## What it reconciles

`PtahSchema` names an OCI artifact and a target database. The controller
resolves the tag to a digest once, verifies the artifact, observes the
database, publishes a plan, applies the approved plan, and then observes again
rather than trusting the apply process's exit.

Database work runs in short-lived Jobs. The controller itself has no permission
to read database Secrets, and registry and database credentials are kept apart
from each other. Destructive plans are disabled by default, and enabling them
still requires an approval bound to the exact plan bytes.

## What it does not deploy

**A versioned migration directory.** `PtahSchema` reconciles a desired schema.
The operator's own documentation states that `PtahMigration` is deliberately
not folded into it: a versioned-migration controller would reuse the OCI
transport, credential isolation and execution protocol while keeping its own
API and state machine, and it does not exist yet.

So a team on the versioned workflow deploys with `ptah migrations up` against a
pinned artifact, as [Deliver a schema change](../deliver/) describes. Running
the operator does not make that step unnecessary, and there is no resource that
applies a migration directory today.

## Maturity

The API is `v1alpha1`, and the operator's README calls it an implementation
preview until its database end-to-end matrix is green and a release is
published. Databases are PostgreSQL and MySQL.

Ptah's version selector on this site does not select an operator version. The
operator tracks its own releases, and its documentation is the authority on
what a given operator version supports.

## Installation

The chart requires the manager, runner and executor images as immutable
SHA-256 references, and the Ptah executor version supplied explicitly rather
than inferred from a tag:

```sh
helm upgrade --install ptah-operator ./charts/ptah-operator \
  --namespace ptah-system \
  --create-namespace \
  --set-string image.digest=sha256:<operator-image-digest> \
  --set-string execution.runnerImage=ghcr.io/stokaro/ptah-operator@sha256:<operator-image-digest> \
  --set-string execution.executorImage=ghcr.io/stokaro/ptah@sha256:<ptah-image-digest> \
  --set-string execution.ptahVersion=<ptah-version>
```

Verify both the executor digest and the version it claims against the
executor's release provenance before installing. The supplied version is
recorded in plans, approvals, Jobs and applied status, so a digest change means
verifying and supplying its version again.

## Canonical documentation

The operator repository owns the detail, and this page does not restate it:

- [Architecture and state machine](https://github.com/stokaro/ptah-operator/blob/main/docs/architecture.md)
- [Security model](https://github.com/stokaro/ptah-operator/blob/main/docs/security.md)
- [Exact-plan approvals](https://github.com/stokaro/ptah-operator/blob/main/docs/approvals.md)
- [Operations and failure recovery](https://github.com/stokaro/ptah-operator/blob/main/docs/operations.md)
- [Condition reason contract](https://github.com/stokaro/ptah-operator/blob/main/docs/condition-reasons.md)
- [Kubernetes support policy](https://github.com/stokaro/ptah-operator/blob/main/docs/kubernetes-support.md)
- [Database support and privileges](https://github.com/stokaro/ptah-operator/blob/main/docs/database-support.md)
- [Releases and provenance](https://github.com/stokaro/ptah-operator/blob/main/docs/releases.md)

## Next steps

- [Deliver a schema change](../deliver/) places the operator in the wider path
  from a reviewed change to a verified database.
- [OCI registry artifacts](../oci-registry/) covers publishing the artifact the
  operator consumes.
