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
operator tracks its own releases and publishes its own guide, with a selector
of its own, and that guide is the authority on what a given operator version
supports.

## Compatibility

Which Ptah build an operator version runs is the operator's claim, not this
site's, and it changes when the operator measures something new. It is
published as a current table at
[the operator compatibility matrix](https://docs.ptah.run/compatibility/operator/),
outside this site's per-version archives so that it answers with the present
catalog rather than with whatever was true when a Ptah release shipped.

The table keeps declared support and verified support apart, and says which
combinations nobody has measured. An untested pairing is not an incompatible
one.

## Its documentation

The operator's guide is a site of its own, with its own versions:
[operator.ptah.run](https://operator.ptah.run/). Installation, the chart values,
the approval model, operations, the security model and the support windows all
live there, and this page deliberately does not restate any of them -- a second
copy of an install command is a second thing to get wrong.

## Next steps

- [Deliver a schema change](../deliver/) places the operator in the wider path
  from a reviewed change to a verified database.
- [OCI registry artifacts](../oci-registry/) covers publishing the artifact the
  operator consumes.
