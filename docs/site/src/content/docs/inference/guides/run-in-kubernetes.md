---
title: Run in Kubernetes
description: Building a generation as Jobs, holding a rollout until it is ready, and what deleting the resources does not delete.
type: how-to
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How do I run an embedding-generation change in Kubernetes and gate a rollout on it?"
goal: "Run an embedding-generation change as Kubernetes Jobs and gate a rollout on its readiness."
sourceOfTruth:
  - "cmd/inference"
  - "examples/kubernetes"
  - "integration/inference_rollout_gate_e2e_test.go"
generated: false
overlaps: []
disposition: keep
---

There is no operator and no custom resource. A generation change in Kubernetes
is the same verbs as everywhere else, run as Jobs, and the only Kubernetes-shaped
decisions are where the specification comes from, where the secrets come from,
and what holds a rollout back.

Complete manifests are in
[`examples/kubernetes`](https://github.com/stokaro/ptah/tree/master/examples/kubernetes).
This page is why they look the way they do.

## The specification comes from the release, not the cluster

Every pod names one OCI reference and nothing else:

```yaml
env:
  - name: PTAH_RELEASE
    value: oci://ghcr.io/example/search-embeddings@sha256:...
```

The release carries the document it was built from, so there is no ConfigMap to
keep in step with a file in a repository, and no chance of two environments
running two copies of a specification that were meant to be one. Publish it once:

```bash
ptah inference plan --spec spec-v2.yaml --db-url "$DB" \
  --publish-evidence oci://ghcr.io/example/search-embeddings:release
```

Then promote that digest. A mutable tag works too, and what it resolved to is
printed on standard error, which is where a pod's logs keep it.

An air-gapped cluster takes the same flag over a directory: copy the release out
with `ptah oci copy oci://... oci-layout://./release`, carry it across, and point
`PTAH_RELEASE` at `oci-layout:///mnt/release`.

## Secrets stay out of argv

`kubectl get pod -o yaml` prints a container's command line in full, and anything
that can read the namespace can read it. So the two values that must not be
there are not:

- the database URL, which carries a password, arrives as `PTAH_DB_URL`;
- the provider token arrives as whatever variable the specification's
  `credential:` names — `env:PTAH_EMBED_TOKEN`, say — and the specification
  records the reference rather than the value.

Every Ptah flag reads a `PTAH_`-prefixed variable; `--help` prints the name
beside each one.

## Building is init containers, in order

The order is not a preference. The outbox has to exist before the boundary is
recorded, or a change made in between is captured by nothing at all. The index
comes after the backfill, because an IVFFlat index trains its lists on the data
present when it is built.

```yaml
initContainers:
  - { name: prepare,  args: ["inference", "prepare", "--worker", "$(POD_NAME)"] }
  - { name: backfill, args: ["inference", "backfill", "--batch-rows", "500"] }
  - { name: catchup,  args: ["inference", "catchup", "--batch-rows", "500"] }
containers:
  - { name: index,    args: ["inference", "index"] }
```

`restartPolicy: OnFailure` re-runs every container from the first, and that is
safe: `prepare` finds its own run and leaves it alone, `backfill` resumes from
its last committed checkpoint, `catchup` from its watermark. Nothing restarts
from row one, and a provider timeout six hours into a corpus costs the batch it
was in the middle of.

Keep `parallelism: 1`. Two coordinators cannot corrupt one run — the store checks
a fencing token inside the write rather than trusting a lease it read — but a
second pod would spend the provider budget twice.

## The rollout gate is an init container that keeps failing

```yaml
initContainers:
  - name: wait-for-embeddings
    image: ghcr.io/stokaro/ptah:latest
    args: ["inference", "status", "--require-ready"]
```

`--require-ready` exits 1 until the generation is verified and ready to cut over,
so the pod does not start and the rollout does not progress. It exits 0 when both
hold. The report is printed either way, so `kubectl logs -c wait-for-embeddings`
says what is missing:

```text
verified: false, cutover ready: false
  - blocked: the required index is absent, invalid or still building
```

The exit code is 1 rather than 2 on purpose. A gate that could not tell "the
condition you asked about is not met" from "this command did not run" would treat
a typo in a database URL as a corpus that is not ready yet, and would wait for it
forever.

The two answers are measured rather than read off the run, so each attempt costs
what `verify` costs: a read of the source and the target. An init container is
naturally rate-limited — Kubernetes backs its restarts off exponentially — but a
loop you write yourself against a corpus of millions of rows should not poll
every few seconds against the database the backfill is still writing to.

For a rollout system that reads structured output rather than a status,
`--format json` carries the same answer:

```json
{
  "readiness": {
    "verified": true,
    "cutover_ready": true,
    "approval_required": true,
    "plan_digest": "…",
    "blockers": []
  }
}
```

### The approval is not part of readiness

`approval_required` is reported beside the two conditions rather than folded into
them. A generation waiting for a person to sign is finished; a gate that waited
for the signature would hold a deployment for something given in the same breath
as the cutover, and would never open.

So the gate waits for the state, and a person approves the cutover.

## Cutover is its own Job, and its digest is not a template variable

```yaml
args:
  - inference
  - cutover
  - --approve
  - "<the digest ptah inference status reported>"
  - --approver
  - "<who approved it>"
  - --stabilize-for
  - 24h
```

A pipeline that read the digest and passed it to the same run has approved
nothing. The shape is: read it, have somebody approve it, put it in the manifest,
apply the manifest. Any change to the evidence produces a different plan, and the
approval stops applying — which is what binding it to a digest is for.

`--stabilize-for` is a promise rather than a mechanism. Keeping the previous
generation a way back means catching it up on a schedule, which is the CronJob
beside the cutover Job in the example. Stop it and the window elapses over a
generation that drifted; `rollback` refuses it, correctly.

## Deleting the resources deletes no vectors

`kubectl delete` on every manifest here removes no generation, no run state and
no embedding. All of it is in your database. `ptah inference retire` is the only
thing that destroys a generation, and it needs its own approval.

That is deliberate rather than an oversight. A cluster being torn down, a
namespace being cleaned up, or a GitOps controller pruning a resource must not be
able to take a corpus with it.

## What Ptah does not own here

Application traffic. Ptah moves a pointer in its own tables and answers questions
about state; your Deployment, your Service and whatever rollout controller you
run decide where requests go. Connecting the pointer to the SQL your application
runs is yours — see
[Support and limitations](../../reference/support-and-limitations/).

## What is checked, and what is not

`TestInferenceRolloutGateE2E` measures the gate against a live PostgreSQL: exit 1
before the state is there, exit 0 after, and the same answer with the database
URL in the environment and an empty argv.

`TestKubernetesExample_EveryCommandIsOneThisBuildHas` reads the `args` of every
container in the example manifests and checks each against the command tree, so
a verb or flag renamed in the code cannot leave a manifest naming the old
spelling.

No cluster runs these manifests in CI. Their structure — the resource kinds, the
ordering, the secret references — is a reading responsibility.
