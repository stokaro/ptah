# Kubernetes inference migration example

## What this example demonstrates

These manifests run an embedding-generation change as Kubernetes Jobs and hold
an application's rollout until the generation is verified and ready to cut over.
The specification is not in the cluster: each pod names one OCI release digest,
and the release carries the document it was built from.

There is no operator and no custom resource. The four files are a Job that
builds the generation, a Job that measures it, an init container that gates a
Deployment, and a Job that moves the pointer on an approval bound to one exact
plan.

## Prerequisites

- A Kubernetes cluster and `kubectl`.
- A PostgreSQL database with the pgvector extension, reachable from the cluster.
- An OCI registry holding a release published by
  `ptah inference plan --publish-evidence`.
- An embedding endpoint the cluster can reach.

Nothing here needs a Ptah account, a hosted control plane, or a Ptah-run
endpoint.

## Run

Replace the two secret values, the release digests, and the registry
references, then apply the files in order. Each step is a decision, so they are
applied one at a time rather than as a directory:

```bash
kubectl apply -f examples/kubernetes/00-secrets.yaml
kubectl apply -f examples/kubernetes/10-build-job.yaml
kubectl wait --for=condition=complete job/ptah-inference-build --timeout=6h

kubectl apply -f examples/kubernetes/20-verify-job.yaml
kubectl wait --for=condition=complete job/ptah-inference-verify --timeout=30m

kubectl apply -f examples/kubernetes/30-rollout-gate.yaml
```

Read the plan digest to approve, put it in `40-cutover-job.yaml`, and apply that
file last:

```bash
kubectl run ptah-status --rm -it --restart=Never \
  --image=ghcr.io/stokaro/ptah:latest \
  --env=PTAH_RELEASE=oci://ghcr.io/example/search-embeddings@sha256:... \
  --env=PTAH_RUN_ID=articles-v2 \
  --env=PTAH_DB_URL="$(kubectl get secret ptah-inference-database -o jsonpath='{.data.url}' | base64 -d)" \
  -- inference status --format json
```

## Expected result

The build Job completes when the corpus is embedded, caught up and indexed. The
verify Job completes when every deterministic layer passes, and fails with exit
1 when one does not — the report is in its logs either way, and attached to the
release in the registry.

The gated Deployment stays at zero ready replicas while
`wait-for-embeddings` exits 1. Its logs say what is missing:

```text
verified: false, cutover ready: false
  - blocked: the required index is absent, invalid or still building
```

When the generation is ready, that container exits 0 and the rollout proceeds.
The approval is reported separately and does not hold the gate.

## Verify

```bash
kubectl logs job/ptah-inference-verify
kubectl get deployment search-api -o jsonpath='{.status.readyReplicas}'
```

`TestInferenceRolloutGateE2E` measures the contract these manifests depend on
against a live database: `--require-ready` exits 1 before the state is there and
0 after, and the same answer comes back with the database URL in the environment
and an empty argv.

`TestKubernetesExample_EveryCommandIsOneThisBuildHas` reads the `args` of every
container in these files and checks each one against the command tree, so a verb
or a flag renamed in the code cannot leave a manifest naming the old spelling.

## Cleanup

```bash
kubectl delete -f examples/kubernetes/40-cutover-job.yaml
kubectl delete -f examples/kubernetes/30-rollout-gate.yaml
kubectl delete -f examples/kubernetes/20-verify-job.yaml
kubectl delete -f examples/kubernetes/10-build-job.yaml
kubectl delete -f examples/kubernetes/00-secrets.yaml
```

Deleting every one of these removes no vector and no generation. The run state,
the registry rows and the embeddings are in your database, and
`ptah inference retire` is the only thing that destroys them — deliberately, so
that a cluster being torn down cannot take a corpus with it.

## Learn more

- [Run in Kubernetes](../../docs/site/src/content/docs/inference/guides/run-in-kubernetes.md)
- [Production rollout](../../docs/site/src/content/docs/inference/strategies/production-rollout.md)
- [Exit codes](../../docs/exit_codes.md)
