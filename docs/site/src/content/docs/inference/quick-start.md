---
title: Quick start
description: Build, verify, and activate an embedding generation with a downloadable PostgreSQL and local-provider fixture.
type: tutorial
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How do I run and verify one complete inference migration?"
goal: "Build, verify, and activate one embedding generation without an external model service."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
  - "docs/site/fixtures/inference-quick-start"
generated: false
lastVerified: "2026-08-30"
searchAliases:
  - pgvector
  - inference migration tutorial
  - local embedding model
overlaps:
  - "/inference/guides/create-first-generation/"
  - "/inference/concepts/lifecycle/"
disposition: keep
---

Build and activate one embedding generation without cloning the Ptah
repository or sending data to an external model service. The downloadable
fixture supplies PostgreSQL 17 with pgvector 0.8.6, three source rows, and a
deterministic local embeddings API.

The **candidate generation** receives new vectors while the application keeps
using the **active generation**. Only cutover changes the active pointer. This
empty fixture has no older generation, so its first candidate becomes active.

## What you need

- Ptah [installed](../../start/install/) as `ptah` on `PATH`;
- Docker with Compose and access to Linux containers;
- Bash, or Windows PowerShell 5.1 or later;
- a ZIP extractor.

Left alone, the fixture publishes PostgreSQL and the embeddings provider on host
ports Docker chooses, and `run.sh up` prints the addresses it got. The steps
below pin them instead, with `PTAH_INFERENCE_POSTGRES_PORT` and
`PTAH_INFERENCE_EMBED_PORT`, so that every command on this page can quote one
fixed URL — if either port is taken on your machine, change the value and start
again. The cleanup step removes the containers, project network, database
volume, locally built images, and extracted fixture.

## 1. Download the fixture

<a href="../../samples/inference-quick-start.zip" download data-ptah-inference-archive>Download the inference quick-start archive</a>
and its
<a href="../../samples/inference-quick-start.zip.sha256" download data-ptah-inference-checksum>SHA-256 checksum</a>.
Both links stay within the documentation version you selected.

Verify and extract the archive with your shell.

**Bash on Linux:**

```bash
sha256sum -c inference-quick-start.zip.sha256
unzip inference-quick-start.zip
cd inference-quick-start
```

On macOS, replace the first command with
`shasum -a 256 -c inference-quick-start.zip.sha256`.

**PowerShell:**

```powershell
$expected = (Get-Content .\inference-quick-start.zip.sha256).Split()[0]
$actual = (Get-FileHash .\inference-quick-start.zip -Algorithm SHA256).Hash.ToLowerInvariant()
if ($actual -ne $expected) { throw "inference fixture checksum mismatch" }
Expand-Archive .\inference-quick-start.zip -DestinationPath .
Set-Location .\inference-quick-start
```

The extracted directory contains the Compose project, database initialization,
provider source, specification template, and Bash and PowerShell helpers. It
has no repository-relative path.

## 2. Start PostgreSQL and pgvector

Pin the ports and name the project before startup. A distinct project name lets
two extracted fixtures run without sharing Compose resources; the ports are
pinned here so the URLs below are the same for every reader.

**Bash:**

```bash
export PTAH_INFERENCE_POSTGRES_PORT=55432
export PTAH_INFERENCE_EMBED_PORT=58080
export PTAH_INFERENCE_PROJECT=ptah-inference-quick-start
./run.sh up

export PTAH_SPEC="$PWD/.ptah-inference/spec.yaml"
export PTAH_DB_URL="postgres://ptah:ptah@127.0.0.1:${PTAH_INFERENCE_POSTGRES_PORT}/ptah?sslmode=disable"
export PTAH_RUN_ID=quick-start
```

**PowerShell:**

```powershell
$env:PTAH_INFERENCE_POSTGRES_PORT = '55432'
$env:PTAH_INFERENCE_EMBED_PORT = '58080'
$env:PTAH_INFERENCE_PROJECT = 'ptah-inference-quick-start'
.\run.ps1 up

$env:PTAH_SPEC = (Resolve-Path .\.ptah-inference\spec.yaml).Path
$env:PTAH_DB_URL = "postgres://ptah:ptah@127.0.0.1:$($env:PTAH_INFERENCE_POSTGRES_PORT)/ptah?sslmode=disable"
$env:PTAH_RUN_ID = 'quick-start'
```

The helper passes the selected ports to Compose and writes their provider URL
to `.ptah-inference/spec.yaml`. It passes an explicit Docker context named by
`PTAH_DOCKER_CONTEXT`, or `default` when the variable is absent. Compose reports
both `postgres` and `embeddings` as healthy.

The remaining `ptah` commands read `PTAH_SPEC`, `PTAH_DB_URL`, and
`PTAH_RUN_ID` through their normal flag environment bindings.

## 3. Review the plan

```console
ptah inference plan
```

Expected output includes these stable facts:

```text
source.estimated_rows = 3 (measured)
target.capability.vector_type = true (measured)
[backfill] embed 3 in-scope source rows
Consistency mode: outbox
```

The plan is read-only. It measures the source rows and confirms that the target
database provides the required vector type.

## 4. Prepare and fill the candidate

```console
ptah inference prepare
ptah inference backfill --batch-rows 10
```

Expected output from backfill:

```text
backfill finished: 3 scanned, 3 embedded, 0 skipped
```

Ptah writes the candidate vectors, generation identity, source version, input
hash, and state. Nothing has cut over.

## 5. Catch up, index, and verify

```console
ptah inference catchup --batch-rows 10
ptah inference index
ptah inference verify
```

Expected verification output includes:

```text
3 source rows, 3 target rows
every deterministic layer passed
```

A passing report makes the generation eligible for cutover. It does not
activate the generation.

Inspect the recorded run and candidate rows:

```console
ptah inference status
```

**Bash:**

```bash
./run.sh rows
```

**PowerShell:**

```powershell
.\run.ps1 rows
```

The query returns three rows. Each row has the same nonempty generation
identity and the state `upsert`.

## 6. Bind approval and cut over

Capture the digest from a deliberately unapproved `ptah inference cutover`.
The helper prints the refusal and returns only its `plan <digest>` value.

**Bash:**

```bash
export PTAH_APPROVE="$(./run.sh approval-digest)"
test -n "$PTAH_APPROVE"
```

**PowerShell:**

```powershell
$env:PTAH_APPROVE = & .\run.ps1 approval-digest
if (-not $env:PTAH_APPROVE) { throw "cutover printed no plan digest" }
```

Bind the approval to that digest. `PTAH_APPROVE` supplies the `--approve` flag.

```console
ptah inference cutover --approver "quick-start operator"
```

Expected output includes `queries now read generation` and the approved plan
digest. This first run has no previous generation to retain as a rollback
target.

## 7. Verify and clean up

Read the pointer that makes the generation active.

**Bash:**

```bash
./run.sh pointer
```

**PowerShell:**

```powershell
.\run.ps1 pointer
```

The row for `docs` names the generation from the candidate rows. The pointer,
not completion of backfill or verification, changes what queries read.

Return to the directory that contains the extracted fixture, then remove every
fixture resource and the extracted files. Run the cleanup helper even when an
earlier step fails.

**Bash:**

```bash
./cleanup.sh
cd ..
rm -rf inference-quick-start
```

**PowerShell:**

```powershell
.\cleanup.ps1
Set-Location ..
Remove-Item .\inference-quick-start -Recurse -Force
```

Next, use [Migrate to another model](../guides/migrate-to-another-model/) to
create a second generation, or read [Generations](../concepts/generations/) for
the active, candidate, previous, and retired state model.
