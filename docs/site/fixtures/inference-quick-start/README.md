# Inference quick-start fixture

This fixture builds one verified embedding generation against disposable
PostgreSQL 17 with pgvector 0.8.6. The local provider returns deterministic
four-dimensional vectors and sends no input to an external service.

## Prerequisites

- `ptah` on `PATH`;
- Docker with Compose;
- Bash for `run.sh`, or Windows PowerShell 5.1 or later for `run.ps1`.

Docker must be able to run Linux containers. The scripts pass an explicit
Docker context. They use `default` unless `PTAH_DOCKER_CONTEXT` names another
context.

## Run the complete fixture

Bash:

```bash
./run.sh all
```

PowerShell:

```powershell
.\run.ps1 all
```

The complete run removes its services after it verifies the active pointer.
Use `run.sh up` or `run.ps1 up` instead when following the detailed tutorial
from the documentation version that supplied this archive.

Expected output includes these stable lines:

```text
backfill finished: 3 scanned, 3 embedded, 0 skipped
every deterministic layer passed
queries now read generation
```

## Configuration

Set these environment variables before a command when the defaults conflict
with another local service:

| Variable | Default | Purpose |
| --- | --- | --- |
| `PTAH_DOCKER_CONTEXT` | `default` | Docker context used by every Compose command |
| `PTAH_FIXTURE_HOST` | `127.0.0.1` | Host that `ptah` uses for both services |
| `PTAH_INFERENCE_POSTGRES_PORT` | Docker chooses | PostgreSQL host port |
| `PTAH_INFERENCE_EMBED_PORT` | Docker chooses | Embeddings provider host port |
| `PTAH_INFERENCE_PROJECT` | `ptah-inference-quick-start` | Compose project and resource prefix |
| `PTAH_BIN` | `ptah` | Installed Ptah executable |

`run.sh up` and `run.ps1 up` write the resolved provider URL to
`.ptah-inference/spec.yaml`. The source `spec.yaml` remains a template so one
archive works with any selected ports.

## Clean up

Bash:

```bash
./cleanup.sh
```

PowerShell:

```powershell
.\cleanup.ps1
```

Cleanup removes the fixture containers, project network, anonymous database
volume, locally built images, and `.ptah-inference` runtime directory.
