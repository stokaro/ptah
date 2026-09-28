---
title: Trace migration runs
description: Build ptah with the observability build tag, send the spans of migrations up, down and status to an OTLP receiver, and read what each run records.
type: how-to
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "How do I send a migration run's traces to OpenTelemetry?"
goal: "Build a ptah that exports migration spans over OTLP/HTTP and find each run's spans in a tracing backend."
sourceOfTruth:
  - "internal/cli/cliobs/otel_observability.go"
  - "internal/cli/cliobs/otel_observability_test.go"
  - "internal/cli/cliobs/otel_export_failure_test.go"
  - "migration/migrator/migrator.go"
  - "migration/migrator/advisory_lock.go"
generated: false
searchAliases:
  - "opentelemetry"
  - "otlp"
overlaps: []
disposition: keep
---

You run `ptah migrations up`, `down` and `status` in a pipeline, and you want
each run as a trace in the backend that already collects your services' traces.
`ptah` exports migration spans over OTLP/HTTP, but only when it is built with
the `observability` build tag. The release archives, the Homebrew formulas, the
container image and the installer are built without it, so they cannot export
traces.

For logs and metrics you need no special build: `--log-format` and
`--log-level` shape the run log, and `--metrics-addr` serves Prometheus
metrics. See [Apply migrations](../../versioned/apply/).

## Prerequisites

- A Go toolchain.
- A receiver that takes OTLP over HTTP, such as an OpenTelemetry Collector with
  the HTTP protocol of its `otlp` receiver enabled. Its default port is `4318`.
  `ptah` has no gRPC exporter.
- A migration directory and a database. The commands below use the SQLite
  example from [Apply migrations](../../versioned/apply/).

## Build ptah with tracing

Install a release with the tag:

```bash illustration
go install -tags observability ptah.run/cmd/ptah@vX.Y.Z
```

Or build from a checkout of the repository:

```bash illustration
go build -tags observability -o bin/ptah ./cmd/ptah
```

The binary records its build tags. Check that the tag is there before you rely
on it:

```bash illustration
go version -m bin/ptah | grep -- -tags
```

Expected output on standard output:

```text illustration
	build	-tags=observability
```

A binary built without the tag prints nothing here, and it ignores every
variable on this page.

## Send a run's spans

Point `OTEL_EXPORTER_OTLP_ENDPOINT` at the receiver and run the command as
usual:

```bash illustration
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
ptah migrations up --db-url "sqlite://app.db" --migrations-dir ./migrations
```

`ptah` sends the spans to `$OTEL_EXPORTER_OTLP_ENDPOINT/v1/traces`. It collects
them during the run and sends them when the command ends, so a run appears in
the backend after the command exits.

If your receiver takes traces at another path, set
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` to the full URL instead. `ptah` uses that
URL as written, and either variable on its own starts the export.

Without either variable, `ptah` exports nothing, even when it was built with
the tag. A variable that is empty or holds only spaces counts as unset.

## Find the run in the backend

Each command reports as its own service, and every span carries the
instrumentation scope `ptah.run`:

| Command | `service.name` |
| --- | --- |
| `ptah migrations up` | `ptah.migrations.up` |
| `ptah migrations down` | `ptah.migrations.down` |
| `ptah migrations status` | `ptah.migrations.status` |

`OTEL_RESOURCE_ATTRIBUTES` adds resource attributes, for example
`deployment.environment=staging`. `OTEL_SERVICE_NAME` has no effect, because
`ptah` sets `service.name` itself.

A run of `migrations up` that applies one migration sends these spans:

```text illustration
ptah.migrate.status           the state before the run
ptah.migrate.up
├── ptah.lock.acquire
└── ptah.migrate.apply        one span per migration
ptah.migrate.status           the state after the run
```

`migrations down` has the same shape, with `ptah.migrate.down` and one
`ptah.migrate.rollback` per migration. `migrations status` sends one
`ptah.migrate.status` span. When there is nothing to apply or roll back,
`ptah.migrate.up` or `ptah.migrate.down` is sent alone, with
`migration.pending_count` set to `0`.

| Span | Attributes |
| --- | --- |
| `ptah.migrate.up` | `db.system`, `migration.direction`, `migration.current_version`, `migration.target_version`, `migration.pending_count`, `lock.wait_ms` |
| `ptah.migrate.down` | the same as `ptah.migrate.up`, and `migration.requested_target_version` |
| `ptah.migrate.apply`, `ptah.migrate.rollback` | `db.system`, `migration.direction`, `migration.version`, `migration.description` |
| `ptah.lock.acquire` | `db.system`, `migration.operation`, `lock.name`, `lock.timeout_ms`, `lock.wait_ms` |
| `ptah.migrate.status` | `db.system`, `migration.current_version`, `migration.pending_count`, `migration.total_count`, `migration.out_of_order_count` |

A span whose operation failed has an error status that carries the error
message, and an `exception` event. A failed migration marks both its
`ptah.migrate.apply` span and the `ptah.migrate.up` span above it.

## Configure the exporter

`ptah` reads the standard OpenTelemetry variables for the OTLP/HTTP exporter.
Each `OTEL_EXPORTER_OTLP_*` variable below also has a traces-only form, such as
`OTEL_EXPORTER_OTLP_TRACES_HEADERS`, which takes precedence.

| Variable | Effect |
| --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | The receiver's base URL; spans go to its `/v1/traces` path. `http://` sends in plain text and `https://` uses TLS. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | The full URL for traces, used as written. It starts the export on its own, and it wins when both endpoint variables are set. |
| `OTEL_EXPORTER_OTLP_HEADERS` | Headers for every export request, as comma-separated `key=value` pairs, for example an authorization token. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf`, the default, or `http/json`. |
| `OTEL_EXPORTER_OTLP_COMPRESSION` | `gzip`, or `none`, the default. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | The time limit for one export request, in milliseconds. The default is `10000`. |
| `OTEL_EXPORTER_OTLP_CERTIFICATE` | A PEM file with the certificate authority to trust for TLS. |
| `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE`, `OTEL_EXPORTER_OTLP_CLIENT_KEY` | A PEM client certificate and its key, for a receiver that asks for one. |
| `OTEL_RESOURCE_ATTRIBUTES` | Extra resource attributes, as comma-separated `key=value` pairs. |

## When no spans arrive

A tracing failure never fails the migration: the command's exit status is the
same with or without a receiver.

- **The binary was built without the tag.** `go version -m` shows no
  `-tags=observability`. Rebuild it as shown above.
- **The receiver refuses the connection or rejects the spans.** `ptah` logs
  `OpenTelemetry tracing failed` at the `warn` level, with the exporter's error:
  the refused connection, or the HTTP status the receiver answered with. The
  record stays visible at `--log-level warn`.
- **The receiver does not answer.** `ptah` waits up to five seconds after the
  run for the export to finish. Then it logs `failed to shut down
  observability` at the `warn` level, and the spans are lost.
- **The receiver speaks only gRPC.** Enable the HTTP protocol on it, or put a
  Collector in front of it.

## Next steps

- To gate a release on the data it leaves behind, see
  [Verify a release against the database](../verify-a-release/).
- For the run log and Prometheus metrics, see
  [Apply migrations](../../versioned/apply/).
