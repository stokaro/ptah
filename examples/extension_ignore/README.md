# PostgreSQL extension comparison example

## What this example demonstrates

This program compares in-memory desired and live schemas under each extension
ignore policy. It shows the default `plpgsql` exception, a replacement ignore
list, an additional ignore entry, and the mode that manages every extension.

## Prerequisites

- The Go toolchain declared in the repository's `go.mod`.
- A Ptah checkout with its module dependencies available.

No PostgreSQL server is required.

## Run

From the repository root:

```bash
go run ./examples/extension_ignore
```

## Expected result

Stable output includes:

```text
1. Default Behavior (ignores 'plpgsql'):
2. Custom Ignore List (ignore 'adminpack' only):
3. Additional Ignored Extensions (default + 'adminpack'):
4. Manage All Extensions (no ignoring):
```

The final case reports both `adminpack` and `plpgsql` for removal because its
explicit empty ignore list manages every extension.

## Verify

The repository example gate executes the program and checks all four policy
headings plus the final removal decision.

## Cleanup

The example uses in-memory values and creates no database or generated file.

## Learn more

Use the [PostgreSQL guide](https://docs.ptah.run/edge/databases/postgresql/)
for extension inspection, configuration, and migration behavior.
