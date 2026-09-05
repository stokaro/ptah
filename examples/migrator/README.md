# Embedded migration files example

## What this example demonstrates

This package embeds three reversible migration pairs in a Go `embed.FS`. An
application can take the `migrations` subdirectory and register it with Ptah's
migrator without copying SQL files beside the executable.

## Prerequisites

- The Go toolchain declared in the repository's `go.mod`.
- A Ptah checkout with its module dependencies available.

## Run

From the repository root:

```bash
go test ./examples/migrator -run TestExampleMigrations -count=1
```

## Expected result

The command exits 0 and reports the package as `ok`. The test reads six embedded
files: one up and one down file for each of the three revisions.

## Verify

Inspect the public accessor and the embedded directory together:

```bash
go doc ./examples/migrator.GetExampleMigrations
```

The function returns an `embed.FS`; call `fs.Sub` with `migrations` before
registering it with a migrator.

## Cleanup

The test opens no database and writes no files.

## Learn more

Use [Reusable components](https://docs.ptah.run/edge/extend/components/)
for the registration pattern and
[Migration directory](https://docs.ptah.run/edge/concepts/migration-directory/)
for file naming, checksums, and revision state.
