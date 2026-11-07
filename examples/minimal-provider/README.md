# Minimal provider example

<!-- ptah:quoted-by docs/site/src/content/docs/extend/write-a-provider/tutorial.md -->
<!-- ptah:quoted-by docs/site/src/content/docs/extend/write-a-provider/testing.md -->
<!-- ptah:quoted-by docs/site/src/content/docs/extend/write-a-provider/sdk-reference.md -->

## What this example demonstrates

This module is a complete executable provider for an engine named `tutorial`,
built with the provider SDK alone. Ptah starts it, reads a database through it,
plans a YAML schema, and applies the plan through its rendering, execution and
lock services. A tutorial database is a SQLite file, so the example needs no
database server.

The [provider tutorial](https://docs.ptah.run/edge/extend/write-a-provider/tutorial/)
builds this module step by step, and every Go block on that page is a verbatim
excerpt of a file here.

## Prerequisites

- The Go toolchain this module's `go.mod` declares.
- A `ptah` binary on `PATH`.
- `shasum`, or another program that prints a SHA-256 digest.

## Run

From this directory, build the provider, write a manifest that trusts the
build, and run the commands the tutorial runs:

```bash
go build -o ptah-tutorial-provider .
digest=$(shasum -a 256 ptah-tutorial-provider | cut -d ' ' -f 1)
cat > providers.json <<EOF
{
  "format_version": 2,
  "providers": [
    {
      "executable": "$PWD/ptah-tutorial-provider",
      "sha256": "$digest",
      "provider_id": "example.com/ptah-tutorial-provider",
      "engines": [{"id": "tutorial", "aliases": []}],
      "environment": []
    }
  ]
}
EOF
export DATABASE_URL="tutorial://$PWD/app.db"
ptah providers check --provider-config providers.json --provider-engine tutorial
ptah schema plan --provider-config providers.json \
  --db-url "$DATABASE_URL" --schema-file schema.yaml --output change.plan.json
ptah schema apply --provider-config providers.json \
  --db-url "$DATABASE_URL" --plan change.plan.json --tx-mode none --auto-approve
```

## Expected result

`providers check` reports the provider as compatible. The plan holds one
statement, `CREATE TABLE "notes" ("id" INTEGER NOT NULL PRIMARY KEY, "body"
TEXT);`, and `schema apply` reports `Schema apply completed successfully.`

## Verify

Plan the same schema again:

```bash
ptah schema plan --provider-config providers.json \
  --db-url "$DATABASE_URL" --schema-file schema.yaml --dry-run --json
```

The report says `"outcome": "no-changes"`. Run the module's own tests, which
drive the services directly and through a provider process, and run the SDK's
conformance suite: offline against the server in process, and in full against
the executable and a database file:

```bash
GOWORK=off go test -race ./...
GOWORK=off go test -race -tags integration ./integration
```

## Cleanup

```bash
rm -f ptah-tutorial-provider providers.json change.plan.json app.db app.db.lock-*
```

## Learn more

Use [Write a provider](https://docs.ptah.run/edge/extend/write-a-provider/tutorial/)
for the walkthrough, and the
[provider SDK reference](https://docs.ptah.run/edge/extend/write-a-provider/sdk-reference/)
for the services this example leaves out.
