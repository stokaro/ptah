---
title: Troubleshooting
description: Fix common Ptah command, database, Graphviz, hash, config, and conformance problems.
---

## SVG output says Graphviz is required

`ptah viz --format svg` shells out to Graphviz `dot`.

Install Graphviz or use another output format:

```bash
ptah viz --root-dir ./models --format mermaid
ptah viz --root-dir ./models --format dot
```

Expected error when `dot` is missing:

```text
Graphviz dot is required for --format svg; install graphviz or use --format dot
```

## Database connection fails

Check the URL with the smallest command first:

```bash
ptah db read --db-url "$DATABASE_URL"
```

For SQLite, use an absolute file URL:

```bash
ptah db read --db-url sqlite:////tmp/app.db
```

For PostgreSQL-like databases, include database name and credentials in the URL, or provide them through the environment your driver expects.

## Hash validation fails

Regenerate the hash after intentionally changing migrations:

```bash
ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations
```

Do not regenerate `ptah.sum` to hide an accidental edit. Review the migration diff first.

## A dialect capability is unsupported

Check the capability matrix before adding renderer behavior:

```bash
ptah schema render --root-dir ./models --dialect sqlite
ptah schema render --root-dir ./models --dialect postgres
```

Reference: [Capabilities](../reference/capabilities/).

## `atlas.hcl` fails to load

Ptah supports a subset of Atlas project config. Unsupported constructs fail clearly and should be treated as an implementation gap or a config change:

```text
unsupported atlas.hcl construct ...
```

Reference: [Atlas project config subset](https://github.com/stokaro/ptah/blob/master/docs/atlas_project_config.md).

## Conformance reports look red

The conformance repo has two kinds of gates:

- Regression-budget gates should stay green when no new gaps appear.
- Full-conformance gates remain red until the known gaps are closed.

This is intentional. A green regression gate does not mean Ptah has complete Atlas OSS parity.
