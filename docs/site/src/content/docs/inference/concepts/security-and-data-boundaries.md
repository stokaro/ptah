---
title: Security and data boundaries
description: What leaves your database during a migration, where the credential lives, and what Ptah refuses to expose.
---

A migration sends your data to a third party. This page says exactly what, and
what the boundaries are.

## What leaves the database

The text Ptah builds from the columns you named, for every row in scope, sent to
the endpoint you configured.

Nothing else. Not the primary keys, not the columns you did not name, not the
schema. `plan` reports it before anything runs:

```text
What leaves the database:
  - bge-small-en at https://api.example.com/v1, declared hosted
  - the text of title, body
  - for 48231 rows
```

Read that section before authorizing a run against an endpoint you do not
operate. It is the same information a data-protection review asks for.

## The endpoint class is declared, not measured

```yaml
model:
  endpoint_class: hosted     # local | hosted | gateway
```

Ptah cannot tell from an address who operates it. `endpoint_class` is your
statement about the endpoint, carried into the plan and the generation identity
so that a change of class is a change of corpus. It is documentation with teeth,
not a check.

## The credential is a reference

The specification names *where* the credential is, never what it is:

```yaml
model:
  credential: env:PTAH_EMBED_TOKEN
```

The value is read at run time. What Ptah records — in the plan, in the run state,
in published evidence — is the reference. A specification is a file you can
commit.

## What the agent surface will not return

Ptah exposes two read-only tools to AI clients over MCP and to Ptah Assist:
`inference_plan` and `inference_status`.

Neither returns a source row, a rendered model input, or a vector. What they
report instead is the **names** of the columns whose text would be sent — which
is what somebody deciding whether to authorize a run needs, and is not the text.

The backfill's resume position is also withheld, because over a keyed source it
is a list of source key values, which is row identity rather than progress.

There is deliberately no tool that prepares, backfills, cuts over, rolls back, or
retires. An agent explaining why a cutover is blocked is a different thing from
an agent authorized to unblock it.

## What Ptah does not protect you from

- **A provider that logs your inputs.** That is a contract question with your
  provider, and Ptah has no visibility into it.
- **A model that changed under a stable name.** If the provider exposes no
  immutable revision, `plan` reports the reproducibility as partial and names the
  reason. It cannot detect the change.
- **Rows you did not mean to include.** `source.filter` is what narrows the set,
  and the row count in the plan is what tells you whether it did.

## Telemetry

None. Ptah sends nothing anywhere except the database you name and the embedding
endpoint you configure.
