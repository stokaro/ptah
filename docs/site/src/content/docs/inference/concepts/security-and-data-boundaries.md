---
title: Security and data boundaries
description: What leaves your database during a migration, where the credential lives, and what Ptah refuses to expose.
type: concept
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How does Ptah model security and data boundaries?"
goal: "Explain Ptah's model for security and data boundaries."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
overlaps: []
disposition: keep
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

`endpoint` is held to the same rule, because it reaches the same wire. A URL
written with userinfo — `https://user:secret@api.example.com/v1` — becomes an
`Authorization: Basic` header on every provider request, so it is a credential
that arrived through the other field. Ptah refuses such a specification:

```text
spec.yaml: model.endpoint carries a credential in its userinfo, before the
"api.example.com" host; a key must not appear in project configuration, so put
it in model.credential as env:NAME or file:/path
```

The refusal is at the document rather than at the request, so it holds for every
verb, and for a release fetched with `--release` as well as a file read with
`--spec`. The message names the host and not the URL: an error that quoted the
value back would write the credential to your terminal and into whatever
collects the log.

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
