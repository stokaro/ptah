---
title: Atlas-compatible output and redaction
description: Template data shapes and credential redaction rules for ptah-compat format output.
type: reference
audience:
  - "atlas-migrator"
readerQuestion: "What data does ptah-compat expose to format templates, and how are URLs redacted?"
goal: "Write an Atlas-compatible format template without exposing URL credentials."
sourceOfTruth:
  - "cmd/atlas"
  - "internal/atlasreport"
generated: false
searchAliases:
  - Atlas format template
  - ptah compat JSON URL
overlaps:
  - "/atlas/migrate-commands/"
  - "/atlas/schema-commands/"
disposition: keep
---

Atlas-compatible `--format` output uses the Atlas data shape expected by
existing templates. Each command page lists the fields its own report exposes;
this page owns the shared URL and redaction behavior.

## URL values

A URL rendered directly in a Go template is a redacted string:

```text
{{ .Env.URL }}
```

`{{ json . }}` emits an Atlas-shaped URL object with these fields where they
apply:

```text
Scheme, User, Host, Path, RawQuery, Fragment, RawPath,
RawFragment, ForceQuery, OmitHost, Schema
```

`Schema` is present for SQLite URLs.

## Redaction

Ptah removes URL userinfo passwords. Query parameters whose keys look like
passwords, tokens, secrets, or API keys are replaced with `xxxxx` before the URL
reaches template output.

Redaction protects the recognized URL fields. A template can still print any
other data the command intentionally exposes, so review custom formats before
using them in shared CI logs.

## Per-command fields

- [Atlas migrate commands](../migrate-commands/#format-template-fields)
- [Atlas schema commands](../schema-commands/#format-template-fields)
