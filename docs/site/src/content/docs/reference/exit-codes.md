---
title: Exit codes
description: How Ptah commands behave when used as automation gates.
---

Ptah uses exit codes as a public scripting contract:

| Code | Meaning |
| --- | --- |
| `0` | Success. |
| `1` | Expected negative result from a check, such as drift, lint findings, pending migrations with `--exit-code`, or migration hash drift. |
| `2` | Command or usage error, including bad flags, invalid input, parse failures, connection failures, unsupported dialects, and recovered internal panics. |
| `3+` | Reserved until documented for a specific use. |

Use the detailed matrix in [CLI exit codes](https://github.com/stokaro/ptah/blob/master/docs/exit_codes.md) when wiring Ptah into CI.

Common gates:

```bash
ptah migrations validate --dir ./migrations
ptah migrations status --db-url "$DATABASE_URL" --migrations-dir ./migrations --exit-code
ptah migrations lint --dir ./migrations --dialect postgres
```

Do not collapse all non-zero outcomes into the same remediation. A `1` usually
means the command successfully found a condition you asked it to check; a `2`
means the command itself did not complete correctly.
