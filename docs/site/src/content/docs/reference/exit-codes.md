---
title: Exit codes
description: How Ptah commands behave when used as automation gates.
---

Ptah uses exit code `0` for success and `1` for command-specific failure states. `migrations status --exit-code` also returns `1` when pending migrations exist.

Use the detailed matrix in [CLI exit codes](https://github.com/stokaro/ptah/blob/master/docs/exit_codes.md) when wiring Ptah into CI.

Common gates:

```bash
ptah migrations validate --dir ./migrations
ptah migrations status --db-url "$DATABASE_URL" --migrations-dir ./migrations --exit-code
ptah migrations lint --dir ./migrations --dialect postgres
```
