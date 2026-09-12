# Derived data

Files here are copies. Each one names where it came from and is rewritten by
automation; editing one states something the source repository never said.

## `operator-compatibility.json`

The Ptah Operator compatibility catalog. The canonical file is
`support/ptah.json` in [stokaro/ptah-operator](https://github.com/stokaro/ptah-operator),
where it is validated offline by a program that knows the whole contract. This
copy carries the repository, the path, the exact commit it was taken from and
the date, and `docs/site/scripts/publish-compatibility.mjs` renders
`/compatibility/operator/` from it at deploy time.

The copy is committed rather than fetched during a build on purpose: an
ordinary publication of already-accepted documentation must not fail because a
neighboring repository is unreachable.

`.github/workflows/operator-compatibility.yml` keeps it current. It runs daily,
on request, and on a `repository_dispatch` the operator repository sends when
the catalog changes — the schedule is what makes a missed notification late
rather than permanent, because every run compares the two files instead of
replaying an event.

### The credential it needs

The refresh opens one pull request on the branch `automation/operator-compatibility`.
A pull request opened with `GITHUB_TOKEN` receives no checks, and a
documentation change nothing checked is what this must not produce, so the
workflow refuses to open one without a credential of its own.

Configure a repository secret **`OPERATOR_COMPATIBILITY_TOKEN`**: a fine-grained
personal access token or a GitHub App installation token scoped to
`stokaro/ptah` alone, with

- **Contents: read and write** — to push the automation branch;
- **Pull requests: read and write** — to open and update the one pull request.

Nothing more. It needs no access to `stokaro/ptah-operator`: that repository is
public and the refresh reads it through the ordinary API with this repository's
own `GITHUB_TOKEN`.

The fast path, where the operator repository notifies this one, additionally
needs a credential **in that repository** able to send a `repository_dispatch`
here. Without it the daily reconciliation still finds the change; the
notification only makes it prompt.
