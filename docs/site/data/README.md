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

The commit it records is the one that last changed `support/ptah.json`, not
whatever the operator's default branch points at. The difference is the whole
cadence: the operator takes commits daily, and against its head the copy would
be stale every day and every run would want a pull request rewriting one field.
Against the catalog's own revision the copy is stale when the catalog moved.

### The credential it needs

The refresh opens one pull request on the branch `automation/operator-compatibility`.
A pull request opened with `GITHUB_TOKEN` receives no checks, and a
documentation change nothing checked is what this must not produce, so the
workflow refuses to open one without a credential of its own.

Either of two shapes works, and the workflow prefers the first:

- a **GitHub App**, as the variable `OPERATOR_COMPATIBILITY_APP_ID` with the
  secret `OPERATOR_COMPATIBILITY_APP_KEY`. The token is minted per run and lives
  an hour, and the job is capped well inside that;
- a repository secret **`OPERATOR_COMPATIBILITY_TOKEN`**, a fine-grained
  personal access token. It is a standing credential somebody has to rotate,
  and an expired one fails the run at the push rather than at a presence check.

Either way the access is `stokaro/ptah` alone, with

- **Contents: read and write** — to push the automation branch;
- **Pull requests: read and write** — to open and update the one pull request.

Nothing more. Neither needs access to `stokaro/ptah-operator`: that repository is
public and the refresh reads it through the ordinary API with this repository's
own `GITHUB_TOKEN`.

Until one is configured, a run that finds the catalog changed fails and says so.
The copy can also be refreshed by hand — the workflow's own script does it:

```sh
gh api "repos/stokaro/ptah-operator/contents/support/ptah.json?ref=<commit>" \
  --header 'Accept: application/vnd.github.raw+json' >/tmp/ptah.json
node docs/site/scripts/refresh-compatibility.mjs \
  --source-commit <commit> --relation ahead --catalog /tmp/ptah.json
```

The fast path, where the operator repository notifies this one, additionally
needs a credential **in that repository** able to send a `repository_dispatch`
here. Without it the daily reconciliation still finds the change; the
notification only makes it prompt.
