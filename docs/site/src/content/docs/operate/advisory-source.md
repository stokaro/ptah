---
title: Choose the advisory source
description: Select where the vulnerability scan reads reviewed advisory snapshots, and what a copy fetched from Ptah's repository may change.
type: how-to
audience:
  - "database-engineer"
  - "platform-engineer"
readerQuestion: "Where does the vulnerability scan read its advisories, and what happens when that source cannot be used?"
goal: "Select the advisory source of a scan, read which snapshot a report used, and recover from a copy the cache keeps refusing."
sourceOfTruth:
  - "internal/advisorysource"
  - "internal/vulnerability"
  - "internal/cli/dbvulnerabilities"
generated: false
overlaps:
  - "/operate/vulnerability-scan/"
disposition: keep
sourceMode: live-database-only
---

[`ptah db vulnerabilities`](../vulnerability-scan/) matches each live database
against a reviewed advisory snapshot for its engine. This page says where that
snapshot comes from. To scan with only the snapshot built into the binary and
make no request, select `bundled`:

```sh
ptah db vulnerabilities --db-url "$DATABASE_URL" --advisory-source bundled
```

## Where the snapshot comes from

The default advisory source, `--advisory-source repository`, is the reviewed
snapshot files on Ptah's master branch, kept current by the daily advisory
refresh, so a released binary reads a merged refresh without a new release.
The request goes to `raw.githubusercontent.com`, path
`/stokaro/ptah/master/internal/vulnerability/<file>`; allow that host where
outbound traffic is filtered.

For each engine it scans, the command makes one HTTPS `GET` for that engine's
file, such as `mysql.json`, so the source learns which engines you scan and
nothing else about the databases. A provider's engine, such as YDB, reads the
snapshot its provider carries.

Each engine's snapshot comes from the first of:

1. `--advisories` names a file, read for every target with no request.
2. `--advisory-source`, or `PTAH_ADVISORY_SOURCE`: `repository`, or the URL of
   a directory that serves the same file names, such as a mirror, over
   `https`, or `http` on a loopback address.
3. The snapshot built into the binary. `bundled` selects it alone, with no
   request.

## What a remote copy may change

The scan does not rely on how changes reach master. Each advisory is matched
against both the built-in snapshot and the remote or cached copy, and the
report keeps the stricter answer, so no advisory reads cleaner than the
built-in snapshot reads it. The two answers combine by these rules:

- An advisory is `affected` when either snapshot says so. Otherwise the more
  uncertain answer wins: `uncertain`, then `fixed`, then `unaffected`.
- The score is the higher of the two, and an advisory either snapshot leaves
  unscored stays unscored, since no severity floor removes it.
- Partial coverage in either snapshot keeps the report partial.
- `retrieved_at`, `valid_until`, `stale_from` and the freshness state come
  from the snapshot retrieved later. A copy retrieved before the built-in
  snapshot's `valid_until` cannot hold the update scheduled for that date, so
  that date still counts unless the copy schedules an earlier one.

A refresh therefore adds an advisory, widens a range, raises a score and
renews the retrieval time without a release. A change that would make an
advisory's answer cleaner waits for a Ptah release: an advisory removed or
moved to another component, a range narrowed, a score lowered or added, a
release line or covered line added, and partial coverage claimed complete.

A copy can still let a run pass that the built-in snapshot alone fails:

- A copy retrieved later makes a `due` or `stale` report `current`. Each copy
  renews it for at most 30 days from its `retrieved_at`, which may be up to
  24 hours ahead of this machine's clock.
- Where the built-in snapshot reads an advisory as `uncertain` and the copy
  reads it as `affected`, the report carries the finding without the coverage
  gap the uncertain answer gives. `--fail-on incomplete` then passes, and so
  does `--fail-on both` when a suppression or `--ignore` records the finding
  as suppressed.

PostgreSQL's covered release lines rotate each year, for example from 14 to
18 onto 15 to 19. A server on the newly covered line reads `uncertain` until a
release carries that line. A server on the dropped line reads `uncertain` for
every advisory the built-in snapshot does not find affected, with a gap saying
that its release line is outside snapshot coverage, within about an hour of
the refresh merging. Under the default `--fail-on both`, every released binary
that reads the default source then fails that server, as a release with the
new lines would. This is intended: the refreshed snapshot does not review the
dropped line.

A copy must also pass the built-in validators, be no older than the built-in
snapshot or the cached copy, and be dated no more than 24 hours after this
machine's clock. In the JSON report the source block describes the copy, and
`source.bundled` names the built-in snapshot it was merged with by
`snapshot_sha256`, `sha256`, `retrieved_at` and `valid_until`.

## When the source is not used

When the source fails or serves a refused copy, the scan reads the cached copy
if it is newer than the built-in snapshot, and the built-in snapshot
otherwise, so dropping the request cannot take a scan back past what this
machine has seen. The command warns on stderr, the table prints an
`Advisory data:` line, and the JSON source block carries `origin`. When a
cached copy exists and is refused, for a hash mismatch, a link or loose
permissions, the reason says that too.

A run that names the source, with `--advisory-source repository`, a URL or
`PTAH_ADVISORY_SOURCE`, counts the fallback as a coverage gap, so
`--fail-on incomplete` and `both` fail. A default run does not count the
fallback as a gap. Findings only the copy carries are absent from that run,
and the warning says the source was not used. `bundled` asks for no source.

[`ptah migrations lint`](../../reference/lint-rules/#extensions-a-migration-installs)
reads the same source, with the same flag and variable, for the extensions a
migration installs on its dev database. It asks only when a migration installs
one. An extension the source could not answer for in a run that names the
source is reported as `coverage/unassessed-extension`, never as clean.

A suppression entry must name an advisory that a snapshot the run reads
pins, and an entry that names none fails the scan. After a fallback the entry
may name an advisory only the copy carries, so it is a warning instead: the
warning names the fallback reason, and the entry suppresses nothing.

A connection not open within 5 seconds fails, and so does a request with no
response headers 10 seconds after it is sent or no complete answer within 15
seconds. A failure is remembered for an hour, so a stalled source costs at most
15 seconds per engine once an hour; with no route to the source, `bundled`
skips the wait.

| `origin` field | Meaning |
| --- | --- |
| `kind` | `remote`, `bundled`, `file` for an `--advisories` file, or `provider`. |
| `url` | The file fetched, or with a `fallback`, the one not used. |
| `fetched_at` | When the source last confirmed the copy. |
| `cache` | `fetched`, `revalidated` after `304 Not Modified`, `fresh` with no request, or `unconfirmed`: the source was not used and the cached copy was read. |
| `fallback` | Why the source was not used this run. |

## The cache

Copies are kept under `ptah/advisories` in the user cache directory:
`$XDG_CACHE_HOME` or `~/.cache` on Linux, `~/Library/Caches` on macOS, and
`%LocalAppData%` on Windows. A copy confirmed less than an hour ago is used
with no request; after that its `ETag` is sent in `If-None-Match`, and
`304 Not Modified` keeps it another hour.

An entry stores the SHA-256 of its copy, and a copy that does not match is not
used. On Linux and macOS the scan ignores a cache directory or entry that is a
symbolic link, that another user owns, or that its group or others can write.
On Windows Ptah relies on the default directory's access control list and does
not read it. Whoever can write the cache chooses which accepted copy a scan
reads, so a cache restored between CI jobs is inside each job's trust boundary.

A copy is never cached when it is dated more than 24 hours after this
machine's clock: its `retrieved_at` would make every later copy read as older.
If a wrong clock leaves a cached copy that keeps newer ones out, delete the
`ptah/advisories` directory in the user cache directory, and the next run
fetches again.
