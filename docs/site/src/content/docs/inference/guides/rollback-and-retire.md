---
title: Rollback and retire
description: Going back to the previous generation, what keeps that possible, and when it is safe to destroy the old vectors.
---

These are two different operations and confusing them is expensive.

- **Rollback** moves the pointer back to the previous generation. Reversible.
- **Retire** destroys a generation's vectors. Permanent.

## Rollback needs three things to be true

A window, a maintained generation, and a generation that is still complete.

### The window

Ask for it at cutover time:

```bash
ptah inference cutover ... --stabilize-for 24h
```

A cutover run without it leaves no rollback at all, and says so:

```text
  - no stabilization window was asked for, so nothing is keeping the previous
    generation current and there is no rollback to it
```

### The maintenance

This is the part people miss.

The previous generation stops receiving changes the moment your queries stop
reading it. An hour later it is behind your source; a day later it may be far
behind. Rolling back to it would answer queries from a corpus that no longer
matches your data — which is worse than not rolling back.

So the window has to be kept, not merely declared:

```bash
ptah inference catchup --spec previous-spec.yaml --db-url "$DB" \
  --run-id previous-run --maintain-for 1h
```

`--maintain-for` does two things at once: it catches the generation up, and it
extends the promise that it is current. Put it on a schedule for the length of
the window. A window extended without a catch-up behind it is a promise nobody
kept; a catch-up whose window expired left a generation current and unusable.

### The freshness

`rollback` measures before it moves anything:

- is the generation's column still there;
- is it still being maintained;
- how many rows are stale, and how many are missing;
- is its index present and valid;
- was the cutover recent enough for the window.

A generation that drifted is refused:

```console
error: rollback refused:
  - 1841 rows of the previous generation are stale, and the policy allows 0
```

That is the honest answer rather than a gap. Going back to it would be going
back to something that is not what it was.

## Doing it

```bash
ptah inference rollback --spec spec.yaml --db-url "$DB" \
  --to <previous-generation> --window 24h
```

The pointer moves back. Your application still reads whatever column its SQL
names, so if you deployed the change that reads the new column, **redeploy the
old one** — the rollback did not do that for you.

## Retire

Retirement drops the generation's index and, with `--drop-column`, its column and
the four bookkeeping columns beside it.

```bash
ptah inference retire --spec spec.yaml --db-url "$DB" \
  --generation <identity> --drop-column
# refuses, prints the plan digest, then:
ptah inference retire --spec spec.yaml --db-url "$DB" \
  --generation <identity> --drop-column \
  --approve <digest> --approver "your name"
```

It is refused while queries still read the generation, and it takes the same
digest-bound approval a cutover does.

## When to retire

Not on the day of the cutover. The reasons to keep a generation are:

- the window in which a rollback is possible;
- an evaluation you have not run yet;
- an answer to "why did the results change" that needs both corpora.

The reasons to retire are storage and the cost of maintaining it — a generation
you keep catching up is a generation you keep paying the provider for.

A common shape: cut over with a 24-hour window, maintain it on a schedule for
those 24 hours, stop maintaining it, and retire a week later once nobody has
asked to go back.

## After retirement

There is no undo. The vectors are gone and rebuilding them means paying for the
whole corpus again. The outbox table and its triggers go with the generation, so
retiring is also how the write-time cost on your source table ends.
