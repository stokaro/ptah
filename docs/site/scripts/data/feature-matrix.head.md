---
title: Feature matrix
description: Ptah and Atlas capabilities side by side, with the evidence behind every row.
---

This page answers one question: for a given capability, what does Ptah do, what
does the open Atlas community binary do, and what does Atlas keep outside its
community build. Every row cites the evidence it rests on.

It is a status index, not an argument. The per-area detail behind these rows is
on [Comparison](../comparison/), the measured evidence is on
[Conformance](../conformance/), and the Atlas-documentation crosswalk is on
[Atlas docs coverage](../docs-coverage/).

## How to read the tables

| Symbol | Meaning |
| --- | --- |
| ✅ | Supported today |
| 🟡 | Partial. The difference column states what is missing |
| ❌ | Not implemented |
| 🔷 | Ptah does this job in its own form. What is missing is wire compatibility with a hosted, account-bound service, which an independent implementation cannot obtain |
| ➖ | Does not apply to that product |
| ❔ | Not established by the evidence this page uses |

🔷 and 🟡 answer different questions, and the difference decides whether a
reader should wait for a release. 🟡 means the work is unfinished: the
difference column names the issue that owns it, and a later Ptah closes it. 🔷
means the capability is already here under a Ptah spelling, running against
infrastructure the team already operates, and the only thing absent is speaking
the wire protocol of a service somebody else hosts behind their own accounts.
No release closes that, so a 🔷 cell carries no tracking issue. A capability
that is merely incomplete is never 🔷, whatever it sits next to.

🔷 appears in the **Ptah** column only. The Atlas columns describe the hosted
product itself, where the distinction does not arise.

Each table has the same columns. **Ptah**, **CE**, and **Pro** carry one symbol
each:

- **Ptah** — the native `ptah` binary plus the separate `ptah-compat` drop-in.
- **CE** — the pinned Atlas community binary, version 1.2.0, which the
  conformance harness runs against.
- **Pro** — capabilities Atlas keeps outside its community build, per the
  [Atlas feature availability](https://atlasgo.io/features) page and
  [pricing page](https://atlasgo.io/pricing).

Every Atlas cell has to come from an Atlas-side source: the command, usage, and
flag inventory the conformance harness reads out of the pinned community
binary, measured behavior of an Atlas binary, or a classification Atlas
publishes. Measurement outranks published classification when they disagree.
Where nothing settles a question, the cell is ❔ rather than a guess.

## Atlas plans are not the CE column

Atlas's public plans are Starter (free), Pro, and Enterprise, and Atlas's own
[pricing page](https://atlasgo.io/pricing) classifies capabilities by plan.
That classification and the **CE** column answer different questions: the CE
column reports what the pinned community binary does, and
the two diverge in both directions. Both examples below were measured on
2026-08-01 against CE v1.2.0:

- The pricing page places migration linting outside the Starter plan, yet the
  CE binary runs `migrate lint` and reports destructive changes.
- The pricing page checks ERD visualization for Starter, yet the CE binary
  rejects `schema inspect --web` as an unknown flag; the ERD lives in the
  hosted service, not in the binary.

Where the pricing page settles a Pro-side question, the Pro column cites it.
Where plan marketing and measured binary behavior differ, the measured behavior
wins the CE cell and the difference column records the tension. The
schema-object rows are the clearest case: the HCL reference marks `partition` a
Pro feature and the pinned community binary plans `PARTITION BY RANGE` anyway,
while a live schema holding a domain, a composite type, a range type, a
sequence and an extension inspects on the same binary to nothing but its one
table. Both readings are in the row.

:::caution
A ✅ in the Ptah column means the capability works, not that it is
byte-identical to Atlas. Ptah is an independent pre-GA implementation, and the
conformance repository states plainly that no number it produces is a
full feature-set parity test. Read the difference column before relying on a
row for a migration decision.
:::
