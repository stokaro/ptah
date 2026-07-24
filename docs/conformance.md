# Atlas Conformance

Ptah's Atlas conformance scoreboard is maintained in the dedicated
[`stokaro/ptah-atlas-conformance`](https://github.com/stokaro/ptah-atlas-conformance)
repository.

That repository is the authoritative, CI-regenerated answer to "are we there
yet?" for Atlas OSS compatibility. It keeps Atlas's Apache-2.0 fixture corpus
outside Ptah's MIT source tree while importing Ptah as the system under test.
The dependency direction is intentionally one-way:

```text
ptah-atlas-conformance -> ptah
ptah                  !-> ptah-atlas-conformance
```

## Current Scoreboard

As of Ptah `18ae5f9d4d63136248986263732524e2314f9d7c`:

| Tier | Purpose | Current result |
| --- | --- | --- |
| Offline Atlas corpus | Can Ptah ingest Atlas OSS fixture artifacts through public APIs? | 636 ok, 0 gap, 0 fail, 0 panic |
| Live round-trip | Can Ptah generate, apply, introspect, and diff first-party schemas on real databases? | 8 ok, 2 known gaps |
| Atlas CE differential | Do Atlas CE and Ptah agree on live end-state facts for shared fixtures? | 1 ok, 4 known gaps |
| CLI surface | Do Atlas CE and Ptah expose compatible command paths, help boundaries, flags, and runtime classifications? | Tracked in `cli-surface.md` |

The offline full-conformance gate is green. The live and differential full gates
remain intentionally red until the known gaps are closed, while their regression
budgets stay green when the reports are current and no new gaps appear.

## Reports

- Offline corpus report:
  [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md)
- Live round-trip report:
  [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md)
- Atlas CE differential report:
  [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md)
- CLI surface report:
  [`cli-surface.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/cli-surface.md)
- Parity scope:
  [`PARITY.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/PARITY.md)

## Local Commands

From this repository:

```bash
make conformance
```

From `ptah-atlas-conformance`:

```bash
make probe        # regenerate gaps.md / gaps.json
make budget       # offline regression budget
make gate         # full offline parity gate
make probe-live   # live DB round-trip report
make budget-live  # live DB regression budget
make probe-diff   # Atlas CE differential report
make budget-diff  # Atlas CE differential regression budget
make probe-cli-surface   # Atlas CE CLI surface report
make budget-cli-surface  # Atlas CE CLI surface regression budget
make gate-cli-surface    # full CLI surface parity gate
```

Live and differential commands require real database URLs, and the differential
tier also requires an Atlas CE binary built from the pinned `atlas.version` in
the conformance repository.
