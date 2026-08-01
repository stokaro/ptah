## How these rows were established

The Ptah column is derived from the built binaries. `ptah --help` and
`ptah-compat --help` are the strongest available proof that a capability
exists, and a row that could not be demonstrated that way is marked 🟡 with the
limitation named, or ❌.

The Atlas columns are narrower on purpose. Ptah is a clean-room implementation
that studies observable behavior only, so the Atlas CE column is derived from
the command, usage, and flag inventory the conformance harness reads out of the
pinned Atlas community binary, and the Pro/Cloud column from the classification
Atlas publishes on its feature availability page. Where neither source settles
a question, the row says so rather than guessing.

The full per-row evidence — the command that was run or the source cited for
every cell — is version-controlled at
`docs/site/scripts/data/feature-matrix-rows.json`, so a row can be re-verified
or disputed without archaeology.

Four sources carry most of the weight:

- [`cli-surface.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/cli-surface.md)
  inventories every command in Atlas CE v1.2.0 and classifies it as an OSS
  parity target or out of scope, with the reason recorded per command.
- [`ce-gating.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/ce-gating.md)
  goes further than the inventory: it runs the pinned CE binary logged out
  through the capability set this page asserts about the CE column and records
  the observed class per scenario — works, community-abort stub, absent verb,
  unknown flag, or silently unenforced. A version bump that changes Atlas's
  gating turns that gate red.
- [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md),
  [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md),
  and [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md)
  record measured outcomes over Atlas fixtures, live databases, and Atlas CE
  differential checks.
- [`docs-surface.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/docs-surface.md)
  indexes the full atlasgo.io documentation universe — 351 pages from the
  site's own sitemap — into a triage registry, so parity is built against the
  whole documented Atlas surface rather than a hand-picked subset. The registry
  starts mostly untriaged and its budget ratchets down as pages are worked
  through
  ([campaign](https://github.com/stokaro/ptah-atlas-conformance/issues/239)); a
  weekly job re-fetches the sitemap so new or renamed Atlas docs pages surface
  as red rather than silently missing from this page.

## What this page does not claim

A green conformance run is a floor on the distance to Atlas, never a ceiling.
The conformance repository states it directly: no number it produces is a
full feature-set parity test, and several runtime dimensions stay unmeasured.

Specifically, this page does not claim that Ptah reproduces Atlas byte for
byte, that a ✅ row behaves identically under every flag combination, or that
the Atlas columns are exhaustive for capabilities Atlas ships outside its
documented CLI surface.

## Next steps

- Per-area detail behind these rows: [Comparison](../comparison/).
- The measured evidence and how to re-run it: [Conformance](../conformance/).
- Which Atlas documentation area maps where: [Atlas docs coverage](../docs-coverage/).
- Why Ptah can be Atlas-compatible without Atlas code:
  [License boundary](../license-boundary/).
