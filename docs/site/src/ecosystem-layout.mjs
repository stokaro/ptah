// Where the ecosystem map draws each component and each relationship. The
// facts live in src/ecosystem.mjs; this file holds only coordinates, keyed by
// the ids declared there, and src/lib/ecosystem-map.mjs refuses to build a map
// whose layout misses or invents an id.
//
// The drawing is a 784-unit-wide grid of three columns (x = 20, 292 and 560;
// the right one is 186 wide for the longest names) with `ptah` in the middle.
// Groups that start work sit left of it, groups that deliver or check it sit
// right of it, and the providers it starts sit below it. Edges are orthogonal: a route lists its points, the
// first and last naming a side of a component (`ptah:top`, `ptah:left:-20`
// for 20 units above the middle of the left side), and a partial point such as
// `{ y: 62 }` keeps the other coordinate of the point before it.
//
// Below the width src/styles/ptah/ecosystem.css sets, the page shows the narrow
// layout instead: a list of the same components with each relationship written
// out, and the full-size action opens this drawing for inspection.

const N = 176; // node width
const H = 48; // node height
const W = 186; // node width in the right column, which holds the longest names

export const wideLayout = {
  width: 784,
  height: 664,
  nodes: {
    'ptah-run': [20, 40, N, H],
    'ptah-sandbox': [20, 132, N, H],
    'homebrew-ptah': [560, 40, W, H],
    'ptah-action': [560, 132, W, H],
    cli: [20, 236, N, H],
    ptah: [280, 132, 200, 198],
    'ptah-atlas-conformance': [560, 222, W, H],
    'ptah-testkit': [560, 286, W, H],
    'ptah-action-e2e': [560, 358, W, H],
    'kubernetes-api': [20, 384, N, H],
    'ptah-operator': [292, 384, 150, H],
    'ptah-console': [20, 484, N, H],
    'ptah-ydb-plugin': [20, 596, N, H],
    'ptah-duckdb-plugin': [292, 596, N, H],
    'ptah-spanner-plugin': [560, 596, W, H],
  },
  frames: {
    discover: [10, 12, 196, 178],
    distribution: [550, 12, 214, 178],
    verification: [550, 196, 214, 222],
    kubernetes: [10, 356, 442, 186],
    providers: [10, 568, 754, 88],
  },
  edges: {
    'site-links-sandbox': { route: ['ptah-run:bottom', 'ptah-sandbox:top'], label: [108, 110] },
    'site-installs-ptah': { route: ['ptah-run:right', { x: 330 }, 'ptah:top:-50'], label: [262, 52], wrap: 24 },
    'sandbox-runs-ptah': { route: ['ptah-sandbox:right', 'ptah:left:-75'], label: [238, 131], wrap: 12 },
    'homebrew-installs-ptah': { route: ['homebrew-ptah:left', { x: 430 }, 'ptah:top:50'], label: [495, 46], wrap: 16 },
    'action-runs-ptah': { route: ['ptah-action:left', 'ptah:right:-75'], label: [520, 131], wrap: 12 },
    'conformance-measures-ptah': { route: ['ptah-atlas-conformance:left', 'ptah:right:15'], label: [520, 221], wrap: 12 },
    'testkit-uses-ptah': { route: ['ptah-testkit:left', 'ptah:right:79'], label: [520, 285], wrap: 12 },
    'e2e-checks-action': { route: ['ptah-action-e2e:right', { x: 774 }, 'ptah-action:right'], label: [744, 348], align: 'end' },
    'cli-runs-ptah': { route: ['cli:right', 'ptah:left:29'], label: [238, 250] },
    'cli-manages-resources': { route: ['cli:bottom', 'kubernetes-api:top'], label: [108, 322] },
    'console-manages-resources': { route: ['ptah-console:top', 'kubernetes-api:bottom'], label: [108, 458] },
    'api-reconciled-by-operator': { route: ['kubernetes-api:right', 'ptah-operator:left'], label: [244, 396] },
    'operator-runs-ptah': { route: ['ptah-operator:top', 'ptah:bottom:-13'], label: [367, 343] },
    'ptah-starts-ydb': { route: ['ptah:bottom:80', { y: 556 }, { x: 68 }, 'ptah-ydb-plugin:top:-40'], label: [270, 556], wrap: 40 },
    'ptah-recognizes-duckdb': { route: ['ptah:bottom:90', { y: 582 }, { x: 380 }, 'ptah-duckdb-plugin:top'], label: [478, 504], wrap: 18, align: 'start' },
    'ptah-starts-spanner': { route: ['ptah:bottom:99', { y: 446 }, { x: 653 }, 'ptah-spanner-plugin:top'], label: [566, 446], wrap: 40 },
  },
};
