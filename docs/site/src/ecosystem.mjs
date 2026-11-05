// The Ptah ecosystem: every public repository, the two entry points that are
// not repositories, and how they relate. One declaration feeds every view of
// it on the "Ptah ecosystem" page (start/ecosystem.mdx): the interactive map,
// its details panel, the narrow-screen layout, the repository table, the
// relationship list, and the full-size SVG under src/pages/diagrams/. A second
// copy of any of these facts is how the map and the table would come to
// disagree, so none exists.
//
// It is plain JavaScript that imports only the site addresses, which import
// nothing, so a Node script reads it the same way Astro does. src/lib/ecosystem-map.mjs validates it when the site
// builds: an unknown group, component or kind, a relationship whose ends are
// not declared, a path naming a relationship it does not cover, or a component
// the layout does not place fails the build rather than drawing a broken map.
//
// What a status may claim. Each status describes what the owning repository
// says about itself, checked against its README, its releases and its
// description. A repository's existence is not a support claim: a scaffold is
// labeled a scaffold even where the host recognizes its engine name. Version
// numbers stay out of this file; each component links to the place that
// publishes its releases and compatibility, so nothing here goes stale when a
// release is cut.
//
// Links. `href` is an external address. `doc` is a route on this site, written
// without the version prefix (`operate/kubernetes-operator/`); the component
// renders it relative to the page, and the build fails when no page has that
// route, or when an `anchor` names a heading that page does not have.

import { OperatorOrigin, PlaygroundOrigin, SiteOrigin } from './lib/docs-origin.mjs';

/** The groups the map draws, in reading order. The table follows this order. */
export const groups = [
  { id: 'core', label: 'Core' },
  { id: 'kubernetes', label: 'Optional Kubernetes workflow' },
  { id: 'providers', label: 'Optional external providers' },
  { id: 'distribution', label: 'Distribution and CI' },
  { id: 'discover', label: 'Discover and try' },
  { id: 'verification', label: 'Verification and test helpers' },
];

/**
 * What a relationship means. Every relationship carries a written label as
 * well; the line pattern only repeats what the label says, so nothing depends
 * on telling the patterns apart.
 */
export const relationshipKinds = [
  { id: 'runs', label: 'Runs or calls', description: 'A process starts, calls or reconciles the other component.' },
  { id: 'installs', label: 'Installs', description: 'Puts a Ptah build on a machine.' },
  { id: 'checks', label: 'Tests or measures', description: 'Exercises the other component and reports what it found.' },
  { id: 'links', label: 'Documents or links', description: 'Sends a reader to the other component.' },
];

/** How far a relationship is implemented. Only the first is the default. */
export const relationshipStates = [
  { id: 'available', label: 'Available' },
  { id: 'experimental', label: 'Experimental', description: 'Works, and may change between releases.' },
  { id: 'not-implemented', label: 'Not implemented', description: 'Declared, with nothing behind it yet.' },
];

/** What the owning repository says about its own maturity. */
export const statuses = [
  { id: 'released', label: 'Released', description: 'Publishes versioned releases.' },
  { id: 'experimental', label: 'Experimental', description: 'Publishes releases and is still under development.' },
  { id: 'scaffold', label: 'Scaffold', description: 'Exists with no working features yet.' },
  { id: 'site', label: 'Published site', description: 'A website, deployed from the repository.' },
  { id: 'evidence', label: 'Evidence', description: 'Produces reports; not something you install.' },
  { id: 'fixture', label: 'Test fixture', description: 'Exists to test another repository.' },
];

/**
 * The components. `kind` is `repository`, or `entry` and `infrastructure` for
 * the two that are not repositories: where a person or a pipeline starts, and
 * the Kubernetes API the operator and the console share.
 *
 * `role` is the short label the map shows beside the name. `badge` is the one
 * word the map puts on the node when its status or optionality matters before
 * a reader opens anything.
 */
export const components = [
  {
    id: 'cli',
    name: 'CLI / automation',
    kind: 'entry',
    group: 'core',
    role: 'Your shell, scripts or CI',
    purpose:
      'Where a person or a pipeline starts. It is an entry point, not a repository: a terminal, a script, or a CI job that runs `ptah` or, on Kubernetes, `kubectl`.',
    whenNeeded: 'Always. The direct path runs `ptah` from here, and the Kubernetes path manages the operator\'s resources from here.',
    links: [{ label: 'Choose a workflow', doc: 'start/choose-a-workflow/' }],
  },
  {
    id: 'ptah',
    name: 'ptah',
    kind: 'repository',
    group: 'core',
    role: 'Engine, CLIs and Go API',
    status: 'released',
    purpose:
      'The schema engine. It reads desired schemas, compares them with live databases, and plans, generates and applies changes. It ships the native `ptah` CLI and `ptah-compat`, the Atlas-compatible CLI, and holds the public Go packages and the provider SDK.',
    whenNeeded: 'Always. Every other component runs, installs, checks or links to it.',
    statusNote:
      'Tagged releases with Sigstore-signed checksums. Ptah is pre-GA, so behavior can change between releases. The provider SDK in the same repository is experimental.',
    links: [
      { label: 'Repository', href: 'https://github.com/stokaro/ptah' },
      { label: 'Releases', href: 'https://github.com/stokaro/ptah/releases' },
      { label: 'Install Ptah', doc: 'start/install/' },
      { label: 'Embed Ptah in Go', doc: 'extend/public-api/' },
    ],
  },
  {
    id: 'kubernetes-api',
    name: 'Kubernetes API',
    kind: 'infrastructure',
    group: 'kubernetes',
    role: 'Operator resources',
    purpose:
      'The Kubernetes API server and the operator\'s resources: schemas, migrations, plans and approvals. It is infrastructure, not a repository. People and tools change the workflow by writing these resources, and the operator acts on them.',
    whenNeeded: 'Only on the Kubernetes path.',
    links: [{ label: 'Kubernetes operator', doc: 'operate/kubernetes-operator/' }],
  },
  {
    id: 'ptah-operator',
    name: 'ptah-operator',
    kind: 'repository',
    group: 'kubernetes',
    role: 'Kubernetes reconciler',
    status: 'released',
    purpose:
      'A Kubernetes control plane. `PtahSchema` converges a database on a desired schema from an OCI artifact, and `PtahMigration` runs a versioned migration sequence from one. Database work runs in short-lived Jobs that run a Ptah release image.',
    whenNeeded:
      'Your databases are managed from Kubernetes, and changes should be reconciled from published artifacts with approvals bound to the exact plan.',
    statusNote:
      'Publishes releases. The API is `v1alpha1`, which carries no compatibility guarantee before v1. The operator publishes which Ptah build each release runs.',
    links: [
      { label: 'Repository', href: 'https://github.com/stokaro/ptah-operator' },
      { label: 'Operator documentation', href: `${OperatorOrigin}/` },
      { label: 'Ptah compatibility', href: `${OperatorOrigin}/support/ptah/` },
      { label: 'Kubernetes operator', doc: 'operate/kubernetes-operator/' },
    ],
  },
  {
    id: 'ptah-console',
    name: 'ptah-console',
    kind: 'repository',
    group: 'kubernetes',
    role: 'Operator web console',
    badge: 'Optional',
    status: 'released',
    purpose:
      'A web console for the operator. It shows schemas, migrations and plans, and lets a person approve an exact plan under their own Kubernetes identity. It reads and writes the same Kubernetes resources as `kubectl`; the operator and Ptah still do the work.',
    whenNeeded:
      'People review or approve operator plans and want a browser view instead of `kubectl`. The operator runs the same without it, and removing it changes nothing the operator does.',
    statusNote: 'Publishes releases. Each release is built against one operator release, so pin the two together.',
    links: [
      { label: 'Repository', href: 'https://github.com/stokaro/ptah-console' },
      { label: 'Compatibility', href: 'https://github.com/stokaro/ptah-console/blob/master/docs/compatibility.md' },
    ],
  },
  {
    id: 'ptah-ydb-plugin',
    name: 'ptah-ydb-plugin',
    kind: 'repository',
    group: 'providers',
    role: 'YDB provider',
    badge: 'Experimental',
    status: 'experimental',
    purpose:
      'The executable provider that gives Ptah its YDB support. Ptah has no YDB implementation of its own and runs YDB work only through this provider.',
    whenNeeded: 'You work with YDB. Install it with `ptah providers install ydb`; no Ptah installation channel includes it.',
    statusNote:
      'Publishes releases and is under development. A release declares the provider contract it needs, and `ptah providers install` picks the newest release this Ptah can run.',
    links: [
      { label: 'Repository', href: 'https://github.com/stokaro/ptah-ydb-plugin' },
      { label: 'Configure the YDB provider', doc: 'extend/providers/ydb/' },
      { label: 'Executable providers', doc: 'extend/executable-providers/' },
    ],
  },
  {
    id: 'ptah-duckdb-plugin',
    name: 'ptah-duckdb-plugin',
    kind: 'repository',
    group: 'providers',
    role: 'DuckDB provider',
    badge: 'Scaffold',
    status: 'scaffold',
    purpose:
      'A provider scaffold. It starts, negotiates with Ptah and declares the `duckdb` engine, but implements no service: it opens no connection and cannot inspect, plan or apply.',
    whenNeeded: 'Not for DuckDB work yet. Ptah has no built-in DuckDB support either.',
    statusNote:
      'No release that Ptah can install: `ptah providers install duckdb` refuses with `provider_no_release`.',
    links: [
      { label: 'Repository', href: 'https://github.com/stokaro/ptah-duckdb-plugin' },
      { label: 'Configure the DuckDB provider', doc: 'extend/providers/duckdb/' },
    ],
  },
  {
    id: 'ptah-spanner-plugin',
    name: 'ptah-spanner-plugin',
    kind: 'repository',
    group: 'providers',
    role: 'GoogleSQL provider',
    badge: 'Scaffold',
    status: 'scaffold',
    purpose:
      'A provider scaffold for the GoogleSQL dialect of Cloud Spanner. It implements no service yet. Ptah\'s built-in `spanner` dialect is a separate path: it reaches Spanner through its PostgreSQL interface and PGAdapter.',
    whenNeeded: 'Not for Spanner work yet. For the PostgreSQL interface, read the built-in dialect\'s support level first.',
    statusNote:
      'No release that Ptah can install: `ptah providers install spanner` refuses with `provider_no_release`.',
    links: [
      { label: 'Repository', href: 'https://github.com/stokaro/ptah-spanner-plugin' },
      { label: 'Configure the Spanner provider', doc: 'extend/providers/spanner/' },
      { label: 'Spanner through PostgreSQL', doc: 'databases/distributed/' },
    ],
  },
  {
    id: 'homebrew-ptah',
    name: 'homebrew-ptah',
    kind: 'repository',
    group: 'distribution',
    role: 'Homebrew tap',
    status: 'released',
    purpose:
      'The Homebrew tap. The `ptah` formula installs the newest release; `ptah-edge` builds the current `master` from source.',
    whenNeeded: 'You install command-line tools on macOS or Linux with Homebrew.',
    statusNote: 'Follows Ptah releases. The edge formula is the development tip, not a release.',
    links: [
      { label: 'Repository', href: 'https://github.com/stokaro/homebrew-ptah' },
      { label: 'Install with Homebrew', doc: 'start/install-options/', anchor: 'install-with-homebrew' },
    ],
  },
  {
    id: 'ptah-action',
    name: 'ptah-action',
    kind: 'repository',
    group: 'distribution',
    role: 'GitHub Action',
    status: 'released',
    purpose:
      'A GitHub Action that runs Ptah on a pull request: migration planning, safety checks, lint, a sticky comment and a destructive-change Check Run. Its source is in the `ptah` repository; this repository is the published copy that workflows use as `stokaro/ptah-action@v1`.',
    whenNeeded: 'You review schema changes in GitHub pull requests.',
    statusNote: 'Published under the `v1` tag. It downloads a Ptah release, `latest` unless you pin one.',
    links: [
      { label: 'Repository', href: 'https://github.com/stokaro/ptah-action' },
      { label: 'Run Ptah in CI', doc: 'testing/ci/' },
    ],
  },
  {
    id: 'ptah-run',
    name: 'ptah.run',
    kind: 'repository',
    group: 'discover',
    role: 'Website and installers',
    status: 'site',
    purpose:
      'The project website at ptah.run: the homepage, the install page and recorded sessions. It serves the `install.sh` and `install.ps1` installers, taken from the `ptah` repository, and the `ptah.run` Go import path.',
    whenNeeded: 'You meet Ptah for the first time, or you want the one-line install command.',
    links: [
      { label: 'Website', href: `${SiteOrigin}/` },
      { label: 'Repository', href: 'https://github.com/stokaro/ptah.run' },
    ],
  },
  {
    id: 'ptah-sandbox',
    name: 'ptah-sandbox',
    kind: 'repository',
    group: 'discover',
    role: 'Browser playground',
    status: 'site',
    purpose:
      'The source of play.ptah.run. Ptah is compiled to WebAssembly and runs in the browser against SQLite or PostgreSQL, also compiled to WebAssembly. Nothing is installed and no output is recorded in advance.',
    whenNeeded: 'You want to try Ptah\'s commands before you install anything.',
    statusNote:
      'Every deploy is built from one pinned Ptah commit, which can be older than the newest release. The playground shows the version it runs.',
    links: [
      { label: 'Playground', href: `${PlaygroundOrigin}/` },
      { label: 'Repository', href: 'https://github.com/stokaro/ptah-sandbox' },
    ],
  },
  {
    id: 'ptah-atlas-conformance',
    name: 'ptah-atlas-conformance',
    kind: 'repository',
    group: 'verification',
    role: 'Atlas compatibility evidence',
    status: 'evidence',
    purpose:
      'A harness that drives Atlas\'s own test fixtures through Ptah\'s public API and the `ptah-compat` binary, and compares selected behavior with the Atlas Community Edition. It depends on Ptah one way; Ptah never imports it.',
    whenNeeded: 'You want to know what Atlas expresses that Ptah does not cover yet, before you rely on `ptah-compat`.',
    statusNote: 'Not a release artifact and not a full parity test. Its reports are the evidence behind Ptah\'s compatibility claims.',
    links: [
      { label: 'Repository', href: 'https://github.com/stokaro/ptah-atlas-conformance' },
      { label: 'Conformance', doc: 'atlas/conformance/' },
    ],
  },
  {
    id: 'ptah-testkit',
    name: 'ptah-testkit',
    kind: 'repository',
    group: 'verification',
    role: 'Test helpers for Go projects',
    status: 'released',
    purpose:
      'An opt-in Go module, `ptah.run/testkit`, for tests that need real databases. It starts PostgreSQL, MySQL, MariaDB or SQLite, applies migrations, seeds rows and takes a schema snapshot, and keeps container dependencies out of Ptah\'s own module.',
    whenNeeded: 'Your Go tests apply migrations to a real database.',
    statusNote: 'Tagged as a Go module. Each version requires a Ptah version through its `go.mod`.',
    links: [
      { label: 'Repository', href: 'https://github.com/stokaro/ptah-testkit' },
      { label: 'Embed Ptah in Go', doc: 'extend/public-api/' },
    ],
  },
  {
    id: 'ptah-action-e2e',
    name: 'ptah-action-e2e',
    kind: 'repository',
    group: 'verification',
    role: 'Action end-to-end checks',
    status: 'fixture',
    purpose:
      'A fixture repository whose pull requests show that `stokaro/ptah-action@v1` writes its sticky comment and its Check Run in a real GitHub workflow.',
    whenNeeded: 'You maintain the Action. Workflows that use the Action do not need it.',
    links: [{ label: 'Repository', href: 'https://github.com/stokaro/ptah-action-e2e' }],
  },
];

/**
 * The relationships. An arrow follows the flow of work, from the component
 * that starts it to the one that carries it on, and `label` is short enough to
 * sit on that arrow. The text views spell each one out as
 * "<from> → <to>: <label>".
 */
export const relationships = [
  { id: 'cli-runs-ptah', from: 'cli', to: 'ptah', kind: 'runs', label: 'Runs directly' },
  { id: 'cli-manages-resources', from: 'cli', to: 'kubernetes-api', kind: 'runs', label: 'Manages with kubectl' },
  { id: 'console-manages-resources', from: 'ptah-console', to: 'kubernetes-api', kind: 'runs', label: 'Reads and approves as you' },
  { id: 'api-reconciled-by-operator', from: 'kubernetes-api', to: 'ptah-operator', kind: 'runs', label: 'Reconciled by' },
  { id: 'operator-runs-ptah', from: 'ptah-operator', to: 'ptah', kind: 'runs', label: 'Runs executor Jobs' },
  { id: 'ptah-starts-ydb', from: 'ptah', to: 'ptah-ydb-plugin', kind: 'runs', state: 'experimental', label: 'Starts the provider' },
  { id: 'ptah-recognizes-duckdb', from: 'ptah', to: 'ptah-duckdb-plugin', kind: 'runs', state: 'not-implemented', label: 'No services yet' },
  { id: 'ptah-recognizes-spanner', from: 'ptah', to: 'ptah-spanner-plugin', kind: 'runs', state: 'not-implemented', label: 'No services yet' },
  { id: 'homebrew-installs-ptah', from: 'homebrew-ptah', to: 'ptah', kind: 'installs', label: 'Installs release and edge builds' },
  { id: 'site-installs-ptah', from: 'ptah-run', to: 'ptah', kind: 'installs', label: 'Serves the installers' },
  { id: 'action-runs-ptah', from: 'ptah-action', to: 'ptah', kind: 'runs', label: 'Runs on pull requests' },
  { id: 'site-links-sandbox', from: 'ptah-run', to: 'ptah-sandbox', kind: 'links', label: 'Links to the playground' },
  { id: 'sandbox-runs-ptah', from: 'ptah-sandbox', to: 'ptah', kind: 'runs', label: 'Runs it as WebAssembly' },
  { id: 'conformance-measures-ptah', from: 'ptah-atlas-conformance', to: 'ptah', kind: 'checks', label: 'Measures compatibility' },
  { id: 'testkit-uses-ptah', from: 'ptah-testkit', to: 'ptah', kind: 'checks', label: 'Wraps the Go API' },
  { id: 'e2e-checks-action', from: 'ptah-action-e2e', to: 'ptah-action', kind: 'checks', label: 'Tests it end to end' },
];

/**
 * The usage paths a reader chooses between. The map offers each as a filter,
 * and the page states each in words. `relationships` is the whole path; a
 * component on the path is any end of one of them.
 */
export const paths = [
  {
    id: 'direct',
    label: 'CLI or CI',
    summary:
      'Run `ptah` from a shell, a script or a CI job, or through the GitHub Action. This path needs no operator and no console.',
    relationships: ['cli-runs-ptah', 'action-runs-ptah'],
    links: [
      { label: 'Get a first result', doc: 'start/quick-start/' },
      { label: 'Run Ptah in CI', doc: 'testing/ci/' },
      { label: 'Embed Ptah in Go', doc: 'extend/public-api/' },
    ],
  },
  {
    id: 'kubernetes',
    label: 'Kubernetes',
    summary:
      'Install the operator and manage its resources with `kubectl` and the `kubectl ptah` plugin. The operator reconciles them and runs Ptah in short-lived executor Jobs.',
    relationships: ['cli-manages-resources', 'api-reconciled-by-operator', 'operator-runs-ptah'],
    links: [
      { label: 'Kubernetes operator', doc: 'operate/kubernetes-operator/' },
      { label: 'Operator documentation', href: `${OperatorOrigin}/` },
    ],
  },
  {
    id: 'console',
    label: 'Kubernetes with the console',
    summary:
      'Add the console to the Kubernetes path when people want a browser view to inspect plans and approve them. It works through the same Kubernetes resources and replaces neither the operator nor Ptah.',
    relationships: ['console-manages-resources', 'api-reconciled-by-operator', 'operator-runs-ptah'],
    links: [
      { label: 'ptah-console', href: 'https://github.com/stokaro/ptah-console' },
      { label: 'Console compatibility', href: 'https://github.com/stokaro/ptah-console/blob/master/docs/compatibility.md' },
    ],
  },
];
