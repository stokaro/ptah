// The Starlight navigation, as a value rather than as text inside
// astro.config.mjs.
//
// It lives here because two readers need it and only one of them is Astro.
// `scripts/check-page-health.mjs` reads the sidebar in both directions — every
// page is named by an entry, every entry names something that exists — and it
// cannot import the config: Astro's config imports `@astrojs/starlight`, whose
// entry point is TypeScript inside `node_modules`, and Node refuses to strip
// types there (`ERR_UNSUPPORTED_NODE_MODULES_TYPE_STRIPPING`). That left the
// gate reading the config as text, with a regex, and a regex was measured blind
// in four ways at once: it cannot see a nested group, it cannot see a `link:`
// entry in either direction, and it accepts a commented-out entry as coverage —
// the page then disappears from every other page's navigation and the gate
// prints OK.
//
// So the array is declared where a plain Node script can import it, and the
// config imports the same value the gate reads. There is no second copy to
// drift.
//
// Keep this module dependency-free for the same reason: the moment it imports
// anything from `node_modules` that is not plain JavaScript, the gate goes back
// to reading text.
//
// Adding a page means adding an entry here in the same change. That is not a
// convention, it is the gate: a page named by no entry is an orphan, and an
// entry naming no page is a dead link.
//
// THE SHAPE OF THE TREE. A group may contain a group, recursively — Starlight's
// `items` schema is a lazy union that includes the group schema itself. This
// sidebar uses at most two levels below a root row, group → subgroup → page,
// and that cap is a reading rule rather than a limit anything enforces: a third
// level would pass every gate in this directory. Hold it in review, along with
// the rule that no list inside a root row runs past about eight.
//
// A group carries a `label` and `items` and nothing else that navigates: the
// schema has no `link` or `slug`. Ptah's Sidebar override recognizes an
// ordinary first item labeled `Overview`, renders its link on the expandable
// group row, and leaves the duplicate child out of the visible list. The item
// stays here as the single route source for Starlight, the docs gates,
// pagination, and breadcrumbs. `src/lib/sidebar.mjs` owns that recognition for
// every consumer.
//
// Only four entry shapes are safe here, because `scripts/lib/docroutes.mjs`
// tests `items` before `link`: a bare slug string, `{ slug }`, `{ slug, label }`
// and `{ label, items }` (optionally with `collapsed`). An object carrying both
// `link` and `items` is counted as a group by the gate and refused by
// Starlight's schema at build time, which is the one way to be green here and
// red in CI.
//
// `collapsed: true` hides a subgroup's items until the reader opens it, and
// still opens the group whenever the current page is inside it, at any depth.

/** @type {import('@astrojs/starlight/types').StarlightUserConfig['sidebar']} */
export const sidebar = [
  {
    label: 'Start',
    items: [
      { slug: 'start/install' },
      { slug: 'start/quick-start' },
      { slug: 'start/quick-start-migrations' },
      { slug: 'start/quick-start-direct' },
      { slug: 'start/choose-a-workflow' },
      { slug: 'start/adopt-an-existing-database' },
    ],
  },
  {
    label: 'Versioned migrations',
    items: [
      { slug: 'versioned/overview', label: 'Overview' },
      { slug: 'versioned/generate' },
      { slug: 'versioned/apply' },
      { slug: 'versioned/rollback' },
      { slug: 'versioned/integrity-and-safety' },
      { slug: 'versioned/lint' },
      { slug: 'versioned/maintain-history' },
      { slug: 'versioned/import' },
      { slug: 'versioned/checkpoints' },
    ],
  },
  {
    label: 'Direct schema changes',
    items: [
      { slug: 'direct/overview', label: 'Overview' },
      { slug: 'direct/inspect' },
      { slug: 'direct/compare-and-drift' },
      { slug: 'direct/plan-and-approve' },
      { slug: 'direct/apply' },
    ],
  },
  {
    label: 'Inference migrations',
    items: [
      { slug: 'inference/overview', label: 'Overview' },
      { slug: 'inference/quick-start' },
      {
        label: 'Concepts',
        items: [
          { slug: 'inference/concepts/embeddings-and-inference-state' },
          { slug: 'inference/concepts/generations' },
          { slug: 'inference/concepts/lifecycle' },
          { slug: 'inference/concepts/consistency' },
          { slug: 'inference/concepts/verification-and-cutover' },
          { slug: 'inference/concepts/security-and-data-boundaries' },
        ],
      },
      {
        label: 'Guides',
        items: [
          { slug: 'inference/guides/create-first-generation' },
          { slug: 'inference/guides/migrate-to-another-model' },
          { slug: 'inference/guides/migrate-a-live-table' },
          { slug: 'inference/guides/migrate-a-paused-source' },
          { slug: 'inference/guides/resume-and-recover' },
          { slug: 'inference/guides/rollback-and-retire' },
        ],
      },
      {
        label: 'Strategies',
        items: [
          { slug: 'inference/strategies/choose-a-consistency-mode' },
          { slug: 'inference/strategies/choose-a-target-layout' },
          { slug: 'inference/strategies/plan-provider-capacity' },
          { slug: 'inference/strategies/plan-vector-indexes' },
          { slug: 'inference/strategies/production-rollout' },
        ],
      },
      {
        label: 'Reference',
        items: [
          { slug: 'inference/reference/specification' },
          { slug: 'inference/reference/commands' },
          { slug: 'inference/reference/run-status-and-findings' },
          { slug: 'inference/reference/support-and-limitations' },
        ],
      },
      { slug: 'inference/troubleshooting' },
    ],
  },
  {
    label: 'Test and CI',
    items: [
      { slug: 'testing/migrations-and-schema' },
      { slug: 'testing/ci' },
    ],
  },
  {
    label: 'Load data',
    items: [
      { slug: 'versioned/reference-data' },
      { slug: 'operate/seed-data' },
    ],
  },
  {
    label: 'Distribute and operate',
    items: [
      { slug: 'operate/oci-registry' },
      { slug: 'operate/troubleshooting' },
    ],
  },
  {
    label: 'Schema',
    items: [
      {
        label: 'Sources',
        items: [
          { slug: 'schema/work-with-a-source' },
          { slug: 'schema/sql' },
          { slug: 'schema/yaml' },
          { slug: 'schema/hcl' },
          { slug: 'schema/dbml' },
          { slug: 'schema/go-annotations' },
          { slug: 'schema/orm-and-external' },
          { slug: 'schema/composite' },
        ],
      },
      {
        label: 'Analysis and documentation',
        items: [
          { slug: 'schema/validate-and-format' },
          { slug: 'schema/visualize' },
          { slug: 'schema/document' },
          { slug: 'schema/serve' },
          { slug: 'schema/stats' },
          { slug: 'schema/lineage' },
          { slug: 'schema/security' },
        ],
      },
      {
        label: 'Contract exports',
        items: [
          { slug: 'schema/export' },
          { slug: 'schema/protobuf' },
        ],
      },
    ],
  },
  {
    label: 'Databases',
    items: [
      { slug: 'databases/support-matrix' },
      { slug: 'databases/postgresql' },
      { slug: 'databases/mysql' },
      { slug: 'databases/sqlite' },
      { slug: 'databases/oracle' },
      { slug: 'databases/clickhouse' },
      { slug: 'databases/distributed' },
      { slug: 'databases/sqlserver' },
    ],
  },
  {
    label: 'Integrations',
    items: [
      {
        label: 'Go integration',
        items: [
          { slug: 'extend/public-api' },
          { slug: 'extend/components' },
          { slug: 'extend/query-builder' },
        ],
      },
      {
        label: 'AI and agents',
        items: [
          { slug: 'operate/ai-agents' },
          { slug: 'operate/ai-assist' },
        ],
      },
    ],
  },
  {
    label: 'Atlas compatibility',
    items: [
      { slug: 'atlas/overview', label: 'Overview' },
      { slug: 'atlas/adoption' },
      {
        label: 'Commands and configuration',
        items: [
          { slug: 'atlas/migrate-commands' },
          { slug: 'atlas/schema-commands' },
          { slug: 'atlas/project-config' },
        ],
      },
      {
        label: 'Differences and evidence',
        collapsed: true,
        items: [
          { slug: 'atlas/feature-matrix' },
          { slug: 'atlas/retained-divergences' },
          { slug: 'atlas/conformance' },
          { slug: 'atlas/docs-coverage' },
          { slug: 'atlas/license-boundary' },
        ],
      },
    ],
  },
  {
    label: 'Concepts and reference',
    items: [
      {
        label: 'Concepts',
        items: [
          { slug: 'concepts/desired-schema-and-sources' },
          { slug: 'concepts/migration-directory' },
          { slug: 'concepts/database-urls-and-dev-databases' },
          { slug: 'concepts/dialects-and-capabilities' },
        ],
      },
      {
        label: 'Command reference',
        items: [
          { slug: 'reference/native-commands' },
          { slug: 'reference/atlas-commands' },
          { slug: 'reference/command-flags' },
      { slug: 'reference/extension-variables' },
          { slug: 'reference/test-cases' },
        ],
      },
      {
        label: 'Format reference',
        items: [
          { slug: 'reference/configuration' },
          { slug: 'reference/go-annotations' },
          { slug: 'reference/hcl-schema' },
          { slug: 'reference/yaml-schema' },
        ],
      },
      {
        label: 'Rules and diagnostics',
        items: [
          { slug: 'reference/capabilities' },
          { slug: 'reference/lint-rules' },
          { slug: 'reference/exit-codes' },
          { slug: 'reference/glossary' },
        ],
      },
    ],
  },
];
