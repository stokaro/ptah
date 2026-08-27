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

/** @type {import('@astrojs/starlight/types').StarlightUserConfig['sidebar']} */
export const sidebar = [
  {
    label: 'Start',
    items: [
      { slug: 'start/install' },
      { slug: 'start/quick-start' },
      { slug: 'start/choose-a-workflow' },
      { slug: 'start/adopt-an-existing-database' },
    ],
  },
  {
    label: 'Model your schema',
    items: [
      { slug: 'schema/work-with-a-source' },
      { slug: 'schema/go-annotations' },
      { slug: 'schema/yaml' },
      { slug: 'schema/hcl' },
      { slug: 'schema/sql' },
      { slug: 'schema/dbml' },
      { slug: 'schema/orm-and-external' },
      { slug: 'schema/composite' },
      { slug: 'schema/visualize' },
      { slug: 'schema/export' },
      { slug: 'schema/protobuf' },
    ],
  },
  {
    label: 'Direct schema changes',
    items: [
      { slug: 'direct/inspect' },
      { slug: 'direct/compare-and-drift' },
      { slug: 'direct/apply' },
    ],
  },
  {
    label: 'Versioned migrations',
    items: [
      { slug: 'versioned/overview' },
      { slug: 'versioned/generate' },
      { slug: 'versioned/apply' },
      { slug: 'versioned/rollback' },
      { slug: 'versioned/integrity-and-safety' },
      { slug: 'versioned/maintain-history' },
      { slug: 'versioned/import' },
      { slug: 'versioned/checkpoints' },
      { slug: 'versioned/reference-data' },
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
    label: 'Distribute and operate',
    items: [
      { slug: 'operate/ai-agents' },
      { slug: 'operate/ai-assist' },
      { slug: 'operate/oci-registry' },
      { slug: 'operate/seed-data' },
      { slug: 'operate/troubleshooting' },
    ],
  },
  {
    label: 'Databases',
    items: [
      { slug: 'databases/support-matrix' },
      { slug: 'databases/postgresql' },
      { slug: 'databases/sqlite' },
      { slug: 'databases/sqlserver' },
    ],
  },
  {
    label: 'Atlas compatibility',
    items: [
      { slug: 'atlas/overview' },
      { slug: 'atlas/adoption' },
      { slug: 'atlas/feature-matrix' },
      { slug: 'atlas/migrate-commands' },
      { slug: 'atlas/schema-commands' },
      { slug: 'atlas/project-config' },
      { slug: 'atlas/comparison' },
      { slug: 'atlas/retained-divergences' },
      { slug: 'atlas/conformance' },
      { slug: 'atlas/docs-coverage' },
      { slug: 'atlas/license-boundary' },
    ],
  },
  {
    label: 'Extend Ptah',
    items: [
      { slug: 'extend/public-api' },
      { slug: 'extend/components' },
      { slug: 'extend/query-builder' },
    ],
  },
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
    label: 'Reference',
    items: [
      { slug: 'reference/native-commands' },
      { slug: 'reference/atlas-commands' },
      { slug: 'reference/go-annotations' },
      { slug: 'reference/configuration' },
      { slug: 'reference/yaml-schema' },
      { slug: 'reference/hcl-schema' },
      { slug: 'reference/test-cases' },
      { slug: 'reference/capabilities' },
      { slug: 'reference/lint-rules' },
      { slug: 'reference/exit-codes' },
      { slug: 'reference/glossary' },
    ],
  },
];
