// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import tailwindcss from '@tailwindcss/vite';

const site = 'https://stokaro.github.io';
const DOCS_VERSION = process.env.DOCS_VERSION || 'edge';
const base = `/ptah/${DOCS_VERSION}/`;

// Moved page routes. Keys and values are docs routes with leading and trailing
// slashes; scripts/check-redirects.mjs verifies that every source is retired
// and every target resolves to a real page. Astro emits the destination
// verbatim into the meta-refresh stub, so the `/ptah/<version>/` base is
// prepended below before the map reaches Astro.
const redirectRoutes = {
  '/getting-started/': '/start/quick-start/',
  '/install/': '/start/install/',
  '/workflows/go-schema/': '/schema/go-annotations/',
  '/workflows/schema-files/': '/schema/yaml/',
  '/workflows/orm-loaders/': '/schema/orm-and-external/',
  '/workflows/api-schema-export/': '/schema/export/',
  '/examples/go-model/': '/schema/go-annotations/',
  '/examples/yaml-schema/': '/schema/yaml/',
  '/examples/atlas-hcl/': '/schema/hcl/',
  '/examples/schema-viz/': '/schema/visualize/',
  '/workflows/migrations/': '/versioned/overview/',
  '/workflows/checkpoints/': '/versioned/checkpoints/',
  '/workflows/reference-data/': '/versioned/reference-data/',
};

const redirects = Object.fromEntries(
  Object.entries(redirectRoutes).map(([from, to]) => [from, `${base}${to.slice(1)}`]),
);

export default defineConfig({
  site,
  base,
  redirects,
  integrations: [
    starlight({
      title: 'Ptah',
      logo: {
        src: './src/assets/logo.svg',
        alt: 'Ptah',
      },
      customCss: ['./src/styles/global.css'],
      components: {
        SiteTitle: './src/components/SiteTitle.astro',
      },
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/stokaro/ptah',
        },
      ],
      sidebar: [
        {
          label: 'Start',
          items: [
            { slug: 'start/install' },
            { slug: 'start/quick-start' },
            { slug: 'start/choose-a-workflow' },
            { slug: 'start/adopt-an-existing-database' },
          ],
        },
        { label: 'Documentation map', slug: 'documentation-map' },
        {
          label: 'Model your schema',
          items: [
            { slug: 'schema/go-annotations' },
            { slug: 'schema/yaml' },
            { slug: 'schema/hcl' },
            { slug: 'schema/sql' },
            { slug: 'schema/orm-and-external' },
            { slug: 'schema/composite' },
            { slug: 'schema/visualize' },
            { slug: 'schema/export' },
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
          label: 'Use Ptah',
          items: [
            { slug: 'workflows/oci-registry' },
            { slug: 'workflows/testing' },
            { slug: 'workflows/atlas-cli' },
            { slug: 'workflows/ci' },
          ],
        },
        {
          label: 'Examples',
          items: [
            { slug: 'examples/atlas-migrations' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { slug: 'reference/commands' },
            { slug: 'reference/configuration' },
            { slug: 'reference/yaml-schema' },
            { slug: 'reference/hcl-schema' },
            { slug: 'reference/atlas-project-config' },
            { slug: 'reference/public-api' },
            { slug: 'reference/reusable-components' },
            { slug: 'reference/query-builder' },
            { slug: 'reference/testing' },
            { slug: 'reference/capabilities' },
            { slug: 'reference/dialect-notes' },
            { slug: 'reference/comparison' },
            { slug: 'reference/atlas-docs-coverage' },
            { slug: 'reference/exit-codes' },
          ],
        },
        {
          label: 'Operate',
          items: [
            { slug: 'operate/troubleshooting' },
            { slug: 'operate/conformance' },
            { slug: 'operate/license-boundary' },
          ],
        },
      ],
    }),
  ],
  vite: {
    plugins: [tailwindcss()],
  },
});
