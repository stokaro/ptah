// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import tailwindcss from '@tailwindcss/vite';

// The navigation is a value in its own module because a plain Node script
// cannot import this file: Starlight's entry point is TypeScript inside
// node_modules and Node refuses to strip types there. scripts/check-page-health.mjs
// reads that module, so the sidebar the gate checks is the sidebar the site
// renders. See the header of src/sidebar.mjs.
import { sidebar } from './src/sidebar.mjs';

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
  '/documentation-map/': '/',
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
  '/workflows/testing/': '/testing/migrations-and-schema/',
  '/workflows/ci/': '/testing/ci/',
  '/workflows/oci-registry/': '/operate/oci-registry/',
  '/workflows/atlas-cli/': '/atlas/overview/',
  '/examples/atlas-migrations/': '/atlas/migrate-commands/',
  '/reference/atlas-project-config/': '/atlas/project-config/',
  '/reference/comparison/': '/atlas/comparison/',
  '/reference/atlas-docs-coverage/': '/atlas/docs-coverage/',
  '/operate/conformance/': '/atlas/conformance/',
  '/operate/license-boundary/': '/atlas/license-boundary/',
  '/reference/commands/': '/reference/native-commands/',
  '/reference/dialect-notes/': '/databases/support-matrix/',
  '/reference/testing/': '/reference/test-cases/',
  '/reference/public-api/': '/extend/public-api/',
  '/reference/reusable-components/': '/extend/components/',
  '/reference/query-builder/': '/extend/query-builder/',
  '/start/quick-start-declarative/': '/start/quick-start-direct/',
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
      sidebar,
    }),
  ],
  vite: {
    plugins: [tailwindcss()],
  },
});
