// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import tailwindcss from '@tailwindcss/vite';

const site = 'https://stokaro.github.io';
const DOCS_VERSION = process.env.DOCS_VERSION || 'edge';
const base = `/ptah/${DOCS_VERSION}/`;

export default defineConfig({
  site,
  base,
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
        { label: 'Start', slug: 'getting-started' },
        { label: 'Install Ptah', slug: 'install' },
        { label: 'Documentation map', slug: 'documentation-map' },
        {
          label: 'Use Ptah',
          items: [
            { slug: 'workflows/go-schema' },
            { slug: 'workflows/schema-files' },
            { slug: 'workflows/api-schema-export' },
            { slug: 'workflows/migrations' },
            { slug: 'workflows/atlas-cli' },
            { slug: 'workflows/ci' },
          ],
        },
        {
          label: 'Examples',
          items: [
            { slug: 'examples/go-model' },
            { slug: 'examples/yaml-schema' },
            { slug: 'examples/atlas-hcl' },
            { slug: 'examples/atlas-migrations' },
            { slug: 'examples/schema-viz' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { slug: 'reference/commands' },
            { slug: 'reference/configuration' },
            { slug: 'reference/capabilities' },
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
