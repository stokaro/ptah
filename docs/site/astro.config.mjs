// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { ExpressiveCodeTheme } from '@astrojs/starlight/expressive-code';
import tailwindcss from '@tailwindcss/vite';

// The navigation is a value in its own module because a plain Node script
// cannot import this file: Starlight's entry point is TypeScript inside
// node_modules and Node refuses to strip types there. scripts/check-page-health.mjs
// reads that module, so the sidebar the gate checks is the sidebar the site
// renders. See the header of src/sidebar.mjs.
import { sidebar } from './src/sidebar.mjs';
import { pagefindRanking } from './src/lib/search-ranking.mjs';
import { Origin, BasePath } from './src/lib/docs-origin.mjs';
import { codeThemeDark, codeThemeLight } from './src/lib/code-theme.mjs';
import { satteri } from '@astrojs/markdown-satteri';
import { pluginLanguageLabel } from './src/lib/expressive-code-language-label.mjs';
import markdownAsides from './src/lib/markdown-asides.mjs';

const DOCS_VERSION = process.env.DOCS_VERSION || 'edge';
// Both from one declaration. They were literals written for a GitHub project
// page, and the move to the apex domain left them behind: `base` is what every
// relative URL on the site is built from, and `site` is what the canonical
// links and the sitemap name (stokaro/ptah#2884).
const site = Origin;
const base = BasePath(DOCS_VERSION);

// Moved page routes. Keys and values are docs routes with leading and trailing
// slashes; scripts/check-redirects.mjs verifies that every source is retired
// and every target resolves to a real page. Astro emits the destination
// verbatim into the meta-refresh stub, so the `/<version>/` base is
// prepended below before the map reaches Astro.
const redirectRoutes = {
  '/getting-started/': '/start/quick-start/',
  '/operate/inference-migrations/': '/inference/overview/',
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
  '/reference/comparison/': '/atlas/feature-matrix/',
  '/atlas/comparison/': '/atlas/feature-matrix/',
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
  markdown: {
    // Astro 7 renders Markdown and MDX through Sätteri, whose plugins visit the
    // mdast and the hast directly; the remark and rehype options are gone with
    // the unified pipeline. Starlight appends its own plugins to this processor
    // at setup, so an mdast plugin here runs before Starlight's asides plugin
    // and a hast plugin here sees the aside Starlight has already shaped.
    processor: satteri({
      hastPlugins: [markdownAsides()],
    }),
  },
  integrations: [
    starlight({
      title: 'Ptah',
      // No `logo` option: SiteTitle.astro, which replaces the component that
      // would read it, imports src/assets/logo.svg itself.
      // fonts.css declares the faces, global.css holds the Tailwind theme and the
      // layout measures, ptah.css is the design shared with ptah.run: tokens for
      // both themes, and every Starlight surface restyled to them.
      customCss: ['./src/styles/fonts.css', './src/styles/global.css', './src/styles/ptah.css'],
      lastUpdated: true,
      pagefind: {
        ranking: pagefindRanking,
      },
      components: {
        PageTitle: './src/components/PageTitle.astro',
        SiteTitle: './src/components/SiteTitle.astro',
        // Text links instead of icons, and a light/dark toggle instead of the
        // three-way select: the header controls ptah.run has. The theme slot
        // also carries the article-layout toggle while the choice between the
        // two layouts is open (HeaderToggles renders both).
        SocialIcons: './src/components/HeaderLinks.astro',
        ThemeSelect: './src/components/HeaderToggles.astro',
        // The rail with collapsed groups and subgroup labels, the contents rail
        // with the page actions above it, the disclosure strip, the footer
        // with the meta row and the previous / next cards.
        Sidebar: './src/components/Sidebar.astro',
        TableOfContents: './src/components/TableOfContents.astro',
        MobileTableOfContents: './src/components/MobileTableOfContents.astro',
        Footer: './src/components/Footer.astro',
        Head: './src/components/Head.astro',
      },
      // Code blocks: the two themes in src/lib/code-theme.mjs color the tokens,
      // and the frame's shape is set in src/styles/ptah/code.css through Expressive
      // Code's CSS variables rather than through `styleOverrides` here. Both
      // halves are explained where they live. One consequence of any change to
      // the themes or to this option: the code stylesheet is named by a hash of
      // its content, so it is renamed, while Astro's content layer keeps each
      // rendered page, token colors and `<link>` included, across runs (only a
      // change to the page's source or to this file outside `integrations`
      // invalidates it). After editing code-theme.mjs or this option, delete
      // node_modules/.astro and .astro before the next `astro dev` or
      // `astro build`, or cached pages keep linking a stylesheet that no longer
      // exists and their code blocks lose every style. CI builds from a fresh
      // checkout, so only a working tree carries the stale cache.
      expressiveCode: {
        themes: [new ExpressiveCodeTheme(codeThemeDark), new ExpressiveCodeTheme(codeThemeLight)],
        // The frame chrome keeps Starlight's variables, which ptah.css overrides.
        useStarlightUiThemeColors: true,
        // The themes' colors are the design's tokens, each measured above 4.5:1
        // in code-theme.mjs; Expressive Code's default of 5.5 would move them.
        minSyntaxHighlightingColorContrast: 0,
        // Copies each fence's language onto its frame header so the bar names
        // it from the fence rather than from a list of languages kept by hand.
        plugins: [pluginLanguageLabel()],
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
