/**
 * What the page furniture computes once and three components read: the source
 * context and the actions built from it (the contents rail's split button and
 * the footer's links), the page-type badge, the dates, and the table of
 * contents as markup.
 *
 * PageTitle, TableOfContents and Footer are separate Starlight overrides that
 * each see the same route data; deriving everything here keeps the split
 * button in the rail and the links in the footer pointing at the same URLs.
 */
import { pageActionsForSource, resolveSourceContext, sourceCommitForBuild } from './source-context.mjs';
import releaseUiOverlay from '../../scripts/data/release-ui-overlay.json';

/** Page types that render no badge: a landing is the section, not a kind of page. */
const badgeFreeTypes = new Set(['landing']);

/**
 * The badge text beside the breadcrumb, as written in frontmatter (`how-to`,
 * not `How-to`): the concept sets it in mono uppercase, so casing is the
 * stylesheet's job.
 */
export function pageTypeBadge(entry) {
  const type = entry?.data?.type;
  if (typeof type !== 'string' || type === '' || badgeFreeTypes.has(type)) return undefined;
  return type;
}

/**
 * Whether the page has a Markdown source the actions can point at. The 404
 * page and the splash landing are rendered from templates the page-source
 * route does not publish.
 */
export function hasPageSource(entry) {
  if (!entry?.filePath) return false;
  if (entry.id === '404' || entry.id.endsWith('/404')) return false;
  return entry.data?.template !== 'splash';
}

/**
 * The source context and the actions for one page. `pageUrl` is the canonical
 * address (Astro.url.href), `baseUrl` the site base (import.meta.env.BASE_URL).
 * Returns `{ hasSource: false }` for pages without a source.
 */
export function pageSourceActions(entry, { pageUrl, baseUrl }) {
  if (!hasPageSource(entry)) return { hasSource: false };

  const legacyGeneratedPage = releaseUiOverlay.legacyGeneratedPages[entry.id];
  const sourceContext = resolveSourceContext({
    documentationVersion: process.env.DOCS_VERSION?.trim() || 'edge',
    sourceCommit: sourceCommitForBuild(),
    renderedSourcePath: `docs/site/${entry.filePath}`,
    generated: entry.data.generated ?? Boolean(legacyGeneratedPage),
    generator: entry.data.generator ?? legacyGeneratedPage?.generator,
    editSource: entry.data.editSource ?? legacyGeneratedPage?.editSource,
  });
  const pageActions = pageActionsForSource(sourceContext, { pageUrl, title: entry.data.title });

  return {
    hasSource: true,
    sourceContext,
    markdownUrl: `${baseUrl}page-source/${entry.id}.md`,
    markdownLabel: pageActions.markdownLabel,
    actions: pageActions.actions,
    // All three, in the order the heading row uses. The footer picked only two
    // of them, so a released version lost the link to its source AS IT WAS at
    // that release -- `data-view-ref` still carried the tag, but nothing a
    // reader could follow did (stokaro/ptah#2956).
    view: pageActions.actions.find((action) => action.kind === 'source'),
    edit: pageActions.actions.find((action) => action.kind === 'edit'),
    report: pageActions.actions.find((action) => action.kind === 'report'),
    sourceUrl: pageActions.sourceUrl,
    /* The footer shows the commit the build rendered, seven characters as Git
     * abbreviates it; a release built from a tag has no commit to show. */
    shortCommit: sourceContext.sourceCommit?.slice(0, 7),
  };
}

/**
 * The site publishes no llms.txt (nothing under public/ or scripts/ writes
 * one), so the split-button menu leaves out the concept's third row. Flip this
 * when an llms.txt route exists; the row's markup is in PageActions.astro.
 */
export const publishesLlmsTxt = false;

/** A calendar date rendered the way Starlight renders "Last updated". */
export function formatDate(date, lang = 'en') {
  return date.toLocaleDateString(lang, { dateStyle: 'medium', timeZone: 'UTC' });
}

/** The "Evidence verified" fact for pages that declare `lastVerified`. */
export function verifiedDate(entry, lang = 'en') {
  const iso = entry?.data?.lastVerified;
  if (!iso) return undefined;
  return { datetime: iso, label: formatDate(new Date(`${iso}T00:00:00Z`), lang) };
}

function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

/**
 * The table of contents as nested lists, the shape Starlight's
 * TableOfContentsList renders: `ul > li > a[href="#slug"] > span`, children in
 * a nested `ul`. Starlight's tracking script reads the `a` elements and their
 * hashes, so the anchors must stay plain links with the slug as the fragment.
 * The list is built here rather than with a recursive component because the
 * two contents components share it and the depth is expressed by nesting, not
 * by a per-level variable.
 */
export function tableOfContentsHtml(items) {
  if (!Array.isArray(items) || items.length === 0) return '';
  const rows = items.map((heading) => {
    const link = `<a href="#${encodeURIComponent(heading.slug)}"><span>${escapeHtml(heading.text)}</span></a>`;
    return `<li>${link}${tableOfContentsHtml(heading.children)}</li>`;
  });
  return `<ul>${rows.join('')}</ul>`;
}
