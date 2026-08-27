#!/usr/bin/env node
// Page health, and the navigation read in both directions.
//
// Per page: `title` and `description` frontmatter, no work-in-progress marker,
// and a sidebar entry that names it. Per sidebar entry: something that exists
// at the other end — a `slug:` names a page, an internal `link:` names a route
// this site publishes or a redirect it declares.
//
// The second direction is the half that was missing, and its absence was
// measured rather than suspected. A `{ slug: 'schema/does-not-exist' }` added
// to the sidebar left this gate printing `OK (68 sidebar entries)`: the number
// it offers as evidence of its own coverage went *up*, counting an entry that
// named nothing, and only a full `npm ci` plus site build found the typo. A
// `{ label, link }` entry was worse in both directions at once — a dead one was
// caught by nothing at all, and a live one did not count as coverage, so a page
// reachable only through it was reported as an orphan.
//
// Three of those four came from reading the sidebar as text. It is read as a
// value now; see the header of ../src/sidebar.mjs for why that module exists
// and why a regex over astro.config.mjs cannot be made to work.
//
// The pages come from git rather than from a directory walk, and the routes
// come from the same module the redirect gate uses, so this gate's spelling of
// a route cannot drift from that one's. That module also models the route the
// way Astro builds it -- a frontmatter `slug:` wins over the file path, a
// basename starting with `_` publishes nothing, and a segment github-slugger
// would rewrite is refused by name instead of guessed at -- and it refuses two
// files that publish one route. Every reason is written out in the header of
// ./lib/docroutes.mjs.
//
// Nothing here reports a pass on an empty set. Zero pages and zero sidebar
// entries are each a finding, because this gate reported `OK (67 sidebar
// entries)` with its page set pointed at an empty directory — a gate inspecting
// nothing, printing the same line it prints when everything is well.
import { readFileSync } from 'node:fs';
import { relative, sep } from 'node:path';
import { pathToFileURL } from 'node:url';

import { astroConfigPath, pages as collectPages, parseRedirectRoutes, sidebarEntries } from './lib/docroutes.mjs';
import { sidebar } from '../src/sidebar.mjs';

const frontmatterPattern = /^---\n([\s\S]*?)\n---/;
const weakMarkers = /\b(TODO|TBD|FIXME|coming soon)\b/i;

// A route in the spelling `redirectRoutes` keys use: leading slash, trailing
// slash, `/` for the site root.
const routePattern = /^\/([^/]+\/)*$/;

// A `link:` that leaves the site: anything carrying a scheme, and a
// protocol-relative `//host/path`. Those are somebody else's routes and this
// gate has no way to resolve them; check-links.mjs makes the same call for the
// same reason. Everything else a `link:` can say is a claim about this site and
// is checked.
const externalLink = /^(?:[a-z][a-z0-9+.-]*:|\/\/)/i;

// The site root is the one route no sidebar entry has to name. Starlight links
// it from the site title in every page's header, so it is reachable from
// everywhere by construction and an entry for it would be a second, redundant
// way to reach the page every reader lands on first. This is the whole of the
// exemption: a *subdirectory* index page is not exempt, and adding one without
// a sidebar entry is reported like any other orphan.
const rootRoute = '/';

function toPosix(value) {
  return value.split(sep).join('/');
}

function frontmatter(source) {
  const match = source.match(frontmatterPattern);
  return match?.[1] ?? '';
}

function hasFrontmatterKey(frontmatterSource, key) {
  return new RegExp(`^${key}:\\s*\\S`, 'm').test(frontmatterSource);
}

/**
 * The single implementation of every rule. `main` and the self-test both call
 * it, so a matcher that stops matching fails the self-test instead of passing
 * every page and every entry in silence.
 *
 * `pages` is `[{ path, route, source }]`, `entries` is what
 * `sidebarEntries()` returns, and `redirectSources` is the set of declared
 * redirect sources — a route the site still answers on, which is why a sidebar
 * link may name one.
 *
 * Returns the findings plus the two counts, so a caller can report what it
 * inspected rather than only that it finished.
 */
export function analyze({ pages, entries, redirectSources = new Set() }) {
  // Refusing an empty side is the first rule, not a precondition, because every
  // rule below is vacuously satisfied by an empty list. Report and stop: with
  // no pages, every sidebar entry is a phantom, and with no entries, every page
  // is an orphan. Either way the second list is noise wrapped around the one
  // fact worth printing.
  if (pages.length === 0) {
    return {
      findings: ['no documentation pages were found; refusing to report a pass having inspected nothing'],
      pages: 0,
      entries: entries.length,
    };
  }
  if (entries.length === 0) {
    return {
      findings: ['the sidebar names no page; refusing to report a pass having inspected nothing'],
      pages: pages.length,
      entries: 0,
    };
  }

  const findings = [];
  const liveRoutes = new Set(pages.map((page) => page.route));

  // Both kinds of entry count as coverage. A page named by `{ label, link }` is
  // reachable from the navigation exactly as a page named by `{ slug }` is, and
  // reporting it as an orphan is a false positive that pushes an author into
  // rewriting a working sidebar.
  const named = new Set(entries.map((entry) => entry.route).filter((route) => route !== null));

  for (const page of pages) {
    const meta = frontmatter(page.source);

    if (!hasFrontmatterKey(meta, 'title')) {
      findings.push(`${page.path}: missing title frontmatter`);
    }
    if (!hasFrontmatterKey(meta, 'description')) {
      findings.push(`${page.path}: missing description frontmatter`);
    }
    if (weakMarkers.test(page.source)) {
      findings.push(`${page.path}: contains TODO/TBD/FIXME/coming soon marker`);
    }
    if (page.route !== rootRoute && !named.has(page.route)) {
      findings.push(`${page.path}: route ${page.route} is not listed in the Starlight sidebar`);
    }
  }

  for (const entry of entries) {
    const where = entry.path === '' ? 'the sidebar' : `sidebar group ${entry.path}`;

    if (entry.kind === 'slug') {
      if (!liveRoutes.has(entry.route)) {
        findings.push(`${where}: entry names slug '${entry.value}', which has no page (route ${entry.route})`);
      }
      continue;
    }

    if (externalLink.test(entry.value)) continue;

    if (entry.route === null || !routePattern.test(entry.route)) {
      findings.push(
        `${where}: entry links '${entry.value}', which is not a /segment/ route with leading and trailing slashes`,
      );
      continue;
    }
    if (!liveRoutes.has(entry.route) && !redirectSources.has(entry.route)) {
      findings.push(`${where}: entry links ${entry.route}, which is neither a page nor a declared redirect`);
    }
  }

  return { findings, pages: pages.length, entries: entries.length };
}

// collect gathers the three inputs from the working tree. Anything it cannot
// read is thrown rather than defaulted: an empty page list, a sidebar that
// names nothing and an absent redirect map are each a broken invocation, and
// substituting a usable-looking value for any of them is how this gate came to
// report on nothing.
function collect() {
  // collectPages is the library's, not a walk and not a second route function:
  // it asks git for the pages, models the route the way Astro builds it -- a
  // frontmatter `slug:` included -- and refuses two files that publish one
  // route. That last refusal has to be on THIS gate's path rather than only on
  // the retirement gate's: with a shadow `schema/dbml/index.md` beside
  // `schema/dbml.md`, a page list built from the file names alone reports
  // `OK (69 pages, ...)` while the build silently serves one body in place of
  // the other.
  const pages = collectPages().map((page) => ({
    path: toPosix(relative(process.cwd(), page.absolute)),
    route: page.route,
    source: page.source,
  }));

  const entries = sidebarEntries(sidebar);

  const redirects = parseRedirectRoutes(readFileSync(astroConfigPath, 'utf8'));
  if (redirects === null) {
    throw new Error('astro.config.mjs: redirectRoutes map not found; moved pages must keep their redirect entries');
  }

  return { pages, entries, redirectSources: new Set(redirects.map(([from]) => from)) };
}

function selftest() {
  const failures = [];
  let asserted = 0;

  const assert = (condition, message) => {
    asserted += 1;
    if (!condition) failures.push(message);
  };

  const page = (route, extra = {}) => ({
    path: `src/content/docs${route === '/' ? '/index.mdx' : `${route.slice(0, -1)}.md`}`,
    route,
    source: extra.source ?? '---\ntitle: A page\ndescription: What it is for.\n---\n\nProse.\n',
  });

  // Fixtures are driven through sidebarEntries() rather than hand-built, so the
  // shape analyze() is tested against is the shape it is called with.
  const cleanPages = [page('/'), page('/schema/dbml/'), page('/schema/yaml/'), page('/atlas/overview/')];
  const cleanSidebar = [
    { label: 'Model', items: [{ slug: 'schema/dbml' }, { slug: 'schema/yaml' }] },
    { label: 'Atlas', items: [{ label: 'Deep', items: [{ slug: 'atlas/overview' }] }] },
    { label: 'Upstream', link: 'https://example.invalid/' },
  ];

  const clean = analyze({ pages: cleanPages, entries: sidebarEntries(cleanSidebar) });
  assert(clean.findings.length === 0, `clean fixture produced findings: ${clean.findings.join('; ')}`);
  // The counts are asserted, not just the emptiness: a matcher that stops
  // seeing pages or entries reports zero findings too.
  assert(clean.pages === 4, `clean fixture inspected ${clean.pages} pages, wanted 4`);
  assert(clean.entries === 4, `clean fixture inspected ${clean.entries} sidebar entries, wanted 4`);

  const fires = (result, needle, message) => {
    asserted += 1;
    if (!result.findings.some((finding) => finding.includes(needle))) {
      failures.push(`${message}: wanted a finding containing "${needle}", got ${JSON.stringify(result.findings)}`);
    }
  };
  const silent = (result, message) => {
    asserted += 1;
    if (result.findings.length > 0) {
      failures.push(`${message}: wanted no findings, got ${JSON.stringify(result.findings)}`);
    }
  };

  // Per-page rules.
  fires(
    analyze({
      pages: [page('/'), page('/schema/dbml/', { source: '---\ndescription: No title.\n---\n' })],
      entries: sidebarEntries(cleanSidebar),
    }),
    'missing title frontmatter',
    'a page without a title',
  );
  fires(
    analyze({
      pages: [page('/'), page('/schema/dbml/', { source: '---\ntitle: No description\n---\n' })],
      entries: sidebarEntries(cleanSidebar),
    }),
    'missing description frontmatter',
    'a page without a description',
  );
  fires(
    analyze({
      pages: [
        page('/'),
        page('/schema/dbml/', {
          source: '---\ntitle: T\ndescription: D\n---\n\nSupport is coming soon.\n',
        }),
      ],
      entries: sidebarEntries(cleanSidebar),
    }),
    'TODO/TBD/FIXME/coming soon marker',
    'a page carrying a work-in-progress marker',
  );

  // Page to sidebar.
  fires(
    analyze({ pages: [...cleanPages, page('/schema/orphan/')], entries: sidebarEntries(cleanSidebar) }),
    'route /schema/orphan/ is not listed in the Starlight sidebar',
    'a page no entry names',
  );
  // The root page is named by no entry here, and that is the exemption.
  silent(
    analyze({ pages: [page('/'), page('/schema/dbml/')], entries: sidebarEntries([{ slug: 'schema/dbml' }]) }),
    'the site root needs no entry of its own',
  );
  // A subdirectory index is not exempt; only the site root is.
  fires(
    analyze({
      pages: [page('/'), page('/schema/'), page('/schema/dbml/')],
      entries: sidebarEntries([{ slug: 'schema/dbml' }]),
    }),
    'route /schema/ is not listed in the Starlight sidebar',
    'a subdirectory index page with no entry',
  );
  // The false positive this gate used to produce: coverage by a link entry.
  silent(
    analyze({
      pages: [page('/'), page('/schema/dbml/')],
      entries: sidebarEntries([{ label: 'Model', items: [{ label: 'DBML', link: '/schema/dbml/' }] }]),
    }),
    'a page reachable only through a link entry is covered, not orphaned',
  );

  // Sidebar to page.
  fires(
    analyze({
      pages: cleanPages,
      entries: sidebarEntries([{ label: 'Model', items: [{ slug: 'schema/does-not-exist' }] }]),
    }),
    "entry names slug 'schema/does-not-exist', which has no page",
    'a slug entry naming no page',
  );
  fires(
    analyze({
      pages: cleanPages,
      entries: sidebarEntries([{ label: 'Model', items: [{ label: 'Phantom', link: '/schema/does-not-exist/' }] }]),
    }),
    'entry links /schema/does-not-exist/, which is neither a page nor a declared redirect',
    'a link entry naming no route',
  );
  // A link at a declared redirect resolves, because Astro publishes a stub
  // there. The control matters: without it, deleting the redirect lookup
  // altogether would read as a fix.
  silent(
    analyze({
      pages: cleanPages,
      entries: sidebarEntries([
        { label: 'Model', items: [{ slug: 'schema/dbml' }, { slug: 'schema/yaml' }] },
        { label: 'Atlas', items: [{ label: 'Deep', items: [{ slug: 'atlas/overview' }] }] },
        { label: 'Old', link: '/workflows/schema-files/' },
      ]),
      redirectSources: new Set(['/workflows/schema-files/']),
    }),
    'a link entry at a declared redirect source',
  );
  fires(
    analyze({
      pages: cleanPages,
      entries: sidebarEntries([{ label: 'Model', items: [{ label: 'Unslashed', link: '/schema/dbml' }] }]),
    }),
    "entry links '/schema/dbml', which is not a /segment/ route",
    'a link entry written without its trailing slash',
  );
  silent(
    analyze({
      pages: cleanPages,
      entries: sidebarEntries([
        { label: 'Model', items: [{ slug: 'schema/dbml' }, { slug: 'schema/yaml' }] },
        { label: 'Atlas', items: [{ label: 'Deep', items: [{ slug: 'atlas/overview' }] }] },
        { label: 'Upstream', link: 'https://example.invalid/anywhere' },
      ]),
    }),
    'an external link is somebody else\'s route and is left alone',
  );

  // The vacuous pass, from both ends. These are the cases that make every
  // assertion above worth something: a gate that reports OK on an empty set
  // reports OK on the day it stops reading.
  const noPages = analyze({ pages: [], entries: sidebarEntries(cleanSidebar) });
  fires(noPages, 'refusing to report a pass having inspected nothing', 'zero pages');
  assert(noPages.pages === 0, 'zero pages reports a page count of zero rather than a pass');
  const noEntries = analyze({ pages: cleanPages, entries: [] });
  fires(noEntries, 'the sidebar names no page', 'zero sidebar entries');

  if (failures.length > 0) {
    console.error('check-page-health.mjs --selftest: FAILED');
    for (const failure of failures) console.error(`- ${failure}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-page-health.mjs --selftest: OK (${asserted} assertions via analyze())`);
}

function main() {
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }

  let input;
  try {
    input = collect();
  } catch (error) {
    console.error(`check-page-health.mjs: ${error.message}`);
    process.exitCode = 2;
    return;
  }

  const { findings, pages, entries } = analyze(input);

  if (findings.length > 0) {
    console.error('Documentation page health check failed:');
    for (const finding of findings) {
      console.error(`- ${finding}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log(`check-page-health.mjs: OK (${pages} pages, ${entries} sidebar entries)`);
}

const invokedPath = process.argv[1];
if (invokedPath !== undefined && import.meta.url === pathToFileURL(invokedPath).href) {
  main();
}
