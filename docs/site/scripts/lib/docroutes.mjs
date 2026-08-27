#!/usr/bin/env node
// docroutes: the one answer to "which routes does this documentation site
// publish, what is each one called, and which of them does the navigation
// name". Four gates need that answer and none of them owns it.
//
// It is written here once because it cannot survive being written twice, and
// the evidence for that is already in the tree. `check-redirects.mjs` spells a
// route `/a/b/`, because that is the spelling a `redirectRoutes` key uses.
// `check-page-health.mjs` spells the same route `a/b`, because that is the
// spelling a Starlight `slug:` uses. Both are individually correct against
// their own copy, both pass their own tests, and the two route gates the
// documentation rewrite adds would have been the third and fourth copy. This
// is the shape AGENTS.md records under "Recognition that spans two functions
// belongs to one of them": the mismatch is invisible from either end, so the
// set has to be recognized in one place and the callers have to drive it.
//
// Why each function cannot become two:
//
//   contentFiles  — discovery. A `readdirSync` walk and `git ls-files` agree on
//     a clean tree (68 pages against 68 when this was written) and stop agreeing
//     the moment a git worktree is parked under the content root: 68 against
//     209, the 141 extra belonging to a different checkout, and the gate that
//     walks believes them. Whichever caller keeps its own walk is the one that
//     will be wrong then. This is the same reasoning `scripts/list-go-modules.sh`
//     and `scripts/check-test-style.sh` already carry, for the same hazard.
//   routeFor      — the file-to-URL spelling, including the two special cases
//     (`index` is `/`, `a/index` is `/a/`). A second copy diverges silently on
//     exactly those two, because every other path maps the obvious way.
//   pages         — the page list every gate reports on, and the one place that
//     reads a page's frontmatter for the route it declares.
//   liveRoutes    — the set the redirect map, the retirement ledger and the
//     sidebar are all compared against. Two spellings of that set make each
//     comparison correct and the pair meaningless.
//   parseRedirectRoutes — one literal, two readers: the gate that validates the
//     declarations and the gate that asks which retirements were declared. A
//     second parser stops agreeing the day the literal's shape changes.
//   sidebarEntries — the navigation, read in both directions (page to sidebar,
//     sidebar to page). It takes the sidebar as a *value*, never as the text of
//     `astro.config.mjs`: a regex over that file cannot see a nested group,
//     cannot see a `link:` entry in either direction, and accepts a
//     commented-out entry as coverage — all three measured against the gate
//     that does it that way today.
//
// THE ROUTE IS ASTRO'S, NOT THIS FILE'S IDEA OF IT. A file path is not the URL
// it publishes, and each of the three places they part company was measured on
// this site rather than reasoned about:
//
//   - A frontmatter `slug:` replaces the path entirely. Astro's glob loader
//     opens with `if (data.slug) return String(data.slug)`
//     (node_modules/astro/dist/content/loaders/glob.js), and Starlight routes on
//     that id. Measured: `slug: schema/dbml-renamed` on `schema/dbml.md` builds
//     `dist/schema/dbml-renamed/`, leaves no `dist/schema/dbml/`, and puts a
//     dead `/ptah/edge/schema/dbml/` in the navigation of all 67 other pages.
//     A path-only route function calls that route live and records the wrong
//     one, so the ledger guards a URL nobody serves.
//   - Every path segment goes through github-slugger. Astro builds the id as
//     `rawSlugSegments.map(githubSlug).join('/')` (astro/dist/content/utils.js).
//     Measured: `reference/CLI.md` builds `dist/reference/cli/`, and a
//     path-only route function names `/reference/CLI/` — a route that does not
//     exist, reported against a page that is fine.
//   - A basename starting with `_` publishes nothing. Starlight's loader globs
//     `**/[^_]*.{...}` and says so in its own comment. Measured:
//     `schema/_partial.md` adds no page to a build and no directory to `dist`,
//     while `_hidden/page.md` builds `dist/_hidden/page/` — the filter is on the
//     basename, not on the directories above it.
//
// This module models the first and the third, because both are exact. It does
// not model the second: github-slugger is a dependency these gates deliberately
// do not have, since `scripts/check-gate-selftests.sh` runs them in a throwaway
// worktree that has no `node_modules`. A segment it cannot map is therefore
// REFUSED by name rather than guessed at — `[a-z0-9_-]+` is the set
// github-slugger leaves alone, and anything else exits the caller with a
// message naming the file. An unmodeled route is loud, never absent.
//
// One completeness caveat, stated here rather than left to be rediscovered:
// every route this site publishes comes from the content collection, because
// `docs/site/src` holds no `pages/` directory. The day a `src/pages/*.astro`
// route appears, `contentFiles` stops being the whole route list and every
// caller inherits the gap. Starlight's loader also globs five further markdown
// extensions (`.markdown`, `.mdown`, `.mkdn`, `.mkd`, `.mdwn`); measured, a
// `.markdown` file under the content root adds no page to a build and no
// directory to `dist`, so enumerating `.md` and `.mdx` is the whole route list
// today.
//
// Nothing here reports; the callers do. What this module refuses to do is hand
// a caller an empty list that reads like an answer — an unusable input throws,
// so a gate built on it cannot report a pass having compared nothing.

import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));

// The Astro site root, the directory holding package.json. Not exported: what
// a caller needs is one of the two locations below, and a second way to spell
// them is the drift this module exists to prevent.
const siteRoot = join(scriptDir, '..', '..');

/** Absolute path of the Starlight content collection, the root of every route. */
export const contentRoot = join(siteRoot, 'src', 'content', 'docs');

/** Absolute path of the Astro config, which holds the `redirectRoutes` literal. */
export const astroConfigPath = join(siteRoot, 'astro.config.mjs');

const pageExtension = /\.(?:md|mdx)$/;

// A leading slash, a leading backslash and a drive letter are refused on every
// platform, including where that spelling is only an odd directory name. What a
// path is allowed to be must not depend on the machine reading it.
const absolutePrefix = /^(?:[/\\]|[A-Za-z]:)/;

// The characters github-slugger leaves alone: it lowercases, drops punctuation
// and turns a space into a hyphen, so a segment already spelled this way maps
// to itself and every other segment maps to something this module would have to
// guess. `_` is in the set deliberately — it survives slugification, which is
// why `_hidden/page.md` publishes `/_hidden/page/` even though `_partial.md`
// publishes nothing.
const slugStableSegment = /^[a-z0-9_-]+$/;

const frontmatterBlock = /^---\r?\n([\s\S]*?)\r?\n---/;

// A simple scalar `slug:` and nothing else. A folded or block value, a list and
// an anchor are all legal YAML this module cannot resolve, so they are refused
// by the caller of routeForDeclaredSlug rather than half-read.
const declaredSlug = /^slug:[ \t]*(.*)$/m;

// Only the platform separator is converted. A backslash is a legal character in
// a POSIX file name, and rewriting it here would turn one page into two path
// segments on the platform where it is not a separator at all.
function toPosix(value) {
  return value.split(sep).join('/');
}

/**
 * An input this module cannot model, as distinct from one it can model and
 * reject. Callers report it and stop; nobody catches it to carry on with a
 * smaller list, because a smaller list is the failure being prevented.
 */
export class UnmodeledRoute extends Error {
  constructor(message) {
    super(`docroutes: ${message}`);
    this.name = 'UnmodeledRoute';
  }
}

/**
 * Whether a page file publishes anything at all.
 *
 * Starlight's loader globs only basenames that do not start with `_`, so such
 * a basename is a partial: it is a real file the style gates still govern, and it
 * is not a route. The filter is on the basename only — measured, a page inside
 * an `_`-prefixed directory does build.
 */
export function isPublished(relativePath) {
  return !basename(toPosix(String(relativePath))).startsWith('_');
}

/**
 * Every published page file under `root`, as a sorted list of paths relative to
 * `root`, in POSIX spelling.
 *
 * The list comes from git — `--cached` so tracked pages count, `--others
 * --exclude-standard` so a page that exists but has not been added yet counts
 * too, and `core.quotePath=false` so a non-ASCII path survives a line-based
 * read. `root` therefore has to live inside this repository; a directory
 * outside one is refused rather than walked.
 *
 * Throws when git cannot answer and when git answers with nothing. An empty
 * content root is a broken invocation, not a result, and returning `[]` for it
 * is how a gate comes to pass without comparing anything.
 */
export function contentFiles(root = contentRoot) {
  let output;
  try {
    output = execFileSync(
      'git',
      [
        '-C',
        root,
        '-c',
        'core.quotePath=false',
        'ls-files',
        '--cached',
        '--others',
        '--exclude-standard',
        '--',
        '*.md',
        '*.mdx',
      ],
      { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'] },
    );
  } catch (cause) {
    const detail = String(cause.stderr ?? cause.message ?? cause).trim().split('\n')[0];
    throw new Error(`docroutes: git could not enumerate the pages under ${root}: ${detail}`, { cause });
  }

  // An unmerged path is listed once per stage, so the same page can arrive
  // three times during a conflicted merge.
  const files = [
    ...new Set(
      output
        .split('\n')
        .map((line) => line.replace(/\r$/, ''))
        .filter((line) => line.length > 0),
    ),
  ]
    .filter((file) => isPublished(file))
    .sort();

  if (files.length === 0) {
    throw new Error(`docroutes: git reports no .md or .mdx pages under ${root}; refusing to answer with an empty list`);
  }

  return files;
}

function assertSlugStable(segments, what) {
  for (const segment of segments) {
    if (slugStableSegment.test(segment)) continue;
    throw new UnmodeledRoute(
      `${what} has the segment ${JSON.stringify(segment)}, which Astro puts through github-slugger; ` +
        'this module models only segments github-slugger leaves alone ([a-z0-9_-]), so rename it or teach docroutes the mapping',
    );
  }
}

/**
 * The route a page file publishes when it declares no `slug:` of its own, in
 * the slash-wrapped spelling that `redirectRoutes` keys and Astro's own
 * redirect map use: `schema/dbml.md` becomes `/schema/dbml/`, `index.mdx`
 * becomes `/`, and `schema/index.md` becomes `/schema/`.
 *
 * Takes a path relative to the content root — what `contentFiles` returns.
 * Refuses a segment Astro would slugify into something else; see the header.
 */
export function routeFor(relativePath) {
  const path = toPosix(String(relativePath));

  if (absolutePrefix.test(path)) {
    throw new Error(`docroutes: routeFor wants a path relative to the content root, got ${relativePath}`);
  }
  if (!pageExtension.test(path)) {
    throw new Error(`docroutes: ${relativePath} is not a .md or .mdx page`);
  }

  const withoutExtension = path.replace(pageExtension, '');
  assertSlugStable(withoutExtension.split('/'), String(relativePath));

  if (withoutExtension === 'index') return '/';
  const route = withoutExtension.endsWith('/index')
    ? withoutExtension.slice(0, -'/index'.length)
    : withoutExtension;
  return `/${route}/`;
}

/**
 * The slash-wrapped route a Starlight `slug:` names. The sidebar says `a/b` and
 * the redirect map says `/a/b/`; this is the whole of the translation between
 * them, and it lives beside `routeFor` so the two spellings cannot drift apart.
 */
export function routeForSlug(slug) {
  const value = toPosix(String(slug));
  if (value === '') return '/';
  return `/${value}/`;
}

/**
 * The `slug:` a page declares in its frontmatter, or null when it declares
 * none.
 *
 * A value this module cannot read as a plain scalar is refused rather than
 * guessed at: it is a route being declared, and reading half of it produces a
 * route nobody serves.
 */
export function frontmatterSlug(source, what = 'a page') {
  const block = source.match(frontmatterBlock);
  if (!block) return null;
  const declared = block[1].match(declaredSlug);
  if (!declared) return null;

  let value = declared[1].trim();
  const comment = value.match(/^([^#]*?)\s+#.*$/);
  if (comment) value = comment[1].trim();
  if ((value.startsWith("'") && value.endsWith("'") && value.length > 1) ||
      (value.startsWith('"') && value.endsWith('"') && value.length > 1)) {
    value = value.slice(1, -1);
  }

  if (value === '') {
    throw new UnmodeledRoute(`${what} declares an empty slug:`);
  }
  return value;
}

/**
 * The route a declared `slug:` publishes.
 *
 * Astro takes the declared value verbatim as the entry id — it does not
 * slugify it — so a value this module cannot map onto a route is refused by
 * name. `index` is the site root, and a declared slug ending in `/index` is
 * refused because the `/index` stripping Astro applies belongs to the derived
 * id and not to a declared one.
 */
export function routeForDeclaredSlug(slug, what = 'a page') {
  const value = toPosix(String(slug));
  if (value.startsWith('/') || value.endsWith('/')) {
    throw new UnmodeledRoute(`${what} declares slug ${JSON.stringify(slug)}, which has a leading or trailing slash`);
  }
  const segments = value.split('/');
  assertSlugStable(segments, `${what} declares slug ${JSON.stringify(slug)}, which`);
  if (value === 'index') return '/';
  if (segments.includes('index')) {
    throw new UnmodeledRoute(
      `${what} declares slug ${JSON.stringify(slug)}, which names an index segment; ` +
        'Astro strips /index from a derived id and not from a declared one, so this route is not modeled here',
    );
  }
  return `/${value}/`;
}

/**
 * The route one page publishes: its declared `slug:` when it has one, its path
 * otherwise.
 */
export function routeForPage(relativePath, source) {
  const slug = frontmatterSlug(source, relativePath);
  if (slug === null) return routeFor(relativePath);
  return routeForDeclaredSlug(slug, relativePath);
}

/**
 * Every published page, as `{ file, absolute, route, source }`, sorted by file.
 *
 * `file` is relative to `root` in POSIX spelling, `absolute` is what a reader
 * opens, `route` is what the site serves and `source` is the file's text — read
 * once here because the route depends on it, so a caller that also wants the
 * text is not made to read it twice.
 *
 * Two files that resolve to one route are an error rather than a list with one
 * fewer route: a `Set` of routes would swallow the collision and every count
 * downstream would be quietly short, while the build resolves it by letting one
 * page's body replace the other's. Measured: adding `schema/dbml/index.md`
 * beside `schema/dbml.md` builds a `dist/schema/dbml/index.html` carrying the
 * new file's body, with a `[WARN]` nothing reads and exit 0.
 */
export function pages(root = contentRoot) {
  const owners = new Map();
  const collected = [];

  for (const file of contentFiles(root)) {
    const absolute = join(root, file);
    const source = readFileSync(absolute, 'utf8');
    const route = routeForPage(file, source);

    const owner = owners.get(route);
    if (owner !== undefined) {
      throw new UnmodeledRoute(`${owner} and ${file} both publish ${route}; one of them has to go`);
    }
    owners.set(route, file);
    collected.push({ file, absolute, route, source });
  }

  return collected;
}

/**
 * Every route the site publishes today, in the `routeFor` spelling.
 */
export function liveRoutes(root = contentRoot) {
  return new Set(pages(root).map((page) => page.route));
}

/**
 * The `redirectRoutes` literal in `astro.config.mjs`, as `[from, to]` pairs.
 *
 * Returns `null` when the literal is absent, which callers report as an error:
 * a config with no redirect map is a config that lost one.
 */
export function parseRedirectRoutes(configSource) {
  const block = configSource.match(/const redirectRoutes = \{([\s\S]*?)\};/);
  if (!block) return null;
  const entries = [];
  for (const match of block[1].matchAll(/'([^']+)':\s*'([^']+)'/g)) {
    entries.push([match[1], match[2]]);
  }
  return entries;
}

function describe(trail, index) {
  const where = trail.length > 0 ? trail.join(' > ') : 'the sidebar';
  return `${where} entry ${index}`;
}

function collectSidebar(items, trail, found) {
  for (const [index, item] of items.entries()) {
    const path = trail.join(' > ');

    // Starlight's shorthand: a bare string is a slug.
    if (typeof item === 'string') {
      found.push({ kind: 'slug', value: item, label: null, path, route: routeForSlug(item) });
      continue;
    }

    if (item === null || typeof item !== 'object' || Array.isArray(item)) {
      throw new Error(`docroutes: ${describe(trail, index)} is neither a slug string nor an entry object`);
    }

    // A group, at any depth. The rewrite adds a second level, and a reader that
    // stopped at the first would report the pages below it as named by nobody.
    if (Array.isArray(item.items)) {
      collectSidebar(item.items, [...trail, typeof item.label === 'string' ? item.label : `[${index}]`], found);
      continue;
    }

    // Starlight can fill a group from a directory. Nothing here can see inside
    // one, so it is refused out loud rather than counted as zero entries.
    if (item.autogenerate !== undefined) {
      throw new Error(
        `docroutes: ${describe(trail, index)} is an autogenerated group; this module cannot enumerate what Starlight fills it with`,
      );
    }

    if (typeof item.slug === 'string') {
      found.push({
        kind: 'slug',
        value: item.slug,
        label: typeof item.label === 'string' ? item.label : null,
        path,
        route: routeForSlug(item.slug),
      });
      continue;
    }

    if (typeof item.link === 'string') {
      found.push({
        kind: 'link',
        value: item.link,
        label: typeof item.label === 'string' ? item.label : null,
        path,
        // Left verbatim, never normalized: a link written without its trailing
        // slash names no route, and a caller has to be able to say so.
        route: item.link.startsWith('/') ? item.link : null,
      });
      continue;
    }

    throw new Error(`docroutes: ${describe(trail, index)} names neither a slug, a link, nor a nested group`);
  }
}

/**
 * Every entry in a Starlight sidebar, flattened out of however many levels of
 * group it is written in.
 *
 * Takes the sidebar array itself, not the text of the file it is written in.
 * `astro.config.mjs` cannot be imported by a plain Node script — Starlight's
 * entry point is TypeScript inside `node_modules` and Node refuses to strip
 * types there — so the array has to live in a dependency-free module the config
 * imports, and this function reads that value.
 *
 * Each entry carries:
 *   kind   'slug' for a page the sidebar names by slug, 'link' for an href;
 *   value  the slug or the href, exactly as written;
 *   label  the entry's own label, or null when it takes the page's title;
 *   path   the group labels leading to it, joined with ' > ', empty at the top;
 *   route  the slash-wrapped route it names, or null for a link that leaves the
 *          site.
 *
 * `kind` is what lets a caller tell the two apart: a `slug:` naming no page and
 * a `link:` naming no route are different defects with different repairs, and
 * a page named only by a `link:` is covered rather than orphaned.
 *
 * A sidebar that yields no entries throws. A navigation naming nothing is not a
 * navigation, and a gate that accepted it would be reporting on an empty set.
 */
export function sidebarEntries(sidebar) {
  if (!Array.isArray(sidebar)) {
    throw new Error(`docroutes: sidebarEntries wants the sidebar array, got ${sidebar === null ? 'null' : typeof sidebar}`);
  }

  const found = [];
  collectSidebar(sidebar, [], found);

  if (found.length === 0) {
    throw new Error('docroutes: the sidebar names no page; refusing to answer with an empty list');
  }

  return found;
}

// ---------------------------------------------------------------------------
// Selftest.
//
// This module is a library rather than a gate, and the gates built on it drive
// their own fixtures through their own `analyze`. That leaves the things only
// this module does unproven by anybody: enumerating pages through git rather
// than through a walk, modelling Astro's route the way Astro builds it, and
// flattening a sidebar that is nested more than one level deep. All of that is
// the kind of code that keeps working while quietly covering less, so each is
// asserted here — including the refusals, because a refusal nobody exercises is
// a refusal that can be deleted without a test going red.

function selftestAssertions() {
  let count = 0;
  return {
    count: () => count,
    assert(condition, message) {
      count += 1;
      if (!condition) throw new Error(`docroutes selftest: ${message}`);
    },
    throws(run, needle, message) {
      count += 1;
      try {
        run();
      } catch (error) {
        if (String(error.message).includes(needle)) return;
        throw new Error(`docroutes selftest: ${message}: expected a message containing ${JSON.stringify(needle)}, got ${JSON.stringify(error.message)}`);
      }
      throw new Error(`docroutes selftest: ${message}: nothing was thrown`);
    },
  };
}

function gitInit(directory) {
  execFileSync('git', ['-c', 'init.defaultBranch=main', 'init', '-q', directory], { stdio: ['ignore', 'pipe', 'pipe'] });
}

const plainPage = '---\ntitle: A page\ndescription: What it is for.\n---\n\nProse.\n';

function selftest() {
  const { assert, throws, count } = selftestAssertions();

  // routeFor — the two special cases are the whole reason a second copy is
  // dangerous, so both are pinned alongside the ordinary one.
  assert(routeFor('index.mdx') === '/', 'the root page is /');
  assert(routeFor('schema/index.md') === '/schema/', 'a directory index loses its /index');
  assert(routeFor('schema/dbml.md') === '/schema/dbml/', 'an ordinary page is slash-wrapped');
  assert(routeFor('atlas/conformance.mdx') === '/atlas/conformance/', '.mdx resolves like .md');
  assert(routeFor(join('schema', 'dbml.md')) === '/schema/dbml/', 'a platform separator is accepted');
  throws(() => routeFor('/schema/dbml.md'), 'relative to the content root', 'refuses a POSIX absolute path');
  throws(() => routeFor('C:/schema/dbml.md'), 'relative to the content root', 'refuses a drive letter on every platform');
  throws(() => routeFor('schema/dbml.txt'), 'not a .md or .mdx page', 'refuses a non-page file');

  // A segment Astro would slugify. Measured on this site: reference/CLI.md
  // builds dist/reference/cli/, so calling its route /reference/CLI/ names a
  // route nobody serves and reports a finding against a page that is fine.
  throws(() => routeFor('reference/CLI.md'), 'github-slugger', 'refuses an uppercase segment rather than inventing /reference/CLI/');
  throws(() => routeFor('schema/My Page.md'), 'github-slugger', 'refuses a segment holding a space');
  throws(() => routeFor('schema/dot.ted/page.md'), 'github-slugger', 'refuses a segment holding a dot');
  assert(routeFor('_hidden/page.md') === '/_hidden/page/', 'an underscore directory is a real segment, because github-slugger keeps it');

  // The `_` filter is on the basename. Both halves matter: a partial publishes
  // nothing, and a page below an `_`-prefixed directory publishes normally.
  assert(isPublished('schema/dbml.md'), 'an ordinary page publishes');
  assert(!isPublished('schema/_partial.md'), 'an underscore basename publishes nothing');
  assert(isPublished('_hidden/page.md'), 'an underscore DIRECTORY does not hide the page below it');

  // A declared slug replaces the path. This is the divergence that made the
  // ledger record a route that 404s while the served route went unrecorded.
  assert(frontmatterSlug(plainPage) === null, 'a page declaring no slug reports null');
  assert(frontmatterSlug('---\ntitle: T\nslug: schema/dbml-renamed\n---\n') === 'schema/dbml-renamed', 'a declared slug is read');
  assert(frontmatterSlug('---\ntitle: T\nslug: "schema/quoted"\n---\n') === 'schema/quoted', 'a quoted slug loses its quotes');
  assert(frontmatterSlug('---\ntitle: T\nslug: schema/x # why\n---\n') === 'schema/x', 'a trailing comment is not part of the slug');
  assert(frontmatterSlug('Body only, no frontmatter.\n') === null, 'a file with no frontmatter declares no slug');
  throws(() => frontmatterSlug('---\ntitle: T\nslug:\n---\n'), 'empty slug', 'refuses an empty declared slug');

  assert(routeForPage('schema/dbml.md', plainPage) === '/schema/dbml/', 'a page with no declared slug keeps its path route');
  assert(
    routeForPage('schema/dbml.md', '---\ntitle: T\nslug: schema/dbml-renamed\n---\n') === '/schema/dbml-renamed/',
    'a declared slug wins over the path',
  );
  assert(routeForDeclaredSlug('index') === '/', 'a declared slug of index is the site root');
  throws(() => routeForDeclaredSlug('/schema/x'), 'leading or trailing slash', 'refuses a declared slug wrapped in slashes');
  throws(() => routeForDeclaredSlug('Schema/DBML'), 'github-slugger', 'refuses a declared slug this module cannot map');
  throws(() => routeForDeclaredSlug('schema/index'), 'index segment', 'refuses a declared slug ending in index');

  // contentFiles — the enumeration is git's, and the fixture that says so is a
  // checkout parked inside the tree being enumerated. That is the hazard
  // measured on this repository: 68 pages by git against 209 by a walk, the
  // extra 141 belonging to another checkout, and every one of them believed.
  // A clean tree cannot tell the two apart, so a selftest that only asserts the
  // real content root proves nothing about which one is running.
  const outsideRepository = mkdtempSync(join(tmpdir(), 'docroutes-nogit-'));
  const emptyRepository = mkdtempSync(join(tmpdir(), 'docroutes-empty-'));
  const nestedProbe = mkdtempSync(join(tmpdir(), 'docroutes-nested-'));
  const collisionProbe = mkdtempSync(join(tmpdir(), 'docroutes-collision-'));
  try {
    throws(
      () => contentFiles(outsideRepository),
      'git could not enumerate',
      'refuses a directory that is in no repository, which a walk would have listed',
    );

    gitInit(emptyRepository);
    throws(
      () => contentFiles(emptyRepository),
      'refusing to answer with an empty list',
      'refuses to report zero pages as a result',
    );

    gitInit(nestedProbe);
    writeFileSync(join(nestedProbe, 'page.md'), plainPage);
    writeFileSync(join(nestedProbe, '_partial.md'), plainPage);
    const parked = join(nestedProbe, 'parked-checkout');
    mkdirSync(parked);
    gitInit(parked);
    writeFileSync(join(parked, 'AGENTS.md'), '# another checkout\n');

    const enumerated = contentFiles(nestedProbe);
    assert(enumerated.includes('page.md'), 'a page in the tree being enumerated is found');
    assert(
      !enumerated.includes('parked-checkout/AGENTS.md'),
      `a checkout parked inside the tree contributes no pages; a walk would have counted it (got ${JSON.stringify(enumerated)})`,
    );
    assert(!enumerated.includes('_partial.md'), `a partial is not a page (got ${JSON.stringify(enumerated)})`);

    // Two files, one route. The build resolves this by letting one body replace
    // the other's and reporting exit 0, so a gate that swallowed it would print
    // a page count one too high and call the shadowed page covered.
    gitInit(collisionProbe);
    mkdirSync(join(collisionProbe, 'schema', 'dbml'), { recursive: true });
    writeFileSync(join(collisionProbe, 'schema', 'dbml.md'), plainPage);
    writeFileSync(join(collisionProbe, 'schema', 'dbml', 'index.md'), plainPage);
    throws(
      () => pages(collisionProbe),
      'both publish /schema/dbml/',
      'refuses two files that publish one route',
    );
    throws(
      () => liveRoutes(collisionProbe),
      'both publish /schema/dbml/',
      'the route set refuses the collision too, rather than returning one fewer route',
    );
  } finally {
    rmSync(outsideRepository, { recursive: true, force: true });
    rmSync(emptyRepository, { recursive: true, force: true });
    rmSync(nestedProbe, { recursive: true, force: true });
    rmSync(collisionProbe, { recursive: true, force: true });
  }

  const files = contentFiles();
  assert(files.length > 0, 'the content root holds pages');
  assert(files.includes('index.mdx'), 'the root page is enumerated');
  assert(files.every((file) => pageExtension.test(file)), 'every enumerated file is a page');
  assert(files.every((file) => !absolutePrefix.test(file)), 'every enumerated path is relative to the content root');
  assert(files.join('\n') === [...files].sort().join('\n'), 'the enumeration is sorted');

  const collected = pages();
  assert(collected.length === files.length, `every enumerated file becomes a page (${collected.length} for ${files.length})`);
  assert(collected.every((page) => page.source.length > 0), 'every page carries its source');
  assert(collected.every((page) => page.absolute.endsWith(page.file.split('/').join(sep))), 'every page carries its absolute path');

  const routes = liveRoutes();
  assert(routes.size === files.length, `every page publishes its own route (${routes.size} routes for ${files.length} pages)`);
  assert(routes.has('/'), 'the root route is live');

  // parseRedirectRoutes — moved here from check-redirects.mjs unchanged, so the
  // two properties its caller rests on are the ones asserted.
  const parsed = parseRedirectRoutes("const redirectRoutes = {\n  '/old/': '/new/',\n  '/older/': '/new/',\n};\n");
  assert(parsed !== null && parsed.length === 2, 'both declared redirects are parsed');
  assert(parsed[0][0] === '/old/' && parsed[0][1] === '/new/', 'a redirect keeps its source and target');
  assert(parseRedirectRoutes('export default {};\n') === null, 'an absent redirect map is null, not an empty map');

  // sidebarEntries — nesting is the point. A reader that stops at the first
  // level finds two of these four entries and says nothing about the others.
  const nested = sidebarEntries([
    { label: 'Start', items: [{ slug: 'start/install' }, 'start/quick-start'] },
    {
      label: 'Databases',
      items: [
        { slug: 'databases/support-matrix' },
        {
          label: 'Engines',
          items: [
            { slug: 'databases/postgresql', label: 'PostgreSQL' },
            { label: 'Release notes', link: '/databases/notes/' },
            { label: 'Upstream', link: 'https://example.invalid/' },
          ],
        },
      ],
    },
  ]);

  assert(nested.length === 6, `every level is flattened (got ${nested.length} entries, a one-level reader finds 2)`);

  const deep = nested.find((entry) => entry.value === 'databases/postgresql');
  assert(deep !== undefined, 'an entry two groups down is found');
  assert(deep.kind === 'slug', 'a slug entry is reported as a slug');
  assert(deep.route === '/databases/postgresql/', 'a slug entry carries its route in the redirect spelling');
  assert(deep.label === 'PostgreSQL', 'an entry keeps its own label');
  assert(deep.path === 'Databases > Engines', `a nested entry says where it sits (got ${JSON.stringify(deep.path)})`);

  const shorthand = nested.find((entry) => entry.value === 'start/quick-start');
  assert(shorthand.kind === 'slug', "Starlight's bare-string shorthand is a slug");
  assert(shorthand.label === null, 'an entry with no label of its own reports null');
  assert(shorthand.path === 'Start', 'a first-level entry says which group holds it');

  const internalLink = nested.find((entry) => entry.value === '/databases/notes/');
  assert(internalLink.kind === 'link', 'a link entry is reported as a link, not folded into the slugs');
  assert(internalLink.route === '/databases/notes/', 'an internal link carries the route it names');

  const externalLink = nested.find((entry) => entry.value === 'https://example.invalid/');
  assert(externalLink.kind === 'link', 'an external entry is still a link');
  assert(externalLink.route === null, 'an external link names no route on this site');

  assert(sidebarEntries(['top-level'])[0].path === '', 'an entry outside every group has an empty path');

  throws(() => sidebarEntries([]), 'refusing to answer with an empty list', 'refuses an empty sidebar');
  throws(() => sidebarEntries([{ label: 'Empty', items: [] }]), 'refusing to answer with an empty list', 'refuses a sidebar whose groups are all empty');
  throws(() => sidebarEntries(undefined), 'wants the sidebar array', 'refuses a sidebar that is not an array');
  throws(
    () => sidebarEntries([{ label: 'Guides', autogenerate: { directory: 'guides' } }]),
    'cannot enumerate what Starlight fills it with',
    'refuses an autogenerated group rather than counting it as nothing',
  );
  throws(() => sidebarEntries([{ label: 'Nothing' }]), 'names neither a slug, a link, nor a nested group', 'refuses an entry that names nothing');
  throws(() => sidebarEntries([42]), 'neither a slug string nor an entry object', 'refuses an entry of the wrong type');

  console.log(`docroutes.mjs --selftest: OK (${count()} assertions)`);
}

const invokedPath = process.argv[1];
if (invokedPath !== undefined && import.meta.url === pathToFileURL(invokedPath).href) {
  // Running the module is running its selftest. Any other argument is refused
  // rather than answered with a pass, so a mistyped flag cannot report OK.
  if (process.argv[2] === undefined || process.argv[2] === '--selftest') {
    selftest();
  } else {
    console.error(`usage: node scripts/lib/docroutes.mjs [--selftest] (got ${process.argv[2]})`);
    process.exitCode = 2;
  }
}
