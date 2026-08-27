#!/usr/bin/env node
// Every internal link has to resolve: to a route this site publishes, and to a
// heading that route's page actually produces.
//
// The pages and their routes come from scripts/lib/docroutes.mjs rather than
// from a walk of the content directory. A walk cannot tell this site's pages
// from a git worktree parked under the content root -- measured, 68 pages by
// git against 209 by a walk, the extra 141 belonging to somebody else's branch
// -- and it cannot see that a frontmatter `slug:` moves a page's route, which
// is the difference between validating links against what the site serves and
// validating them against what the file names suggest.
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { dirname, join, relative, sep } from 'node:path';
import path from 'node:path/posix';
import { fileURLToPath } from 'node:url';

import { pages as collectPages } from './lib/docroutes.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const docsRoot = join(siteRoot, 'src', 'content', 'docs');
const externalSchemes = /^[a-z][a-z0-9+.-]*:/i;

function toPosix(value) {
  return value.split(sep).join('/');
}

function normalizeRoute(route) {
  let normalized = path.normalize(route);
  if (!normalized.startsWith('/')) normalized = `/${normalized}`;
  if (!normalized.endsWith('/')) normalized = `${normalized}/`;
  return normalized;
}

function stripFencedCode(source) {
  return source.replace(/```[\s\S]*?```/g, '');
}

// Punctuation github-slugger drops. Starlight generates heading ids with that
// package, so anchor validation has to match it character for character: `-`
// and `_` survive, which is why `--schema` becomes `--schema` and not `schema`.
const sluggerPunctuation = /[ -⁯⸀-⹿\\'!"#$%&()*+,./:;<=>?@[\]^`{|}~]/g;

// headingText renders inline markdown down to the text a reader sees, because
// that text - not the source - is what the id is built from.
function headingText(markdown) {
  return markdown
    .replace(/!\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/`([^`]*)`/g, '$1')
    .replace(/\*\*([^*]*)\*\*/g, '$1')
    .replace(/(?<!\w)[*_]([^*_]+)[*_](?!\w)/g, '$1')
    .replace(/<[^>]+>/g, '')
    .trim();
}

function slugify(text) {
  return text.toLowerCase().trim().replace(sluggerPunctuation, '').replace(/ /g, '-');
}

// headingAnchors returns every id a page exposes, including the duplicate
// suffixes github-slugger appends when two headings render the same text.
function headingAnchors(source) {
  const anchors = new Set();
  const counts = new Map();
  for (const line of stripFencedCode(source).split('\n')) {
    const match = line.match(/^(#{1,6})\s+(.*?)\s*#*\s*$/);
    if (!match) continue;
    const base = slugify(headingText(match[2]));
    if (!base) continue;
    const seen = counts.get(base) ?? 0;
    counts.set(base, seen + 1);
    anchors.add(seen === 0 ? base : `${base}-${seen}`);
  }
  return anchors;
}

function extractLinks(pageSource) {
  const source = stripFencedCode(pageSource);
  const links = [];
  const patterns = [
    // Link text may wrap. Excluding \n here made every hard-wrapped link
    // invisible to this checker -- anchor and route alike -- which is most of
    // them on a page written to 80 columns. A blank line still ends the run,
    // so an unmatched `[` cannot swallow the rest of the file.
    /(?<!!)\[(?:[^\]\n]|\n(?!\s*\n))+\]\(([^)\s]+)(?:\s+["'][^"']*["'])?\)/g,
    /\bhref=["']([^"']+)["']/g,
    /^\s+link:\s+([^\s]+)\s*$/gm,
  ];

  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) {
      links.push(normalizeHref(match[1]));
    }
  }

  return links;
}

function normalizeHref(href) {
  const trimmed = href.trim();
  if (trimmed.startsWith('<') && trimmed.endsWith('>')) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function targetPath(href) {
  const withoutHash = href.split('#', 1)[0];
  return withoutHash.split('?', 1)[0];
}

function targetAnchor(href) {
  const index = href.indexOf('#');
  return index === -1 ? '' : decodeURIComponent(href.slice(index + 1));
}

function resolveRoute(sourceRoute, href) {
  const parts = sourceRoute.split('/').filter(Boolean);
  for (const segment of href.split('/')) {
    if (segment === '' || segment === '.') continue;
    if (segment === '..') {
      if (parts.length === 0) return { escaped: true, route: null };
      parts.pop();
      continue;
    }
    parts.push(segment);
  }
  return { escaped: false, route: normalizeRoute(`/${parts.join('/')}/`) };
}

// anchorError reports an anchor that no heading on the target page produces.
// A renamed heading silently breaks every link into it; nothing 404s, the
// reader just lands at the top of a long page and has to hunt.
function anchorError(anchorsByRoute, page, href, route, cwd) {
  const anchor = targetAnchor(href);
  if (!anchor) return null;
  const anchors = anchorsByRoute.get(route);
  // A route with no recorded headings is a page this checker does not parse
  // (an .mdx built from components, say); do not invent failures for it.
  if (!anchors || anchors.size === 0) return null;
  if (anchors.has(anchor)) return null;
  return (
    `${toPosix(relative(cwd, page.absolute))}: ${href} points at #${anchor}, which no heading on ${route} produces`
  );
}

function validateLink(routes, anchorsByRoute, page, href, cwd) {
  if (!href || externalSchemes.test(href)) {
    return null;
  }

  // The page's own route, as the site serves it. Taking it from the file path
  // would disagree with the route set the moment a page declares a `slug:`.
  const sourceRoute = page.route;

  // A bare #anchor stays on the page it was written on.
  if (href.startsWith('#')) {
    return anchorError(anchorsByRoute, page, href, sourceRoute, cwd);
  }

  const cleanHref = targetPath(href);
  if (!cleanHref) return null;

  if (cleanHref.startsWith('/')) {
    return `${toPosix(relative(cwd, page.absolute))}: ${href} is root-relative; use a docs-relative link so GitHub Pages keeps /ptah/<version>/ in the URL`;
  }

  const { escaped, route: resolved } = resolveRoute(sourceRoute, cleanHref);
  if (escaped) {
    return `${toPosix(relative(cwd, page.absolute))}: ${href} escapes the docs route root`;
  }
  if (routes.has(resolved)) return anchorError(anchorsByRoute, page, href, resolved, cwd);

  return `${toPosix(relative(cwd, page.absolute))}: ${href} resolves to missing route ${resolved}`;
}

function checkLinks(root, cwd) {
  const files = collectPages(root);
  const routes = new Set(files.map((page) => page.route));
  const anchorsByRoute = new Map(files.map((page) => [page.route, headingAnchors(page.source)]));
  const errors = [];
  let links = 0;

  for (const page of files) {
    for (const href of extractLinks(page.source)) {
      links += 1;
      const error = validateLink(routes, anchorsByRoute, page, href, cwd);
      if (error) errors.push(error);
    }
  }

  const anchors = [...anchorsByRoute.values()].reduce((sum, set) => sum + set.size, 0);
  return { errors, files, routes, anchors, links };
}

function writeDoc(root, name, content) {
  const file = join(root, name);
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file, content);
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  const tmp = mkdtempSync(join(tmpdir(), 'ptah-doc-links-'));
  const root = join(tmp, 'src', 'content', 'docs');

  try {
    // The fixture is a real checkout because the pages are git's answer, not a
    // walk's. A plain directory would make this selftest exercise a discovery
    // path the gate no longer has.
    execFileSync('git', ['-c', 'init.defaultBranch=main', 'init', '-q', tmp], { stdio: ['ignore', 'pipe', 'pipe'] });
    writeDoc(root, 'index.mdx', '---\ntitle: Home\n---\n[Start](./reference/comparison/)\n');
    writeDoc(root, 'operate/conformance.md', '---\ntitle: Conformance\n---\n');
    writeDoc(root, 'workflows/go-schema.md', '---\ntitle: Go schema\n---\n');
    writeDoc(root, 'reference/exit-codes.md', '---\ntitle: Exit codes\n---\n');
    writeDoc(
      root,
      'reference/comparison.md',
      [
        '---',
        'title: Comparison',
        '---',
        '[Conformance](../operate/conformance/)',
        '[Go schema workflow](../workflows/go-schema/)',
        '[Exit codes](./exit-codes/)',
      ].join('\n'),
    );

    const broken = checkLinks(root, tmp);
    assert(broken.errors.length === 3, `expected 3 broken fixture links, got ${broken.errors.length}`);
    assert(broken.errors.some((error) => error.includes('/reference/operate/conformance/')), 'catches conformance over-relative link');
    assert(broken.errors.some((error) => error.includes('/reference/workflows/go-schema/')), 'catches workflow over-relative link');
    assert(broken.errors.some((error) => error.includes('/reference/comparison/exit-codes/')), 'catches same-folder exit code link');

    writeDoc(root, 'index.mdx', '---\ntitle: Home\n---\n[Escapes](../reference/comparison/)\n');
    const escaped = checkLinks(root, tmp);
    assert(escaped.errors.some((error) => error.includes('escapes the docs route root')), 'catches route-root escape from index');
    writeDoc(root, 'index.mdx', '---\ntitle: Home\n---\n[Start](./reference/comparison/)\n');

    writeDoc(
      root,
      'reference/comparison.md',
      [
        '---',
        'title: Comparison',
        '---',
        '[Conformance](../../operate/conformance/)',
        '[Go schema workflow](../../workflows/go-schema/)',
        '[Exit codes](../exit-codes/)',
      ].join('\n'),
    );

    const fixed = checkLinks(root, tmp);
    assert(fixed.errors.length === 0, `expected fixed fixture links to pass, got ${fixed.errors.join('; ')}`);

    // Anchors. The slugger has to agree with github-slugger, which Starlight
    // uses: punctuation disappears, hyphens survive, and two headings that
    // render the same text get a numeric suffix.
    assert(slugify(headingText('Diff and plan policy')) === 'diff-and-plan-policy', 'plain heading');
    assert(
      slugify(headingText('Migration down / rollback')) === 'migration-down--rollback',
      'a removed slash leaves both of its spaces behind, so the slug has two hyphens',
    );
    assert(
      slugify(headingText('Scope the comparison with `--schema` and `--include`')) ===
        'scope-the-comparison-with---schema-and---include',
      'code spans vanish but their hyphens do not',
    );
    assert(
      slugify(headingText('Schema diff, apply, formatting, and cleanup')) ===
        'schema-diff-apply-formatting-and-cleanup',
      'commas are dropped without leaving a hyphen',
    );
    const duplicates = headingAnchors('## Notes\n\n## Notes\n');
    assert(duplicates.has('notes') && duplicates.has('notes-1'), 'repeated headings get suffixed ids');

    writeDoc(
      root,
      'reference/anchors.md',
      ['---', 'title: Anchors', '---', '## Real section', '', '### Nested `--flag` heading'].join('\n'),
    );
    writeDoc(
      root,
      'reference/links.md',
      [
        '---',
        'title: Links',
        '---',
        '## Local target',
        '[same page ok](#local-target)',
        '[same page broken](#no-such-heading)',
        '[cross page ok](../anchors/#real-section)',
        '[cross page code ok](../anchors/#nested---flag-heading)',
        '[cross page broken](../anchors/#renamed-section)',
        '[link text that',
        'wraps](#wrapped-and-broken)',
      ].join('\n'),
    );
    const anchored = checkLinks(root, tmp);
    const anchorErrors = anchored.errors.filter((error) => error.includes('points at'));
    assert(anchorErrors.length === 3, `expected 3 anchor errors, got ${anchored.errors.join('; ')}`);
    assert(anchorErrors.some((error) => error.includes('#no-such-heading')), 'catches a same-page anchor');
    assert(anchorErrors.some((error) => error.includes('#renamed-section')), 'catches a cross-page anchor');
    // Every fixture above sits on one line, which is why link text wrapping
    // across a newline went unchecked for as long as it did: the pattern
    // excluded \n, so a hard-wrapped link was not a link at all here.
    assert(anchorErrors.some((error) => error.includes('#wrapped-and-broken')), 'catches a link whose text wraps');
    // Six links on links.md, three on comparison.md, one on index.mdx. The
    // count is asserted because a pattern that silently stopped matching would
    // leave every other assertion here comparing two empty sets.
    assert(anchored.links === 10, `expected 10 links, got ${anchored.links}`);

    console.log('check-links.mjs --selftest: OK');
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function main() {
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }

  if (!existsSync(docsRoot)) {
    console.error(`error: docs content directory not found: ${docsRoot}`);
    process.exitCode = 2;
    return;
  }

  let result;
  try {
    result = checkLinks(docsRoot, process.cwd());
  } catch (error) {
    // docroutes refuses an input it cannot answer for rather than handing back
    // a shorter list, and a refusal is exit 2 the way a missing content root
    // already is: nothing was compared, so nothing can be reported.
    console.error(`check-links.mjs: ${error.message}`);
    process.exitCode = 2;
    return;
  }
  const { errors, files, routes, anchors, links } = result;

  if (errors.length > 0) {
    console.error('Broken internal documentation links:');
    for (const error of errors) {
      console.error(`- ${error}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log(
    `check-links.mjs: OK (${files.length} pages, ${routes.size} routes, ${anchors} heading anchors, ${links} links)`,
  );
}

main();
