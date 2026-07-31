#!/usr/bin/env node
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, extname, join, relative, sep } from 'node:path';
import path from 'node:path/posix';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const docsRoot = join(siteRoot, 'src', 'content', 'docs');
const docExts = new Set(['.md', '.mdx']);
const externalSchemes = /^[a-z][a-z0-9+.-]*:/i;

function toPosix(value) {
  return value.split(sep).join('/');
}

function walk(dir) {
  const files = [];
  const entries = readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name));
  for (const entry of entries) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...walk(fullPath));
      continue;
    }
    if (entry.isFile() && docExts.has(extname(entry.name))) {
      files.push(fullPath);
    }
  }
  return files;
}

function routeFor(root, file) {
  let route = toPosix(relative(root, file)).replace(/\.(md|mdx)$/, '');
  if (route === 'index') return '/';
  if (route.endsWith('/index')) route = route.slice(0, -'/index'.length);
  return `/${route}/`;
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

function extractLinks(file) {
  const source = stripFencedCode(readFileSync(file, 'utf8'));
  const links = [];
  const patterns = [
    /(?<!!)\[[^\]\n]+\]\(([^)\s]+)(?:\s+["'][^"']*["'])?\)/g,
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
function anchorError(anchorsByRoute, file, href, route, cwd) {
  const anchor = targetAnchor(href);
  if (!anchor) return null;
  const anchors = anchorsByRoute.get(route);
  // A route with no recorded headings is a page this checker does not parse
  // (an .mdx built from components, say); do not invent failures for it.
  if (!anchors || anchors.size === 0) return null;
  if (anchors.has(anchor)) return null;
  return (
    `${toPosix(relative(cwd, file))}: ${href} points at #${anchor}, which no heading on ${route} produces`
  );
}

function validateLink(root, routes, anchorsByRoute, file, href, cwd) {
  if (!href || externalSchemes.test(href)) {
    return null;
  }

  const sourceRoute = routeFor(root, file);

  // A bare #anchor stays on the page it was written on.
  if (href.startsWith('#')) {
    return anchorError(anchorsByRoute, file, href, sourceRoute, cwd);
  }

  const cleanHref = targetPath(href);
  if (!cleanHref) return null;

  if (cleanHref.startsWith('/')) {
    return `${toPosix(relative(cwd, file))}: ${href} is root-relative; use a docs-relative link so GitHub Pages keeps /ptah/<version>/ in the URL`;
  }

  const { escaped, route: resolved } = resolveRoute(sourceRoute, cleanHref);
  if (escaped) {
    return `${toPosix(relative(cwd, file))}: ${href} escapes the docs route root`;
  }
  if (routes.has(resolved)) return anchorError(anchorsByRoute, file, href, resolved, cwd);

  return `${toPosix(relative(cwd, file))}: ${href} resolves to missing route ${resolved}`;
}

function checkLinks(root, cwd) {
  const files = walk(root);
  const routes = new Set(files.map((file) => routeFor(root, file)));
  const anchorsByRoute = new Map(
    files.map((file) => [routeFor(root, file), headingAnchors(readFileSync(file, 'utf8'))]),
  );
  const errors = [];

  for (const file of files) {
    for (const href of extractLinks(file)) {
      const error = validateLink(root, routes, anchorsByRoute, file, href, cwd);
      if (error) errors.push(error);
    }
  }

  const anchors = [...anchorsByRoute.values()].reduce((sum, set) => sum + set.size, 0);
  return { errors, files, routes, anchors };
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
      ].join('\n'),
    );
    const anchored = checkLinks(root, tmp);
    const anchorErrors = anchored.errors.filter((error) => error.includes('points at'));
    assert(anchorErrors.length === 2, `expected 2 anchor errors, got ${anchored.errors.join('; ')}`);
    assert(anchorErrors.some((error) => error.includes('#no-such-heading')), 'catches a same-page anchor');
    assert(anchorErrors.some((error) => error.includes('#renamed-section')), 'catches a cross-page anchor');

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

  const { errors, files, routes, anchors } = checkLinks(docsRoot, process.cwd());

  if (errors.length > 0) {
    console.error('Broken internal documentation links:');
    for (const error of errors) {
      console.error(`- ${error}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log(`check-links.mjs: OK (${files.length} pages, ${routes.size} routes, ${anchors} heading anchors)`);
}

main();
