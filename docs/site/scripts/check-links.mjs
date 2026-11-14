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
//
// The rest of the repository's Markdown -- README.md and its translations,
// docs/*.md, the example and package READMEs -- is read on GitHub, where a
// relative link names a file of the checkout rather than a route. The route
// check never reads those files, so the second half of this gate does: every
// relative link in them has to name a file or a directory git knows. Both the
// files and the targets come from git for the same reason the pages do.
import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { basename, dirname, join, relative, sep } from 'node:path';
import path from 'node:path/posix';
import { fileURLToPath } from 'node:url';

import { pages as collectPages } from './lib/docroutes.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const docsRoot = join(siteRoot, 'src', 'content', 'docs');
const externalSchemes = /^[a-z][a-z0-9+.-]*:/i;

// The fragments build-feature-matrix.mjs assembles into atlas/feature-matrix.md.
// Their links are routes of the page they become, and the route check reads
// that page, so the repository check leaves them out. A fragment that is
// renamed without this list is reported rather than silently checked as a file.
const pageFragments = ['feature-matrix.head.md', 'feature-matrix.tail.md'].map((name) => join(scriptDir, 'data', name));

// The repository corpus has to stay above these counts. A pathspec or a pattern
// that stopped matching reads nothing and reports a clean tree, which is the
// failure a floor exists to catch. Measured on master: 217 files, 666 links.
const repositoryFloor = { files: 150, links: 400 };

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

// Link text may wrap. Excluding \n here made every hard-wrapped link invisible
// to this checker -- anchor and route alike -- which is most of them on a page
// written to 80 columns. A blank line still ends the run, so an unmatched `[`
// cannot swallow the rest of the file.
const inlineLink = /(?<!!)\[(?:[^\]\n]|\n(?!\s*\n))+\]\(([^)\s]+)(?:\s+["'][^"']*["'])?\)/g;
const hrefAttribute = /\bhref=["']([^"']+)["']/g;
const sidebarLink = /^\s+link:\s+([^\s]+)\s*$/gm;
const pageLinks = [inlineLink, hrefAttribute, sidebarLink];

// GitHub renders an image and a reference definition from a file of the
// checkout the way it renders a link, so a repository document's links include
// both. A site page's images are build inputs Astro resolves itself.
const imageLink = /!\[[^\]\n]*\]\(([^)\s]+)(?:\s+["'][^"']*["'])?\)/g;
const srcAttribute = /\bsrc=["']([^"']+)["']/g;
const referenceDefinition = /^ {0,3}\[[^\]\n]+\]:\s*(\S+)/gm;
const repositoryLinks = [inlineLink, imageLink, hrefAttribute, srcAttribute, referenceDefinition];

function extractLinks(pageSource, patterns = pageLinks) {
  const source = stripFencedCode(pageSource);
  const links = [];

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

function publicFileForHref(docsContentRoot, sourceRoute, href) {
  const parts = sourceRoute.split('/').filter(Boolean);
  for (const segment of href.split('/')) {
    if (segment === '' || segment === '.') continue;
    if (segment === '..') {
      if (parts.length === 0) return null;
      parts.pop();
      continue;
    }
    parts.push(segment);
  }

  // docsContentRoot is <site>/src/content/docs. Files in <site>/public are
  // published beside the documentation routes inside every version root.
  return join(docsContentRoot, '..', '..', '..', 'public', ...parts);
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

function validateLink(routes, anchorsByRoute, docsContentRoot, page, href, cwd) {
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

  const publicFile = publicFileForHref(docsContentRoot, sourceRoute, cleanHref);
  if (publicFile && existsSync(publicFile)) {
    const metadata = statSync(publicFile);
    if (!metadata.isFile()) {
      return `${toPosix(relative(cwd, page.absolute))}: ${href} resolves to a public path that is not a file`;
    }
    if (metadata.size === 0) {
      return `${toPosix(relative(cwd, page.absolute))}: ${href} resolves to an empty public file`;
    }
    return null;
  }

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
      const error = validateLink(routes, anchorsByRoute, root, page, href, cwd);
      if (error) errors.push(error);
    }
  }

  const anchors = [...anchorsByRoute.values()].reduce((sum, set) => sum + set.size, 0);
  return { errors, files, routes, anchors, links };
}

// gitFiles lists the files git knows under `repositoryRoot`, as POSIX paths
// relative to it: tracked ones, and untracked ones that are not ignored, so a
// file about to be added counts. A path that is gone from the working tree is
// left out, since nobody can read it. Throws when git cannot answer.
function gitFiles(repositoryRoot, pathspecs = []) {
  let output;
  try {
    output = execFileSync(
      'git',
      ['-C', repositoryRoot, 'ls-files', '-z', '--cached', '--others', '--exclude-standard', '--', ...pathspecs],
      { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'] },
    );
  } catch (cause) {
    const detail = String(cause.stderr ?? cause.message ?? cause).trim().split('\n')[0];
    throw new Error(`git could not enumerate the files under ${repositoryRoot}: ${detail}`, { cause });
  }
  // An unmerged path is listed once per stage.
  return [...new Set(output.split('\0').filter((file) => file.length > 0))]
    .filter((file) => existsSync(join(repositoryRoot, file)))
    .sort();
}

// repositoryTarget resolves a relative link the way GitHub does for a file of
// the checkout: against the file's directory, or against the repository root
// when it starts with `/`. Returns the repository-relative path, `.` for the
// root itself, or an error, and whether a trailing slash asks for a directory:
// GitHub answers `docs/guide.md/` with a 404.
function repositoryTarget(file, href) {
  const target = targetPath(href);
  if (!target) return { target: null };
  let decoded;
  try {
    decoded = decodeURIComponent(target);
  } catch {
    return { error: 'is not a valid percent-encoded path' };
  }
  const joined = decoded.startsWith('/') ? decoded.slice(1) : path.join(path.dirname(file), decoded);
  const resolved = path.normalize(joined || '.').replace(/(.)\/+$/, '$1');
  if (resolved === '..' || resolved.startsWith('../')) return { error: 'escapes the repository root' };
  return { target: resolved, directory: decoded.endsWith('/') };
}

// checkRepositoryLinks reads every Markdown file git lists under
// `repositoryRoot` outside the site's content root and the page fragments, and
// requires each relative link in it to name a file or a directory git lists.
// `contentRoot` and `fragments` are repository-relative POSIX paths.
function checkRepositoryLinks(repositoryRoot, { contentRoot, fragments = [], floor }, cwd) {
  const known = gitFiles(repositoryRoot);
  const directories = new Set(['.']);
  for (const file of known) {
    for (let directory = path.dirname(file); !directories.has(directory); directory = path.dirname(directory)) {
      directories.add(directory);
    }
  }
  const targets = new Set(known);
  const markdown = gitFiles(repositoryRoot, ['*.md', '*.mdx']);
  const skipped = new Set(fragments);
  const files = markdown.filter((file) => !file.startsWith(contentRoot) && !skipped.has(file));
  const shown = (file) => toPosix(relative(cwd, join(repositoryRoot, file)));
  const errors = [];

  for (const fragment of fragments) {
    if (!markdown.includes(fragment)) {
      errors.push(`${fragment} is left out as a page fragment, but git lists no such file; update the fragment list in check-links.mjs`);
    }
  }

  let links = 0;
  for (const file of files) {
    const source = readFileSync(join(repositoryRoot, file), 'utf8');
    for (const href of new Set(extractLinks(source, repositoryLinks))) {
      if (!href || externalSchemes.test(href) || href.startsWith('#')) continue;
      links += 1;
      const { target, directory, error } = repositoryTarget(file, href);
      if (error) {
        errors.push(`${shown(file)}: ${href} ${error}`);
        continue;
      }
      if (target === null || directories.has(target)) continue;
      if (directory) {
        errors.push(`${shown(file)}: ${href} resolves to ${target}/, which is not a directory in this repository`);
        continue;
      }
      if (targets.has(target)) continue;
      errors.push(`${shown(file)}: ${href} resolves to ${target}, which is not a file or a directory in this repository`);
    }
  }

  if (files.length < floor.files) {
    errors.push(
      `read ${files.length} repository Markdown files, below the floor of ${floor.files}; a pathspec that stopped matching reads as a clean tree`,
    );
  }
  if (links < floor.links) {
    errors.push(
      `read ${links} relative links in repository Markdown, below the floor of ${floor.links}; a pattern that stopped matching reads as a clean tree`,
    );
  }
  return { errors, files, links };
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

    // Version-relative downloads live under public/ rather than in the
    // content collection. They must resolve from the page's route and must
    // contain bytes; a placeholder file is not a usable download.
    writeDoc(join(tmp, 'public'), 'samples/example.zip', 'archive bytes');
    writeDoc(join(tmp, 'public'), 'samples/empty.zip', '');
    writeDoc(
      root,
      'tutorial/downloads.md',
      [
        '---',
        'title: Downloads',
        '---',
        '[Fixture](../../samples/example.zip)',
        '[Empty fixture](../../samples/empty.zip)',
      ].join('\n'),
    );
    const downloads = checkLinks(root, tmp);
    assert(!downloads.errors.some((error) => error.includes('example.zip')), 'accepts a non-empty public file');
    assert(downloads.errors.some((error) => error.includes('empty public file')), 'rejects an empty public file');

    repositorySelftest();

    console.log('check-links.mjs --selftest: OK');
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

// repositorySelftest drives the repository half over a checkout of its own, so
// what git ignores and what the site owns are git's answer here too.
function repositorySelftest() {
  const tmp = mkdtempSync(join(tmpdir(), 'ptah-repository-links-'));
  try {
    execFileSync('git', ['-c', 'init.defaultBranch=main', 'init', '-q', tmp], { stdio: ['ignore', 'pipe', 'pipe'] });
    writeDoc(tmp, '.gitignore', 'ignored/\n');
    writeDoc(tmp, 'docs/guide.md', '# Guide\n\n[Back](../README.md)\n[Sibling](./guide%20two.md)\n');
    writeDoc(tmp, 'docs/guide two.md', '# Two\n');
    writeDoc(tmp, 'assets/logo.svg', '<svg/>\n');
    // Present on disk, ignored by git: neither a target nor a file this gate
    // reads, which is what keeps another worktree's files out of both sets.
    writeDoc(tmp, 'ignored/notes.md', '[Nowhere](nowhere.md)\n');
    // A site page and a page fragment link routes, not files.
    writeDoc(tmp, 'src/content/docs/page.md', '---\ntitle: Page\n---\n[Route](../other/)\n');
    writeDoc(tmp, 'scripts/data/head.md', '[Route](../conformance/)\n');
    writeDoc(
      tmp,
      'README.md',
      [
        '[Guide](docs/guide.md)',
        '[Guide section](docs/guide.md#usage)',
        '[Docs directory](./docs/)',
        '[From the root](/docs/guide.md)',
        '[Spaced](docs/guide%20two.md)',
        '[Web](https://example.com/missing.md)',
        '[Here](#top)',
        '[Missing](docs/missing.md)',
        '[Outside](../outside.md)',
        '[Ignored](ignored/notes.md)',
        '[Bad escape](docs/%E0%A4%A.md)',
        '[File as a directory](docs/guide.md/)',
        '![Logo](assets/logo.svg)',
        '![Chart](assets/chart.svg)',
        '<img src="assets/missing.png">',
        '',
        '[gone]: docs/gone.md',
        '',
        '```text',
        '[In a fence](docs/fenced.md)',
        '```',
      ].join('\n'),
    );
    const scope = { contentRoot: 'src/content/docs/', fragments: ['scripts/data/head.md'], floor: { files: 0, links: 0 } };

    const broken = checkRepositoryLinks(tmp, scope, tmp);
    const want = [
      'README.md: docs/missing.md resolves to docs/missing.md',
      'README.md: ../outside.md escapes the repository root',
      'README.md: ignored/notes.md resolves to ignored/notes.md',
      'README.md: docs/%E0%A4%A.md is not a valid percent-encoded path',
      'README.md: assets/chart.svg resolves to assets/chart.svg',
      'README.md: assets/missing.png resolves to assets/missing.png',
      'README.md: docs/gone.md resolves to docs/gone.md',
      'README.md: docs/guide.md/ resolves to docs/guide.md/, which is not a directory',
    ];
    assert(broken.errors.length === want.length, `expected ${want.length} broken repository links, got ${broken.errors.join('; ')}`);
    for (const prefix of want) {
      assert(broken.errors.some((error) => error.startsWith(prefix)), `reports ${prefix}`);
    }
    // README.md, docs/guide.md and docs/guide two.md; sixteen relative links
    // among them. The counts are asserted because a pattern or a pathspec that
    // stopped matching leaves every assertion above comparing fewer findings.
    assert(broken.files.length === 3, `expected 3 repository files, got ${broken.files.join(', ')}`);
    assert(broken.links === 16, `expected 16 relative links, got ${broken.links}`);

    // The control for the two exclusions: without them the route links of the
    // site page and the fragment are read as files and fail.
    const unscoped = checkRepositoryLinks(tmp, { ...scope, contentRoot: 'nothing/', fragments: [] }, tmp);
    assert(unscoped.errors.some((error) => error.startsWith('src/content/docs/page.md: ../other/')), 'a site page is read without the content root');
    assert(unscoped.errors.some((error) => error.startsWith('scripts/data/head.md: ../conformance/')), 'a fragment is read without the list');

    const stale = checkRepositoryLinks(tmp, { ...scope, fragments: ['scripts/data/head.md', 'scripts/data/gone.md'] }, tmp);
    assert(stale.errors.some((error) => error.startsWith('scripts/data/gone.md is left out as a page fragment')), 'a stale fragment entry is reported');

    const thin = checkRepositoryLinks(tmp, { ...scope, floor: { files: 4, links: 17 } }, tmp);
    assert(thin.errors.some((error) => error.startsWith('read 3 repository Markdown files, below the floor of 4')), 'the file floor holds');
    assert(thin.errors.some((error) => error.startsWith('read 16 relative links in repository Markdown, below the floor of 17')), 'the link floor holds');

    writeDoc(tmp, 'README.md', '[Guide](docs/guide.md)\n![Logo](assets/logo.svg)\n[guide]: ./docs/guide.md\n');
    const fixed = checkRepositoryLinks(tmp, scope, tmp);
    assert(fixed.errors.length === 0, `expected fixed repository links to pass, got ${fixed.errors.join('; ')}`);
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
  let repository;
  try {
    result = checkLinks(docsRoot, process.cwd());
    // Both paths are resolved through symlinks, because git answers with the
    // real one and a temporary directory on macOS is reached through a link.
    const repositoryRoot = execFileSync('git', ['-C', siteRoot, 'rev-parse', '--show-toplevel'], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    }).trim();
    // The directory is resolved rather than the file, so a fragment that is
    // gone still has a path to be reported under.
    const top = realpathSync(repositoryRoot);
    const inRepository = (file) => toPosix(relative(top, join(realpathSync(dirname(file)), basename(file))));
    repository = checkRepositoryLinks(
      repositoryRoot,
      { contentRoot: `${inRepository(docsRoot)}/`, fragments: pageFragments.map(inRepository), floor: repositoryFloor },
      process.cwd(),
    );
  } catch (error) {
    // docroutes refuses an input it cannot answer for rather than handing back
    // a shorter list, and a refusal is exit 2 the way a missing content root
    // already is: nothing was compared, so nothing can be reported. A git that
    // cannot list the repository is the same refusal.
    console.error(`check-links.mjs: ${error.message}`);
    process.exitCode = 2;
    return;
  }
  const { files, routes, anchors, links } = result;
  const errors = [...result.errors, ...repository.errors];

  if (errors.length > 0) {
    console.error('Broken internal documentation links:');
    for (const error of errors) {
      console.error(`- ${error}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log(
    `check-links.mjs: OK (${files.length} pages, ${routes.size} routes, ${anchors} heading anchors, ${links} links; ` +
      `${repository.files.length} repository Markdown files, ${repository.links} relative links)`,
  );
}

main();
