#!/usr/bin/env node
// Imports each declared provider's `docs/` folder into the content collection.
//
// A provider's pages live in the provider's repository, beside the code they
// describe, and this site renders them. `scripts/data/provider-docs.json`
// names each provider's repository and the release whose documentation the
// site shows; this script fetches that release's `docs/` folder and writes each
// page under `src/content/docs/_providers/<engine>/` with the route
// `extend/providers/<engine>/<page>/`. `src/lib/provider-docs.mjs` explains why
// the directory starts with `_`, and is what every gate reads the result
// through.
//
// Usage:
//   node scripts/import-provider-docs.mjs                      # import every declared provider
//   node scripts/import-provider-docs.mjs --source ydb=<dir>   # import one from a local checkout
//   node scripts/import-provider-docs.mjs --check              # no network: is the import current?
//   node scripts/import-provider-docs.mjs --force              # import again, even what is current
//   node scripts/import-provider-docs.mjs --cache <dir>        # keep the clones in <dir>
//   node scripts/import-provider-docs.mjs --selftest
//
// The fetch is a blobless clone of the provider repository, kept in
// `docs/site/.cache/provider-docs/` (or `--cache`) and reused while it holds
// the commit, so a second run and a CI run with that directory cached need no
// network. A run whose manifest already matches the declaration writes
// nothing. An import from a local checkout stays until `--force` or another
// `--source` replaces it, so `npm run dev` keeps showing the pages being
// written.
//
// WHAT AN IMPORT CHANGES. A page is a GitHub page in its own repository and a
// site page here, and the two read links differently, so the import rewrites
// what a site reader would otherwise follow to the wrong place:
//
//   - a link to another page of the folder becomes the relative route of that
//     page, and a link to the folder's README.md becomes the host page that
//     introduces the provider, `extend/providers/<engine>/`;
//   - a link to any other file of the provider repository becomes a GitHub
//     address at the imported commit, so it shows what the page describes;
//   - an address of a page in the site's edge version becomes a relative
//     route, so a reader of a released version stays in that version and
//     check-links can see it;
//   - a repository path in sourceOfTruth or evidence becomes a GitHub address
//     at the imported commit;
//   - a leading `# Title` equal to the frontmatter title is dropped, because
//     the site renders the title itself.
//
// It adds `slug`, `lastUpdated` (the date of the last commit that changed the
// file) and the three `provider*` fields the page actions read, and it refuses
// what it cannot carry: a link to a path the commit does not hold, a relative
// image, an MDX page, and a frontmatter key that would move the route or claim
// a host gate's input. Every refusal names the file and the line.
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import path from 'node:path/posix';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { BasePath, Origin } from '../src/lib/docs-origin.mjs';
import {
  contentRoot as siteContentRoot,
  declaredProviders,
  digest,
  hostPageFor,
  importDirectoryName,
  importedProviders,
  importerHash,
  manifestName,
  ProviderDocsError,
  routePrefix,
  validateDeclaration,
} from '../src/lib/provider-docs.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const defaultCacheRoot = join(siteRoot, '.cache', 'provider-docs');
// The edge version is the one a provider page links: it is the documentation
// of the Ptah a provider is developed against.
const docsOrigin = `${Origin}${BasePath('edge')}`;

// Frontmatter keys a provider page may not write. The import writes the first
// group itself; the rest are inputs of host gates that read only the
// repository's own pages, so a provider page claiming them would be claiming
// something nothing checks.
const forbiddenKeys = new Map([
  ['slug', 'the import sets the route, under extend/providers/<engine>/'],
  ['lastUpdated', 'the import sets it from the commit that last changed the file'],
  ['providerRepository', 'the import sets it'],
  ['providerCommit', 'the import sets it'],
  ['providerSource', 'the import sets it'],
  ['owns', 'the feature inventory reads only the host repository'],
  ['quickstart', 'the quick-start runner reads only the host repository'],
  ['quickstartContinues', 'the quick-start runner reads only the host repository'],
  ['faqVerdicts', 'the FAQ rail belongs to the host FAQ page'],
]);

const externalScheme = /^[a-z][a-z0-9+.-]*:/i;
const issueReference = /^[a-z0-9_.-]+\/[a-z0-9_.-]+#\d+$/i;
const githubRepository = /^github:[a-z0-9_.-]+\/[a-z0-9_.-]+$/i;
const namedEvidence = /^evidence:[a-z0-9_.-]+(?:\/[a-z0-9_.-]+)*$/i;
const frontmatterBlock = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?/;

/** A page or a declaration the import cannot carry onto the site. */
class ImportRefusal extends Error {
  constructor(problems) {
    super(`import-provider-docs: refused\n  ${problems.join('\n  ')}`);
    this.name = 'ImportRefusal';
    this.problems = problems;
  }
}

function git(args, options = {}) {
  return execFileSync('git', args, {
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
    stdio: ['ignore', 'pipe', 'pipe'],
    ...options,
  });
}

function tryGit(args, options) {
  try {
    return git(args, options);
  } catch {
    return null;
  }
}

function unquote(value) {
  const text = value.trim();
  if (text.startsWith('"') && text.endsWith('"') && text.length > 1) return JSON.parse(text);
  if (text.startsWith("'") && text.endsWith("'") && text.length > 1) return text.slice(1, -1).replaceAll("''", "'");
  return text;
}

// ---------------------------------------------------------------------------
// Sources: a provider repository at one commit
// ---------------------------------------------------------------------------

// A source is what the transform reads: the commit, the paths the commit
// holds (files and directories), a file's text, and the date the file last
// changed. Two kinds: the blobless clone of a declared release, and a local
// checkout, which is how the provider's own pull request renders its pages.

function remoteSource(provider, cacheRoot) {
  const [owner, name] = provider.repository.split('/');
  const directory = join(cacheRoot, owner, `${name}.git`);
  const url = `https://github.com/${provider.repository}.git`;
  if (!existsSync(join(directory, 'HEAD'))) {
    rmSync(directory, { recursive: true, force: true });
    mkdirSync(dirname(directory), { recursive: true });
    git(['clone', '--quiet', '--bare', '--filter=blob:none', url, directory]);
  }
  const at = { cwd: directory };
  const resolveCommit = (revision) => tryGit(['rev-parse', '--verify', '--quiet', `${revision}^{commit}`], at)?.trim() || null;
  const fetch = (...refspecs) => git(['fetch', '--quiet', '--filter=blob:none', 'origin', ...refspecs], at);

  let release = resolveCommit(`refs/tags/${provider.release}`);
  if (release === null) {
    fetch(`+refs/tags/${provider.release}:refs/tags/${provider.release}`);
    release = resolveCommit(`refs/tags/${provider.release}`);
  }
  if (release === null) throw new ImportRefusal([`${provider.engine}: ${provider.repository} has no tag ${provider.release}`]);

  let commit = release;
  if (provider.ref !== null) {
    commit = resolveCommit(provider.ref);
    if (commit === null) {
      fetch(provider.ref);
      commit = resolveCommit(provider.ref);
    }
    if (commit === null) throw new ImportRefusal([`${provider.engine}: ${provider.repository} has no commit ${provider.ref}`]);
    // The override exists for a release that predates the folder. Once the
    // release carries one, the release is what the site shows.
    if ((tryGit(['ls-tree', '-d', '--name-only', release, 'docs'], at) ?? '').trim() !== '') {
      throw new ImportRefusal([
        `${provider.engine}: ${provider.release} carries a docs/ folder, so drop "ref" and "refReason" from the declaration and import the release`,
      ]);
    }
  }
  return gitSource({ cwd: directory, commit });
}

function localSource(directory) {
  const at = { cwd: directory };
  const commit = tryGit(['rev-parse', 'HEAD'], at)?.trim();
  if (!commit) throw new ImportRefusal([`--source ${directory} is not a git checkout`]);
  const listed = git(['-c', 'core.quotePath=false', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], at)
    .split('\0')
    .filter((file) => file !== '' && existsSync(join(directory, file)));
  const { files, directories } = pathsOf(listed);
  return {
    commit,
    local: true,
    files,
    directories,
    read: (file) => readFileSync(join(directory, file), 'utf8'),
    lastChanged: (file) => tryGit(['log', '-1', '--format=%cs', '--', file], at)?.trim() || today(),
  };
}

function gitSource({ cwd, commit }) {
  const at = { cwd };
  const listed = git(['-c', 'core.quotePath=false', 'ls-tree', '-r', '--name-only', '-z', commit], at)
    .split('\0')
    .filter(Boolean);
  const { files, directories } = pathsOf(listed);
  return {
    commit,
    local: false,
    files,
    directories,
    read: (file) => git(['cat-file', 'blob', `${commit}:${file}`], at),
    lastChanged: (file) => git(['log', '-1', '--format=%cs', commit, '--', file], at).trim(),
  };
}

function pathsOf(listed) {
  const files = new Set(listed);
  const directories = new Set();
  for (const file of listed) {
    let parent = path.dirname(file);
    while (parent !== '.' && !directories.has(parent)) {
      directories.add(parent);
      parent = path.dirname(parent);
    }
  }
  return { files, directories };
}

function today() {
  return new Date().toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// The transform: one provider page into one site page
// ---------------------------------------------------------------------------

/** The pages of a source: every Markdown file under docs/ but a README. */
export function pagesOf(source) {
  return [...source.files]
    .filter((file) => file.startsWith('docs/') && /\.(?:md|mdx)$/.test(file) && path.basename(file) !== 'README.md')
    .sort();
}

function routeOf(engine, file) {
  return `/${routePrefix(engine)}/${file.slice('docs/'.length).replace(/\.md$/, '')}/`;
}

function relativeRoute(from, to) {
  const fromParts = from.split('/').filter(Boolean);
  const toParts = to.split('/').filter(Boolean);
  let common = 0;
  while (common < fromParts.length && common < toParts.length && fromParts[common] === toParts[common]) common += 1;
  const up = '../'.repeat(fromParts.length - common);
  const down = toParts.slice(common).join('/');
  const joined = `${up}${down}${down === '' ? '' : '/'}`;
  return joined === '' ? './' : joined;
}

// Blanks fenced code blocks and inline code spans in place, so offsets into the
// result are offsets into the source and a link is never read out of code.
function maskCode(text) {
  const lines = text.split('\n');
  let fence = null;
  const masked = lines.map((line) => {
    const opener = line.match(/^ {0,3}(`{3,}|~{3,})/);
    if (fence === null && opener) {
      fence = opener[1];
      return ' '.repeat(line.length);
    }
    if (fence !== null) {
      if (new RegExp(`^ {0,3}${fence[0]}{${fence.length},}\\s*$`).test(line)) fence = null;
      return ' '.repeat(line.length);
    }
    return line.replace(/(`+)([\s\S]*?[^`])\1(?!`)/g, (span) => ' '.repeat(span.length));
  });
  return masked.join('\n');
}

function lineAt(text, offset) {
  return text.slice(0, offset).split('\n').length;
}

/**
 * Rewrites one page: returns `{ text, title, label, order }` or throws an
 * ImportRefusal naming each problem.
 */
export function transformPage({ provider, source, file, pages }) {
  const problems = [];
  const where = (line) => `${provider.repository}/${file}${line ? `:${line}` : ''}`;
  const refuse = (line, message) => problems.push(`${where(line)}: ${message}`);
  const raw = source.read(file).replace(/\r\n/g, '\n');
  const route = routeOf(provider.engine, file);

  if (file.endsWith('.mdx')) {
    throw new ImportRefusal([`${where()}: an MDX page imports modules by path, which the site cannot resolve; write Markdown`]);
  }
  const block = raw.match(frontmatterBlock);
  if (!block) {
    throw new ImportRefusal([`${where(1)}: a page needs the site's frontmatter (title, description, type and the editorial fields); see docs/site/README.md#provider-documentation`]);
  }
  const frontmatterLineCount = block[0].split('\n').length - 1;

  // Frontmatter: read the keys the import needs, refuse the ones it owns, and
  // rewrite repository paths in the two source lists.
  const lines = block[1].split('\n');
  let title = null;
  let label = null;
  let order = null;
  let generated = null;
  let list = null;
  let inSidebar = false;
  const rewritten = lines.map((line, index) => {
    const lineNumber = index + 2;
    const key = line.match(/^([A-Za-z][A-Za-z0-9]*):(?:[ \t]*(.*))?$/);
    if (key) {
      const [, name, value = ''] = key;
      list = name === 'sourceOfTruth' || name === 'evidence' ? name : null;
      inSidebar = name === 'sidebar';
      if (forbiddenKeys.has(name)) refuse(lineNumber, `the frontmatter key "${name}" is not accepted: ${forbiddenKeys.get(name)}`);
      if (name === 'title') title = unquote(value);
      if (name === 'generated') generated = value.trim();
      return line;
    }
    if (inSidebar) {
      const nested = line.match(/^ {2}([A-Za-z]+):[ \t]*(.*)$/);
      if (nested?.[1] === 'order') order = Number(unquote(nested[2]));
      if (nested?.[1] === 'label') label = unquote(nested[2]);
      return line;
    }
    const item = list && line.match(/^( {2}-[ \t]+)(.*)$/);
    if (!item) return line;
    const reference = unquote(item[2]);
    if (externalScheme.test(reference) || issueReference.test(reference) ||
        githubRepository.test(reference) || namedEvidence.test(reference)) return line;
    const normalized = path.normalize(reference).replace(/\/$/, '');
    if (normalized.startsWith('..') || path.isAbsolute(normalized)) {
      refuse(lineNumber, `${list} entry ${JSON.stringify(reference)} must be a path inside the provider repository`);
      return line;
    }
    const kind = source.files.has(normalized) ? 'blob' : source.directories.has(normalized) ? 'tree' : null;
    if (kind === null) {
      refuse(lineNumber, `${list} entry ${JSON.stringify(reference)} names no path at ${source.commit}`);
      return line;
    }
    return `${item[1]}${JSON.stringify(`https://github.com/${provider.repository}/${kind}/${source.commit}/${normalized}`)}`;
  });
  if (title === null || title === '') refuse(1, 'the frontmatter has no title');
  if (generated !== 'false') refuse(1, 'a provider page declares "generated: false"; the import does not carry a generator in another repository');
  if (order !== null && !Number.isFinite(order)) refuse(1, 'sidebar.order must be a number');

  // Body: one title, then links a site reader can follow.
  let body = raw.slice(block[0].length);
  let removedLines = 0;
  const bodyLine = (offset) => frontmatterLineCount + removedLines + lineAt(body, offset);
  const leading = body.match(/^\s*# (.+?)\s*#*\s*\n/);
  if (leading) {
    if (leading[1].trim() !== title) {
      refuse(bodyLine(leading.index), `the heading "# ${leading[1].trim()}" differs from the frontmatter title ${JSON.stringify(title)}`);
    }
    const rest = body.slice(leading[0].length);
    const trimmed = rest.replace(/^\n+/, '');
    removedLines = body.length - trimmed.length === 0 ? 0 : body.slice(0, body.length - trimmed.length).split('\n').length - 1;
    body = trimmed;
  }
  const masked = maskCode(body);
  for (const heading of masked.matchAll(/^# .*/gm)) {
    refuse(bodyLine(heading.index), 'a page has one title, its frontmatter title; use ## for a section');
  }

  const docsDirectory = path.dirname(file);
  const pageSet = new Set(pages);
  const rewriteDestination = (destination, offset, image) => {
    const line = bodyLine(offset);
    if (destination.startsWith('#')) return destination;
    if (destination.startsWith(docsOrigin)) {
      const [target, fragment] = destination.slice(docsOrigin.length).split(/#(.*)/s);
      const targetRoute = `/${target.replace(/\/?$/, '/')}`.replace(/^\/\/$/, '/');
      return `${relativeRoute(route, targetRoute)}${fragment === undefined ? '' : `#${fragment}`}`;
    }
    const selfDocs = new RegExp(`^https://github\\.com/${provider.repository.replace(/\./g, '\\.')}/(?:blob|tree)/[^/]+/docs/[^#?]*\\.md(?:[#?]|$)`);
    if (selfDocs.test(destination)) {
      refuse(line, `${destination} sends a site reader to GitHub for a page the site publishes; link the file relatively`);
      return destination;
    }
    if (externalScheme.test(destination)) return destination;
    if (destination.startsWith('/')) {
      refuse(line, `${destination} is relative to a host nobody can name; link a file relatively or by its full address`);
      return destination;
    }
    if (image) {
      refuse(line, `the image ${destination} is not imported; the site publishes no file of a provider repository but its pages`);
      return destination;
    }
    const [target, fragment] = destination.split(/#(.*)/s);
    if (target.includes('?')) {
      refuse(line, `${destination} carries a query, which no repository file answers`);
      return destination;
    }
    const resolved = path.normalize(path.join(docsDirectory, decodeURI(target))).replace(/\/$/, '');
    if (resolved.startsWith('..')) {
      refuse(line, `${destination} leaves the provider repository`);
      return destination;
    }
    const withFragment = (value) => `${value}${fragment === undefined ? '' : `#${fragment}`}`;
    if (pageSet.has(resolved)) return withFragment(relativeRoute(route, routeOf(provider.engine, resolved)));
    if (resolved === 'docs/README.md') {
      if (fragment !== undefined) {
        refuse(line, `${destination} names a section of the folder's README, which the site replaces with the provider's host page`);
        return destination;
      }
      return relativeRoute(route, `/${routePrefix(provider.engine)}/`);
    }
    if (source.files.has(resolved)) {
      return withFragment(`https://github.com/${provider.repository}/blob/${source.commit}/${encodeURI(resolved)}`);
    }
    if (source.directories.has(resolved) || resolved === '.') {
      return withFragment(`https://github.com/${provider.repository}/tree/${source.commit}/${encodeURI(resolved === '.' ? '' : resolved)}`);
    }
    refuse(line, `${destination} names no path at ${source.commit}`);
    return destination;
  };

  // Inline links and images, then reference definitions. Replacements run from
  // the end so earlier offsets stay valid.
  const edits = [];
  const inline = /(!?)\[(?:[^\]\\]|\\.)*\]\(\s*(<[^>\n]*>|[^)\s]+)(?:\s+(?:"[^"\n]*"|'[^'\n]*'))?\s*\)/g;
  for (const match of masked.matchAll(inline)) {
    const destination = match[2];
    const start = match.index + match[0].lastIndexOf(destination);
    const bare = destination.startsWith('<') ? destination.slice(1, -1) : destination;
    const replacement = rewriteDestination(bare, match.index, match[1] === '!');
    if (replacement !== bare) edits.push({ start, end: start + destination.length, text: replacement });
  }
  for (const match of masked.matchAll(/^ {0,3}\[[^\]\n]+\]:[ \t]*(\S+)/gm)) {
    const start = match.index + match[0].lastIndexOf(match[1]);
    const replacement = rewriteDestination(match[1], match.index, false);
    if (replacement !== match[1]) edits.push({ start, end: start + match[1].length, text: replacement });
  }
  for (const match of masked.matchAll(/<(?:a|img)\b[^>]*\b(?:href|src)=["']([^"']+)["']/gi)) {
    if (!externalScheme.test(match[1]) && !match[1].startsWith('#')) {
      refuse(bodyLine(match.index), `the HTML link ${match[1]} is not rewritten; use a Markdown link`);
    }
  }
  for (const edit of edits.sort((a, b) => b.start - a.start)) {
    body = `${body.slice(0, edit.start)}${edit.text}${body.slice(edit.end)}`;
  }

  if (problems.length > 0) throw new ImportRefusal(problems);

  const injected = [
    `# Imported from ${provider.repository} at ${source.commit}: edit ${file} there.`,
    `slug: ${route.slice(1, -1)}`,
    `lastUpdated: ${source.lastChanged(file)}`,
    `providerRepository: ${provider.repository}`,
    `providerCommit: ${source.commit}`,
    `providerSource: ${file}`,
  ];
  const text = `---\n${[...rewritten, ...injected].join('\n')}\n---\n\n${body.replace(/^\n+/, '')}`;
  return { text, route, title, label, order };
}

// ---------------------------------------------------------------------------
// One provider, written to the content collection
// ---------------------------------------------------------------------------

/**
 * Writes one provider's pages and, last, its manifest. Returns the manifest.
 * `root` is the content collection; the self-test passes a temporary one.
 */
export function importProvider({ provider, source, root = siteContentRoot, hash = importerHash() }) {
  if (!existsSync(join(root, hostPageFor(provider.engine)))) {
    throw new ImportRefusal([`${provider.engine}: the host page ${hostPageFor(provider.engine)} that introduces the provider does not exist`]);
  }
  const files = pagesOf(source);
  if (files.length === 0) throw new ImportRefusal([`${provider.engine}: ${provider.repository} has no page under docs/ at ${source.commit}`]);

  const problems = [];
  const transformed = [];
  for (const file of files) {
    try {
      transformed.push({ file, ...transformPage({ provider, source, file, pages: files }) });
    } catch (error) {
      if (!(error instanceof ImportRefusal)) throw error;
      problems.push(...error.problems);
    }
  }
  if (problems.length > 0) throw new ImportRefusal(problems);

  const ordered = transformed.sort((a, b) =>
    (a.order ?? Number.POSITIVE_INFINITY) - (b.order ?? Number.POSITIVE_INFINITY) || a.title.localeCompare(b.title));
  const directory = join(root, importDirectoryName, provider.engine);
  rmSync(directory, { recursive: true, force: true });
  const pages = ordered.map((page) => {
    const relative = `${importDirectoryName}/${provider.engine}/${page.file.slice('docs/'.length)}`;
    mkdirSync(dirname(join(root, relative)), { recursive: true });
    writeFileSync(join(root, relative), page.text);
    return {
      file: relative,
      source: page.file,
      route: page.route,
      slug: page.route.slice(1, -1),
      title: page.title,
      label: page.label,
      sha256: digest(Buffer.from(page.text)),
    };
  });
  const manifest = {
    notice: 'Written by docs/site/scripts/import-provider-docs.mjs. Edit the pages in the provider repository.',
    engine: provider.engine,
    repository: provider.repository,
    release: provider.release,
    ref: provider.ref,
    commit: source.commit,
    local: source.local,
    importer: hash,
    pages,
  };
  writeFileSync(join(directory, manifestName), `${JSON.stringify(manifest, null, 2)}\n`);
  return manifest;
}

/** Removes every imported directory no declared provider owns. */
function removeUndeclared(providers, root = siteContentRoot) {
  const importRoot = join(root, importDirectoryName);
  if (!existsSync(importRoot)) return;
  const declared = new Set(providers.map((provider) => provider.engine));
  for (const name of readdirSync(importRoot)) {
    if (!declared.has(name)) rmSync(join(importRoot, name), { recursive: true, force: true });
  }
}

// What a run without --source does with the import on disk: keep it when it
// matches the declaration, keep a local one written by this import step, and
// import otherwise.
function existingImport(provider, hash, root = siteContentRoot) {
  let manifest;
  try {
    manifest = JSON.parse(readFileSync(join(root, importDirectoryName, provider.engine, manifestName), 'utf8'));
  } catch {
    return 'absent';
  }
  if (manifest.repository !== provider.repository || manifest.importer !== hash) return 'stale';
  if (manifest.local === true) return 'local';
  return manifest.release === provider.release && (manifest.ref ?? null) === provider.ref ? 'current' : 'stale';
}

function optionValue(argv, name) {
  const index = argv.indexOf(name);
  if (index === -1) return undefined;
  const value = argv[index + 1];
  if (value === undefined || value.startsWith('--')) throw new ImportRefusal([`${name} wants a value`]);
  return value;
}

function parseSources(argv) {
  const sources = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] !== '--source') continue;
    const value = argv[index + 1] ?? '';
    const [engine, directory] = value.split(/=(.*)/s);
    if (!engine || !directory) throw new ImportRefusal([`--source wants <engine>=<directory>, got ${JSON.stringify(value)}`]);
    sources.set(engine, resolve(directory));
    index += 1;
  }
  return sources;
}

function run(argv) {
  if (argv.includes('--check')) {
    const providers = importedProviders();
    const pages = providers.reduce((count, provider) => count + provider.pages.length, 0);
    console.log(`import-provider-docs.mjs --check: OK (${providers.length} providers, ${pages} pages)`);
    return;
  }
  const providers = declaredProviders();
  const sources = parseSources(argv);
  for (const engine of sources.keys()) {
    if (!providers.some((provider) => provider.engine === engine)) {
      throw new ImportRefusal([`--source ${engine}: the declaration names no such provider`]);
    }
  }
  const hash = importerHash();
  const cache = optionValue(argv, '--cache');
  const cacheRoot = cache === undefined ? defaultCacheRoot : resolve(cache);
  removeUndeclared(providers);
  for (const provider of providers) {
    const local = sources.get(provider.engine);
    const existing = existingImport(provider, hash);
    if (local === undefined && !argv.includes('--force') && existing === 'current') {
      console.log(`${provider.engine}: current (${provider.repository} ${provider.ref ?? provider.release})`);
      continue;
    }
    if (local === undefined && !argv.includes('--force') && existing === 'local') {
      console.log(`${provider.engine}: kept the import from a local checkout; --force imports ${provider.ref ?? provider.release}`);
      continue;
    }
    const source = local === undefined ? remoteSource(provider, cacheRoot) : localSource(local);
    const manifest = importProvider({ provider, source, hash });
    const from = local === undefined ? (provider.ref ?? provider.release) : `the checkout at ${local}`;
    console.log(`${provider.engine}: imported ${manifest.pages.length} pages from ${provider.repository} ${from} (${source.commit.slice(0, 12)})`);
  }
  importedProviders();
}

// ---------------------------------------------------------------------------
// Self-test: the transform and the manifest round trip, on in-memory sources
// ---------------------------------------------------------------------------

function memorySource(files, commit = 'a'.repeat(40)) {
  const listed = Object.keys(files);
  const { files: fileSet, directories } = pathsOf(listed);
  return {
    commit,
    local: false,
    files: fileSet,
    directories,
    read: (file) => files[file],
    lastChanged: () => '2026-01-02',
  };
}

const fixtureProvider = { engine: 'demo', name: 'Demo', repository: 'example/demo-plugin', release: 'v1.0.0', ref: null, refReason: null };

function fixturePage(body, extra = '') {
  return [
    '---',
    'title: Using the demo provider',
    'description: How the demo provider reaches its engine.',
    'type: how-to',
    'audience:',
    '  - database-engineer',
    'readerQuestion: "How do I use the demo provider?"',
    'goal: "Connect Ptah to the demo engine."',
    'sourceOfTruth:',
    '  - internal/connect',
    '  - stokaro/ptah#4204',
    'generated: false',
    'overlaps: []',
    'disposition: keep',
    'sidebar:',
    '  order: 2',
    ...(extra === '' ? [] : [extra]),
    '---',
    '',
    '# Using the demo provider',
    '',
    body,
  ].join('\n');
}

function selftest() {
  let assertions = 0;
  const failures = [];
  const assert = (condition, message) => {
    assertions += 1;
    if (!condition) failures.push(message);
  };
  const refusal = (run) => {
    try {
      run();
    } catch (error) {
      return error instanceof ImportRefusal ? error.problems.join('\n') : `unexpected: ${error.message}`;
    }
    return '';
  };

  const body = [
    'See [the other page](other.md#a-section), [the index](README.md), the',
    '[provider README](../README.md#connection-addresses), [the source](../internal/connect),',
    `[a host page](${docsOrigin}extend/executable-providers/#install-an-official-provider)`,
    `and [the home page](${docsOrigin}), with [a link that`,
    `wraps](${docsOrigin}atlas/overview/) and [an anchor](#local).`,
    '',
    'Code keeps its text: `[x](missing.md)`.',
    '',
    '```md',
    '[y](missing.md)',
    '```',
    '',
    '[ref]: other.md',
    '',
    '## Local',
  ].join('\n');
  const files = {
    'README.md': '# Demo\n',
    'internal/connect/connect.go': 'package connect\n',
    'docs/README.md': '# Index\n',
    'docs/usage.md': fixturePage(body),
    // Ordered after usage.md although "Other" sorts first: sidebar.order decides.
    'docs/other.md': fixturePage('## A section\n')
      .replace('title: Using the demo provider', 'title: Other')
      .replace('# Using the demo provider', '# Other')
      .replace('  order: 2', '  order: 3'),
  };
  const source = memorySource(files);
  const pages = pagesOf(source);
  assert(JSON.stringify(pages) === JSON.stringify(['docs/other.md', 'docs/usage.md']), `the pages are ${JSON.stringify(pages)}; docs/README.md is the GitHub index and is not one`);

  const page = transformPage({ provider: fixtureProvider, source, file: 'docs/usage.md', pages });
  const expected = [
    '[the other page](../other/#a-section)',
    '[the index](../)',
    `[provider README](https://github.com/example/demo-plugin/blob/${'a'.repeat(40)}/README.md#connection-addresses)`,
    `[the source](https://github.com/example/demo-plugin/tree/${'a'.repeat(40)}/internal/connect)`,
    '(../../../executable-providers/#install-an-official-provider)',
    '[the home page](../../../../)',
    'wraps](../../../../atlas/overview/)',
    '[an anchor](#local)',
    '`[x](missing.md)`',
    '[y](missing.md)',
    '[ref]: ../other/',
    'slug: extend/providers/demo/usage',
    'lastUpdated: 2026-01-02',
    'providerRepository: example/demo-plugin',
    `providerCommit: ${'a'.repeat(40)}`,
    'providerSource: docs/usage.md',
    `  - "https://github.com/example/demo-plugin/tree/${'a'.repeat(40)}/internal/connect"`,
    '  - stokaro/ptah#4204',
  ];
  for (const text of expected) assert(page.text.includes(text), `the imported page lacks ${JSON.stringify(text)}:\n${page.text}`);
  assert(!/^# Using the demo provider$/m.test(page.text), 'the leading title heading survived the import');
  assert(page.order === 2 && page.route === '/extend/providers/demo/usage/', `order ${page.order}, route ${page.route}`);

  const refusals = [
    ['a link to a missing page', fixturePage('[x](gone.md)'), 'names no path'],
    ['a relative image', fixturePage('![diagram](diagram.svg)'), 'is not imported'],
    ['a section of the folder README', fixturePage('[x](README.md#topic)'), "folder's README"],
    ['a path outside the repository', fixturePage('[x](../../elsewhere.md)'), 'leaves the provider repository'],
    ['a site-absolute path', fixturePage('[x](/docs/usage.md)'), 'relative to a host'],
    ['a GitHub link to an imported page', fixturePage('[x](https://github.com/example/demo-plugin/blob/master/docs/other.md)'), 'sends a site reader to GitHub'],
    ['a relative HTML link', fixturePage('<a href="other.md">x</a>'), 'HTML link'],
    ['a second title', fixturePage('# Another title'), 'one title'],
    ['a different title heading', fixturePage('').replace('# Using the demo provider', '# Something else'), 'differs from the frontmatter title'],
    ['a declared slug', fixturePage('', 'slug: elsewhere'), '"slug" is not accepted'],
    ['a feature claim', fixturePage('', 'owns:\n  - cli:ptah'), '"owns" is not accepted'],
    ['a missing source path', fixturePage('').replace('  - internal/connect', '  - internal/gone'), 'names no path'],
    ['a generated page', fixturePage('').replace('generated: false', 'generated: true'), 'generated: false'],
    ['no frontmatter', '# Bare\n\nText.\n', "site's frontmatter"],
  ];
  for (const [name, text, needle] of refusals) {
    const fixture = memorySource({ ...files, 'docs/usage.md': text });
    const message = refusal(() => transformPage({ provider: fixtureProvider, source: fixture, file: 'docs/usage.md', pages: pagesOf(fixture) }));
    assert(message.includes(needle), `${name}: wanted a refusal containing ${JSON.stringify(needle)}, got ${JSON.stringify(message)}`);
  }
  const mdx = memorySource({ ...files, 'docs/page.mdx': fixturePage('') });
  assert(refusal(() => transformPage({ provider: fixtureProvider, source: mdx, file: 'docs/page.mdx', pages: pagesOf(mdx) })).includes('MDX'),
    'an MDX page was not refused');

  // Declaration rules.
  const declarationRefusal = (providers) => {
    try {
      validateDeclaration({ providers });
    } catch (error) {
      return error instanceof ProviderDocsError ? error.message : `unexpected: ${error.message}`;
    }
    return '';
  };
  const good = { engine: 'demo', name: 'Demo', repository: 'example/demo-plugin', release: 'v1.0.0' };
  assert(declarationRefusal([good]) === '', 'a valid declaration was refused');
  assert(declarationRefusal([{ ...good, ref: 'b'.repeat(40) }]).includes('go together'), 'a ref without a reason was accepted');
  assert(declarationRefusal([{ ...good, ref: 'master', refReason: 'x' }]).includes('full lowercase commit'), 'a branch ref was accepted');
  assert(declarationRefusal([good, good]).includes('declared twice'), 'a duplicate engine was accepted');
  assert(declarationRefusal([{ ...good, release: 'latest' }]).includes('release tag'), 'a release that is not a tag was accepted');
  assert(declarationRefusal([{ ...good, version: '1' }]).includes('unknown key'), 'an unknown key was accepted');

  // The manifest round trip and every way the strict reader refuses it.
  const root = mkdtempSync(join(tmpdir(), 'ptah-provider-docs-'));
  try {
    mkdirSync(join(root, 'extend', 'providers'), { recursive: true });
    writeFileSync(join(root, hostPageFor('demo')), '---\ntitle: Demo\n---\n');
    const hash = 'c'.repeat(64);
    const manifest = importProvider({ provider: fixtureProvider, source, root, hash });
    assert(manifest.pages.map((entry) => entry.file).join(',') === '_providers/demo/usage.md,_providers/demo/other.md',
      `the manifest lists ${manifest.pages.map((entry) => entry.file).join(',')}; ordered pages come first`);
    const read = (providers = [fixtureProvider], digest = hash) => {
      try {
        return importedProviders({ providers, root, hash: digest });
      } catch (error) {
        return error.message;
      }
    };
    assert(Array.isArray(read()) && read()[0].pages.length === 2, `the strict reader refused a fresh import: ${read()}`);
    assert(String(read([{ ...fixtureProvider, release: 'v2.0.0' }])).includes('release changed'), 'a release bump was not reported as stale');
    assert(String(read([fixtureProvider], 'd'.repeat(64))).includes('import step changed'), 'a changed import step was not reported as stale');
    assert(String(read([])).includes('not a declared provider'), 'an undeclared import was not reported');
    writeFileSync(join(root, importDirectoryName, 'demo', 'usage.md'), 'edited in place');
    assert(String(read()).includes('differs from what the import wrote'), 'a page edited in place was not reported');
    importProvider({ provider: fixtureProvider, source, root, hash });
    writeFileSync(join(root, importDirectoryName, 'demo', 'stray.md'), 'x');
    assert(String(read()).includes('not in the manifest'), 'a page the manifest does not list was not reported');
    rmSync(join(root, importDirectoryName, 'demo', 'stray.md'));
    rmSync(join(root, importDirectoryName, 'demo', 'other.md'));
    assert(String(read()).includes('not on disk'), 'a missing page was not reported');
    rmSync(join(root, importDirectoryName, 'demo', manifestName));
    assert(String(read()).includes('not imported'), 'a missing manifest was not reported');
    rmSync(join(root, hostPageFor('demo')));
    assert(refusal(() => importProvider({ provider: fixtureProvider, source, root, hash })).includes('host page'),
      'an import with no host page was not refused');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }

  if (failures.length > 0) {
    console.error('import-provider-docs.mjs --selftest: FAILED');
    for (const failure of failures) console.error(`- ${failure}`);
    process.exitCode = 1;
    return;
  }
  console.log(`import-provider-docs.mjs --selftest: OK (${assertions} assertions)`);
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  if (process.argv.includes('--selftest')) {
    selftest();
  } else {
    try {
      run(process.argv.slice(2));
    } catch (error) {
      if (!(error instanceof ImportRefusal) && !(error instanceof ProviderDocsError)) throw error;
      console.error(error.message);
      process.exitCode = 1;
    }
  }
}
