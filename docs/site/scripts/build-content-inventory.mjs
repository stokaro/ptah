#!/usr/bin/env node
// Build the factual page inventory from the content collection, sidebar,
// link graph, and editorial metadata carried by each page.
//
// Nothing stores the result. The checks that read it import
// buildContentInventory and compute it from the tree they are checking. A
// stored copy changes with every page edit, so any two open pull requests that
// touch pages would conflict on it, and a copy one of them did not regenerate
// would describe a different tree from the one being checked.
//
// Usage:
//   node scripts/build-content-inventory.mjs --print     # write it to stdout
//   node scripts/build-content-inventory.mjs --selftest

import { realpathSync } from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';
import path from 'node:path/posix';
import { fileURLToPath } from 'node:url';

import { sidebar as siteSidebar } from '../src/sidebar.mjs';
import { validatePageMetadata } from '../src/lib/content-metadata.mjs';
import { contentRoot, pages as collectPages, sidebarEntries } from './lib/docroutes.mjs';

const scriptPath = fileURLToPath(import.meta.url);
const scriptDir = dirname(scriptPath);
const siteRoot = resolve(scriptDir, '..');
const repoRoot = resolve(siteRoot, '..', '..');
const externalScheme = /^[a-z][a-z0-9+.-]*:/i;
const frontmatterBlock = /^---\r?\n([\s\S]*?)\r?\n---/;
const arrayFields = new Set([
  'audience',
  'sourceOfTruth',
  'owns',
  'evidence',
  'searchAliases',
  'overlaps',
]);

function toPosix(value) {
  return value.split(sep).join('/');
}

function unquote(value) {
  const text = value.trim();
  if (text.startsWith('"') && text.endsWith('"')) return JSON.parse(text);
  if (text.startsWith("'") && text.endsWith("'")) {
    return text.slice(1, -1).replaceAll("''", "'");
  }
  if (text === 'true') return true;
  if (text === 'false') return false;
  return text;
}

function parseFrontmatter(source, file) {
  const matched = source.match(frontmatterBlock);
  if (!matched) throw new Error(`${file}: missing frontmatter`);

  const data = {};
  const lines = matched[1].split(/\r?\n/);
  for (let index = 0; index < lines.length; index += 1) {
    const keyValue = lines[index].match(/^([A-Za-z][A-Za-z0-9]*):(?:[ \t]*(.*))?$/);
    if (!keyValue) continue;
    const [, key, raw = ''] = keyValue;

    if (arrayFields.has(key)) {
      if (raw.trim() === '[]') {
        data[key] = [];
        continue;
      }
      if (raw.trim() !== '') throw new Error(`${file}: ${key} must use a block list or []`);
      const values = [];
      while (index + 1 < lines.length) {
        const item = lines[index + 1].match(/^  -[ \t]+(.*)$/);
        if (!item) break;
        values.push(unquote(item[1]));
        index += 1;
      }
      data[key] = values;
      continue;
    }

    if (raw.trim() !== '') data[key] = unquote(raw);
  }
  return data;
}

function withoutFrontmatter(source) {
  return source.replace(frontmatterBlock, '');
}

function visibleWordCount(source) {
  const text = withoutFrontmatter(source)
    .replace(/```[\s\S]*?```/g, ' ')
    .replace(/<script\b[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/!\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/[`*_#>|{}()[\]]/g, ' ')
    .replace(/&[a-z0-9#]+;/gi, ' ');
  return text.trim() === '' ? 0 : text.trim().split(/\s+/).length;
}

function stripFencedCode(source) {
  return source.replace(/```[\s\S]*?```/g, '');
}

function extractLinks(source) {
  const text = stripFencedCode(source);
  const links = [];
  const patterns = [
    /(?<!!)\[(?:[^\]\n]|\n(?!\s*\n))+\]\(([^)\s]+)(?:\s+["'][^"']*["'])?\)/g,
    /\bhref=["']([^"']+)["']/g,
  ];
  for (const pattern of patterns) {
    for (const match of text.matchAll(pattern)) links.push(match[1]);
  }
  return links;
}

function normalizeRoute(route) {
  let normalized = path.normalize(route);
  if (!normalized.startsWith('/')) normalized = `/${normalized}`;
  if (!normalized.endsWith('/')) normalized = `${normalized}/`;
  return normalized;
}

function resolveInternalRoute(sourceRoute, href) {
  if (!href || href.startsWith('#') || externalScheme.test(href) || href.startsWith('/')) return null;
  const clean = href.split('#', 1)[0].split('?', 1)[0];
  if (!clean) return null;

  const parts = sourceRoute.split('/').filter(Boolean);
  for (const segment of clean.split('/')) {
    if (segment === '' || segment === '.') continue;
    if (segment === '..') {
      if (parts.length === 0) return null;
      parts.pop();
    } else {
      parts.push(segment);
    }
  }
  return normalizeRoute(`/${parts.join('/')}/`);
}

function metadataProblems(data, file, repositoryRoot = repoRoot) {
  return validatePageMetadata(data, { repositoryRoot }).map((problem) =>
    `${file}: ${problem.path.join('.')} ${problem.message}`,
  );
}

/**
 * The page inventory of the documentation site, computed from the tree.
 *
 * Returns `{ notice, sources, summary, pages }`. `pages` holds one record per
 * published page in route order, carrying its metadata, sidebar placement,
 * internal links in both directions, visible word count, and source size.
 *
 * Throws when a page's metadata is invalid, when two pages own one feature,
 * when an overlap names no live route, or when a page has no sidebar entry. A
 * caller reports the error and stops; an inventory built from a partial page
 * list would read as an answer.
 *
 * The options exist for the self-test: `pages` in the shape `docroutes.pages()`
 * returns, the sidebar array, and the repository root that metadata paths
 * resolve against. A caller checking the site passes none of them.
 */
export function buildContentInventory({
  pages = collectPages(),
  sidebar = siteSidebar,
  repositoryRoot = repoRoot,
} = {}) {
  const liveRoutes = new Set(pages.map((page) => page.route));
  const navigation = new Map();
  for (const entry of sidebarEntries(sidebar)) {
    if (entry.route === null) continue;
    if (navigation.has(entry.route)) {
      throw new Error(`${entry.route}: appears more than once in the sidebar`);
    }
    navigation.set(entry.route, { path: entry.path, label: entry.label });
  }

  const problems = [];
  const owners = new Map();
  const records = pages.map((page) => {
    const metadata = parseFrontmatter(page.source, page.file);
    problems.push(...metadataProblems(metadata, page.file, repositoryRoot));

    for (const owner of metadata.owns ?? []) {
      const first = owners.get(owner);
      if (first) problems.push(`${page.file}: owns ${owner}, already owned by ${first}`);
      else owners.set(owner, page.file);
    }
    for (const overlap of metadata.overlaps ?? []) {
      if (!liveRoutes.has(overlap)) problems.push(`${page.file}: overlap ${overlap} is not a live route`);
      if (overlap === page.route) problems.push(`${page.file}: overlaps itself`);
    }

    const outbound = [...new Set(
      extractLinks(page.source)
        .map((href) => resolveInternalRoute(page.route, href))
        .filter((route) => route !== null && liveRoutes.has(route) && route !== page.route),
    )].sort();
    const nav = page.route === '/' ? { path: '', label: 'Home' } : navigation.get(page.route);
    if (!nav) problems.push(`${page.file}: no sidebar entry`);

    return {
      path: toPosix(relative(repositoryRoot, page.absolute)),
      route: page.route,
      title: metadata.title,
      description: metadata.description,
      sidebarPath: nav?.path ?? null,
      sidebarLabel: nav?.label ?? null,
      type: metadata.type,
      audience: metadata.audience,
      readerQuestion: metadata.readerQuestion,
      goal: metadata.goal,
      sourceOfTruth: metadata.sourceOfTruth,
      owns: metadata.owns ?? [],
      generated: metadata.generated,
      generator: metadata.generator ?? null,
      editSource: metadata.editSource ?? null,
      lastVerified: metadata.lastVerified ?? null,
      evidence: metadata.evidence ?? [],
      searchAliases: metadata.searchAliases ?? [],
      sourceMode: metadata.sourceMode ?? null,
      overlaps: metadata.overlaps,
      disposition: metadata.disposition,
      outboundLinks: outbound,
      inboundLinks: [],
      visibleWords: visibleWordCount(page.source),
      sourceBytes: Buffer.byteLength(page.source),
    };
  });

  const byRoute = new Map(records.map((record) => [record.route, record]));
  for (const record of records) {
    for (const target of record.outboundLinks) byRoute.get(target).inboundLinks.push(record.route);
  }
  for (const record of records) record.inboundLinks.sort();

  if (problems.length > 0) {
    throw new Error(`content inventory refused:\n${problems.map((problem) => `  ${problem}`).join('\n')}`);
  }

  const countsByType = Object.fromEntries(
    [...new Set(records.map((record) => record.type))]
      .sort()
      .map((type) => [type, records.filter((record) => record.type === type).length]),
  );
  return {
    notice: 'Computed from page frontmatter, the content collection, the sidebar, and internal links by docs/site/scripts/build-content-inventory.mjs. It is not stored; edit page metadata or source.',
    sources: [
      'docs/site/src/content/docs/**',
      'docs/site/src/sidebar.mjs',
      'docs/site/src/lib/content-metadata.mjs',
    ],
    summary: {
      pages: records.length,
      authored: records.filter((record) => !record.generated).length,
      generated: records.filter((record) => record.generated).length,
      byType: countsByType,
    },
    pages: records,
  };
}

let assertions = 0;

function assert(condition, message) {
  assertions += 1;
  if (!condition) throw new Error(message);
}

function refusal(run) {
  try {
    run();
  } catch (error) {
    return error.message;
  }
  return '';
}

// A page in the shape docroutes.pages() returns. Its source is never read from
// disk, so the fixture needs no file; its metadata paths do have to exist,
// because the shared validator resolves them against the repository.
function fixturePage(file, route, fields, body) {
  const frontmatter = [
    '---',
    `title: ${fields.title}`,
    `description: ${fields.description}`,
    `type: ${fields.type}`,
    'audience:',
    '  - operator',
    `readerQuestion: ${fields.readerQuestion}`,
    `goal: ${fields.goal}`,
    'sourceOfTruth:',
    '  - docs/site/src/sidebar.mjs',
    ...(fields.owns ? ['owns:', ...fields.owns.map((owner) => `  - ${owner}`)] : []),
    ...(fields.overlaps && fields.overlaps.length > 0
      ? ['overlaps:', ...fields.overlaps.map((overlap) => `  - ${overlap}`)]
      : ['overlaps: []']),
    'generated: false',
    'disposition: keep',
    '---',
  ];
  return {
    file,
    absolute: join(contentRoot, file),
    route,
    source: `${frontmatter.join('\n')}\n${body}\n`,
  };
}

function inventorySelftest() {
  const home = fixturePage('index.mdx', '/', {
    title: 'Home',
    description: 'The documentation home.',
    type: 'landing',
    readerQuestion: 'Where do I start?',
    goal: 'Pick a first page.',
  }, 'Start with [the install page](./start/install/) or [nowhere](./missing/).');
  const install = fixturePage('start/install.md', '/start/install/', {
    title: 'Install',
    description: 'Install the binary.',
    type: 'how-to',
    readerQuestion: 'How do I install it?',
    goal: 'Have a working binary.',
    owns: ['cli:ptah'],
  }, 'Run the **installer** and [go home](../../).\n\n```sh\nnot counted as prose\n```');
  const sidebar = [{ label: 'Start', items: [{ slug: 'start/install', label: 'Install Ptah' }] }];

  const built = buildContentInventory({ pages: [home, install], sidebar });
  const byRoute = new Map(built.pages.map((page) => [page.route, page]));
  assert(built.summary.pages === 2 && built.pages.length === 2, 'records one entry per page');
  assert(built.summary.authored === 2 && built.summary.generated === 0, 'counts authored and generated pages');
  assert(JSON.stringify(built.summary.byType) === '{"how-to":1,"landing":1}', 'counts pages by type in sorted order');
  assert(byRoute.get('/').outboundLinks.join(',') === '/start/install/', 'keeps a link to a live route and drops one to a missing route');
  assert(byRoute.get('/start/install/').inboundLinks.join(',') === '/', 'derives inbound links from the other pages\' outbound links');
  assert(byRoute.get('/start/install/').outboundLinks.join(',') === '/', 'resolves a parent-relative link');
  assert(byRoute.get('/start/install/').visibleWords === 6, 'counts visible words from the page source, not fenced code');
  assert(byRoute.get('/start/install/').sourceBytes === Buffer.byteLength(install.source), 'records the source size');
  assert(byRoute.get('/start/install/').sidebarLabel === 'Install Ptah', 'takes the label from the sidebar');
  assert(byRoute.get('/start/install/').sidebarPath === 'Start', 'takes the group path from the sidebar');
  assert(byRoute.get('/').sidebarLabel === 'Home', 'places the root page without a sidebar entry');
  assert(byRoute.get('/start/install/').path === 'docs/site/src/content/docs/start/install.md', 'records a repository-relative path');
  assert(byRoute.get('/start/install/').owns.join(',') === 'cli:ptah', 'carries the ownership claim');

  const orphan = fixturePage('start/orphan.md', '/start/orphan/', {
    title: 'Orphan',
    description: 'Not in the sidebar.',
    type: 'how-to',
    readerQuestion: 'Where is this page?',
    goal: 'Nothing.',
  }, 'Body.');
  assert(
    refusal(() => buildContentInventory({ pages: [home, install, orphan], sidebar })).includes('start/orphan.md: no sidebar entry'),
    'refuses a page the sidebar does not name',
  );
  const rival = { ...orphan, source: orphan.source.replace('overlaps: []', 'owns:\n  - cli:ptah\noverlaps: []') };
  const rivalSidebar = [{ label: 'Start', items: [{ slug: 'start/install' }, { slug: 'start/orphan' }] }];
  assert(
    refusal(() => buildContentInventory({ pages: [home, install, rival], sidebar: rivalSidebar }))
      .includes('owns cli:ptah, already owned by start/install.md'),
    'refuses a second owner of one feature',
  );
  const deadOverlap = { ...install, source: install.source.replace('overlaps: []', 'overlaps:\n  - /retired/') };
  assert(
    refusal(() => buildContentInventory({ pages: [home, deadOverlap], sidebar })).includes('overlap /retired/ is not a live route'),
    'refuses an overlap naming no live route',
  );
  const noQuestion = { ...install, source: install.source.replace('How do I install it?', 'How do I install it') };
  assert(
    refusal(() => buildContentInventory({ pages: [home, noQuestion], sidebar })).includes('start/install.md: readerQuestion'),
    'refuses invalid page metadata',
  );
  assert(
    refusal(() => buildContentInventory({ pages: [home, install], sidebar: [...sidebar, 'start/install'] }))
      .includes('/start/install/: appears more than once in the sidebar'),
    'refuses a sidebar that names one route twice',
  );
}

function selftest() {
  const parsed = parseFrontmatter(
    [
      '---',
      'title: "A page"',
      'type: how-to',
      'audience:',
      '  - database-engineer',
      'sourceOfTruth:',
      '  - internal/cli/schema',
      'overlaps: []',
      'generated: false',
      '---',
      'Words in the body.',
    ].join('\n'),
    'fixture.md',
  );
  assert(parsed.title === 'A page', 'parses quoted scalars');
  assert(parsed.type === 'how-to', 'parses unquoted scalars');
  assert(parsed.generated === false, 'parses booleans');
  assert(parsed.audience.join(',') === 'database-engineer', 'parses block arrays');
  assert(Array.isArray(parsed.overlaps) && parsed.overlaps.length === 0, 'parses explicit empty arrays');
  assert(resolveInternalRoute('/direct/apply/', '../overview/') === '/direct/overview/', 'resolves a sibling route');
  assert(resolveInternalRoute('/', './start/install/') === '/start/install/', 'resolves from the site root');
  assert(visibleWordCount('---\ntitle: Test\n---\nOne **two** [three](./x/).') === 3, 'counts visible prose');
  assert(
    metadataProblems({
      type: 'status', audience: ['operator'], readerQuestion: 'What is measured?', goal: 'Read the evidence.',
      sourceOfTruth: ['source'], overlaps: [], disposition: 'keep', generated: false,
    }, 'fixture.md').some((problem) => problem.includes('lastVerified')),
    'status pages require a verification date',
  );
  assert(
    metadataProblems({
      description: 'A description.', type: 'concept', audience: ['operator'], readerQuestion: 'What is it?',
      goal: 'A description.', sourceOfTruth: ['source'], overlaps: [], disposition: 'keep', generated: false,
    }, 'fixture.md').some((problem) => problem.includes('reader outcome')),
    'a goal cannot repeat the description',
  );
  const metadataFixture = {
    type: 'status', audience: ['operator'], readerQuestion: 'What is measured?', goal: 'Read the evidence.',
    sourceOfTruth: ['internal/cli/schema'], overlaps: [], disposition: 'keep', generated: false,
    evidence: ['stokaro/ptah#2571'],
  };
  assert(
    metadataProblems({ ...metadataFixture, lastVerified: '2026-02-30' }, 'fixture.md')
      .some((problem) => problem.includes('real calendar date')),
    'impossible verification dates fail',
  );
  assert(
    validatePageMetadata({ ...metadataFixture, lastVerified: '2026-08-31' }, { repositoryRoot: repoRoot, today: '2026-08-30' })
      .some((problem) => problem.message.includes('future')),
    'future verification dates fail',
  );
  assert(
    validatePageMetadata({ ...metadataFixture, lastVerified: '2026-08-30', sourceOfTruth: ['cmd/does-not-exist'] }, { repositoryRoot: repoRoot, today: '2026-08-30' })
      .some((problem) => problem.message.includes('missing repository path')),
    'mistyped repository paths fail',
  );
  assert(
    validatePageMetadata({ ...metadataFixture, lastVerified: '2026-08-30', evidence: ['stokaro/ptah#2571', 'github:stokaro/ptah-atlas-conformance'] }, { repositoryRoot: repoRoot, today: '2026-08-30' }).length === 0,
    'issue and explicitly typed external repository identifiers pass',
  );
  assert(
    validatePageMetadata({ ...metadataFixture, lastVerified: '2026-08-30', evidence: ['https://example.com/evidence', 'evidence:conformance/run/edge'] }, { repositoryRoot: repoRoot, today: '2026-08-30' }).length === 0,
    'URLs and explicitly typed named evidence identifiers pass',
  );
  assert(
    validatePageMetadata({ ...metadataFixture, lastVerified: '2026-08-30', sourceOfTruth: ['cmdd/schema'] }, { repositoryRoot: repoRoot, today: '2026-08-30' })
      .some((problem) => problem.message.includes('missing repository path')),
    'a mistyped local path cannot masquerade as an external repository identifier',
  );
  assert(
    validatePageMetadata({ ...metadataFixture, lastVerified: '2026-08-30', readerQuestion: 'What is measured' }, { repositoryRoot: repoRoot, today: '2026-08-30' })
      .some((problem) => problem.path[0] === 'readerQuestion' && problem.message.includes('ending in ?')),
    'readerQuestion syntax is enforced by shared metadata validation',
  );
  assert(
    validatePageMetadata({
      ...metadataFixture, lastVerified: '2026-08-30', generated: true,
      generator: 'docs/site/scripts/does-not-exist.mjs', editSource: 'internal/does-not-exist',
    }, { repositoryRoot: repoRoot, today: '2026-08-30' })
      .filter((problem) => problem.message.includes('missing repository path')).length === 2,
    'mistyped generator and edit-source paths fail',
  );
  assert(
    validatePageMetadata({ ...metadataFixture, lastVerified: '2026-08-30', lengthWaiver: 'old field' }, { repositoryRoot: repoRoot, today: '2026-08-30' })
      .some((problem) => problem.path[0] === 'lengthWaiver'),
    'the retired lengthWaiver field fails',
  );
  assert(
    validatePageMetadata({
      ...metadataFixture, lastVerified: '2026-08-30', generator: 'docs/site/scripts/build-content-inventory.mjs',
    }, { repositoryRoot: repoRoot, today: '2026-08-30' })
      .some((problem) => problem.path[0] === 'generator' && problem.message.includes('generated is true')),
    'authored pages cannot declare a dead generator action',
  );
  assert(
    validatePageMetadata({
      ...metadataFixture, lastVerified: '2026-08-30', editSource: 'docs/site/src/content/docs/index.mdx',
    }, { repositoryRoot: repoRoot, today: '2026-08-30' })
      .some((problem) => problem.path[0] === 'editSource' && problem.message.includes('generated is true')),
    'authored pages cannot declare a misleading editSource action',
  );
  assert(
    validatePageMetadata({
      ...metadataFixture, lastVerified: '2026-08-30', generated: true,
      generator: 'https://example.com/generator', editSource: 'stokaro/ptah#2571',
    }, { repositoryRoot: repoRoot, today: '2026-08-30' })
      .filter((problem) => problem.message.includes('repository-relative path')).length === 2,
    'generated-page actions require repository-local generator and edit-source paths',
  );
  assert(
    validatePageMetadata({ ...metadataFixture, lastVerified: '2026-08-30', evidence: [] }, {
      repositoryRoot: repoRoot, today: '2026-08-30',
    }).some((problem) => problem.path[0] === 'evidence'),
    'an explicitly present evidence list cannot be empty',
  );
  assert(
    validatePageMetadata({
      ...metadataFixture, lastVerified: '2026-08-30', owns: [''], searchAliases: [''], overlaps: [''],
    }, { repositoryRoot: repoRoot, today: '2026-08-30' })
      .filter((problem) => ['owns', 'searchAliases', 'overlaps'].includes(problem.path[0])).length === 3,
    'shared validation rejects empty entries in optional and required string arrays',
  );
  inventorySelftest();
  console.log(`build-content-inventory.mjs --selftest: OK (${assertions} assertions)`);
}

function main() {
  const argument = process.argv[2];
  if (process.argv.length !== 3 || (argument !== '--selftest' && argument !== '--print')) {
    console.error(`usage: node scripts/build-content-inventory.mjs --print|--selftest (got ${process.argv.slice(2).join(' ') || 'no argument'})`);
    process.exitCode = 2;
    return;
  }
  if (argument === '--selftest') {
    selftest();
    return;
  }

  let inventory;
  try {
    inventory = buildContentInventory();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
    return;
  }
  process.stdout.write(`${JSON.stringify(inventory, null, 2)}\n`);
}

// The checks import this module for buildContentInventory, so running the file
// is what selects the command line. The argument path is resolved because Node
// resolves the module's own: a symlinked checkout would otherwise run nothing
// and exit 0, which reads as a passing self-test.
function invokedDirectly() {
  if (process.argv[1] === undefined) return false;
  try {
    return realpathSync(process.argv[1]) === realpathSync(scriptPath);
  } catch {
    return false;
  }
}

if (invokedDirectly()) main();
