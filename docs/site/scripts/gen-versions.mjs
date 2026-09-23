#!/usr/bin/env node
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { EDGE, compareReleases, isRelease, isVersionFolder } from './lib/doc-versions.mjs';

// The site root, from the one declaration. It is empty because the site is
// served at the apex of its own domain; it was `/ptah` while the site was a
// GitHub project page, and this stub kept sending readers there after the move
// (stokaro/ptah#2884).
export const PAGES_PREFIX = '';

// computeDefault names the version the apex redirect serves, and the one its
// canonical link points at.
//
// Edge, while Ptah is pre-GA. A release is a snapshot of what shipped; edge is
// what master documents, and before v1 the difference between them is most of
// the product -- a reader arriving at the apex is asking what Ptah does, not
// what the last tag did. Nothing is hidden by the choice: every release the
// retention window keeps has its own stable URL, and the picker lists each of
// them, so a reader who wants the version they installed is one selection
// away.
//
// The newest tag is still the answer where there is no edge folder at all: a
// deployment assembled from tags alone needs a default, and the highest one is
// the only sensible pick.
export function computeDefault(slugs) {
  if (slugs.includes(EDGE)) return EDGE;
  const releases = slugs.filter(isRelease).sort(compareReleases);
  return releases.length ? releases[releases.length - 1] : EDGE;
}

// buildIndex is the version index the picker reads: edge first, then the
// releases newest first. `latest` names the newest release, and each release
// carries `released`, the day its tag was made, from `released` (a map from
// slug to YYYY-MM-DD).
//
// A release whose tag this checkout cannot see is listed without a date, so a
// shallow checkout can still assemble a root. The deploy's own checkout has
// every tag, because it builds each release folder from one, and
// indexProblems is what holds the index it publishes to a date per release.
export function buildIndex(slugs, released = new Map()) {
  const tags = slugs.filter(isRelease).sort((a, b) => compareReleases(b, a));
  const ordered = [];
  if (slugs.includes(EDGE)) ordered.push(EDGE);
  ordered.push(...tags);
  return {
    default: computeDefault(slugs),
    ...(tags.length > 0 ? { latest: tags[0] } : {}),
    versions: ordered.map((slug) =>
      released.has(slug) ? { slug, label: slug, released: released.get(slug) } : { slug, label: slug },
    ),
  };
}

// indexProblems judges a version index about to be published: every release
// carries the day it was made, and `latest` names the newest release.
// check-pages-root.mjs applies it to the root the deploy assembled.
export function indexProblems(index) {
  const problems = [];
  const releases = (index?.versions ?? []).filter((version) => isRelease(version?.slug));
  for (const version of releases) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(version.released ?? '')) {
      problems.push(`versions.json lists ${version.slug} with no release date`);
    }
  }
  const newest = releases.map((version) => version.slug).sort(compareReleases).at(-1);
  if (index?.latest !== newest) {
    problems.push(`versions.json names ${index?.latest} as latest, want ${newest}`);
  }
  return problems;
}

// releaseDates reads the day each tag was made, in UTC, from the repository
// this script belongs to. Release tags are annotated, so the date is the
// tagger's, which is when the release was cut; a lightweight tag answers with
// its commit's date instead.
export function releaseDates(repository = join(dirname(fileURLToPath(import.meta.url)), '..', '..', '..')) {
  const output = execFileSync(
    'git',
    ['-C', repository, 'for-each-ref', '--format=%(refname:strip=2)%09%(creatordate:iso-strict)', 'refs/tags'],
    { encoding: 'utf8' },
  );
  const dates = new Map();
  for (const line of output.split('\n')) {
    const [slug, stamp] = line.split('\t');
    if (!slug || !stamp || !isRelease(slug)) continue;
    dates.set(slug, new Date(stamp).toISOString().slice(0, 10));
  }
  return dates;
}

export function renderRedirectHtml(defaultSlug) {
  const target = `${PAGES_PREFIX}/${defaultSlug}/`;
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta http-equiv="refresh" content="0; url=${target}" />
    <link rel="canonical" href="${target}" />
    <title>Ptah documentation</title>
    <script>location.replace(${JSON.stringify(target)});</script>
  </head>
  <body>
    <p>Redirecting to the <a href="${target}">Ptah documentation</a>...</p>
  </body>
</html>
`;
}

function renderVersionsJson(index) {
  return `${JSON.stringify(index, null, 2)}\n`;
}

export function generate(dir, released = new Map()) {
  const slugs = readdirSync(dir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && isVersionFolder(entry.name))
    .map((entry) => entry.name);
  const index = buildIndex(slugs, released);
  writeFileSync(join(dir, 'versions.json'), renderVersionsJson(index));
  writeFileSync(join(dir, 'index.html'), renderRedirectHtml(index.default));
  return index;
}

function selftest() {
  const assert = (condition, message) => {
    if (!condition) throw new Error(message);
  };

  assert(isVersionFolder('edge'), 'edge is accepted');
  assert(isVersionFolder('v1.2.0'), 'semver tag is accepted');
  assert(isVersionFolder('v1.2'), 'minor tag is accepted');
  assert(!isVersionFolder('latest'), 'latest is not accepted');
  assert(!isVersionFolder('_astro'), '_astro is not accepted');
  assert(computeDefault(['edge']) === 'edge', 'edge is default without tags');
  assert(computeDefault(['edge', 'v1.2.0', 'v1.10.0']) === 'edge', 'edge outranks a release');
  // Without edge the newest tag wins, and the compare is numeric rather than
  // lexical: 'v1.10.0' sorts before 'v1.2.0' as a string.
  assert(computeDefault(['v1.2.0', 'v1.10.0']) === 'v1.10.0', 'numeric compare');
  assert(computeDefault([]) === 'edge', 'edge is the answer with nothing to choose from');

  const dates = new Map([['v1.0.0', '2026-01-02'], ['v1.2.0', '2026-03-04'], ['v1.10.0', '2026-05-06']]);
  const index = buildIndex(['v1.0.0', 'edge', 'v1.2.0'], dates);
  assert(index.default === 'edge', 'edge is default beside releases');
  assert(index.versions.map((v) => v.slug).join(',') === 'edge,v1.2.0,v1.0.0', 'stable order');
  assert(index.latest === 'v1.2.0', 'latest is not the newest release');
  assert(buildIndex(['edge', 'v1.2.0', 'v1.10.0'], dates).latest === 'v1.10.0', 'latest compares lexically');
  assert(!('latest' in buildIndex(['edge'])), 'an index with no release names a latest one');
  assert(
    index.versions.map((v) => v.released ?? '-').join(',') === '-,2026-03-04,2026-01-02',
    'release dates are not carried to their versions, or edge carries one',
  );
  const undated = buildIndex(['edge', 'v1.2.0', 'v9.9.9'], dates);
  assert(
    undated.versions.find((v) => v.slug === 'v9.9.9') && !('released' in undated.versions.find((v) => v.slug === 'v9.9.9')),
    'a release with no known date was dropped, or given one',
  );
  assert(indexProblems(index).length === 0, `a complete index was refused: ${indexProblems(index).join('; ')}`);
  assert(
    indexProblems(undated).some((problem) => problem.includes('v9.9.9 with no release date')),
    'an index with an undated release was accepted',
  );
  assert(
    indexProblems({ ...index, latest: 'v1.0.0' }).some((problem) => problem.includes('as latest')),
    'an index naming an older release latest was accepted',
  );
  assert(indexProblems(buildIndex(['edge'])).length === 0, 'an index with edge alone was refused');
  const repositoryDates = releaseDates();
  assert(
    [...repositoryDates.values()].every((date) => /^\d{4}-\d{2}-\d{2}$/.test(date)),
    'a tag date is not a YYYY-MM-DD day',
  );
  assert(![...repositoryDates.keys()].some((slug) => !isRelease(slug)), 'a tag that is not a release has a date');

  const tmp = mkdtempSync(join(tmpdir(), 'ptah-docs-versions-'));
  try {
    for (const version of ['edge', 'v1.0.0', 'v1.2.0', '_astro']) {
      mkdirSync(join(tmp, version));
    }
    generate(tmp, dates);
    const json1 = readFileSync(join(tmp, 'versions.json'), 'utf8');
    const html1 = readFileSync(join(tmp, 'index.html'), 'utf8');
    assert(html1.includes('/edge/'), 'redirect targets default');
    assert(!html1.includes('/v1.2.0/'), 'redirect does not target a release while edge exists');
    assert(!json1.includes('_astro'), 'non-version folders are ignored');
    generate(tmp, dates);
    assert(json1 === readFileSync(join(tmp, 'versions.json'), 'utf8'), 'versions json is idempotent');
    assert(html1 === readFileSync(join(tmp, 'index.html'), 'utf8'), 'redirect html is idempotent');
    console.log('gen-versions.mjs --selftest: OK');
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function main() {
  const arg = process.argv[2];
  if (arg === '--selftest') {
    selftest();
    return;
  }
  if (!arg) {
    console.error('usage: node scripts/gen-versions.mjs <site-dir> | --selftest');
    process.exitCode = 2;
    return;
  }
  if (!existsSync(arg)) {
    console.error(`error: directory not found: ${arg}`);
    process.exitCode = 2;
    return;
  }
  const index = generate(arg, releaseDates());
  console.log(`wrote ${join(arg, 'versions.json')} (default=${index.default}, latest=${index.latest ?? 'none'})`);
}

// Only when this file is the program. check-pages-root.mjs imports
// indexProblems, and a module that ran its CLI on import would print a usage
// line and set a failing exit code inside that gate.
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main();
}
