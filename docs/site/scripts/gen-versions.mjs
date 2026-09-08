#!/usr/bin/env node
import { existsSync, mkdtempSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// The site root, from the one declaration. It is empty because the site is
// served at the apex of its own domain; it was `/ptah` while the site was a
// GitHub project page, and this stub kept sending readers there after the move
// (stokaro/ptah#2884).
export const PAGES_PREFIX = '';

const EDGE = 'edge';
const VERSION_RE = /^v(\d+)\.(\d+)(?:\.(\d+))?$/;

export function isVersionFolder(name) {
  return name === EDGE || VERSION_RE.test(name);
}

export function parseSemver(name) {
  const match = VERSION_RE.exec(name);
  if (!match) return null;
  return [Number(match[1]), Number(match[2]), match[3] === undefined ? 0 : Number(match[3])];
}

function compareSemver(a, b) {
  for (let i = 0; i < 3; i += 1) {
    if (a[i] !== b[i]) return a[i] - b[i];
  }
  return 0;
}

// computeDefault names the version the apex redirect serves, and the one its
// canonical link points at.
//
// Edge, while Ptah is pre-GA. A release is a snapshot of what shipped; edge is
// what master documents, and before v1 the difference between them is most of
// the product -- a reader arriving at the apex is asking what Ptah does, not
// what the last tag did. Nothing is hidden by the choice: every release keeps
// its own stable URL, and the picker lists them all, so a reader who wants the
// version they installed is one selection away.
//
// The newest tag is still the answer where there is no edge folder at all: a
// deployment assembled from tags alone needs a default, and the highest one is
// the only sensible pick.
export function computeDefault(slugs) {
  if (slugs.includes(EDGE)) return EDGE;
  let best = null;
  for (const slug of slugs) {
    const semver = parseSemver(slug);
    if (semver && (best === null || compareSemver(semver, best.semver) > 0)) {
      best = { slug, semver };
    }
  }
  return best ? best.slug : EDGE;
}

export function buildIndex(slugs) {
  const tags = slugs
    .filter((slug) => parseSemver(slug))
    .sort((a, b) => compareSemver(parseSemver(b), parseSemver(a)));
  const ordered = [];
  if (slugs.includes(EDGE)) ordered.push(EDGE);
  ordered.push(...tags);
  return {
    default: computeDefault(slugs),
    versions: ordered.map((slug) => ({ slug, label: slug })),
  };
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

export function generate(dir) {
  const slugs = readdirSync(dir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && isVersionFolder(entry.name))
    .map((entry) => entry.name);
  const index = buildIndex(slugs);
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

  const index = buildIndex(['v1.0.0', 'edge', 'v1.2.0']);
  assert(index.default === 'edge', 'edge is default beside releases');
  assert(index.versions.map((v) => v.slug).join(',') === 'edge,v1.2.0,v1.0.0', 'stable order');

  const tmp = mkdtempSync(join(tmpdir(), 'ptah-docs-versions-'));
  try {
    for (const version of ['edge', 'v1.0.0', 'v1.2.0', '_astro']) {
      mkdirSync(join(tmp, version));
    }
    generate(tmp);
    const json1 = readFileSync(join(tmp, 'versions.json'), 'utf8');
    const html1 = readFileSync(join(tmp, 'index.html'), 'utf8');
    assert(html1.includes('/edge/'), 'redirect targets default');
    assert(!html1.includes('/v1.2.0/'), 'redirect does not target a release while edge exists');
    assert(!json1.includes('_astro'), 'non-version folders are ignored');
    generate(tmp);
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
  const index = generate(arg);
  console.log(`wrote ${join(arg, 'versions.json')} (default=${index.default})`);
}

main();
