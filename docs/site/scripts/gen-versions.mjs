#!/usr/bin/env node
import { existsSync, mkdtempSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

export const PAGES_PREFIX = '/ptah';

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

export function computeDefault(slugs) {
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
  assert(computeDefault(['edge', 'v1.2.0', 'v1.10.0']) === 'v1.10.0', 'numeric compare');

  const index = buildIndex(['v1.0.0', 'edge', 'v1.2.0']);
  assert(index.default === 'v1.2.0', 'highest tag is default');
  assert(index.versions.map((v) => v.slug).join(',') === 'edge,v1.2.0,v1.0.0', 'stable order');

  const tmp = mkdtempSync(join(tmpdir(), 'ptah-docs-versions-'));
  try {
    for (const version of ['edge', 'v1.0.0', 'v1.2.0', '_astro']) {
      mkdirSync(join(tmp, version));
    }
    generate(tmp);
    const json1 = readFileSync(join(tmp, 'versions.json'), 'utf8');
    const html1 = readFileSync(join(tmp, 'index.html'), 'utf8');
    assert(html1.includes('/ptah/v1.2.0/'), 'redirect targets default');
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
