#!/usr/bin/env node
// Search is a reader journey, not merely a build artifact. This check asks the
// Pagefind index the same questions readers use and requires the canonical page
// to appear in the first three results.
import { existsSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadChromium, startBuiltSite } from './lib/built-site.mjs';
import { pagefindRanking } from '../src/lib/search-ranking.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');

export const searchCases = [
  ['install Ptah', '/start/install/'],
  ['first migration', '/start/quick-start-migrations/'],
  ['generate migration', '/versioned/generate/', { before: ['/schema/go-annotations/'] }],
  ['generate migrations from SQL', '/versioned/generate/'],
  ['generate migrations from YAML', '/versioned/generate/'],
  ['generate migrations from HCL', '/versioned/generate/'],
  ['generate migrations from DBML', '/versioned/generate/'],
  ['generate migration without Go', '/versioned/generate/'],
  ['apply migrations', '/versioned/apply/'],
  ['rollback migration', '/versioned/rollback/'],
  ['schema drift', '/direct/compare-and-drift/', { before: ['/schema/go-annotations/'] }],
  ['apply desired schema', '/direct/apply/', { before: ['/schema/go-annotations/'] }],
  ['adopt existing database', '/start/adopt-an-existing-database/'],
  ['adopt database as SQL', '/start/adopt-an-existing-database/'],
  ['adopt database as HCL', '/start/adopt-an-existing-database/'],
  ['adopt database as DBML', '/start/adopt-an-existing-database/'],
  ['migrate from Atlas', '/atlas/adoption/'],
  ['retained divergences', '/atlas/retained-divergences/'],
  ['checksum mismatch', '/versioned/integrity-and-safety/'],
  ['PostgreSQL extension', '/databases/postgresql/'],
  ['SQL Server support', '/databases/sqlserver/'],
  ['MySQL supported versions', '/databases/support-matrix/'],
  ['pgvector', '/inference/quick-start/'],
  ['change embedding model', '/inference/guides/migrate-to-another-model/'],
  ['resume inference migration', '/inference/guides/resume-and-recover/'],
  ['MCP', '/operate/ai-agents/'],
  ['Go annotations', '/schema/go-annotations/'],
  ['generate migration from Go structs', '/schema/go-annotations/'],
  ['test YAML schema', '/testing/migrations-and-schema/'],
  ['test DBML schema', '/testing/migrations-and-schema/'],
  ['external schema loader', '/schema/orm-and-external/'],
  ['OCI schema source', '/operate/oci-registry/'],
  ['use Ptah Action with schema file', '/testing/ci/'],
  ['visualize schema', '/schema/visualize/'],
  ['generate protobuf', '/schema/protobuf/'],
  ['exit code 2', '/reference/exit-codes/'],
];

function normalizeRoute(url, base) {
  let path = new URL(url, 'http://ptah.invalid').pathname;
  if (base && path.startsWith(base)) path = path.slice(base.length) || '/';
  return path.endsWith('/') ? path : `${path}/`;
}

export function rankingProblems(cases, rankings, base = '') {
  const observed = new Map(rankings.map(({ query, urls }) => [query, urls.map((url) => normalizeRoute(url, base))]));
  const problems = [];

  for (const [query, expected, options = {}] of cases) {
    const urls = observed.get(query) ?? [];
    const rank = urls.indexOf(expected);
    if (rank === -1 || rank >= 3) {
      const position = rank === -1 ? 'not returned' : `ranked ${rank + 1}`;
      problems.push(
        `${JSON.stringify(query)}: ${expected} was ${position}; top three: ${urls.slice(0, 3).join(', ') || '(none)'}`,
      );
      continue;
    }
    for (const route of options.before ?? []) {
      const otherRank = urls.indexOf(route);
      if (otherRank !== -1 && otherRank < rank) {
        problems.push(
          `${JSON.stringify(query)}: source-neutral ${expected} ranked ${rank + 1}, below ${route} at ${otherRank + 1}`,
        );
      }
    }
  }
  return problems;
}

function selftest() {
  const cases = [['find alpha', '/alpha/', { before: ['/go-alpha/'] }]];
  const passing = rankingProblems(cases, [{ query: 'find alpha', urls: ['/docs/other/', '/docs/alpha/', '/docs/go-alpha/'] }], '/docs');
  const missing = rankingProblems(cases, [{ query: 'find alpha', urls: ['/docs/other/'] }], '/docs');
  const fourth = rankingProblems(
    cases,
    [{ query: 'find alpha', urls: ['/docs/one/', '/docs/two/', '/docs/three/', '/docs/alpha/'] }],
    '/docs',
  );
  const goFirst = rankingProblems(
    cases,
    [{ query: 'find alpha', urls: ['/docs/go-alpha/', '/docs/alpha/'] }],
    '/docs',
  );
  if (passing.length !== 0 || missing.length !== 1 || fourth.length !== 1 || !fourth[0].includes('ranked 4') ||
      goFirst.length !== 1 || !goFirst[0].includes('below /go-alpha/')) {
    console.error('check-search-ranking.mjs --selftest: FAILED');
    process.exitCode = 1;
    return;
  }
  console.log('check-search-ranking.mjs --selftest: OK (missing, below-top-three, and wrong-source-order results rejected)');
}

async function main() {
  if (process.argv.includes('--selftest')) {
    selftest();
    return;
  }

  const distIndex = process.argv.indexOf('--dist');
  const distRoot = join(siteRoot, distIndex === -1 ? 'dist' : process.argv[distIndex + 1]);
  if (!existsSync(distRoot)) {
    console.error(
      `check-search-ranking.mjs: ${relative(siteRoot, distRoot)} not found; run "npm run build" first.`,
    );
    process.exitCode = 1;
    return;
  }

  const chromium = await loadChromium('check-search-ranking.mjs');
  if (!chromium) return;

  const built = await startBuiltSite(distRoot);
  let browser;
  try {
    browser = await chromium.launch();
    const page = await browser.newPage();
    await page.goto(`http://127.0.0.1:${built.port}${built.base}/`, { waitUntil: 'load' });
    const rankings = await page.evaluate(async ({ base, cases, ranking }) => {
      const pagefind = await import(`${base}/pagefind/pagefind.js`);
      await pagefind.options({ ranking });
      const output = [];
      for (const [query] of cases) {
        const search = await pagefind.search(query);
        const results = await Promise.all(search.results.slice(0, 10).map((result) => result.data()));
        output.push({ query, urls: results.map(({ url }) => url) });
      }
      return output;
    }, { base: built.base, cases: searchCases, ranking: pagefindRanking });

    const problems = rankingProblems(searchCases, rankings, built.base);
    if (problems.length > 0) {
      console.error('check-search-ranking.mjs: FAILED');
      for (const problem of problems) console.error(`- ${problem}`);
      process.exitCode = 1;
      return;
    }
    if (process.argv.includes('--verbose')) {
      const observed = new Map(rankings.map(({ query, urls }) => [
        query,
        urls.map((url) => normalizeRoute(url, built.base)),
      ]));
      for (const [query, expected] of searchCases) {
        console.log(`${JSON.stringify(query)}: ${expected} rank ${(observed.get(query) ?? []).indexOf(expected) + 1}`);
      }
    }
    console.log(`check-search-ranking.mjs: OK (${searchCases.length} queries; canonical page in top three)`);
  } finally {
    if (browser) await browser.close();
    await new Promise((resolve) => built.server.close(resolve));
  }
}

await main();
