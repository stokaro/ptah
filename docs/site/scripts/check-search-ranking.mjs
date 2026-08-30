#!/usr/bin/env node
// Search is a reader journey, not merely a build artifact. This check asks the
// Pagefind index the same questions readers use and requires the canonical page
// to appear in the first three results.
import { existsSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadChromium, startBuiltSite } from './lib/built-site.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');

export const searchCases = [
  ['install Ptah', '/start/install/'],
  ['first migration', '/start/quick-start-migrations/'],
  ['apply migrations', '/versioned/apply/'],
  ['rollback migration', '/versioned/rollback/'],
  ['schema drift', '/direct/compare-and-drift/'],
  ['apply desired schema', '/direct/apply/'],
  ['adopt existing database', '/start/adopt-an-existing-database/'],
  ['migrate from Atlas', '/atlas/adoption/'],
  ['checksum mismatch', '/versioned/integrity-and-safety/'],
  ['PostgreSQL extension', '/databases/postgresql/'],
  ['SQL Server support', '/databases/sqlserver/'],
  ['MySQL supported versions', '/databases/support-matrix/'],
  ['pgvector', '/inference/quick-start/'],
  ['change embedding model', '/inference/guides/migrate-to-another-model/'],
  ['resume inference migration', '/inference/guides/resume-and-recover/'],
  ['MCP', '/operate/ai-agents/'],
  ['Go annotations', '/schema/go-annotations/'],
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

  for (const [query, expected] of cases) {
    const urls = observed.get(query) ?? [];
    const rank = urls.indexOf(expected);
    if (rank === -1 || rank >= 3) {
      const position = rank === -1 ? 'not returned' : `ranked ${rank + 1}`;
      problems.push(
        `${JSON.stringify(query)}: ${expected} was ${position}; top three: ${urls.slice(0, 3).join(', ') || '(none)'}`,
      );
    }
  }
  return problems;
}

function selftest() {
  const cases = [['find alpha', '/alpha/']];
  const passing = rankingProblems(cases, [{ query: 'find alpha', urls: ['/docs/other/', '/docs/alpha/'] }], '/docs');
  const missing = rankingProblems(cases, [{ query: 'find alpha', urls: ['/docs/other/'] }], '/docs');
  const fourth = rankingProblems(
    cases,
    [{ query: 'find alpha', urls: ['/docs/one/', '/docs/two/', '/docs/three/', '/docs/alpha/'] }],
    '/docs',
  );
  if (passing.length !== 0 || missing.length !== 1 || fourth.length !== 1 || !fourth[0].includes('ranked 4')) {
    console.error('check-search-ranking.mjs --selftest: FAILED');
    process.exitCode = 1;
    return;
  }
  console.log('check-search-ranking.mjs --selftest: OK (missing and below-top-three results rejected)');
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
    const rankings = await page.evaluate(async ({ base, cases }) => {
      const pagefind = await import(`${base}/pagefind/pagefind.js`);
      const output = [];
      for (const [query] of cases) {
        const search = await pagefind.search(query);
        const results = await Promise.all(search.results.slice(0, 10).map((result) => result.data()));
        output.push({ query, urls: results.map(({ url }) => url) });
      }
      return output;
    }, { base: built.base, cases: searchCases });

    const problems = rankingProblems(searchCases, rankings, built.base);
    if (problems.length > 0) {
      console.error('check-search-ranking.mjs: FAILED');
      for (const problem of problems) console.error(`- ${problem}`);
      process.exitCode = 1;
      return;
    }
    console.log(`check-search-ranking.mjs: OK (${searchCases.length} queries; canonical page in top three)`);
  } finally {
    if (browser) await browser.close();
    await new Promise((resolve) => built.server.close(resolve));
  }
}

await main();
