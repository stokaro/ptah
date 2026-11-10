// Sitemap discovery is built from the final HTML, after release aliases and
// archive canonicals have been written. Astro builds each version separately
// and cannot see which routes the assembled site redirects or canonicalizes.
import { existsSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Origin } from '../../src/lib/docs-origin.mjs';
import { archivePages, isArticle, pageRoute } from './archive-version-banner.mjs';
import { EDGE, LATEST } from './doc-versions.mjs';

const canonical = /<link\b(?=[^>]*\brel=["']canonical["'])[^>]*\bhref=["']([^"']+)["'][^>]*>/gi;
const sitemapFile = /^sitemap-\d+\.xml$/;

function versionUrls(siteDir) {
  const urls = new Map();
  for (const page of archivePages(siteDir)) {
    if (!urls.has(page.version)) urls.set(page.version, []);
    if (!isArticle(page.html, page.path)) continue;
    const self = `${Origin}/${page.version}/${pageRoute(page.path)}`;
    const found = [...page.html.matchAll(canonical)].map((match) => match[1]);
    if (found.length === 1 && found[0] === self) urls.get(page.version).push(self);
  }
  for (const entries of urls.values()) entries.sort();
  return urls;
}

function urlset(urls) {
  return `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${urls.map((url) => `  <url><loc>${url}</loc></url>`).join('\n')}${urls.length ? '\n' : ''}</urlset>\n`;
}

function sitemapIndex(urls) {
  return `<?xml version="1.0" encoding="UTF-8"?>\n<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${urls.map((url) => `  <sitemap><loc>${url}</loc></sitemap>`).join('\n')}${urls.length ? '\n' : ''}</sitemapindex>\n`;
}

function activeVersions(urls) {
  return [EDGE, LATEST].filter((version) => urls.has(version));
}

function expected(siteDir) {
  const files = new Map();
  const urls = versionUrls(siteDir);
  for (const [version, entries] of urls) {
    files.set(`${version}/sitemap-0.xml`, urlset(entries));
    files.set(`${version}/sitemap-index.xml`, sitemapIndex([`${Origin}/${version}/sitemap-0.xml`]));
  }
  const active = activeVersions(urls).map((version) => `${Origin}/${version}/sitemap-index.xml`);
  files.set('sitemap-index.xml', sitemapIndex(active));
  files.set('robots.txt', `User-agent: *\nAllow: /\n${active.map((url) => `Sitemap: ${url}\n`).join('')}`);
  return files;
}

export function publishSitemaps(siteDir) {
  const files = expected(siteDir);
  for (const [path, content] of files) {
    const file = join(siteDir, path);
    mkdirSync(join(file, '..'), { recursive: true });
    writeFileSync(file, content);
  }
  for (const version of versionUrls(siteDir).keys()) {
    for (const name of readdirSync(join(siteDir, version))) {
      if (sitemapFile.test(name) && name !== 'sitemap-0.xml') rmSync(join(siteDir, version, name));
    }
  }
}

export function sitemapProblems(siteDir) {
  const problems = [];
  const urls = versionUrls(siteDir);
  for (const version of activeVersions(urls)) {
    if (urls.get(version).length === 0) problems.push(`${version} has no indexable articles`);
  }
  for (const [path, content] of expected(siteDir)) {
    const file = join(siteDir, path);
    if (!existsSync(file) || readFileSync(file, 'utf8') !== content) {
      problems.push(`${path} disagrees with the final article canonicals`);
    }
  }
  for (const version of versionUrls(siteDir).keys()) {
    for (const name of readdirSync(join(siteDir, version))) {
      if (sitemapFile.test(name) && name !== 'sitemap-0.xml') problems.push(`${version}/${name} is a stale sitemap`);
    }
  }
  return problems;
}

export function sitemapSelftest() {
  const site = mkdtempSync(join(tmpdir(), 'ptah-sitemaps-'));
  const article = (url) => `<html><head><link rel="canonical" href="${url}"></head><body><main data-pagefind-body><h1>Docs</h1></main></body></html>`;
  try {
    writeFileSync(join(site, 'versions.json'), JSON.stringify({ latest: 'v0.2.0', versions: ['edge', 'latest', 'v0.2.0', 'v0.1.0'].map((slug) => ({ slug })) }));
    for (const version of ['edge', 'latest', 'v0.2.0', 'v0.1.0']) {
      mkdirSync(join(site, version));
      writeFileSync(join(site, version, 'index.html'), article(`${Origin}/${version}/`));
    }
    writeFileSync(join(site, 'edge', 'redirect.html'), '<html><head><meta http-equiv="refresh" content="0;url=/edge/"></head><body><main data-pagefind-body>Redirect</main></body></html>');
    writeFileSync(join(site, 'edge', '404.html'), article(`${Origin}/edge/404.html`));
    writeFileSync(join(site, 'edge', 'sitemap-1.xml'), '<urlset><url><loc>stale</loc></url></urlset>');
    writeFileSync(join(site, 'v0.2.0', 'index.html'), article(`${Origin}/latest/`));
    publishSitemaps(site);
    if (sitemapProblems(site).length) throw new Error('valid sitemaps failed');
    if (existsSync(join(site, 'edge', 'sitemap-1.xml'))) throw new Error('a stale sitemap shard survived publication');
    const edge = readFileSync(join(site, 'edge', 'sitemap-0.xml'), 'utf8');
    if (!edge.includes(`${Origin}/edge/`) || edge.includes('redirect') || edge.includes('404.html')) {
      throw new Error('edge sitemap includes a redirect or error page, or omits the article');
    }
    if (readFileSync(join(site, 'v0.2.0', 'sitemap-0.xml'), 'utf8').includes('<url>')) throw new Error('newest numbered release was advertised');
    const root = readFileSync(join(site, 'sitemap-index.xml'), 'utf8');
    if (!root.includes('/edge/sitemap-index.xml') || !root.includes('/latest/sitemap-index.xml')) throw new Error('active sitemaps are not discoverable');
    publishSitemaps(site);
    if (readFileSync(join(site, 'sitemap-index.xml'), 'utf8') !== root) throw new Error('sitemap publication is not idempotent');
    writeFileSync(join(site, 'edge', 'sitemap-0.xml'), edge.replace('</urlset>', `<url><loc>${Origin}/edge/redirect/</loc></url></urlset>`));
    if (!sitemapProblems(site).length) throw new Error('a redirected route passed the sitemap gate');
  } finally {
    rmSync(site, { recursive: true, force: true });
  }
}
