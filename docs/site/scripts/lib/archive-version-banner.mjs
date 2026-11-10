// Archive warnings belong to the assembled site, where every release and the
// latest release's routes are known. Historical Astro components cannot put
// a warning into releases that predate the maintained UI overlay.
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative, sep } from 'node:path';
import { compareReleases, isRelease, isVersionFolder, LATEST } from './doc-versions.mjs';
import { Origin } from '../../src/lib/docs-origin.mjs';

const MAIN = /<main\b[^>]*\bdata-pagefind-body\b[^>]*>/;
const START = '<!-- ptah-archive-banner:start -->';
const END = '<!-- ptah-archive-banner:end -->';
const INSERTED = /<!-- ptah-archive-banner:start -->[\s\S]*?<!-- ptah-archive-banner:end -->/g;
const STYLESHEET = '<link rel="stylesheet" href="/version-picker.css">';
const CANONICAL = /<link\b(?=[^>]*\brel=["']canonical["'])[^>]*>\s*/gi;
const REFRESH = /<meta\b(?=[^>]*http-equiv=["']refresh["'])[^>]*content=["'][^"']*?url=([^"']+)["']/i;
const NOINDEX = /<meta\b(?=[^>]*\bname=["']robots["'])(?=[^>]*\bcontent=["'][^"']*\bnoindex\b)[^>]*>/i;

export function isArticle(html, path) {
  return !/(^|\/)404\.html$/.test(path) && MAIN.test(html) && !REFRESH.test(html) && !NOINDEX.test(html);
}

function latestTarget(path, latestPaths) {
  return latestPaths instanceof Map ? latestPaths.get(path) : latestPaths.has(path) ? path : undefined;
}

export function pageRoute(path) {
  return path.replace(/(^|\/)index\.html$/, '$1').split('/').map(encodeURIComponent).join('/');
}

function articleBody(html) {
  const start = MAIN.exec(html);
  if (!start) return undefined;
  const end = html.indexOf('</main>', start.index + start[0].length);
  return end < 0 ? undefined : html.slice(start.index + start[0].length, end).replace(INSERTED, '');
}

function equivalentArticle(left, right) {
  const title = /<title\b[^>]*>[\s\S]*?<\/title>/i;
  const body = articleBody(left);
  const leftTitle = left.match(title)?.[0];
  return body !== undefined && Boolean(leftTitle) && body === articleBody(right) &&
    leftTitle === right.match(title)?.[0];
}

function canonicalForPage(html, { version, latest, path, latestFiles }) {
  const clean = html.replace(CANONICAL, '');
  if (!isArticle(clean, path)) return clean;
  const latestHtml = latestFiles.get(path);
  // A numbered latest release is copied into /latest/. An older release only
  // shares that URL when the rendered title and article body are byte-identical.
  const equivalent = latestHtml && isArticle(latestHtml, path) &&
    (version === latest || (olderRelease(version, latest) && equivalentArticle(clean, latestHtml)));
  const destination = equivalent ? LATEST : version;
  const canonical = `<link rel="canonical" href="${Origin}/${destination}/${pageRoute(path)}">`;
  if (clean.includes('</head>')) return clean.replace('</head>', `${canonical}</head>`);
  // A real article without an explicit closing head can still have a body.
  if (/<body\b/.test(clean)) return clean.replace(/<body\b/, `${canonical}<body`);
  throw new Error(`${path} has no head boundary for its canonical URL`);
}

function olderRelease(version, latest) {
  return isRelease(version) && isRelease(latest) && compareReleases(version, latest) < 0;
}

function bannerMarkup(version, latest, path, latestPaths) {
  const target = latestTarget(path, latestPaths);
  const home = !target;
  const href = `/${LATEST}/${target ? pageRoute(target) : ''}`;
  return `${START}<div class="ptah-version-banner" role="note" data-pagefind-ignore>` +
    `<p class="ptah-version-banner__text">This page documents ${version}, an older release. ` +
    `<a class="ptah-version-banner__link" href="${href}">${home ? 'Go to ' : 'Read it in '}${latest}, the latest release</a>` +
    `</p></div>${END}`;
}

// Render the warning into the HTML so it works without JavaScript or a picker.
// The picker recognizes the same class and leaves this warning in place.
export function decorateArchivePage(html, page) {
  const { version, latest, path, latestPaths } = page;
  const clean = html.replace(INSERTED, '');
  if (!isArticle(clean, path) || !olderRelease(version, latest)) return canonicalForPage(clean, page);
  let decorated = clean.replace(MAIN, (main) => main + bannerMarkup(version, latest, path, latestPaths));
  if (!decorated.includes('href="/version-picker.css"')) {
    if (!decorated.includes('</head>')) throw new Error(`${version}/${path} has no closing head for the archive stylesheet`);
    decorated = decorated.replace('</head>', `${STYLESHEET}</head>`);
  }
  return canonicalForPage(decorated, page);
}

export function htmlPaths(root) {
  const paths = [];
  function walk(directory) {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) walk(path);
      else if (entry.isFile() && entry.name.endsWith('.html')) {
        paths.push(relative(root, path).split(sep).join('/'));
      }
    }
  }
  walk(root);
  return paths;
}

export function* archivePages(siteDir) {
  const index = JSON.parse(readFileSync(join(siteDir, 'versions.json'), 'utf8'));
  const versions = index.versions?.map((version) => version.slug);
  if (!Array.isArray(versions) || versions.some((version) => !isVersionFolder(version))) {
    throw new Error('versions.json must list documentation version directories');
  }
  const newest = versions.filter(isRelease).sort(compareReleases).at(-1);
  if (index.latest !== newest) throw new Error(`versions.json names ${index.latest} as latest, want ${newest}`);
  const latestFiles = new Map();
  if (newest) {
    const root = join(siteDir, LATEST);
    for (const path of htmlPaths(root)) {
      if (path !== '404.html') latestFiles.set(path, readFileSync(join(root, path), 'utf8'));
    }
  }
  const latestPaths = new Map();
  // Follow retired-route redirects to an actual latest article. A missing,
  // external, cyclic, or error-page target supplies no newer-page link.
  function resolveTarget(path, seen = new Set()) {
    if (seen.has(path) || !latestFiles.has(path)) return undefined;
    const html = latestFiles.get(path);
    const refresh = html.match(REFRESH);
    if (!refresh) return isArticle(html, path) ? path : undefined;
    try {
      const url = new URL(refresh[1], `${Origin}/${LATEST}/${pageRoute(path)}`);
      if (url.origin !== Origin || !url.pathname.startsWith(`/${LATEST}/`)) return undefined;
      const route = decodeURIComponent(url.pathname.slice(`/${LATEST}/`.length));
      return resolveTarget(route.endsWith('/') || !route ? `${route}index.html` : route, new Set([...seen, path]));
    } catch {
      return undefined;
    }
  }
  for (const path of latestFiles.keys()) latestPaths.set(path, resolveTarget(path));
  for (const version of versions) {
    const root = join(siteDir, version);
    let documents = 0;
    for (const path of htmlPaths(root)) {
      const file = join(root, path);
      const html = readFileSync(file, 'utf8');
      if (isArticle(html, path)) documents += 1;
      yield { file, html, version, latest: newest, path, latestPaths, latestFiles };
    }
    if (olderRelease(version, newest) && documents === 0) {
      throw new Error(`${version} has no documentation pages to warn on`);
    }
  }
}

export function publishArchiveBanners(siteDir) {
  let warned = 0;
  for (const page of archivePages(siteDir)) {
    const decorated = decorateArchivePage(page.html, page);
    if (decorated !== page.html) writeFileSync(page.file, decorated);
    if (isArticle(page.html, page.path) && olderRelease(page.version, page.latest)) warned += 1;
  }
  return warned;
}

// The deployment gate reads every real article, including pre-overlay releases.
export function archiveBannerProblems(siteDir) {
  const problems = [];
  for (const page of archivePages(siteDir)) {
    const bannerCount = (page.html.match(/class="ptah-version-banner"/g) ?? []).length;
    const expectedCount = isArticle(page.html, page.path) && olderRelease(page.version, page.latest) ? 1 : 0;
    if (bannerCount !== expectedCount) {
      problems.push(`${page.version}/${page.path} has ${bannerCount} archive warnings, want ${expectedCount}`);
    }
    if (decorateArchivePage(page.html, page) !== page.html) {
      problems.push(`${page.version}/${page.path} has an incorrect canonical, archive warning, or stylesheet`);
    }
  }
  return problems;
}

export function archiveBannerSelftest() {
  const assert = (condition, message) => { if (!condition) throw new Error(message); };
  const site = mkdtempSync(join(tmpdir(), 'ptah-archive-banners-'));
  const article = `<!doctype html><html><head><title>Docs</title><link rel="canonical" href="${Origin}/v0.2.0/"></head><body><main data-pagefind-body><h1>Original content</h1></main></body></html>`;
  const versions = ['edge', LATEST, ...Array.from({ length: 12 }, (_, index) => `v0.${index + 1}.0`)];
  const put = (version, path, html) => {
    const file = join(site, version, path);
    mkdirSync(dirname(file), { recursive: true });
    writeFileSync(file, html);
  };
  try {
    writeFileSync(join(site, 'versions.json'), JSON.stringify({ latest: 'v0.12.0', versions: versions.map((slug) => ({ slug })) }));
    for (const version of versions) put(version, 'index.html', article);
    put('edge', 'only/index.html', article);
    put('v0.2.0', 'atlas/project-config/index.html', article);
    put('v0.2.0', 'gone/index.html', article);
    put('v0.2.0', 'changed/index.html', article);
    put(LATEST, 'changed/index.html', article.replace('Original content', 'New content'));
    put('v0.12.0', 'atlas/project-config/index.html', article);
    put(LATEST, 'atlas/project-config/index.html', article);
    put('v0.2.0', 'retired/index.html', article);
    put(LATEST, 'retired/index.html', '<!doctype html><title>Redirect</title><meta http-equiv="refresh" content="0;url=/latest/atlas/project-config/"><body>Redirecting</body>');
    put('v0.2.0', 'cyclic/index.html', article);
    put(LATEST, 'cyclic/index.html', '<html><head><meta http-equiv="refresh" content="0;url=/latest/cyclic/"></head></html>');
    put('v0.2.0', 'external/index.html', article);
    put(LATEST, 'external/index.html', '<html><head><meta http-equiv="refresh" content="0;url=https://example.com/"></head></html>');
    put('v0.2.0', 'error/index.html', article);
    put(LATEST, 'error/index.html', '<html><head><meta http-equiv="refresh" content="0;url=/latest/404.html"></head></html>');
    put('v0.2.0', 'noindex/index.html', article);
    put(LATEST, 'noindex/index.html', article.replace('<title>Docs</title>', '<title>Docs</title><meta name="robots" content="noindex">'));
    put('v0.2.0', 'malformed/index.html', article);
    put(LATEST, 'malformed/index.html', '<html><head><meta http-equiv="refresh" content="0;url=http://["></head></html>');
    put('v0.2.0', 'redirect/index.html', '<html><head><meta http-equiv="refresh" content="0;url=../"></head></html>');
    put('v0.2.0', 'samples/index.html', '<main><h1>Standalone sample</h1></main>');
    put('v0.2.0', '404.html', article);
    put(LATEST, '404.html', article);
    assert(archiveBannerProblems(site).length > 0, 'an archive without warnings passed');
    assert(publishArchiveBanners(site) === 20, 'not every old release article received a warning');
    assert(archiveBannerProblems(site).length === 0, 'published warnings failed the gate');
    const file = join(site, 'v0.2.0', 'atlas/project-config/index.html');
    const html = readFileSync(file, 'utf8');
    assert(html.includes('This page documents v0.2.0, an older release.'), 'the warning does not identify the historical version');
    assert(html.includes('href="/latest/atlas/project-config/"'), 'the matching latest page was not linked');
    assert(html.includes(`rel="canonical" href="${Origin}/latest/atlas/project-config/"`), 'equivalent historical content did not canonicalize to latest');
    const gone = readFileSync(join(site, 'v0.2.0', 'gone/index.html'), 'utf8');
    assert(gone.includes('href="/latest/"') && gone.includes(`${Origin}/v0.2.0/gone/`), 'a missing latest page lost its historical canonical or home link');
    const changed = readFileSync(join(site, 'v0.2.0', 'changed/index.html'), 'utf8');
    assert(changed.includes('href="/latest/changed/"') && changed.includes(`${Origin}/v0.2.0/changed/`), 'different article content canonicalized to latest');
    const retired = readFileSync(join(site, 'v0.2.0', 'retired/index.html'), 'utf8');
    assert(retired.includes('href="/latest/atlas/project-config/"') && retired.includes(`${Origin}/v0.2.0/retired/`), 'a retired latest route changed the historical canonical');
    const cyclic = readFileSync(join(site, 'v0.2.0', 'cyclic/index.html'), 'utf8');
    assert(cyclic.includes('href="/latest/"') && cyclic.includes(`${Origin}/v0.2.0/cyclic/`), 'a cyclic redirect supplied a broken banner link');
    for (const route of ['external', 'error', 'noindex', 'malformed']) {
      const page = readFileSync(join(site, 'v0.2.0', route, 'index.html'), 'utf8');
      assert(page.includes('href="/latest/"') && page.includes(`${Origin}/v0.2.0/${route}/`), `${route} supplied a broken banner link`);
    }
    assert(!readFileSync(join(site, 'edge', 'index.html'), 'utf8').includes(START), 'edge received an archive warning');
    assert(readFileSync(join(site, 'edge', 'index.html'), 'utf8').includes(`${Origin}/edge/`), 'edge does not canonicalize to itself');
    assert(readFileSync(join(site, 'edge', 'only/index.html'), 'utf8').includes(`${Origin}/edge/only/`), 'an edge-only article has no self canonical');
    assert(readFileSync(join(site, LATEST, 'index.html'), 'utf8').includes(`${Origin}/latest/`), 'latest does not canonicalize to itself');
    assert(readFileSync(join(site, 'v0.12.0', 'index.html'), 'utf8').includes(`${Origin}/latest/`), 'the newest numbered release does not consolidate with latest');
    assert(!readFileSync(join(site, 'v0.12.0', 'index.html'), 'utf8').includes(START), 'the latest release received an archive warning');
    assert(!readFileSync(join(site, 'v0.2.0', 'samples/index.html'), 'utf8').includes(START), 'a standalone sample was decorated');
    assert(!readFileSync(join(site, 'v0.2.0', 'redirect/index.html'), 'utf8').includes('rel="canonical"'), 'a redirect retained a canonical');
    assert(!readFileSync(join(site, 'v0.2.0', '404.html'), 'utf8').includes('rel="canonical"'), 'a 404 page retained a canonical');
    publishArchiveBanners(site);
    assert(readFileSync(file, 'utf8') === html, 'a second publication changed or duplicated the warning');
    for (const broken of [html.replace(/<!-- ptah-archive-banner:start -->[\s\S]*?<!-- ptah-archive-banner:end -->/, ''), html.replace('href="/latest/atlas/project-config/"', 'href="/latest/missing/"'), html.replace('data-pagefind-ignore', ''), html.replace(STYLESHEET, ''), html.replace(`${Origin}/latest/atlas/project-config/`, `${Origin}/v0.2.0/atlas/project-config/`)]) {
      writeFileSync(file, broken);
      assert(archiveBannerProblems(site).length > 0, 'an incomplete or incorrect warning passed');
    }
    writeFileSync(file, html);
    assert(!decorateArchivePage(article, { version: 'v0.10.0', latest: 'v0.9.0', path: 'index.html', latestPaths: new Set(), latestFiles: new Map() }).includes(START), 'release comparison is lexical');
    rmSync(join(site, 'v0.1.0', 'index.html'));
    let refused = false;
    try { publishArchiveBanners(site); } catch (error) { refused = /no documentation pages/.test(error.message); }
    assert(refused, 'an empty historical release passed');
  } finally {
    rmSync(site, { recursive: true, force: true });
  }
}
