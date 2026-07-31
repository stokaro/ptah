#!/usr/bin/env node
// Renders every built page at desktop and mobile widths and fails on layout
// defects a reader would hit: a page that scrolls sideways, or an element wider
// than the viewport.
//
// This replaces the manual "review the site on a desktop and a phone" pass that
// documentation restructuring work kept deferring. A human still judges wording
// and information architecture, but overflow is mechanical, and mechanical
// checks are the ones that should not depend on someone remembering.
//
// Usage:
//   node scripts/check-responsive.mjs [--dist <dir>] [--selftest]
//
// Requires Playwright's chromium. A contributor without it is told what to
// install and the check skips; in CI it fails instead, because a green check
// that measured nothing is worse than a red one.
import { createServer } from 'node:http';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, extname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');

// Widths chosen for what they expose: 390 is a common phone viewport and the
// width at which Starlight collapses navigation; 1280 is a laptop.
const viewports = [
  { name: 'mobile', width: 390, height: 844 },
  { name: 'desktop', width: 1280, height: 900 },
];

// A few pixels of slack: sub-pixel rounding in the layout engine otherwise
// reports overflow on pages that look correct.
const overflowTolerance = 2;

// The tallest a table cell may render at desktop width. Character count cannot
// see this: a short cell in a column squeezed narrow by an unbreakable code
// token in its neighbor renders just as tall as a long one, and that is the
// shape readers actually complain about.
//
// Measured at desktop only. At 390px a wide table scrolls inside its own
// container, so its columns keep their desktop widths and a mobile limit would
// either duplicate this one or punish tables for being on a phone.
const maxCellLines = 8;

// Pages whose tables are lookup references, where an exhaustive cell is the
// content rather than a mistake. Each entry states why.
//
// checkStaleAllowlist below fails when an allowlisted route stops having a
// finding, so an entry cannot outlive the reason it was added.
const denseTableAllowlist = new Map([
  [
    '/reference/exit-codes/',
    'per-command cause lists are the reference content, and check-exit-codes.mjs pins these rows verbatim to docs/exit_codes.md',
  ],
]);

const mimeTypes = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.webp': 'image/webp',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ico': 'image/x-icon',
  '.xml': 'application/xml; charset=utf-8',
  '.txt': 'text/plain; charset=utf-8',
  '.pagefind': 'application/octet-stream',
};

function toPosix(value) {
  return value.split(sep).join('/');
}

function argValue(flag, fallback) {
  const index = process.argv.indexOf(flag);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function htmlRoutes(distRoot) {
  const routes = [];
  const walk = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
        continue;
      }
      if (entry.isFile() && entry.name === 'index.html') {
        // Redirect stubs navigate away the moment they load, so measuring them
        // is meaningless and races the evaluate. check-redirects.mjs owns them.
        if (/http-equiv=["']?refresh/i.test(readFileSync(full, 'utf8'))) continue;
        const route = '/' + toPosix(relative(distRoot, dir)).replace(/^\.$/, '');
        routes.push(route === '/' ? '/' : `${route}/`);
      }
    }
  };
  walk(distRoot);
  return routes;
}

// detectBase reads the base path the site was built with. Astro prefixes every
// asset URL with it, so a server that ignores it 404s every stylesheet and
// silently measures unstyled pages - which look fine to an overflow check
// because none of the layout that causes overflow has been applied.
function detectBase(distRoot) {
  const home = join(distRoot, 'index.html');
  if (!existsSync(home)) return '';
  const match = readFileSync(home, 'utf8').match(/(?:href|src)="([^"]*)\/_astro\//);
  return match ? match[1] : '';
}

function startServer(distRoot, base) {
  const server = createServer((request, response) => {
    let url = decodeURIComponent((request.url ?? '/').split('?')[0]);
    if (base && url.startsWith(base)) url = url.slice(base.length) || '/';
    let filePath = join(distRoot, url);
    if (existsSync(filePath) && statSync(filePath).isDirectory()) {
      filePath = join(filePath, 'index.html');
    }
    if (!existsSync(filePath) || !statSync(filePath).isFile()) {
      response.writeHead(404);
      response.end('not found');
      return;
    }
    response.writeHead(200, { 'content-type': mimeTypes[extname(filePath)] ?? 'application/octet-stream' });
    response.end(readFileSync(filePath));
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve({ server, port: server.address().port }));
  });
}

// measure runs in the page. It reports the document's own horizontal overflow
// plus the specific elements that stick out, so a failure names the culprit
// instead of only the page.
const measure = ({ tolerance, cellLineLimit }) => {
  const doc = document.documentElement;
  const viewportWidth = doc.clientWidth;

  // An element inside a scrollable container is allowed to be wider than the
  // viewport; that is exactly how a wide table or code block is supposed to
  // behave. Only content that escapes every scroll container can push the page
  // sideways, so ancestors are checked before an element is reported.
  const escapesScrollContainer = (element) => {
    for (let node = element.parentElement; node && node !== doc; node = node.parentElement) {
      const overflowX = getComputedStyle(node).overflowX;
      if (overflowX === 'auto' || overflowX === 'scroll' || overflowX === 'hidden') return false;
    }
    return true;
  };

  const offenders = [];
  for (const element of document.querySelectorAll('main *')) {
    const rect = element.getBoundingClientRect();
    if (rect.width === 0 && rect.height === 0) continue;
    if (rect.right <= viewportWidth + tolerance) continue;
    if (!escapesScrollContainer(element)) continue;
    // Report the outermost offender only: a wide table makes every cell inside
    // it look wide too, and listing all of them buries the cause.
    if (offenders.some((entry) => entry.element.contains(element))) continue;
    offenders.push({
      element,
      tag: element.tagName.toLowerCase(),
      className: typeof element.className === 'string' ? element.className.slice(0, 60) : '',
      right: Math.round(rect.right),
    });
  }
  // Rendered height in lines, not characters. A cell squeezed into a narrow
  // column is exactly as unreadable as a cell holding an essay.
  const tallCells = [];
  for (const cell of document.querySelectorAll('main table tbody td')) {
    const rect = cell.getBoundingClientRect();
    if (rect.width === 0) continue;
    const lineHeight = parseFloat(getComputedStyle(cell).lineHeight);
    if (!Number.isFinite(lineHeight) || lineHeight <= 0) continue;
    const lines = Math.round(rect.height / lineHeight);
    if (lines <= cellLineLimit) continue;
    tallCells.push({ lines, width: Math.round(rect.width), text: cell.textContent.trim().slice(0, 50) });
  }

  return {
    scrollWidth: doc.scrollWidth,
    clientWidth: viewportWidth,
    offenders: offenders.slice(0, 5).map(({ tag, className, right }) => ({ tag, className, right })),
    tallCells: tallCells.sort((a, b) => b.lines - a.lines).slice(0, 5),
  };
};

async function loadChromium() {
  try {
    const { chromium } = await import('playwright');
    return chromium;
  } catch {
    return null;
  }
}

// unavailable reports the browser as missing. In CI that is a failure: the
// dependency is installed by the workflow, so its absence means the check would
// have reported success without rendering a single page.
function unavailable(reason) {
  const install = 'npm i -D playwright && npx playwright install chromium';
  if (process.env.CI) {
    console.error(`check-responsive.mjs: ${reason}; CI must not report success without rendering the site.`);
    console.error(`Install it with: ${install}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-responsive.mjs: SKIPPED (${reason}; run "${install}")`);
}

// watchStylesheets records failed stylesheet responses for the page. A page
// served without its stylesheets has none of the layout that causes overflow,
// so it looks clean and the check reports a false OK. Watching for failed
// STYLESHEET requests catches that directly: it is how a wrong base path, a
// renamed asset, or a broken build shows up. Other 404s are ignored on purpose
// - versions.json, for one, is produced by the deploy workflow rather than by
// `npm run build`, and its absence locally says nothing about layout.
function watchStylesheets(page) {
  let failures = [];
  page.on('response', (response) => {
    if (response.status() < 400) return;
    const path = new URL(response.url()).pathname;
    if (response.request().resourceType() === 'stylesheet' || path.endsWith('.css')) {
      failures.push(`${response.status()} ${path}`);
    }
  });
  return {
    reset: () => {
      failures = [];
    },
    failures: () => [...new Set(failures)],
  };
}

// writeFixtureDist builds the smallest site that exercises base detection: one
// page whose stylesheet lives under the build's base path.
function writeFixtureDist(base) {
  const root = mkdtempSync(join(tmpdir(), 'ptah-responsive-'));
  mkdirSync(join(root, '_astro'), { recursive: true });
  writeFileSync(join(root, '_astro', 'style.css'), 'main { color: #000; }\n');
  writeFileSync(
    join(root, 'index.html'),
    `<!doctype html><html><head><link rel="stylesheet" href="${base}/_astro/style.css"></head>` +
      '<body><main><p>fits</p></main></body></html>\n',
  );
  return root;
}

async function main() {
  if (process.argv.includes('--selftest')) {
    // The self-test proves both halves of the check fire: the overflow detector
    // on a page built to overflow, and the unstyled-page guard on a site served
    // at the wrong base. The second half is not hypothetical - an early version
    // of this script ignored the base path, rendered every page unstyled, and
    // reported 289 overflow findings that were entirely its own fault.
    const chromium = await loadChromium();
    if (!chromium) {
      unavailable('playwright is not installed');
      return;
    }
    const failures = [];
    let browser;
    try {
      browser = await chromium.launch();
    } catch (error) {
      unavailable(`chromium failed to launch (${error.message.split('\n')[0]})`);
      return;
    }
    try {
      const page = await browser.newPage({ viewport: { width: 390, height: 844 } });
      await page.setContent('<main><div style="width:1200px">wide</div></main>');
      const wide = await page.evaluate(measure, { tolerance: overflowTolerance, cellLineLimit: maxCellLines });
      await page.setContent('<main><div style="width:100px">narrow</div></main>');
      const narrow = await page.evaluate(measure, { tolerance: overflowTolerance, cellLineLimit: maxCellLines });
      if (wide.offenders.length === 0) failures.push('overflow detector did not fire on a 1200px element at 390px');
      if (narrow.offenders.length > 0) failures.push('overflow detector fired on a page that fits');

      // The tall-cell rule, both ways. The fixture reproduces the shape that
      // motivated it: a neighbor cell holding an unbreakable token squeezes a
      // column narrow, and ordinary text in it stacks one word per line.
      // table-layout:fixed is what makes the column widths obeyed; with auto
      // layout the browser widens the column and the fixture proves nothing.
      // The explicit line-height matters too: an unstyled page computes
      // `normal`, which is not a number, and the rule skips cells it cannot
      // measure in lines.
      const row = (cell) =>
        '<main><table style="table-layout:fixed;width:380px;line-height:20px">' +
        `<tbody><tr>${cell}</tr></tbody></table></main>`;
      await page.setContent(
        row(`<td style="width:60px">${'word '.repeat(60)}</td><td style="width:320px">short</td>`),
      );
      const tall = await page.evaluate(measure, { tolerance: overflowTolerance, cellLineLimit: maxCellLines });
      await page.setContent(row('<td style="width:200px">a short cell</td><td>another</td>'));
      const short = await page.evaluate(measure, { tolerance: overflowTolerance, cellLineLimit: maxCellLines });
      if (tall.tallCells.length === 0) {
        failures.push(`tall-cell detector did not fire on a cell far over ${maxCellLines} lines`);
      }
      if (short.tallCells.length > 0) {
        failures.push(`tall-cell detector fired on a one-line cell (${short.tallCells[0]?.lines} lines)`);
      }

      const fixtureBase = '/ptah/edge';
      const distRoot = writeFixtureDist(fixtureBase);
      try {
        const detected = detectBase(distRoot);
        if (detected !== fixtureBase) {
          failures.push(`base detection returned ${JSON.stringify(detected)}, expected ${fixtureBase}`);
        }
        if (htmlRoutes(distRoot).length !== 1) failures.push('route discovery did not find the fixture page');

        const watcher = watchStylesheets(page);

        // Correct base: the stylesheet resolves, so the guard stays quiet.
        const good = await startServer(distRoot, detected);
        watcher.reset();
        await page.goto(`http://127.0.0.1:${good.port}${detected}/`, { waitUntil: 'load' });
        if (watcher.failures().length > 0) {
          failures.push(`unstyled-page guard fired on a correctly served page: ${watcher.failures().join(', ')}`);
        }
        good.server.close();

        // Wrong base, which is the defect this guard exists for: the stylesheet
        // 404s and the page renders unstyled.
        const bad = await startServer(distRoot, '');
        watcher.reset();
        await page.goto(`http://127.0.0.1:${bad.port}/`, { waitUntil: 'load' });
        if (watcher.failures().length === 0) {
          failures.push('unstyled-page guard did not fire when every stylesheet 404s');
        }
        bad.server.close();
      } finally {
        rmSync(distRoot, { recursive: true, force: true });
      }
    } finally {
      await browser.close();
    }

    if (failures.length > 0) {
      console.error('check-responsive.mjs --selftest: FAILED');
      for (const failure of failures) console.error(`- ${failure}`);
      process.exitCode = 1;
      return;
    }
    console.log('check-responsive.mjs --selftest: OK (overflow, tall-cell, and unstyled-page guards verified)');
    return;
  }

  const distRoot = join(siteRoot, argValue('--dist', 'dist'));
  if (!existsSync(distRoot)) {
    console.error(`check-responsive.mjs: ${toPosix(relative(siteRoot, distRoot))} not found; run "npm run build" first.`);
    process.exitCode = 1;
    return;
  }

  const chromium = await loadChromium();
  if (!chromium) {
    unavailable('playwright is not installed');
    return;
  }

  const routes = htmlRoutes(distRoot);
  const base = detectBase(distRoot);
  const { server, port } = await startServer(distRoot, base);
  let browser;
  try {
    browser = await chromium.launch();
  } catch (error) {
    server.close();
    unavailable(`chromium failed to launch (${error.message.split('\n')[0]})`);
    return;
  }
  const errors = [];
  const routesWithTallCells = new Set();

  try {
    for (const viewport of viewports) {
      const context = await browser.newContext({ viewport: { width: viewport.width, height: viewport.height } });
      const page = await context.newPage();
      const stylesheets = watchStylesheets(page);

      for (const route of routes) {
        stylesheets.reset();
        await page.goto(`http://127.0.0.1:${port}${base}${route}`, { waitUntil: 'load' });
        if (stylesheets.failures().length > 0) {
          errors.push(
            `${route} [${viewport.name}]: stylesheet request(s) failed ` +
              `(${stylesheets.failures().slice(0, 3).join(', ')}); the page rendered unstyled, ` +
              'so any overflow result would be meaningless',
          );
          continue;
        }
        const result = await page.evaluate(measure, { tolerance: overflowTolerance, cellLineLimit: maxCellLines });
        if (result.scrollWidth > result.clientWidth + overflowTolerance) {
          errors.push(
            `${route} [${viewport.name} ${viewport.width}px]: page scrolls horizontally ` +
              `(scrollWidth ${result.scrollWidth} > ${result.clientWidth})`,
          );
        }
        for (const offender of result.offenders) {
          errors.push(
            `${route} [${viewport.name} ${viewport.width}px]: <${offender.tag}${offender.className ? ` class="${offender.className}"` : ''}> ` +
              `extends to ${offender.right}px, past the ${result.clientWidth}px viewport`,
          );
        }
        if (viewport.name === 'desktop' && result.tallCells.length > 0) {
          routesWithTallCells.add(route);
          if (!denseTableAllowlist.has(route)) {
            for (const cell of result.tallCells) {
              errors.push(
                `${route}: a table cell renders ${cell.lines} lines tall in a ${cell.width}px column, ` +
                  `over the ${maxCellLines}-line limit — "${cell.text}"`,
              );
            }
          }
        }
      }
      await context.close();
    }
  } finally {
    await browser.close();
    server.close();
  }

  // An allowlist entry that no longer suppresses anything is a claim about the
  // documentation that stopped being true. Failing here is what stops the list
  // from growing into a list of pages nobody checks.
  for (const [route, reason] of denseTableAllowlist) {
    if (!routes.includes(route)) {
      errors.push(`dense-table allowlist names ${route}, which is not a built route; remove the entry`);
      continue;
    }
    if (!routesWithTallCells.has(route)) {
      errors.push(
        `dense-table allowlist still exempts ${route} ("${reason}"), but no cell there exceeds ` +
          `${maxCellLines} lines; remove the entry so the page is checked`,
      );
    }
  }

  if (errors.length > 0) {
    console.error('Documentation responsive check failed:');
    for (const error of errors) console.error(`- ${error}`);
    console.error('\nWide content must scroll inside its own container, not the page.');
    console.error('A cell taller than a short paragraph belongs in a section with a heading.');
    process.exitCode = 1;
    return;
  }
  console.log(
    `check-responsive.mjs: OK (${routes.length} routes x ${viewports.length} viewports, ` +
      `cells within ${maxCellLines} lines)`,
  );
}

await main();
