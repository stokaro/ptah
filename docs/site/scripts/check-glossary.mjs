#!/usr/bin/env node
// Opens every glossary panel on every built page and fails on the two ways the
// affordance breaks for a reader: a panel that appears somewhere other than
// beside the term that opened it, and one whose content is cut off by its own
// border.
//
// Both had shipped. check:responsive measures the CLOSED state -- a trigger is
// a one-character button and overflows nothing -- so neither defect could show
// there, and the component's own doc comment recorded desktop and mobile
// measurements of the open panel taken by hand, once, before the page carried
// thirteen of them.
//
// Measured on the built site before the fix (stokaro/ptah#1454):
//
//   desktop  worst gap to its own term 204px   worst clipped content 117px
//   mobile   worst gap to its own term 483px   worst clipped content 117px
//
// The gap is not cosmetic drift. `anchor-name` is a GLOBAL dashed-ident, so a
// constant one on every use site made `position-anchor` resolve to the last
// element in tree order carrying it: clicking a term in the first row of a
// table opened its panel beside the last row.
//
// Usage:
//   node scripts/check-glossary.mjs [--dist <dir>] [--selftest]
//
// Requires Playwright's chromium, on the same terms as check-responsive.mjs: a
// contributor without it is told what to install and the check skips; in CI it
// fails, because a green check that opened nothing is worse than a red one.
import { createServer } from 'node:http';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, extname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');

const viewports = [
  { name: 'desktop', width: 1280, height: 900 },
  { name: 'mobile', width: 390, height: 844 },
];

// A panel is placed by the browser against its anchor, so the two either touch
// or are separated by the browser's own offset. The tolerance covers that
// offset and sub-pixel rounding; it is far below the 204px a wrong anchor
// produced, so widening it cannot hide the defect this exists for.
const anchorGapTolerance = 24;

// Content wider than the panel's content box by more than a rounding error.
const clipTolerance = 1;

const mimeTypes = {
  '.html': 'text/html', '.css': 'text/css', '.js': 'text/javascript',
  '.json': 'application/json', '.svg': 'image/svg+xml', '.woff2': 'font/woff2',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.webp': 'image/webp', '.ico': 'image/x-icon',
};

function startServer(distRoot) {
  const server = createServer((request, response) => {
    const url = decodeURIComponent((request.url ?? '/').split('?')[0]);
    let filePath = join(distRoot, url);
    if (existsSync(filePath) && statSync(filePath).isDirectory()) filePath = join(filePath, 'index.html');
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

// pagesWithTerms reads the built output rather than a list someone maintains.
// A page that starts using the component is covered the day it does, which a
// hand-written list is exactly the wrong shape for.
function pagesWithTerms(distRoot) {
  const found = [];
  const walk = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) { walk(full); continue; }
      if (entry.name !== 'index.html') continue;
      if (!readFileSync(full, 'utf8').includes('ptah-glossary-trigger')) continue;
      found.push('/' + relative(distRoot, dirname(full)).split(/[\\/]/).join('/') + '/');
    }
  };
  walk(distRoot);
  return found.map((route) => (route === '//' ? '/' : route)).sort();
}

// inspectOpenPanel runs in the page, after one trigger has been clicked. It
// reports the open panel's relationship to ITS OWN anchor and to its own
// content box, which are the two questions a reader's complaint reduces to.
const inspectOpenPanel = (clickedIndex) => {
  const panel = document.querySelector('.ptah-glossary-panel:popover-open');
  if (!panel) return { open: false };

  const trigger = [...document.querySelectorAll('.ptah-glossary-trigger')][clickedIndex];
  const anchor = trigger.closest('.ptah-glossary').getBoundingClientRect();
  const box = panel.getBoundingClientRect();
  const style = getComputedStyle(panel);
  const contentRight = box.right - parseFloat(style.paddingRight) - parseFloat(style.borderRightWidth);

  // Distance from the panel to its own anchor along each axis, zero when the
  // two overlap or touch.
  const gapX = Math.max(0, anchor.left - box.right, box.left - anchor.right);
  const gapY = Math.max(0, anchor.top - box.bottom, box.top - anchor.bottom);

  // Two readings of the same defect. A block child that runs past the content
  // box is the visible half; scrollWidth catches an inline one the child's own
  // rect does not report.
  let widest = 0;
  for (const child of panel.children) {
    widest = Math.max(widest, child.getBoundingClientRect().right - contentRight);
  }

  return {
    open: true,
    gapX: Math.round(gapX),
    gapY: Math.round(gapY),
    clipped: Math.round(Math.max(widest, panel.scrollWidth - panel.clientWidth)),
    term: (panel.querySelector('.ptah-glossary-term') || {}).textContent || '',
  };
};

async function checkPage(page, route, viewport, baseURL) {
  const problems = [];
  await page.setViewportSize({ width: viewport.width, height: viewport.height });
  await page.goto(baseURL + route, { waitUntil: 'networkidle' });

  const count = (await page.$$('.ptah-glossary-trigger')).length;
  for (let index = 0; index < count; index += 1) {
    const triggers = await page.$$('.ptah-glossary-trigger');
    await triggers[index].scrollIntoViewIfNeeded();
    await triggers[index].click();
    // The panel is placed after the click; anchor positioning resolves in the
    // same frame, so one animation frame is enough and a fixed sleep is not.
    await page.evaluate(() => new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r))));

    const result = await page.evaluate(inspectOpenPanel, index);
    await page.keyboard.press('Escape');

    if (!result.open) {
      problems.push(`${route} ${viewport.name}: panel ${index} did not open`);
      continue;
    }
    if (result.gapX > anchorGapTolerance || result.gapY > anchorGapTolerance) {
      problems.push(
        `${route} ${viewport.name}: panel for ${JSON.stringify(result.term)} opened ${result.gapX}px across and ` +
          `${result.gapY}px down from the term that opened it (tolerance ${anchorGapTolerance}px)`,
      );
    }
    if (result.clipped > clipTolerance) {
      problems.push(
        `${route} ${viewport.name}: panel for ${JSON.stringify(result.term)} clips ${result.clipped}px of its own content`,
      );
    }
  }
  return { count, problems };
}

async function loadChromium() {
  try {
    const { chromium } = await import('playwright');
    return chromium;
  } catch {
    return null;
  }
}

function unavailable(reason) {
  const install = 'npm i -D playwright && npx playwright install chromium';
  if (process.env.CI) {
    console.error(`check-glossary.mjs: ${reason}; CI must not report success without opening a panel.`);
    console.error(`Install it with: ${install}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-glossary.mjs: SKIPPED (${reason}; run "${install}")`);
}

// The selftest builds a page carrying both defects and requires the check to
// name them. Without it a check that opened nothing, or measured the closed
// state, would report OK forever -- which is how the defects it exists for
// reached a reader in the first place.
const brokenPage = `<!doctype html><html><head><style>
  .ptah-glossary { anchor-name: --shared; white-space: nowrap; }
  .ptah-glossary-panel {
    position: fixed; inset: auto; position-anchor: --shared;
    position-area: block-end span-inline-end;
    max-width: 20ch; padding: 8px; border: 1px solid #000; white-space: normal;
  }
  .ptah-glossary-source { display: block; }
  p { margin: 120px 0; }
</style></head><body>
  <p><span class="ptah-glossary"><button class="ptah-glossary-trigger" popovertarget="a">i</button></span></p>
  <p><span class="ptah-glossary"><button class="ptah-glossary-trigger" popovertarget="b">i</button></span></p>
  <span popover="auto" id="a" class="ptah-glossary-panel"><span class="ptah-glossary-term">first</span>
    <span class="ptah-glossary-source">core/platform/capability/capability_measured_lines_test.go</span></span>
  <span popover="auto" id="b" class="ptah-glossary-panel"><span class="ptah-glossary-term">second</span>ok</span>
</body></html>`;

async function selftest(chromium) {
  const root = mkdtempSync(join(tmpdir(), 'ptah-glossary-selftest-'));
  try {
    mkdirSync(join(root, 'broken'), { recursive: true });
    writeFileSync(join(root, 'broken', 'index.html'), brokenPage);
    const { server, port } = await startServer(root);
    const browser = await chromium.launch();
    const page = await browser.newPage();
    const { problems } = await checkPage(page, '/broken/', viewports[0], `http://127.0.0.1:${port}`);
    await browser.close();
    server.close();

    const sawGap = problems.some((line) => line.includes('from the term that opened it'));
    const sawClip = problems.some((line) => line.includes('clips'));
    if (!sawGap || !sawClip) {
      console.error('check-glossary.mjs --selftest: FAILED');
      if (!sawGap) console.error('  a shared anchor name was not reported as a misplaced panel');
      if (!sawClip) console.error('  an unbreakable source path was not reported as clipped content');
      for (const line of problems) console.error(`  reported: ${line}`);
      process.exitCode = 1;
      return;
    }
    console.log('check-glossary.mjs --selftest: OK (misplaced-panel and clipped-content guards verified)');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

const args = process.argv.slice(2);
const distIndex = args.indexOf('--dist');
const distRoot = distIndex === -1 ? join(siteRoot, 'dist') : args[distIndex + 1];

const chromium = await loadChromium();
if (!chromium) {
  unavailable('playwright is not installed');
} else if (args.includes('--selftest')) {
  await selftest(chromium);
} else if (!existsSync(distRoot)) {
  console.error('check-glossary.mjs: dist not found; run "npm run build" first.');
  process.exitCode = 1;
} else {
  const routes = pagesWithTerms(distRoot);
  if (routes.length === 0) {
    console.error('check-glossary.mjs: no built page uses the glossary component; the check would measure nothing.');
    process.exitCode = 1;
  } else {
    const { server, port } = await startServer(distRoot);
    let browser;
    try {
      browser = await chromium.launch();
    } catch (error) {
      server.close();
      unavailable(`chromium failed to launch (${error.message.split('\n')[0]})`);
      process.exit(process.exitCode ?? 0);
    }
    const page = await browser.newPage();
    const problems = [];
    let panels = 0;
    for (const route of routes) {
      for (const viewport of viewports) {
        const result = await checkPage(page, route, viewport, `http://127.0.0.1:${port}`);
        panels += result.count;
        problems.push(...result.problems);
      }
    }
    await browser.close();
    server.close();

    if (problems.length > 0) {
      console.error('check-glossary.mjs: FAILED');
      for (const line of problems) console.error(`  ${line}`);
      process.exitCode = 1;
    } else {
      console.log(`check-glossary.mjs: OK (${panels} panel openings across ${routes.length} page(s) x ${viewports.length} viewports)`);
    }
  }
}
