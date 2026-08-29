#!/usr/bin/env node
// Keeps the navigation contract explicit: sidebar headings disclose children,
// their first child is the section landing, and the breadcrumb links to that
// child. It also exercises the keyboard path through the page actions.
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { sidebar } from '../src/sidebar.mjs';
import { loadChromium, startBuiltSite } from './lib/built-site.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const inventoryPath = join(siteRoot, 'content-inventory.json');

function routeForSlug(slug) {
  return `/${slug.replace(/^\/+|\/+$/g, '')}/`;
}

function firstDescendantLink(items) {
  for (const item of items) {
    if (typeof item === 'string') return item;
    if (item.slug) return item.slug;
    if (item.items) {
      const nested = firstDescendantLink(item.items);
      if (nested) return nested;
    }
  }
  return undefined;
}

export function navigationModel(groups, pages) {
  const pagesByRoute = new Map(pages.map((page) => [page.route, page]));
  const journeys = [];
  const problems = [];

  for (const group of groups) {
    if (!group.items || group.items.length === 0) {
      problems.push(`${group.label}: top-level entry is not a non-empty disclosure group`);
      continue;
    }
    if ('link' in group || 'slug' in group) {
      problems.push(`${group.label}: group heading must not navigate`);
    }

    const first = group.items[0];
    const landingSlug = typeof first === 'string' ? first : first?.slug;
    if (!landingSlug) {
      problems.push(`${group.label}: first child is not a landing page`);
      continue;
    }
    const landingRoute = routeForSlug(landingSlug);
    const landing = pagesByRoute.get(landingRoute);
    if (!landing || landing.type !== 'landing') {
      problems.push(`${group.label}: first child ${landingRoute} is not typed as landing`);
      continue;
    }

    const destinationSlug = firstDescendantLink(group.items.slice(1));
    if (!destinationSlug) {
      problems.push(`${group.label}: no child page exists to exercise its parent breadcrumb`);
      continue;
    }
    journeys.push({ group: group.label, landingRoute, destinationRoute: routeForSlug(destinationSlug) });
  }

  return { journeys, problems };
}

function selftest() {
  const good = [{ label: 'Good', items: [{ slug: 'good/overview' }, { slug: 'good/task' }] }];
  const pages = [
    { route: '/good/overview/', type: 'landing' },
    { route: '/good/task/', type: 'how-to' },
  ];
  const passing = navigationModel(good, pages);
  const wrongType = navigationModel(good, [{ ...pages[0], type: 'how-to' }, pages[1]]);
  const linkedGroup = navigationModel([{ ...good[0], link: '/good/overview/' }], pages);
  if (
    passing.problems.length !== 0 || passing.journeys.length !== 1 ||
    wrongType.problems.length !== 1 || linkedGroup.problems.length !== 1
  ) {
    console.error('check-navigation.mjs --selftest: FAILED');
    process.exitCode = 1;
    return;
  }
  console.log('check-navigation.mjs --selftest: OK (missing landing and linked group heading rejected)');
}

async function main() {
  if (process.argv.includes('--selftest')) {
    selftest();
    return;
  }

  const inventory = JSON.parse(readFileSync(inventoryPath, 'utf8'));
  const model = navigationModel(sidebar, inventory.pages);
  if (model.problems.length > 0) {
    console.error('check-navigation.mjs: FAILED');
    for (const problem of model.problems) console.error(`- ${problem}`);
    process.exitCode = 1;
    return;
  }

  const distIndex = process.argv.indexOf('--dist');
  const distRoot = join(siteRoot, distIndex === -1 ? 'dist' : process.argv[distIndex + 1]);
  if (!existsSync(distRoot)) {
    console.error(`check-navigation.mjs: ${relative(siteRoot, distRoot)} not found; run "npm run build" first.`);
    process.exitCode = 1;
    return;
  }

  const chromium = await loadChromium('check-navigation.mjs');
  if (!chromium) return;
  const built = await startBuiltSite(distRoot);
  let browser;
  const problems = [];

  try {
    browser = await chromium.launch();
    const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
    await context.grantPermissions(
      ['clipboard-read', 'clipboard-write'],
      { origin: `http://127.0.0.1:${built.port}` },
    );
    const page = await context.newPage();

    for (const journey of model.journeys) {
      await page.goto(`http://127.0.0.1:${built.port}${built.base}${journey.destinationRoute}`, {
        waitUntil: 'load',
      });
      const crumb = page.locator('a.group-crumb', { hasText: journey.group }).first();
      if (await crumb.count() !== 1) {
        problems.push(`${journey.destinationRoute}: ${journey.group} breadcrumb is not a link`);
      } else {
        const href = new URL(await crumb.getAttribute('href'), 'http://ptah.invalid').pathname;
        const expected = `${built.base}${journey.landingRoute}`;
        if (href !== expected) {
          problems.push(`${journey.destinationRoute}: parent links to ${href}, expected ${expected}`);
        } else {
          await crumb.click();
          const arrived = new URL(page.url()).pathname;
          if (arrived !== expected) {
            problems.push(`${journey.destinationRoute}: parent click arrived at ${arrived}, expected ${expected}`);
          }
        }
      }
    }

    await page.goto(`http://127.0.0.1:${built.port}${built.base}/versioned/generate/`, { waitUntil: 'load' });
    const groupSummary = page.locator('nav.sidebar summary', { hasText: 'Versioned migrations' }).first();
    if (await groupSummary.count() !== 1 || await groupSummary.locator('a').count() !== 0) {
      problems.push('/versioned/generate/: section heading is not a disclosure-only summary');
    }

    const copy = page.getByRole('button', { name: 'Copy page as Markdown' });
    const more = page.locator('ptah-page-actions summary');
    await copy.waitFor({ state: 'visible' });
    await page.waitForFunction(() => !document.querySelector('[data-copy-page]')?.disabled);
    await copy.click();
    const copiedMarkdown = await page.evaluate(() => navigator.clipboard.readText());
    if (!copiedMarkdown.startsWith('# Generate migrations\n')) {
      problems.push('/versioned/generate/: Copy page did not put the generated Markdown on the clipboard');
    }
    await more.focus();
    await page.keyboard.press('Enter');
    const actions = await page.locator('.page-actions-menu strong').allTextContents();
    const expectedActions = ['Edit this page', 'View source', 'Report a documentation issue'];
    if (await copy.count() !== 1 || JSON.stringify(actions) !== JSON.stringify(expectedActions)) {
      problems.push(`/versioned/generate/: page action order is Copy, ${actions.join(', ') || '(none)'}`);
    }
    await page.keyboard.press('Escape');
    if (await page.locator('ptah-page-actions details').getAttribute('open') !== null) {
      problems.push('/versioned/generate/: Escape did not close the page actions menu');
    }
    if (!(await more.evaluate((element) => element === document.activeElement))) {
      problems.push('/versioned/generate/: Escape did not restore focus to the menu control');
    }
    if (!(await page.locator('footer').getByText('Last updated:', { exact: false }).count())) {
      problems.push('/versioned/generate/: Git-derived last-updated information is absent');
    }

    await page.goto(`http://127.0.0.1:${built.port}${built.base}/databases/support-matrix/`, { waitUntil: 'load' });
    if (!(await page.getByText('Evidence verified', { exact: false }).count())) {
      problems.push('/databases/support-matrix/: evidence verification date is absent');
    }
  } finally {
    if (browser) await browser.close();
    await new Promise((resolve) => built.server.close(resolve));
  }

  if (problems.length > 0) {
    console.error('check-navigation.mjs: FAILED');
    for (const problem of problems) console.error(`- ${problem}`);
    process.exitCode = 1;
    return;
  }
  console.log(
    `check-navigation.mjs: OK (${model.journeys.length} section landings, linked breadcrumbs, and keyboard page actions)`,
  );
}

await main();
