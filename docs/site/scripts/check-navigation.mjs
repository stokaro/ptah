#!/usr/bin/env node
// Keeps the navigation contract explicit: sidebar headings disclose children,
// their first child is the section landing, and the breadcrumb links to that
// child. It also exercises the keyboard path through the page actions.
import { existsSync, readFileSync } from 'node:fs';
import { Origin, PageURL } from '../src/lib/docs-origin.mjs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { sidebar } from '../src/sidebar.mjs';
import { pageActionsForSource, resolveSourceContext } from '../src/lib/source-context.mjs';
import { loadChromium, startBuiltSite } from './lib/built-site.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const inventoryPath = join(siteRoot, 'content-inventory.json');
const inferenceDownloads = [
  {
    fileName: 'inference-quick-start.zip',
    selector: 'a[data-ptah-inference-archive][download]',
    format: 'zip',
  },
  {
    fileName: 'inference-quick-start.zip.sha256',
    selector: 'a[data-ptah-inference-checksum][download]',
    format: 'sha256',
  },
];

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

export function inferenceDownloadProblems({ base, downloads }) {
  const problems = [];
  const normalizedBase = base.endsWith('/') ? base.slice(0, -1) : base;
  for (const expected of inferenceDownloads) {
    const observed = downloads[expected.fileName];
    if (!observed) {
      problems.push(`versioned download ${expected.fileName} is missing`);
      continue;
    }
    const expectedPath = `${normalizedBase}/samples/${expected.fileName}`;
    if (observed.pathname !== expectedPath) {
      problems.push(`${expected.fileName} links to ${observed.pathname}, want ${expectedPath}`);
    }
    if (observed.status !== 200) {
      problems.push(`${expected.fileName} returned HTTP ${observed.status}`);
      continue;
    }
    const body = Buffer.from(observed.body ?? []);
    if (expected.format === 'zip') {
      if (body.length < 4 || !body.subarray(0, 4).equals(Buffer.from([0x50, 0x4b, 0x03, 0x04]))) {
        problems.push(`${expected.fileName} is not a ZIP archive`);
      }
    } else if (!/^[0-9a-f]{64}  inference-quick-start\.zip\r?\n?$/.test(body.toString('utf8'))) {
      problems.push(`${expected.fileName} is not a checksum for inference-quick-start.zip`);
    }
  }
  return problems;
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

  const commit = '0123456789abcdef0123456789abcdef01234567';
  const actionCases = [
    {
      label: 'authored edge',
      version: 'edge',
      generated: false,
      labels: ['Edit this page', 'View source', 'Report a documentation issue'],
    },
    {
      label: 'generated edge',
      version: 'edge',
      generated: true,
      labels: ['Edit generator source', 'View generated source', 'Report a documentation issue'],
    },
    {
      label: 'authored release',
      version: 'v1.2.3',
      generated: false,
      labels: ['View source at v1.2.3', 'Edit latest documentation', 'Report a documentation issue'],
    },
    {
      label: 'generated release',
      version: 'v1.2.3',
      generated: true,
      labels: [
        'View generated source at v1.2.3',
        'Edit generator source in latest documentation',
        'Report a documentation issue',
      ],
    },
  ];
  for (const actionCase of actionCases) {
    const source = resolveSourceContext({
      documentationVersion: actionCase.version,
      sourceCommit: commit,
      renderedSourcePath: 'docs/site/src/content/docs/reference/generated.md',
      generated: actionCase.generated,
      ...(actionCase.generated ? {
        generator: 'internal/cmd/docsync',
        editSource: 'internal/cmdref/markdown.go',
      } : {}),
    });
    const model = pageActionsForSource(source, {
      pageUrl: PageURL(actionCase.version, 'reference/generated/'),
      title: 'Generated reference',
    });
    const labels = model.actions.map(({ label }) => label);
    if (JSON.stringify(labels) !== JSON.stringify(actionCase.labels)) {
      throw new Error(`${actionCase.label} actions are ${labels.join(', ')}`);
    }
    const wantedViewRef = actionCase.version === 'edge' ? commit : actionCase.version;
    if (!model.sourceUrl.includes(`/blob/${wantedViewRef}/`)) {
      throw new Error(`${actionCase.label} source does not use ${wantedViewRef}`);
    }
    const wantedEditPath = actionCase.generated
      ? 'internal/cmdref/markdown.go'
      : 'docs/site/src/content/docs/reference/generated.md';
    if (!model.editUrl.includes(`/edit/master/${wantedEditPath}`)) {
      throw new Error(`${actionCase.label} edit does not use latest ${wantedEditPath}`);
    }
    if (actionCase.generated && model.editUrl.includes('/reference/generated.md')) {
      throw new Error(`${actionCase.label} offers a direct edit of generated Markdown`);
    }
    const reportBody = new URL(model.reportUrl).searchParams.get('body') ?? '';
    for (const expected of [
      `Documentation version: ${actionCase.version}`,
      `Source ref: ${wantedViewRef}`,
      'Rendered source: docs/site/src/content/docs/reference/generated.md',
      ...(actionCase.generated ? [
        'Generator: internal/cmd/docsync',
        'Edit source: internal/cmdref/markdown.go',
      ] : []),
    ]) {
      if (!reportBody.includes(expected)) throw new Error(`${actionCase.label} issue body omitted ${expected}`);
    }
  }
  const downloadBase = '/edge';
  const validDownloads = {
    'inference-quick-start.zip': {
      pathname: `${downloadBase}/samples/inference-quick-start.zip`,
      status: 200,
      body: Buffer.from([0x50, 0x4b, 0x03, 0x04, 0x14, 0x00]),
    },
    'inference-quick-start.zip.sha256': {
      pathname: `${downloadBase}/samples/inference-quick-start.zip.sha256`,
      status: 200,
      body: Buffer.from(`${'a'.repeat(64)}  inference-quick-start.zip\n`),
    },
  };
  if (inferenceDownloadProblems({ base: downloadBase, downloads: validDownloads }).length !== 0) {
    throw new Error('valid inference download contract failed');
  }
  const swappedDownloads = structuredClone(validDownloads);
  [swappedDownloads['inference-quick-start.zip'].pathname, swappedDownloads['inference-quick-start.zip.sha256'].pathname] = [
    swappedDownloads['inference-quick-start.zip.sha256'].pathname,
    swappedDownloads['inference-quick-start.zip'].pathname,
  ];
  if (!inferenceDownloadProblems({ base: downloadBase, downloads: swappedDownloads })
    .some((problem) => problem.includes('links to'))) {
    throw new Error('swapped inference archive/checksum links passed');
  }
  console.log(
    'check-navigation.mjs --selftest: OK (landings, four version-aware actions, and exact inference downloads)',
  );
}

async function checkPageActionContract(page, built, buildInfo, entry, problems, { copyHeading } = {}) {
  const routeUrl = `http://127.0.0.1:${built.port}${built.base}${entry.route}`;
  await page.goto(routeUrl, { waitUntil: 'load' });
  const canonicalPageUrl = `${Origin}${built.base}${entry.route}`;
  const source = resolveSourceContext({
    documentationVersion: buildInfo.documentation_version,
    sourceCommit: buildInfo.source_commit,
    renderedSourcePath: entry.path,
    generated: entry.generated,
    generator: entry.generator,
    editSource: entry.editSource,
  });
  const expected = pageActionsForSource(source, { pageUrl: canonicalPageUrl, title: entry.title });
  const root = page.locator('ptah-page-actions');
  await root.waitFor({ state: 'visible' });

  const attributes = {
    'data-documentation-version': source.documentationVersion,
    'data-view-ref': source.viewRef,
    'data-edit-ref': source.editRef,
    'data-rendered-source': source.renderedSourcePath,
    'data-edit-source': source.editSourcePath,
    'data-generated': String(source.generated),
  };
  for (const [name, value] of Object.entries(attributes)) {
    const actual = await root.getAttribute(name);
    if (actual !== value) problems.push(`${entry.route}: ${name} is ${JSON.stringify(actual)}, want ${JSON.stringify(value)}`);
  }

  const copy = root.getByRole('button', { name: expected.markdownLabel });
  await copy.waitFor({ state: 'visible' });
  await page.waitForFunction(() => !document.querySelector('[data-copy-page]')?.disabled);
  if (copyHeading) {
    await copy.click();
    const copiedMarkdown = await page.evaluate(() => navigator.clipboard.readText());
    if (!copiedMarkdown.startsWith(`# ${copyHeading}\n`)) {
      problems.push(`${entry.route}: Copy page did not put the generated Markdown on the clipboard`);
    }
  }

  // The menu is about the page's Markdown: a copy row and a view link, no
  // more. The source links (edit, source, report) are about the page's source
  // and belong after the article, in the footer, where a reader who has
  // finished looks for them; a copy menu that also opened GitHub was the
  // unrelated location the redesign took them out of (stokaro/ptah#2893).
  const more = root.locator('summary');
  await more.focus();
  await page.keyboard.press('Enter');
  const menu = root.locator('.page-actions-menu');
  if (!(await menu.locator('[data-copy-page-row]').count())) {
    problems.push(`${entry.route}: the copy menu has no Copy as Markdown row`);
  }
  const viewHref = await menu.locator('[data-view-markdown]').getAttribute('href');
  const expectedMarkdownUrl = `${built.base.replace(/\/$/, '')}/page-source/${entry.route.replace(/^\/|\/$/g, '')}.md`;
  if (viewHref !== expectedMarkdownUrl) {
    problems.push(`${entry.route}: View as Markdown links to ${viewHref}, want ${expectedMarkdownUrl}`);
  }
  const menuSourceLinks = await root.locator('[data-page-action]').count();
  if (menuSourceLinks !== 0) {
    problems.push(`${entry.route}: the copy menu carries ${menuSourceLinks} source link(s); they belong in the footer`);
  }

  const footer = page.locator('footer.ptah-footer');
  const footerLinks = {
    edit: footer.locator('[data-footer-action="edit"]'),
    source: footer.locator('.ptah-footer-commit'),
    report: footer.locator('[data-footer-action="report"]'),
  };
  for (const action of expected.actions) {
    const link = footerLinks[action.kind];
    if (!link || (await link.count()) !== 1) {
      problems.push(`${entry.route}: ${action.kind} link is missing from the footer`);
      continue;
    }
    const href = await link.getAttribute('href');
    if (href !== action.url) problems.push(`${entry.route}: footer ${action.kind} links to ${href}, want ${action.url}`);
    if (action.kind === 'edit') {
      const text = (await link.textContent())?.trim() ?? '';
      if (!text.startsWith(action.label)) {
        problems.push(`${entry.route}: footer edit link reads ${JSON.stringify(text)}, want ${JSON.stringify(action.label)}`);
      }
    }
  }

  const reportHref = await footerLinks.report.getAttribute('href');
  const reportBody = reportHref ? new URL(reportHref).searchParams.get('body') ?? '' : '';
  for (const expectedLine of [
    `Page: ${canonicalPageUrl}`,
    `Documentation version: ${source.documentationVersion}`,
    `Source ref: ${source.viewRef}`,
    `Rendered source: ${source.renderedSourcePath}`,
    ...(source.generated ? [
      `Generator: ${source.generator}`,
      `Edit source: ${source.editSourcePath}`,
    ] : []),
  ]) {
    if (!reportBody.includes(expectedLine)) problems.push(`${entry.route}: issue body omitted ${expectedLine}`);
  }
  if (source.generated) {
    const editHref = await footerLinks.edit.getAttribute('href');
    if (editHref?.endsWith(source.renderedSourcePath)) {
      problems.push(`${entry.route}: generated Markdown has a direct edit action`);
    }
  }

  await page.keyboard.press('Escape');
  if (await root.locator('details').getAttribute('open') !== null) {
    problems.push(`${entry.route}: Escape did not close the page actions menu`);
  }
  if (!(await more.evaluate((element) => element === document.activeElement))) {
    problems.push(`${entry.route}: Escape did not restore focus to the menu control`);
  }
}

async function checkVersionedInferenceDownloads(page, built, problems) {
  const route = '/inference/quick-start/';
  await page.goto(`http://127.0.0.1:${built.port}${built.base}${route}`, { waitUntil: 'load' });
  const downloads = {};
  for (const { fileName, selector } of inferenceDownloads) {
    const link = page.locator(selector).first();
    if (await link.count() !== 1) {
      continue;
    }
    const href = await link.evaluate((element) => element.href);
    const response = await page.request.get(href);
    downloads[fileName] = {
      pathname: new URL(href).pathname,
      status: response.status(),
      body: await response.body(),
    };
  }
  for (const problem of inferenceDownloadProblems({ base: built.base, downloads })) {
    problems.push(`${route}: ${problem}`);
  }
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
  const distRoot = distIndex === -1
    ? join(siteRoot, 'dist')
    : resolve(siteRoot, process.argv[distIndex + 1]);
  if (!existsSync(distRoot)) {
    console.error(`check-navigation.mjs: ${relative(siteRoot, distRoot)} not found; run "npm run build" first.`);
    process.exitCode = 1;
    return;
  }
  const buildInfo = JSON.parse(readFileSync(join(distRoot, 'build-info.json'), 'utf8'));

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

    const authored = inventory.pages.find(({ route }) => route === '/versioned/generate/');
    const generated = inventory.pages.find(({ route }) => route === '/reference/command-flags/');
    if (!authored || !generated) {
      problems.push('page-action fixtures are absent from the content inventory');
    } else {
      await checkPageActionContract(page, built, buildInfo, authored, problems, { copyHeading: 'Generate migrations' });
      await checkPageActionContract(page, built, buildInfo, generated, problems);
    }
    await checkVersionedInferenceDownloads(page, built, problems);

    await page.goto(`http://127.0.0.1:${built.port}${built.base}/versioned/generate/`, { waitUntil: 'load' });
    if (!(await page.locator('footer').getByText('Last updated', { exact: false }).count())) {
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
