#!/usr/bin/env node
// Capture selected page shapes and enforce the manifest-backed visual-proof
// contract. Pixel diffs remain review artifacts; geometry and inspectability
// are deterministic gates.
import { existsSync } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadChromium, startBuiltSite } from './lib/built-site.mjs';
import { readVisualManifests } from './lib/visual-contract.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const { assets: visualManifest } = readVisualManifests(siteRoot);
const routes = visualManifest.snapshotRoutes;
const proofs = visualManifest.proofs.filter(({ enforced }) => enforced);
const viewports = [
  { name: 'mobile', width: 390, height: 844 },
  { name: 'desktop', width: 1440, height: 900 },
];
const themes = ['light', 'dark'];

function snapshotName(route, viewport, theme) {
  return `${route.name}-${viewport.name}-${theme}.png`;
}

function selftest() {
  const routeNames = routes.map(({ name }) => name);
  const files = routes.flatMap((route) => viewports.flatMap((viewport) => themes.map((theme) => snapshotName(route, viewport, theme))));
  if (new Set(routeNames).size !== routeNames.length || new Set(files).size !== files.length) {
    console.error('check-visual-snapshots.mjs --selftest: FAILED (duplicate route or snapshot name)');
    process.exitCode = 1;
    return;
  }
  if (routes.some(({ route }) => !route.startsWith('/')) ||
      proofs.some(({ route, selector }) => !routes.some((entry) => entry.route === route) || !selector)) {
    console.error('check-visual-snapshots.mjs --selftest: FAILED (invalid route or proof contract)');
    process.exitCode = 1;
    return;
  }
  if (routes.some((route) => Object.hasOwn(route, 'minimumImages'))) {
    console.error('check-visual-snapshots.mjs --selftest: FAILED (image counts are not visual proof)');
    process.exitCode = 1;
    return;
  }
  console.log(`check-visual-snapshots.mjs --selftest: OK (${files.length} light/dark desktop/mobile snapshots, no image-count contracts)`);
}

async function actionResolves(page, figure, action) {
  const link = figure.locator(`[data-preview-action="${action}"]`).first();
  if (await link.count() !== 1 || !(await link.isVisible())) return false;
  const href = await link.getAttribute('href');
  if (!href) return false;
  const response = await page.request.get(new URL(href, page.url()).href);
  return response.ok();
}

async function inspectProof(page, proof, viewport, theme) {
  const prefix = `${proof.route} [${viewport.name}/${theme}]`;
  const problems = [];
  const figure = page.locator(proof.selector).first();
  if (await figure.count() !== 1 || !(await figure.isVisible())) return [`${prefix} primary visual is absent or hidden`];

  const result = await figure.evaluate((element, headingText) => {
    const image = element.querySelector('img');
    const box = image?.getBoundingClientRect();
    const caption = element.querySelector('figcaption')?.textContent?.replace(/\s+/g, ' ').trim() ?? '';
    // Measure reader prose, not breadcrumbs, the page title, or page actions.
    // The contract's distance is about how far a reader scans inside the page
    // before meeting proof; Starlight chrome is constant and not authored.
    const main = element.closest('.sl-markdown-content') ?? element.closest('main');
    let words = 0;
    let headingFound = !headingText;
    if (main) {
      const headings = [...main.querySelectorAll('h2, h3')];
      const heading = headingText
        ? headings.reverse().find((candidate) =>
          candidate.textContent?.replace(/\s+/g, ' ').trim() === headingText &&
          Boolean(candidate.compareDocumentPosition(element) & Node.DOCUMENT_POSITION_FOLLOWING))
        : null;
      headingFound = !headingText || Boolean(heading);
      let countWords = !heading;
      const walker = document.createTreeWalker(main, NodeFilter.SHOW_TEXT);
      for (let node = walker.nextNode(); node; node = walker.nextNode()) {
        if (element.contains(node)) break;
        if (heading?.contains(node)) {
          countWords = true;
          continue;
        }
        if (!countWords) continue;
        const parent = node.parentElement;
        if (!parent || parent.closest('[hidden], [aria-hidden="true"]')) continue;
        words += (node.textContent?.trim().match(/\S+/g) ?? []).length;
      }
    }
    return {
      caption,
      complete: image?.complete ?? false,
      naturalWidth: image?.naturalWidth ?? 0,
      naturalHeight: image?.naturalHeight ?? 0,
      width: box?.width ?? 0,
      height: box?.height ?? 0,
      wordsBefore: words,
      headingFound,
    };
  }, proof.headingText ?? null);

  if (!result.complete || result.naturalWidth === 0 || result.naturalHeight === 0) problems.push(`${prefix} primary visual did not load`);
  if (result.width < proof.minimumRenderedWidth || result.height < proof.minimumRenderedHeight) {
    problems.push(`${prefix} primary visual is ${Math.round(result.width)}x${Math.round(result.height)}; want at least ${proof.minimumRenderedWidth}x${proof.minimumRenderedHeight}`);
  }
  if (!result.caption.includes(proof.expectedCaption)) problems.push(`${prefix} expected caption is absent`);
  if (!result.headingFound) problems.push(`${prefix} heading ${JSON.stringify(proof.headingText)} is absent or follows the visual`);
  if (result.wordsBefore > proof.maxVisibleWordsBeforeVisual) problems.push(`${prefix} primary visual starts after ${result.wordsBefore} visible words; maximum is ${proof.maxVisibleWordsBeforeVisual}`);

  for (const [required, action] of [
    [proof.fullSizeAction, 'full-size'],
    [proof.downloadAction, 'download'],
    [proof.sourceAction, 'source'],
  ]) {
    if (required && !await actionResolves(page, figure, action)) problems.push(`${prefix} ${action} action is absent or does not resolve`);
  }
  for (const variant of proof.requiredVariants) {
    if (await figure.locator(`[data-preview-variant="${variant}"]`).count() !== 1) problems.push(`${prefix} variant ${variant} is absent`);
  }

  const fullSize = figure.locator('[data-preview-action="full-size"]').first();
  if (proof.fullSizeAction && await fullSize.count() === 1) {
    await fullSize.focus();
    if (!await fullSize.evaluate((element) => element === document.activeElement)) problems.push(`${prefix} full-size action cannot receive keyboard focus`);
  }
  return problems;
}

async function main() {
  if (process.argv.includes('--selftest')) {
    selftest();
    return;
  }

  const chromium = await loadChromium('check-visual-snapshots.mjs');
  if (!chromium) return;
  const distIndex = process.argv.indexOf('--dist');
  const outputIndex = process.argv.indexOf('--output');
  const distRoot = join(siteRoot, distIndex === -1 ? 'dist' : process.argv[distIndex + 1]);
  const outputRoot = outputIndex === -1
    ? join(tmpdir(), 'ptah-docs-visual-snapshots')
    : resolve(process.cwd(), process.argv[outputIndex + 1]);
  if (!existsSync(distRoot)) {
    console.error(`check-visual-snapshots.mjs: ${relative(siteRoot, distRoot)} not found; run "npm run build" first.`);
    process.exitCode = 1;
    return;
  }

  await mkdir(outputRoot, { recursive: true });
  const built = await startBuiltSite(distRoot);
  const browser = await chromium.launch();
  const origin = `http://127.0.0.1:${built.port}`;
  const manifest = [];
  const problems = [];
  try {
    for (const theme of themes) {
      for (const viewport of viewports) {
        const context = await browser.newContext({ viewport, colorScheme: theme, reducedMotion: 'reduce' });
        const page = await context.newPage();
        for (const entry of routes) {
          const response = await page.goto(`${origin}${built.base}${entry.route}`, { waitUntil: 'load' });
          if (!response?.ok()) {
            problems.push(`${entry.route} [${viewport.name}/${theme}] returned ${response?.status() ?? 'no response'}`);
            continue;
          }
          await page.locator('main img').evaluateAll(async (images) => {
            for (const image of images) image.loading = 'eager';
            await Promise.all(images.map((image) => image.decode().catch(() => undefined)));
          });

          const geometry = await page.evaluate(() => ({
            title: document.title,
            documentWidth: document.documentElement.scrollWidth,
            viewportWidth: document.documentElement.clientWidth,
          }));
          if (geometry.documentWidth > geometry.viewportWidth + 1) {
            problems.push(`${entry.route} [${viewport.name}/${theme}] overflows by ${geometry.documentWidth - geometry.viewportWidth}px`);
          }
          for (const proof of proofs.filter(({ route, themes: proofThemes }) => route === entry.route && proofThemes.includes(theme))) {
            problems.push(...await inspectProof(page, proof, viewport, theme));
          }

          await page.evaluate(() => window.scrollTo(0, 0));

          const file = snapshotName(entry, viewport, theme);
          await page.screenshot({ path: join(outputRoot, file), fullPage: true });
          manifest.push({
            route: entry.route,
            viewport: viewport.name,
            theme,
            width: viewport.width,
            height: viewport.height,
            title: geometry.title,
            proofs: proofs.filter(({ route }) => route === entry.route).map(({ primaryVisualId }) => primaryVisualId),
            file,
          });
        }
        await context.close();
      }
    }
  } finally {
    await browser.close();
    await new Promise((resolve) => built.server.close(resolve));
  }

  await writeFile(join(outputRoot, 'manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`);
  if (problems.length > 0) {
    console.error('check-visual-snapshots.mjs: FAILED');
    for (const problem of problems) console.error(`- ${problem}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-visual-snapshots.mjs: OK (${manifest.length} screenshots and ${proofs.length} manifest-backed proof in ${outputRoot})`);
}

await main();
