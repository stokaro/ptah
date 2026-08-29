#!/usr/bin/env node
// Captures reviewable screenshots for the page shapes whose visual content is
// part of the documentation contract. The gate asserts rendering and geometry;
// it does not compare pixels across operating systems, where font rasterization
// would turn a healthy page into a false regression.
import { mkdir, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadChromium, startBuiltSite } from './lib/built-site.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const routes = [
  { route: '/', name: 'home', minimumImages: 1 },
  { route: '/inference/overview/', name: 'inference-overview', minimumImages: 1 },
  { route: '/schema/visualize/', name: 'schema-visualize', minimumImages: 1 },
  { route: '/schema/document/', name: 'schema-document', minimumImages: 1 },
  { route: '/schema/serve/', name: 'schema-serve', minimumImages: 2 },
];
const viewports = [
  { name: 'mobile', width: 390, height: 844 },
  { name: 'desktop', width: 1440, height: 900 },
];

function snapshotName(route, viewport) {
  return `${route.name}-${viewport.name}.png`;
}

function selftest() {
  const routeNames = routes.map(({ name }) => name);
  const files = routes.flatMap((route) => viewports.map((viewport) => snapshotName(route, viewport)));
  if (new Set(routeNames).size !== routeNames.length || new Set(files).size !== files.length) {
    console.error('check-visual-snapshots.mjs --selftest: FAILED (duplicate route or snapshot name)');
    process.exitCode = 1;
    return;
  }
  if (routes.some(({ route, minimumImages }) => !route.startsWith('/') || minimumImages < 1)) {
    console.error('check-visual-snapshots.mjs --selftest: FAILED (invalid route contract)');
    process.exitCode = 1;
    return;
  }
  console.log(`check-visual-snapshots.mjs --selftest: OK (${files.length} unique snapshots)`);
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
    for (const viewport of viewports) {
      const context = await browser.newContext({
        viewport,
        colorScheme: 'light',
        reducedMotion: 'reduce',
      });
      const page = await context.newPage();
      for (const entry of routes) {
        const response = await page.goto(`${origin}${built.base}${entry.route}`, { waitUntil: 'load' });
        if (!response?.ok()) {
          problems.push(`${entry.route} [${viewport.name}] returned ${response?.status() ?? 'no response'}`);
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
          images: [...document.querySelectorAll('main img')].filter((image) => {
            const box = image.getBoundingClientRect();
            return image.complete && image.naturalWidth > 0 && box.width >= 120 && box.height >= 60;
          }).length,
        }));
        if (geometry.documentWidth > geometry.viewportWidth + 1) {
          problems.push(`${entry.route} [${viewport.name}] overflows by ${geometry.documentWidth - geometry.viewportWidth}px`);
        }
        if (geometry.images < entry.minimumImages) {
          problems.push(`${entry.route} [${viewport.name}] rendered ${geometry.images} useful images; want at least ${entry.minimumImages}`);
        }

        const file = snapshotName(entry, viewport);
        await page.screenshot({ path: join(outputRoot, file), fullPage: true });
        manifest.push({
          route: entry.route,
          viewport: viewport.name,
          width: viewport.width,
          height: viewport.height,
          title: geometry.title,
          usefulImages: geometry.images,
          file,
        });
      }
      await context.close();
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
  console.log(`check-visual-snapshots.mjs: OK (${manifest.length} screenshots in ${outputRoot})`);
}

await main();
