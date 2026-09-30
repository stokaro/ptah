#!/usr/bin/env node
// Exercise the reader's full-size actions, including a very tall image, on the
// built site. Screenshots are for review; interaction and geometry are gates.
import { existsSync, readFileSync, mkdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { detectBase, loadChromium, startBuiltSite } from './lib/built-site.mjs';

const siteRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const assert = (condition, message) => { if (!condition) throw new Error(message); };
const viewports = [{ name: 'mobile', width: 390, height: 844 }, { name: 'desktop', width: 1280, height: 900 }];

export function previewProblems(reading, originalUrl) {
  const problems = [];
  if (!reading.open) problems.push('preview is closed');
  if (reading.url !== originalUrl) problems.push('preview left the documentation');
  if (!reading.focusInside) problems.push('focus escaped the modal');
  if (reading.dialog.x < -1 || reading.dialog.y < -1 || reading.dialog.right > reading.screen.width + 1 || reading.dialog.bottom > reading.screen.height + 1) problems.push('dialog exceeds the viewport');
  if (reading.viewport.width < 200 || reading.viewport.height < 200) problems.push('image viewport is too small');
  if (!reading.naturalWidth || !reading.naturalHeight || !reading.image.width || !reading.image.height) problems.push('image is absent');
  if (reading.image.width > reading.viewport.width + 1 || reading.image.height > reading.viewport.height + 1) problems.push('fit image clips the overview');
  if (reading.overflow !== 'hidden') problems.push('background scrolling is unlocked');
  return problems;
}

export function graphicPreviewSelftest() {
  const valid = {
    open: true, url: 'https://example.test/edge/', focusInside: true, overflow: 'hidden',
    screen: { width: 390, height: 844 }, dialog: { x: 0, y: 0, right: 390, bottom: 844 },
    viewport: { width: 360, height: 600 }, image: { width: 50, height: 580 }, naturalWidth: 1200, naturalHeight: 12000,
  };
  assert(previewProblems(valid, valid.url).length === 0, 'a usable image preview was rejected');
  const defects = [
    { open: false }, { url: 'https://example.test/image.svg' }, { focusInside: false }, { overflow: '' },
    { dialog: { ...valid.dialog, x: -10 } }, { dialog: { ...valid.dialog, bottom: 1000 } },
    { viewport: { ...valid.viewport, height: 100 } }, { naturalWidth: 0 }, { naturalHeight: 0 },
    { image: { ...valid.image, width: 500 } }, { image: { ...valid.image, height: 10000 } },
  ];
  for (const defect of defects) assert(previewProblems({ ...valid, ...defect }, valid.url).length > 0, `missed ${JSON.stringify(defect)}`);
  console.log(`check-graphic-preview: self-test passed (${defects.length} defects rejected)`);
}

async function reading(page) {
  return page.evaluate(() => {
    const dialog = document.querySelector('ptah-graphic-preview dialog');
    const viewport = dialog.querySelector('[data-preview-viewport]');
    const image = dialog.querySelector('[data-preview-image]');
    return {
      open: dialog.open, url: location.href, focusInside: dialog.contains(document.activeElement),
      screen: { width: innerWidth, height: innerHeight }, dialog: dialog.getBoundingClientRect().toJSON(),
      viewport: viewport.getBoundingClientRect().toJSON(), image: image.getBoundingClientRect().toJSON(),
      naturalWidth: image.naturalWidth, naturalHeight: image.naturalHeight, source: image.src,
      overflow: document.documentElement.style.overflow,
    };
  });
}

async function openImage(page, link) {
  await link.scrollIntoViewIfNeeded();
  const position = await page.evaluate(() => scrollY);
  const originalUrl = page.url();
  await link.focus();
  await page.keyboard.press('Enter');
  await page.locator('ptah-graphic-preview [data-preview-image]').waitFor({ state: 'visible' });
  const value = await reading(page);
  assert(previewProblems(value, originalUrl).length === 0, previewProblems(value, originalUrl).join('; '));
  return { originalUrl, position, value };
}

async function closeImage(page, link, opened) {
  await page.keyboard.press('Escape');
  await page.locator('ptah-graphic-preview dialog').waitFor({ state: 'hidden' });
  assert(page.url() === opened.originalUrl, 'closing the preview changed the article');
  assert(await link.evaluate((element) => element === document.activeElement), 'closing the preview lost the opener focus');
  assert(Math.abs(await page.evaluate(() => scrollY) - opened.position) <= 1, 'closing the preview lost the reading position');
}

export async function checkGraphicPreviews({ dist = join(siteRoot, 'dist'), output = join(tmpdir(), 'ptah-docs-graphic-previews') } = {}) {
  const { default: AxeBuilder } = await import('@axe-core/playwright');
  assert(existsSync(join(dist, 'index.html')), 'run npm run build first');
  mkdirSync(output, { recursive: true });
  const base = detectBase(dist);
  const fixtureRoute = `${base}/graphic-preview-fixture/`;
  const tall = `${fixtureRoute}tall.svg`;
  const thumbnail = `${fixtureRoute}thumbnail.svg`;
  const thumbnailSvg = '<svg xmlns="http://www.w3.org/2000/svg" width="120" height="120"><rect width="120" height="120" fill="white"/><text x="60" y="60" text-anchor="middle">Thumbnail</text></svg>';
  const tallSvg = '<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="12000" viewBox="0 0 1200 12000"><title>A long diagram from Start to End</title><rect width="1200" height="12000" fill="white"/><path d="M600 80V11920" stroke="#111417" stroke-width="8"/><text x="600" y="50" text-anchor="middle" font-size="30">Start</text><text x="600" y="11980" text-anchor="middle" font-size="30">End</text></svg>';
  const fixture = `<figure class="ptah-product-preview" data-product-preview data-visual-id="tall-fixture"><div class="ptah-product-preview__bar"><a href="${tall}" data-preview-action="full-size">Full size</a></div><a class="ptah-product-preview__media" href="${tall}"><img src="${thumbnail}" alt="A long diagram from Start to End" width="120" height="120"></a><figcaption><p>Long image fixture</p></figcaption></figure><figure data-product-preview data-visual-id="missing-fixture"><a data-preview-action="full-size" href="${fixtureRoute}missing.svg">Missing image</a><img src="${thumbnail}" alt="A missing full-size image"></figure><figure data-product-preview data-visual-id="missing-report"><a data-preview-action="full-size" href="${fixtureRoute}missing.html">Missing report</a><img src="${thumbnail}" alt="A missing report"></figure>`;
  const home = readFileSync(join(dist, 'index.html'), 'utf8');
  const fixtureHtml = home.replace(/(<div\b[^>]*class="[^"]*\bsl-markdown-content\b[^"]*"[^>]*>)/, `$1${fixture}`);
  assert(fixtureHtml !== home, 'fixture could not find the article');
  const built = await startBuiltSite(dist, base, (path) => {
    if (path === fixtureRoute) return { status: 200, body: fixtureHtml, type: 'text/html' };
    if (path === tall) return { status: 200, body: tallSvg, type: 'image/svg+xml' };
    if (path === thumbnail) return { status: 200, body: thumbnailSvg, type: 'image/svg+xml' };
    if (path === '/versions.json') return { status: 200, body: JSON.stringify({ default: base.slice(1), versions: [{ slug: base.slice(1), label: base.slice(1) }] }), type: 'application/json' };
    if (path === '/version-picker.js' || path === '/version-picker.css') return { status: 200, body: readFileSync(join(siteRoot, 'public', path.slice(1))), type: path.endsWith('.js') ? 'text/javascript' : 'text/css' };
    return undefined;
  });
  const chromium = await loadChromium('check-graphic-preview.mjs');
  if (!chromium) { await new Promise((done) => built.server.close(done)); return; }
  const browser = await chromium.launch();
  const origin = `http://127.0.0.1:${built.port}`;
  let checks = 0;
  try {
    for (const theme of ['light', 'dark']) {
      for (const viewport of viewports) {
        const context = await browser.newContext({ viewport, colorScheme: theme, reducedMotion: 'reduce', isMobile: viewport.name === 'mobile', hasTouch: viewport.name === 'mobile' });
        const page = await context.newPage();
        const errors = [];
        page.on('pageerror', (error) => errors.push(error.message));
        await page.goto(`${origin}${fixtureRoute}`, { waitUntil: 'networkidle' });
        assert(await page.locator('[data-preview-image]').getAttribute('src') === null, 'the full image was loaded before opening the preview');
        const link = page.locator('[data-visual-id="tall-fixture"] [data-preview-action="full-size"]');
        const opened = await openImage(page, link);
        assert(opened.value.naturalHeight === 12000, 'the viewer uses the thumbnail instead of the original');
        for (let tab = 0; tab < 12; tab += 1) {
          await page.keyboard.press('Tab');
          assert(await page.locator('ptah-graphic-preview dialog').evaluate((dialog) => dialog.contains(document.activeElement)), 'Tab escaped the modal');
        }
        const axe = await new AxeBuilder({ page }).include('ptah-graphic-preview').withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa']).analyze();
        assert(axe.violations.length === 0, `preview accessibility: ${axe.violations.map((violation) => violation.id)}`);
        const pane = page.locator('[data-preview-viewport]');
        const image = page.locator('[data-preview-image]');
        await page.locator('[data-preview-control="width"]').click();
        await page.waitForFunction(() => {
          const pane = document.querySelector('[data-preview-viewport]');
          const image = document.querySelector('[data-preview-image]');
          return Math.abs(image.getBoundingClientRect().width - (pane.clientWidth - 32)) < 2 && pane.scrollHeight > pane.clientHeight * 5;
        });
        await pane.focus();
        await page.keyboard.press('End');
        await page.waitForFunction(() => {
          const pane = document.querySelector('[data-preview-viewport]');
          return pane.scrollTop + pane.clientHeight >= pane.scrollHeight - 2;
        });
        assert(await image.evaluate((image) => image.getBoundingClientRect().bottom <= image.closest('[data-preview-viewport]').getBoundingClientRect().bottom), 'the bottom of the long image is unreachable');
        await page.screenshot({ path: join(output, `long-image-bottom-${viewport.name}-${theme}.png`) });
        await page.locator('[data-preview-control="actual"]').click();
        assert(Math.abs((await image.boundingBox()).width - opened.value.naturalWidth) <= 1, '100% is not the original pixel width');
        await pane.evaluate((element) => element.scrollTo(0, element.scrollHeight / 3));
        const beforeDrag = await pane.evaluate((element) => element.scrollTop);
        const box = await pane.boundingBox();
        await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
        await page.mouse.down();
        await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2 - 80, { steps: 5 });
        await page.mouse.up();
        assert(await pane.evaluate((element) => element.scrollTop) >= beforeDrag + 70, 'dragging did not pan the long image');
        const beforeZoom = (await image.boundingBox()).width;
        await page.locator('[data-preview-control="in"]').click();
        assert((await image.boundingBox()).width > beforeZoom * 1.2, 'Zoom in did not enlarge the image');
        await page.locator('[data-preview-control="out"]').click();
        assert(Math.abs((await image.boundingBox()).width - beforeZoom) < 2, 'Zoom out did not restore the scale');
        if (viewport.name === 'mobile') {
          const session = await context.newCDPSession(page);
          const center = { x: box.x + box.width / 2, y: box.y + box.height / 2 };
          await session.send('Input.dispatchTouchEvent', { type: 'touchStart', touchPoints: [{ id: 1, x: center.x - 30, y: center.y }, { id: 2, x: center.x + 30, y: center.y }] });
          await session.send('Input.dispatchTouchEvent', { type: 'touchMove', touchPoints: [{ id: 1, x: center.x - 90, y: center.y }, { id: 2, x: center.x + 90, y: center.y }] });
          await session.send('Input.dispatchTouchEvent', { type: 'touchEnd', touchPoints: [] });
          assert((await image.boundingBox()).width > beforeZoom * 1.5, 'pinching did not enlarge the image');
          assert(await page.evaluate(() => visualViewport.scale) === 1, 'pinching zoomed the whole documentation');
          await session.detach();
        }
        await page.screenshot({ path: join(output, `long-image-detail-${viewport.name}-${theme}.png`) });
        await closeImage(page, link, opened);
        for (const fixture of ['missing-fixture', 'missing-report']) {
          const missing = page.locator(`[data-visual-id="${fixture}"] [data-preview-action="full-size"]`);
          await missing.click();
          await page.locator('[data-preview-error]').waitFor({ state: 'visible' });
          assert(page.url() === opened.originalUrl, 'a failed preview left the article');
          await page.locator('[data-preview-control="close"]').click();
        }
        checks += 1;

        for (const route of ['schema/visualize/', 'schema/document/', 'schema/serve/', 'operate/deliver/']) {
          await page.goto(`${origin}${base}/${route}`, { waitUntil: 'networkidle' });
          const trigger = route === 'operate/deliver/'
            ? page.locator('.sl-markdown-content [data-preview-inline-image]').first()
            : page.locator('[data-product-preview] [data-preview-action="full-size"]').first();
          const expectedSource = route === 'schema/visualize/' ? await page.locator('[data-product-preview] img').first().evaluate((image) => image.currentSrc) : null;
          const real = await openImage(page, trigger);
          if (expectedSource) assert(real.value.source === expectedSource, `the full-size diagram lost its displayed theme: ${real.value.source}, expected ${expectedSource}`);
          await page.screenshot({ path: join(output, `${route.split('/')[0]}-${route.split('/')[1]}-${viewport.name}-${theme}.png`) });
          if (route === 'schema/visualize/') {
            const svg = await (await page.request.get(real.value.source)).text();
            const viewBoxWidth = Number(svg.match(/viewBox="([^"]+)"/)[1].split(/\s+/)[2]);
            await page.locator('[data-preview-control="actual"]').click();
            assert(Math.abs((await image.boundingBox()).width - viewBoxWidth) < 1, '100% uses a small browser fallback instead of responsive SVG coordinates');
          }
          if (route === 'schema/document/') {
            assert(real.value.source === new URL(await trigger.getAttribute('href'), page.url()).href, 'the full-page screenshot uses its cropped thumbnail');
            await page.locator('[data-preview-control="actual"]').click();
            assert(Math.abs((await image.boundingBox()).width - real.value.naturalWidth) <= 1, 'the full-page screenshot cannot be read at its original size');
            await pane.evaluate((element) => element.scrollTo(element.scrollWidth / 3, element.scrollHeight / 3));
            await page.screenshot({ path: join(output, `schema-document-detail-${viewport.name}-${theme}.png`) });
          }
          await closeImage(page, trigger, real);
          if (route === 'schema/serve/') {
            const variant = page.locator('[data-preview-variant="drift"] a[data-graphic-preview]').first();
            const variantUrl = new URL(await variant.getAttribute('href'), page.url()).href;
            const variantOpen = await openImage(page, variant);
            assert(variantOpen.value.source === variantUrl, 'a variant opened the primary image');
            await closeImage(page, variant, variantOpen);
          }
          checks += 1;
        }
        await page.goto(`${origin}${base}/versioned/generate/`, { waitUntil: 'networkidle' });
        const report = page.locator('[data-visual-id="migration-safety-report"] [data-preview-action="full-size"]');
        await report.click();
        await page.frameLocator('[data-preview-document]').locator('h1').waitFor();
        assert(page.url().includes('/versioned/generate/'), 'the HTML report left the documentation');
        assert((await page.locator('[data-preview-document]').boundingBox()).height > viewport.height / 2, 'the HTML report viewport collapsed');
        await page.frameLocator('[data-preview-document]').locator('body').click({ position: { x: 15, y: 15 } });
        await page.keyboard.press('Escape');
        await page.locator('ptah-graphic-preview dialog').waitFor({ state: 'hidden' });
        assert(await report.evaluate((element) => element === document.activeElement), 'Escape inside the HTML report lost the opener');
        assert(errors.length === 0, `browser errors: ${errors.join('; ')}`);
        await context.close();
        checks += 1;
      }
    }
    const context = await browser.newContext({ javaScriptEnabled: false });
    const page = await context.newPage();
    await page.goto(`${origin}${base}/schema/visualize/`);
    const link = page.locator('[data-preview-action="full-size"]').first();
    const href = new URL(await link.getAttribute('href'), page.url()).href;
    await link.click();
    await page.waitForURL(href);
    assert(page.url() === href, 'the no-script full-size link is broken');
    await context.close();
    console.log(`check-graphic-preview: passed (${checks} light/dark mobile/desktop cases, long-image details, pan, pinch, keyboard, failures, variants, HTML reports, and no-script fallback)`);
  } finally {
    await browser.close();
    await new Promise((done) => built.server.close(done));
  }
}

async function main() {
  if (process.argv.includes('--selftest')) { graphicPreviewSelftest(); return; }
  const distIndex = process.argv.indexOf('--dist');
  const outputIndex = process.argv.indexOf('--output');
  await checkGraphicPreviews({
    dist: resolve(siteRoot, distIndex < 0 ? 'dist' : process.argv[distIndex + 1]),
    output: outputIndex < 0 ? undefined : resolve(process.argv[outputIndex + 1]),
  });
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error) => { console.error(`check-graphic-preview: ${error.stack}`); process.exitCode = 1; });
}
