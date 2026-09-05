#!/usr/bin/env node
// Regenerates the real product screenshots used by the schema documentation.
// The fixture has no network dependency, personal data, or credentials.
import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const repositoryRoot = join(siteRoot, '..', '..');
const fixtureRoot = join(siteRoot, 'fixtures', 'schema-ui');
const documentSchema = join(siteRoot, 'fixtures', 'source-equivalence', 'schema.sql');
const desiredModels = join(fixtureRoot, 'internal', 'models');
const baseModels = join(fixtureRoot, 'internal', 'base-models');
const assetsRoot = join(siteRoot, 'src', 'assets');
const samplesRoot = join(siteRoot, 'public', 'samples');
const ptah = process.env.PTAH_BIN
  ? resolve(process.env.PTAH_BIN)
  : join(repositoryRoot, 'bin', 'ptah');
const viewport = { width: 1200, height: 760 };

function run(args, cwd) {
  const result = spawnSync(ptah, args, { cwd, encoding: 'utf8' });
  if (result.status !== 0) {
    throw new Error(
      `${ptah} ${args.join(' ')} failed with ${result.status}\n${result.stdout}${result.stderr}`,
    );
  }
  return result.stdout;
}

function startServer(databaseURL, cwd) {
  const child = spawn(
    ptah,
    [
      'schema', 'serve',
      '--root-dir', desiredModels,
      '--db-url', databaseURL,
      '--addr', '127.0.0.1:0',
      '--refresh', '0',
      '--title', 'Shop schema',
    ],
    { cwd, stdio: ['ignore', 'pipe', 'pipe'] },
  );

  let stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (chunk) => { stderr += chunk; });

  const address = new Promise((resolve, reject) => {
    let stdout = '';
    const timer = setTimeout(() => reject(new Error('schema serve did not print its address')), 10_000);
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk) => {
      stdout += chunk;
      const match = stdout.match(/Serving a read-only schema view on (http:\/\/[^\s]+)/);
      if (!match) return;
      clearTimeout(timer);
      resolve(match[1]);
    });
    child.once('exit', (code) => {
      clearTimeout(timer);
      reject(new Error(`schema serve exited ${code} before listening\n${stdout}${stderr}`));
    });
  });

  return { address, child, stderr: () => stderr };
}

async function stopServer(server) {
  server.child.kill('SIGINT');
  const code = await new Promise((resolve) => server.child.once('exit', resolve));
  if (code !== 130) throw new Error(`schema serve exited ${code}; expected 130\n${server.stderr()}`);
}

// The exported document's footer names the binary that wrote it, and a binary
// built from a working tree reports a pseudo-version that changes with every
// commit. Pinning it here is the same normalization the dashboard's timestamps
// and database path already get: the fixture is declared volatileDataNormalized
// in visual-assets.json, and a committed sample that churns on every commit is
// a sample nobody can review.
function stabilizeDocument(path) {
  const original = readFileSync(path, 'utf8');
  const stabilized = original.replace(/(<\/svg>)ptah [^<]+(<\/span>)/, '$1ptah dev$2');
  if (stabilized === original) {
    throw new Error(`${path}: no footer version to normalize; has the footer changed?`);
  }
  writeFileSync(path, stabilized);
}

async function stabilizeDashboard(page) {
  await page.evaluate(() => {
    for (const element of document.querySelectorAll('time')) {
      element.textContent = 'Aug 30, 2026, 12:00 UTC';
      element.setAttribute('datetime', '2026-08-30T12:00:00Z');
    }
    for (const element of document.querySelectorAll('*')) {
      if (element.children.length > 0) continue;
      if (element.textContent?.includes('sqlite:///')) {
        element.textContent = element.textContent.replace(/sqlite:\/\/\/[^\s<]+/, 'sqlite:///tmp/shop.db');
      }
      if (element.textContent?.startsWith('compared ')) {
        element.textContent = 'compared 2026-08-30T12:00:00Z';
      }
    }
  });
}

async function screenshotDashboard(browser, databaseURL, output, fullOutput, sampleOutput, cwd) {
  const server = startServer(databaseURL, cwd);
  try {
    const address = await server.address;
    const page = await browser.newPage({ viewport });
    await page.goto(address, { waitUntil: 'networkidle' });
    await stabilizeDashboard(page);
    await page.screenshot({
      path: output,
      clip: { x: 0, y: 0, width: viewport.width, height: 620 },
    });
    await page.screenshot({ path: fullOutput, fullPage: true });
    writeFileSync(sampleOutput, `${await page.content()}\n`);
    await page.close();
  } finally {
    await stopServer(server);
  }
}

const workRoot = mkdtempSync(join(tmpdir(), 'ptah-schema-ui-'));
mkdirSync(assetsRoot, { recursive: true });
mkdirSync(samplesRoot, { recursive: true });

let browser;
try {
  const sample = join(samplesRoot, 'schema-document.html');
  run(['schema', 'export', '--to', 'html', '--schema-file', documentSchema, '--out', sample], workRoot);
  stabilizeDocument(sample);

  browser = await chromium.launch();
  const documentPage = await browser.newPage({ viewport });
  await documentPage.goto(pathToFileURL(sample).href, { waitUntil: 'load' });
  await documentPage.screenshot({
    path: join(assetsRoot, 'schema-document.png'),
    clip: { x: 0, y: 0, width: viewport.width, height: 620 },
  });
  await documentPage.locator('#books').screenshot({ path: join(assetsRoot, 'schema-document-table.png') });
  await documentPage.screenshot({ path: join(assetsRoot, 'schema-document-full.png'), fullPage: true });
  await documentPage.close();

  const matchingDB = join(workRoot, 'matching.db');
  const driftDB = join(workRoot, 'drift.db');
  run(['schema', 'apply', '--root-dir', desiredModels, '--db-url', `sqlite://${matchingDB}`, '--auto-approve'], workRoot);
  run(['schema', 'apply', '--root-dir', baseModels, '--db-url', `sqlite://${driftDB}`, '--auto-approve'], workRoot);

  await screenshotDashboard(
    browser,
    `sqlite://${matchingDB}`,
    join(assetsRoot, 'schema-serve-matches.png'),
    join(assetsRoot, 'schema-serve-matches-full.png'),
    join(samplesRoot, 'schema-serve-matches.html'),
    workRoot,
  );
  await screenshotDashboard(
    browser,
    `sqlite://${driftDB}`,
    join(assetsRoot, 'schema-serve-drift.png'),
    join(assetsRoot, 'schema-serve-drift-full.png'),
    join(samplesRoot, 'schema-serve-drift.html'),
    workRoot,
  );

  console.log('generate-schema-ui-assets.mjs: wrote');
  for (const file of [
    join(assetsRoot, 'schema-document.png'),
    join(assetsRoot, 'schema-document-table.png'),
    join(assetsRoot, 'schema-document-full.png'),
    join(assetsRoot, 'schema-serve-matches.png'),
    join(assetsRoot, 'schema-serve-matches-full.png'),
    join(assetsRoot, 'schema-serve-drift.png'),
    join(assetsRoot, 'schema-serve-drift-full.png'),
    join(samplesRoot, 'schema-serve-matches.html'),
    join(samplesRoot, 'schema-serve-drift.html'),
    sample,
  ]) {
    console.log(`  ${relative(repositoryRoot, file)}`);
  }
} finally {
  if (browser) await browser.close();
  rmSync(workRoot, { recursive: true, force: true });
}
