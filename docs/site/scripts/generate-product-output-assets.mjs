#!/usr/bin/env node
// Regenerate the deterministic Ptah outputs used by visual product pages.
// Every image below is rendered from a committed fixture and a real command;
// volatile file names are copied to stable, versioned sample names.
import { spawnSync } from 'node:child_process';
import {
  copyFileSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { chromium } from 'playwright';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const repositoryRoot = join(siteRoot, '..', '..');
const fixtureRoot = join(siteRoot, 'fixtures', 'product-output');
const vizFixture = join(siteRoot, 'fixtures', 'schema-ui', 'internal', 'models');
const commonSchema = join(siteRoot, 'fixtures', 'source-equivalence', 'schema.sql');
const assetsRoot = join(siteRoot, 'src', 'assets');
const samplesRoot = join(siteRoot, 'public', 'samples');
const vizSamples = join(samplesRoot, 'visualize');
const reportSamples = join(samplesRoot, 'reports');
const contractSamples = join(samplesRoot, 'contracts');
const ptah = process.env.PTAH_BIN || join(repositoryRoot, 'bin', 'ptah');
const viewport = { width: 1200, height: 720 };

function execute(args, cwd, allowedStatuses = [0]) {
  const result = spawnSync(ptah, args, { cwd, encoding: 'utf8' });
  if (!allowedStatuses.includes(result.status)) {
    throw new Error(
      `${ptah} ${args.join(' ')} failed with ${result.status}\n${result.stdout}${result.stderr}`,
    );
  }
  return result;
}

function run(args, cwd) {
  return execute(args, cwd).stdout;
}

// The image that renders every committed SVG, and the one place it is named.
//
// Graphviz draws these files, not Ptah, and their bytes are decided by two
// independent variables: the Graphviz build and the fonts installed beside it.
// Measured at one Graphviz version, the same DOT rendered 928pt wide with no
// font package and 1002pt with font-dejavu present -- so pinning the package is
// not enough and the environment has to be pinned whole (stokaro/ptah#2931).
//
// `docs/site/graphviz/Dockerfile` is that environment, and its base is pinned
// by digest. Measured: two from-scratch builds of it produce byte-identical
// SVGs, and its output differs from the host's -- 1002pt against 895pt on
// Graphviz 14.1.5 -- so the pin decides the answer rather than agreeing with
// whatever is installed.
const GRAPHVIZ_IMAGE = process.env.PTAH_GRAPHVIZ_IMAGE || 'ghcr.io/stokaro/ptah-graphviz:1';

// PTAH_DOCKER_CONTEXT follows check-inference-quick-start.sh, which is the
// precedent for a generator that needs a daemon: a remote context is how this
// runs on a machine with no local Docker.
const DOCKER_CONTEXT = process.env.PTAH_DOCKER_CONTEXT || '';

// renderSVG draws one DOT document in the pinned environment.
//
// `ptah viz --format svg` is exactly `dot -Tsvg` of `ptah viz --format dot` --
// measured byte-identical on the schema-ui fixture -- so this substitutes the
// renderer without changing what Ptah is asked for. The DOT beside each SVG is
// still what Ptah wrote, which is what makes a change to the arguments Ptah
// passes visible in both files rather than in neither.
function renderSVG(dot) {
  const args = DOCKER_CONTEXT ? ['--context', DOCKER_CONTEXT] : [];
  args.push('run', '--rm', '--interactive', GRAPHVIZ_IMAGE, 'dot', '-Tsvg');
  const result = spawnSync('docker', args, { input: dot, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  if (result.error) {
    throw new Error(`docker ${args.join(' ')} could not run: ${result.error.message}`);
  }
  if (result.status !== 0) {
    throw new Error(
      `docker ${args.join(' ')} failed with ${result.status}\n${result.stdout}${result.stderr}`,
    );
  }
  // An empty rendering is a failure this would otherwise write to disk. Graphviz
  // exits 0 for an empty input, so the status alone does not say a diagram was
  // drawn.
  if (!result.stdout.includes('<svg')) {
    throw new Error(`docker ${args.join(' ')} produced no SVG document`);
  }
  return result.stdout;
}

function writeSample(path, value) {
  writeFileSync(path, value.endsWith('\n') ? value : `${value}\n`);
}

// Every HTML report Ptah writes names the binary that produced it, and a binary
// built from a working tree reports a pseudo-version that changes with every
// commit. Pinning it is the same normalization the schema-UI generator applies:
// these fixtures are declared volatileDataNormalized in visual-assets.json, and
// a committed sample that churns on every commit is one nobody can review.
//
// It throws rather than passing a page through unchanged, because a footer this
// cannot find is a footer that moved, and a silent no-op would put the real
// version back into the samples without anything saying so.
function pinReportVersion(path) {
  const original = readFileSync(path, 'utf8');
  const stabilized = original.replace(/(<\/svg>)ptah [^<]+(<\/span>)/, '$1ptah dev$2');
  if (stabilized === original) {
    throw new Error(`${path}: no footer version to normalize; has the report footer changed?`);
  }
  writeFileSync(path, stabilized);
}

async function renderMermaid(browser, source, output, theme, title, description) {
  const page = await browser.newPage({ viewport, colorScheme: theme });
  await page.setContent('<main id="diagram"></main>');
  await page.addScriptTag({ path: join(siteRoot, 'node_modules', 'mermaid', 'dist', 'mermaid.min.js') });
  const svg = await page.evaluate(async ({ diagram, colorScheme, heading, detail }) => {
    window.mermaid.initialize({
      startOnLoad: false,
      securityLevel: 'strict',
      theme: colorScheme === 'dark' ? 'dark' : 'default',
      look: 'classic',
      layout: 'dagre',
      handDrawnSeed: 20260830,
      er: { useMaxWidth: false },
    });
    const rendered = await window.mermaid.render('ptahSchemaDiagram', diagram);
    document.querySelector('#diagram').innerHTML = rendered.svg;
    const root = document.querySelector('#diagram svg');
    root.setAttribute('role', 'img');
    root.setAttribute('aria-labelledby', 'ptah-viz-title ptah-viz-description');
    root.removeAttribute('style');
    root.setAttribute('width', '100%');
    root.setAttribute('height', 'auto');
    const namespace = 'http://www.w3.org/2000/svg';
    const titleNode = document.createElementNS(namespace, 'title');
    titleNode.id = 'ptah-viz-title';
    titleNode.textContent = heading;
    const descriptionNode = document.createElementNS(namespace, 'desc');
    descriptionNode.id = 'ptah-viz-description';
    descriptionNode.textContent = detail;
    root.prepend(descriptionNode);
    root.prepend(titleNode);
    return root.outerHTML;
  }, { diagram: source, colorScheme: theme, heading: title, detail: description });
  writeFileSync(output, `${svg}\n`);
  await page.close();
}

async function screenshotHTML(browser, html, output, height = 620) {
  const page = await browser.newPage({ viewport });
  await page.goto(pathToFileURL(html).href, { waitUntil: 'load' });
  await page.screenshot({
    path: output,
    clip: { x: 0, y: 0, width: viewport.width, height },
  });
  await page.close();
}

mkdirSync(assetsRoot, { recursive: true });
mkdirSync(vizSamples, { recursive: true });
mkdirSync(reportSamples, { recursive: true });
mkdirSync(contractSamples, { recursive: true });

const workRoot = mkdtempSync(join(tmpdir(), 'ptah-product-output-'));
let browser;
try {
  browser = await chromium.launch();

  const relationships = run(['viz', '--root-dir', vizFixture, '--format', 'mermaid'], repositoryRoot);
  const columns = run(['viz', '--root-dir', vizFixture, '--format', 'mermaid', '--include-columns'], repositoryRoot);
  const excluded = run([
    'viz', '--root-dir', vizFixture, '--format', 'mermaid', '--exclude-tables', 'products',
  ], repositoryRoot);
  writeSample(join(vizSamples, 'schema-relationships.mmd'), relationships);
  writeSample(join(vizSamples, 'schema-columns.mmd'), columns);
  writeSample(join(vizSamples, 'schema-excluded.mmd'), excluded);

  await renderMermaid(
    browser,
    relationships,
    join(assetsRoot, 'schema-viz-relationships-light.svg'),
    'light',
    'Ptah schema relationships',
    'Customers place orders, orders contain order items, and order items refer to products.',
  );
  await renderMermaid(
    browser,
    relationships,
    join(assetsRoot, 'schema-viz-relationships-dark.svg'),
    'dark',
    'Ptah schema relationships in dark colors',
    'Customers place orders, orders contain order items, and order items refer to products.',
  );
  await renderMermaid(
    browser,
    columns,
    join(assetsRoot, 'schema-viz-columns.svg'),
    'light',
    'Ptah schema relationships with columns',
    'Four schema tables show their fields, keys, and three foreign-key relationships.',
  );
  await renderMermaid(
    browser,
    excluded,
    join(assetsRoot, 'schema-viz-excluded.svg'),
    'light',
    'Ptah schema after excluding products',
    'The products table is omitted while the remaining relationship-only diagram stays compact.',
  );

  const dot = run(['viz', '--root-dir', vizFixture, '--format', 'dot', '--include-columns'], repositoryRoot);
  writeSample(join(vizSamples, 'schema.dot'), dot);
  const dotSVG = renderSVG(dot);
  writeSample(join(vizSamples, 'schema-dot.svg'), dotSVG);
  const securityDOT = run([
    'viz', '--root-dir', vizFixture, '--format', 'dot', '--include-columns', '--security', '--dialect', 'postgres',
  ], repositoryRoot);
  writeSample(join(vizSamples, 'schema-security.dot'), securityDOT);
  const securityLight = renderSVG(run([
    'viz', '--root-dir', vizFixture, '--format', 'dot', '--include-columns', '--security', '--dialect', 'postgres', '--theme', 'light',
  ], repositoryRoot));
  const securityDark = renderSVG(run([
    'viz', '--root-dir', vizFixture, '--format', 'dot', '--include-columns', '--security', '--dialect', 'postgres', '--theme', 'dark',
  ], repositoryRoot));
  writeSample(join(assetsRoot, 'schema-viz-security-light.svg'), securityLight);
  writeSample(join(assetsRoot, 'schema-viz-security-dark.svg'), securityDark);
  copyFileSync(join(assetsRoot, 'schema-viz-security-light.svg'), join(vizSamples, 'schema-security.svg'));

  const currentDB = join(workRoot, 'current.db');
  run([
    'schema', 'apply', '--schema-file', join(fixtureRoot, 'current.sql'),
    '--db-url', `sqlite://${currentDB}`, '--auto-approve',
  ], workRoot);
  const generatedMigrations = join(workRoot, 'generated-migrations');
  run([
    'migrations', 'generate', '--schema-file', join(fixtureRoot, 'schema.sql'),
    '--db-url', `sqlite://${currentDB}`, '--migrations-dir', generatedMigrations,
    '--name', 'remove_legacy_inventory', '--report', 'html',
  ], workRoot);
  const safetyName = readdirSync(generatedMigrations).find((name) => name.endsWith('.safety.html'));
  if (!safetyName) throw new Error('migrations generate wrote no .safety.html report');
  const safetySample = join(reportSamples, 'migration-safety-report.html');
  copyFileSync(join(generatedMigrations, safetyName), safetySample);
  pinReportVersion(safetySample);
  await screenshotHTML(browser, safetySample, join(assetsRoot, 'migration-safety-report.png'), 560);

  const migrationArgs = [
    'migrations', 'test', '--dir', join(fixtureRoot, 'migration-tests'),
    '--migrations-dir', join(fixtureRoot, 'migrations'), '--report', 'html',
  ];
  const migrationPass = execute([...migrationArgs, '--run', '^products accept a row$'], workRoot);
  const migrationFail = execute([...migrationArgs, '--run', '^empty catalog has two products$'], workRoot, [1]);
  const migrationPassSample = join(reportSamples, 'migration-test-pass.html');
  const migrationFailSample = join(reportSamples, 'migration-test-fail.html');
  writeSample(migrationPassSample, migrationPass.stdout);
  writeSample(migrationFailSample, migrationFail.stdout);
  pinReportVersion(migrationPassSample);
  pinReportVersion(migrationFailSample);
  await screenshotHTML(browser, migrationPassSample, join(assetsRoot, 'migration-test-pass.png'), 500);
  await screenshotHTML(browser, migrationFailSample, join(assetsRoot, 'migration-test-fail.png'), 500);

  const schemaArgs = [
    'schema', 'test', '--dir', join(fixtureRoot, 'schema-tests'),
    '--schema-file', join(fixtureRoot, 'schema.sql'), '--report', 'html',
  ];
  const schemaPass = execute([...schemaArgs, '--run', '^product schema accepts a row$'], workRoot);
  const schemaFail = execute([...schemaArgs, '--run', '^empty product schema has two rows$'], workRoot, [1]);
  const schemaPassSample = join(reportSamples, 'schema-test-pass.html');
  const schemaFailSample = join(reportSamples, 'schema-test-fail.html');
  writeSample(schemaPassSample, schemaPass.stdout);
  writeSample(schemaFailSample, schemaFail.stdout);
  pinReportVersion(schemaPassSample);
  pinReportVersion(schemaFailSample);
  await screenshotHTML(browser, schemaPassSample, join(assetsRoot, 'schema-test-pass.png'), 500);
  await screenshotHTML(browser, schemaFailSample, join(assetsRoot, 'schema-test-fail.png'), 500);

  writeSample(join(samplesRoot, 'schema-lineage.txt'), run([
    'schema', 'lineage', '--schema-file', join(fixtureRoot, 'lineage.sql'), '--dialect', 'sqlite',
  ], workRoot));
  writeSample(join(samplesRoot, 'schema-lineage.json'), run([
    'schema', 'lineage', '--schema-file', join(fixtureRoot, 'lineage.sql'), '--dialect', 'sqlite', '--format', 'json',
  ], workRoot));
  writeSample(join(samplesRoot, 'schema-lineage.dot'), run([
    'schema', 'lineage', '--schema-file', join(fixtureRoot, 'lineage.sql'), '--dialect', 'sqlite', '--format', 'dot',
  ], workRoot));

  const statsDB = join(workRoot, 'stats.db');
  run([
    'schema', 'apply', '--schema-file', commonSchema,
    '--db-url', `sqlite://${statsDB}`, '--auto-approve',
  ], workRoot);
  writeSample(join(samplesRoot, 'schema-stats.openmetrics.txt'), run([
    'schema', 'stats', '--db-url', `sqlite://${statsDB}`,
  ], workRoot).replaceAll(statsDB, '/tmp/ptah-docs-stats.db'));

  writeSample(join(contractSamples, 'shop-openapi.yaml'), run([
    'schema', 'export', '--to', 'openapi-v3', '--schema-file', commonSchema, '--title', 'Library schema',
  ], workRoot));
  writeSample(join(contractSamples, 'shop.graphql'), run([
    'schema', 'export', '--to', 'graphql', '--schema-file', commonSchema,
  ], workRoot));
  const proto = join(workRoot, 'library', 'v1', 'library.proto');
  mkdirSync(dirname(proto), { recursive: true });
  run([
    'schema', 'export', '--to', 'protobuf', '--schema-file', commonSchema,
    '--out', proto, '--proto-package', 'library.v1',
  ], workRoot);
  copyFileSync(proto, join(contractSamples, 'shop.proto'));

  console.log('generate-product-output-assets.mjs: wrote deterministic product outputs');
  for (const file of [
    'schema-viz-relationships-light.svg',
    'schema-viz-relationships-dark.svg',
    'schema-viz-columns.svg',
    'schema-viz-excluded.svg',
    'schema-viz-security-light.svg',
    'schema-viz-security-dark.svg',
    'migration-safety-report.png',
    'migration-test-pass.png',
    'migration-test-fail.png',
    'schema-test-pass.png',
    'schema-test-fail.png',
  ]) console.log(`  ${relative(repositoryRoot, join(assetsRoot, file))}`);
} finally {
  if (browser) await browser.close();
  rmSync(workRoot, { recursive: true, force: true });
}
