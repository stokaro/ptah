#!/usr/bin/env node
// Enforces the visual source policy: authored diagrams are semantic SVG,
// raster assets are real product screenshots with a deterministic generator,
// and pages provide useful alternatives.
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const repositoryRoot = join(siteRoot, '..', '..');
const screenshotFiles = new Set([
  'schema-document.png',
  'schema-serve-drift.png',
  'schema-serve-matches.png',
]);
const authoredSVGs = new Set([
  'inference-generation-lifecycle.svg',
  'product-journeys.svg',
]);
const screenshotLimit = 250 * 1024;

function filesBelow(root, extensions) {
  const files = [];
  const walk = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.isFile() && extensions.has(extname(entry.name))) files.push(full);
    }
  };
  walk(root);
  return files.sort();
}

export function analyze({ assetsRoot, contentRoot, samplePath, sourcePath }) {
  const problems = [];
  const assets = filesBelow(assetsRoot, new Set(['.md', '.png', '.svg']));
  const diagrams = new Map();

  for (const file of assets) {
    const name = basename(file);
    const source = readFileSync(file, extname(file) === '.png' ? undefined : 'utf8');
    if (name.endsWith('.source.md') && source.includes('OpenAI image generation')) {
      problems.push(`${name}: raster image-generation prompt is not an editable semantic source`);
    }
    if (extname(file) === '.png') {
      if (!screenshotFiles.has(name)) problems.push(`${name}: PNG is not an approved real-UI screenshot`);
      if (statSync(file).size > screenshotLimit) problems.push(`${name}: screenshot exceeds ${screenshotLimit} bytes`);
    }
    if (authoredSVGs.has(name)) {
      if (!source.includes('<title')) problems.push(`${name}: authored SVG has no title`);
      if (!source.includes('<desc')) problems.push(`${name}: authored SVG has no description`);
      diagrams.set(name, false);
    }
  }

  for (const page of filesBelow(contentRoot, new Set(['.md', '.mdx']))) {
    const source = readFileSync(page, 'utf8');
    for (const match of source.matchAll(/!\[([^\]]*)\]\(([^)]+\.(?:png|svg))\)/g)) {
      const alt = match[1].trim();
      if (alt.length < 12) problems.push(`${basename(page)}: image alternative is empty or too vague: ${JSON.stringify(alt)}`);
      const name = basename(match[2]);
      if (diagrams.has(name)) diagrams.set(name, true);
    }
  }

  for (const [name, used] of diagrams) {
    if (!used) problems.push(`${name}: authored diagram is not used by a reader page`);
  }

  const source = readFileSync(sourcePath, 'utf8');
  for (const name of screenshotFiles) {
    if (!source.includes(name)) problems.push(`${basename(sourcePath)}: does not name ${name}`);
  }
  const sample = readFileSync(samplePath, 'utf8');
  for (const fragment of ['Schema reference', 'customers', 'orders', 'Entity relationship diagram']) {
    if (!sample.includes(fragment)) problems.push(`${basename(samplePath)}: missing ${JSON.stringify(fragment)}`);
  }

  return problems;
}

function selftest() {
  const root = mkdtempSync(join(tmpdir(), 'ptah-visual-assets-'));
  try {
    const assets = join(root, 'assets');
    const content = join(root, 'content');
    mkdirSync(assets);
    mkdirSync(content);
    for (const name of screenshotFiles) writeFileSync(join(assets, name), Buffer.from('png'));
    writeFileSync(join(assets, 'product-journeys.svg'), '<svg><title>title</title></svg>');
    writeFileSync(join(assets, 'inference-generation-lifecycle.svg'), '<svg><title>title</title><desc>desc</desc></svg>');
    writeFileSync(join(assets, 'bad.source.md'), 'Tool: OpenAI image generation');
    writeFileSync(
      join(content, 'page.md'),
      '![A useful product journey alternative](../assets/product-journeys.svg)\n' +
        '![A useful inference lifecycle alternative](../assets/inference-generation-lifecycle.svg)\n',
    );
    const sourcePath = join(assets, 'schema-ui.source.md');
    writeFileSync(sourcePath, [...screenshotFiles].join('\n'));
    const samplePath = join(root, 'sample.html');
    writeFileSync(samplePath, 'Schema reference customers orders Entity relationship diagram');
    const problems = analyze({ assetsRoot: assets, contentRoot: content, samplePath, sourcePath });
    if (!problems.some((problem) => problem.includes('no description')) ||
        !problems.some((problem) => problem.includes('image-generation prompt'))) {
      console.error('check-visual-assets.mjs --selftest: FAILED');
      process.exitCode = 1;
      return;
    }
    console.log('check-visual-assets.mjs --selftest: OK (prompt-backed raster and inaccessible SVG rejected)');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

if (process.argv.includes('--selftest')) {
  selftest();
} else {
  const problems = analyze({
    assetsRoot: join(siteRoot, 'src', 'assets'),
    contentRoot: join(siteRoot, 'src', 'content', 'docs'),
    samplePath: join(siteRoot, 'public', 'samples', 'schema-document.html'),
    sourcePath: join(siteRoot, 'src', 'assets', 'schema-ui.source.md'),
  });
  if (problems.length > 0) {
    console.error('check-visual-assets.mjs: FAILED');
    for (const problem of problems) console.error(`- ${problem}`);
    process.exitCode = 1;
  } else {
    console.log('check-visual-assets.mjs: OK (semantic diagrams, 3 sourced UI screenshots, and image alternatives)');
  }
}
