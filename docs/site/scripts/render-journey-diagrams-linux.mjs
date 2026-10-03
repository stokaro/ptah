#!/usr/bin/env node
// Use the same Linux font metrics as CI from a non-Linux development machine.
import { execFile } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const siteRoot = join(dirname(fileURLToPath(import.meta.url)), '..');
const context = process.env.PTAH_DOCKER_CONTEXT;
if (!context) throw new Error('Set PTAH_DOCKER_CONTEXT explicitly for the Linux diagram renderer.');
const check = process.argv.includes('--check');
const execute = promisify(execFile);
const docker = (args) => execute('docker', ['--context', context, ...args], { maxBuffer: 8 * 1024 * 1024 });
const version = JSON.parse(readFileSync(join(siteRoot, 'node_modules/playwright/package.json'), 'utf8')).version;
const image = `mcr.microsoft.com/playwright:v${version}-noble`;
const container = `ptah-journey-diagrams-${process.pid}-${Date.now()}`;
const work = mkdtempSync(join(tmpdir(), 'ptah-journey-diagrams-'));
const archive = join(work, 'sources.tar.gz');
const names = ['product-journeys', 'inference-generation-lifecycle'];
let imageExisted = false;
let containerCreated = false;
try {
  try { await docker(['image', 'inspect', image]); imageExisted = true; } catch { /* The task will pull its own image. */ }
  const inputs = [
    'package.json', 'package-lock.json', 'diagrams',
    'src/fonts/instrument-sans-var-latin.woff2', 'scripts/generate-journey-diagrams.mjs',
  ];
  await execute('tar', ['-czf', archive, ...inputs], { cwd: siteRoot, env: { ...process.env, COPYFILE_DISABLE: '1' } });
  await docker(['create', '--name', container, '-w', '/work', image, 'sleep', '1800']);
  containerCreated = true;
  await docker(['cp', archive, `${container}:/tmp/sources.tar.gz`]);
  await docker(['start', container]);
  await docker(['exec', container, 'sh', '-c',
    'tar -xzf /tmp/sources.tar.gz -C /work && mkdir -p /work/src/assets && npm ci && npm run diagrams:write']);
  const rendered = [];
  for (const name of names) {
    for (const theme of ['light', 'dark']) {
      const file = `${name}-${theme}.svg`;
      const output = join(work, file);
      await docker(['cp', `${container}:/work/src/assets/${file}`, output]);
      if (check && readFileSync(output, 'utf8') !== readFileSync(join(siteRoot, 'src/assets', file), 'utf8')) {
        throw new Error(`${file} is stale; run npm run diagrams:docker`);
      }
      rendered.push({ file, output });
    }
  }
  if (!check) {
    // Copy only after every diagram rendered successfully.
    const { copyFileSync } = await import('node:fs');
    for (const { file, output } of rendered) copyFileSync(output, join(siteRoot, 'src/assets', file));
  }
  console.log(`Journey SVGs ${check ? 'verified' : 'rendered'} on Linux using Docker context ${context}.`);
} finally {
  if (containerCreated) await docker(['rm', '-f', container]);
  if (!imageExisted && containerCreated) {
    try { await docker(['image', 'rm', image]); }
    catch (error) { console.error(`Could not remove the task image ${image}: ${error.message}`); }
  }
  rmSync(work, { recursive: true, force: true });
}
