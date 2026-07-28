#!/usr/bin/env node
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, extname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const routePattern = /^\/(?:[a-z0-9-]+\/)+$|^\/$/;

function toPosix(value) {
  return value.split(sep).join('/');
}

function walk(dir) {
  const files = [];
  for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...walk(fullPath));
      continue;
    }
    if (entry.isFile() && ['.md', '.mdx'].includes(extname(entry.name))) {
      files.push(fullPath);
    }
  }
  return files;
}

function routeFor(root, file) {
  let route = toPosix(relative(root, file)).replace(/\.(md|mdx)$/, '');
  if (route === 'index') return '/';
  if (route.endsWith('/index')) route = route.slice(0, -'/index'.length);
  return `/${route}/`;
}

export function parseRedirectRoutes(configSource) {
  const block = configSource.match(/const redirectRoutes = \{([\s\S]*?)\};/);
  if (!block) return null;
  const entries = [];
  for (const match of block[1].matchAll(/'([^']+)':\s*'([^']+)'/g)) {
    entries.push([match[1], match[2]]);
  }
  return entries;
}

export function checkRedirects(root, configSource) {
  const errors = [];
  const entries = parseRedirectRoutes(configSource);

  if (entries === null) {
    return { errors: ['astro.config.mjs: redirectRoutes map not found; moved pages must keep their redirect entries'], count: 0 };
  }

  const routes = new Set(walk(root).map((file) => routeFor(root, file)));
  const sources = new Set(entries.map(([from]) => from));

  for (const [from, to] of entries) {
    if (!routePattern.test(from)) {
      errors.push(`redirect source ${from} is not a /segment/ route with leading and trailing slashes`);
    }
    if (!routePattern.test(to)) {
      errors.push(`redirect target ${to} is not a /segment/ route with leading and trailing slashes`);
      continue;
    }
    if (from === to) {
      errors.push(`redirect ${from} points to itself`);
      continue;
    }
    if (routes.has(from)) {
      errors.push(`redirect source ${from} still exists as a content page; remove the page or the redirect`);
    }
    if (!routes.has(to)) {
      errors.push(`redirect ${from} targets ${to}, which is not an existing route`);
    }
    if (sources.has(to)) {
      errors.push(`redirect ${from} targets ${to}, which is itself a redirect source (chain)`);
    }
  }

  return { errors, count: entries.length };
}

function writeDoc(root, name, content) {
  const file = join(root, name);
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file, content);
}

function configWith(body) {
  return `const redirectRoutes = {\n${body}\n};\n`;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  const tmp = mkdtempSync(join(tmpdir(), 'ptah-doc-redirects-'));
  const root = join(tmp, 'src', 'content', 'docs');

  try {
    writeDoc(root, 'index.mdx', '---\ntitle: Home\n---\n');
    writeDoc(root, 'start/quick-start.md', '---\ntitle: Quick start\n---\n');
    writeDoc(root, 'lingering.md', '---\ntitle: Lingering\n---\n');

    const ok = checkRedirects(root, configWith("  '/getting-started/': '/start/quick-start/',"));
    assert(ok.errors.length === 0, `expected valid redirect to pass, got ${ok.errors.join('; ')}`);
    assert(ok.count === 1, `expected 1 parsed redirect, got ${ok.count}`);

    const missingTarget = checkRedirects(root, configWith("  '/getting-started/': '/start/missing/',"));
    assert(missingTarget.errors.some((error) => error.includes('not an existing route')), 'catches missing target route');

    const liveSource = checkRedirects(root, configWith("  '/lingering/': '/start/quick-start/',"));
    assert(liveSource.errors.some((error) => error.includes('still exists as a content page')), 'catches source that is still a page');

    const chain = checkRedirects(
      root,
      configWith("  '/a/': '/getting-started/',\n  '/getting-started/': '/start/quick-start/',"),
    );
    assert(chain.errors.some((error) => error.includes('redirect source (chain)')), 'catches redirect chains');

    const selfTarget = checkRedirects(root, configWith("  '/getting-started/': '/getting-started/',"));
    assert(selfTarget.errors.some((error) => error.includes('points to itself')), 'catches self-redirects');

    const malformed = checkRedirects(root, configWith("  '/getting-started/': 'start/quick-start',"));
    assert(malformed.errors.some((error) => error.includes('leading and trailing slashes')), 'catches malformed routes');

    const absent = checkRedirects(root, 'export default {};\n');
    assert(absent.errors.some((error) => error.includes('redirectRoutes map not found')), 'catches missing redirect map');

    console.log('check-redirects.mjs --selftest: OK');
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function main() {
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }

  const docsRoot = join(siteRoot, 'src', 'content', 'docs');
  const configPath = join(siteRoot, 'astro.config.mjs');

  if (!existsSync(docsRoot) || !existsSync(configPath)) {
    console.error(`error: expected ${docsRoot} and ${configPath} to exist`);
    process.exitCode = 2;
    return;
  }

  const { errors, count } = checkRedirects(docsRoot, readFileSync(configPath, 'utf8'));

  if (errors.length > 0) {
    console.error('Redirect check failed:');
    for (const error of errors) {
      console.error(`- ${error}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log(`check-redirects.mjs: OK (${count} redirects)`);
}

main();
