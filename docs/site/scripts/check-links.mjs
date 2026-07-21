#!/usr/bin/env node
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, extname, join, relative, sep } from 'node:path';
import path from 'node:path/posix';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const docsRoot = join(siteRoot, 'src', 'content', 'docs');
const docExts = new Set(['.md', '.mdx']);
const externalSchemes = /^[a-z][a-z0-9+.-]*:/i;

function toPosix(value) {
  return value.split(sep).join('/');
}

function walk(dir) {
  const files = [];
  const entries = readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name));
  for (const entry of entries) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...walk(fullPath));
      continue;
    }
    if (entry.isFile() && docExts.has(extname(entry.name))) {
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

function normalizeRoute(route) {
  let normalized = path.normalize(route);
  if (!normalized.startsWith('/')) normalized = `/${normalized}`;
  if (!normalized.endsWith('/')) normalized = `${normalized}/`;
  return normalized;
}

function stripFencedCode(source) {
  return source.replace(/```[\s\S]*?```/g, '');
}

function extractLinks(file) {
  const source = stripFencedCode(readFileSync(file, 'utf8'));
  const links = [];
  const patterns = [
    /(?<!!)\[[^\]\n]+\]\(([^)\s]+)(?:\s+["'][^"']*["'])?\)/g,
    /\bhref=["']([^"']+)["']/g,
    /^\s+link:\s+([^\s]+)\s*$/gm,
  ];

  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) {
      links.push(normalizeHref(match[1]));
    }
  }

  return links;
}

function normalizeHref(href) {
  const trimmed = href.trim();
  if (trimmed.startsWith('<') && trimmed.endsWith('>')) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function targetPath(href) {
  const withoutHash = href.split('#', 1)[0];
  return withoutHash.split('?', 1)[0];
}

function resolveRoute(sourceRoute, href) {
  const parts = sourceRoute.split('/').filter(Boolean);
  for (const segment of href.split('/')) {
    if (segment === '' || segment === '.') continue;
    if (segment === '..') {
      if (parts.length === 0) return { escaped: true, route: null };
      parts.pop();
      continue;
    }
    parts.push(segment);
  }
  return { escaped: false, route: normalizeRoute(`/${parts.join('/')}/`) };
}

function validateLink(root, routes, file, href, cwd) {
  if (!href || href.startsWith('#') || externalSchemes.test(href)) {
    return null;
  }

  const cleanHref = targetPath(href);
  if (!cleanHref) return null;

  if (cleanHref.startsWith('/')) {
    return `${toPosix(relative(cwd, file))}: ${href} is root-relative; use a docs-relative link so GitHub Pages keeps /ptah/<version>/ in the URL`;
  }

  const sourceRoute = routeFor(root, file);
  const { escaped, route: resolved } = resolveRoute(sourceRoute, cleanHref);
  if (escaped) {
    return `${toPosix(relative(cwd, file))}: ${href} escapes the docs route root`;
  }
  if (routes.has(resolved)) return null;

  return `${toPosix(relative(cwd, file))}: ${href} resolves to missing route ${resolved}`;
}

function checkLinks(root, cwd) {
  const files = walk(root);
  const routes = new Set(files.map((file) => routeFor(root, file)));
  const errors = [];

  for (const file of files) {
    for (const href of extractLinks(file)) {
      const error = validateLink(root, routes, file, href, cwd);
      if (error) errors.push(error);
    }
  }

  return { errors, files, routes };
}

function writeDoc(root, name, content) {
  const file = join(root, name);
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file, content);
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  const tmp = mkdtempSync(join(tmpdir(), 'ptah-doc-links-'));
  const root = join(tmp, 'src', 'content', 'docs');

  try {
    writeDoc(root, 'index.mdx', '---\ntitle: Home\n---\n[Start](./reference/comparison/)\n');
    writeDoc(root, 'operate/conformance.md', '---\ntitle: Conformance\n---\n');
    writeDoc(root, 'workflows/go-schema.md', '---\ntitle: Go schema\n---\n');
    writeDoc(root, 'reference/exit-codes.md', '---\ntitle: Exit codes\n---\n');
    writeDoc(
      root,
      'reference/comparison.md',
      [
        '---',
        'title: Comparison',
        '---',
        '[Conformance](../operate/conformance/)',
        '[Go schema workflow](../workflows/go-schema/)',
        '[Exit codes](./exit-codes/)',
      ].join('\n'),
    );

    const broken = checkLinks(root, tmp);
    assert(broken.errors.length === 3, `expected 3 broken fixture links, got ${broken.errors.length}`);
    assert(broken.errors.some((error) => error.includes('/reference/operate/conformance/')), 'catches conformance over-relative link');
    assert(broken.errors.some((error) => error.includes('/reference/workflows/go-schema/')), 'catches workflow over-relative link');
    assert(broken.errors.some((error) => error.includes('/reference/comparison/exit-codes/')), 'catches same-folder exit code link');

    writeDoc(root, 'index.mdx', '---\ntitle: Home\n---\n[Escapes](../reference/comparison/)\n');
    const escaped = checkLinks(root, tmp);
    assert(escaped.errors.some((error) => error.includes('escapes the docs route root')), 'catches route-root escape from index');
    writeDoc(root, 'index.mdx', '---\ntitle: Home\n---\n[Start](./reference/comparison/)\n');

    writeDoc(
      root,
      'reference/comparison.md',
      [
        '---',
        'title: Comparison',
        '---',
        '[Conformance](../../operate/conformance/)',
        '[Go schema workflow](../../workflows/go-schema/)',
        '[Exit codes](../exit-codes/)',
      ].join('\n'),
    );

    const fixed = checkLinks(root, tmp);
    assert(fixed.errors.length === 0, `expected fixed fixture links to pass, got ${fixed.errors.join('; ')}`);
    console.log('check-links.mjs --selftest: OK');
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function main() {
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }

  if (!existsSync(docsRoot)) {
    console.error(`error: docs content directory not found: ${docsRoot}`);
    process.exitCode = 2;
    return;
  }

  const { errors, files, routes } = checkLinks(docsRoot, process.cwd());

  if (errors.length > 0) {
    console.error('Broken internal documentation links:');
    for (const error of errors) {
      console.error(`- ${error}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log(`check-links.mjs: OK (${files.length} pages, ${routes.size} routes)`);
}

main();
