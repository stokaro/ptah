#!/usr/bin/env node
import { readdirSync, readFileSync } from 'node:fs';
import { dirname, extname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const docsRoot = join(siteRoot, 'src', 'content', 'docs');
const astroConfigPath = join(siteRoot, 'astro.config.mjs');
const frontmatterPattern = /^---\n([\s\S]*?)\n---/;
const weakMarkers = /\b(TODO|TBD|FIXME|coming soon)\b/i;

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

function routeFor(file) {
  let route = toPosix(relative(docsRoot, file)).replace(/\.(md|mdx)$/, '');
  if (route === 'index') return '/';
  if (route.endsWith('/index')) route = route.slice(0, -'/index'.length);
  return route;
}

function frontmatter(source) {
  const match = source.match(frontmatterPattern);
  return match?.[1] ?? '';
}

function hasFrontmatterKey(frontmatterSource, key) {
  return new RegExp(`^${key}:\\s*\\S`, 'm').test(frontmatterSource);
}

function sidebarSlugs() {
  const source = readFileSync(astroConfigPath, 'utf8');
  const slugs = new Set();
  for (const match of source.matchAll(/\bslug:\s*'([^']+)'/g)) {
    slugs.add(match[1]);
  }
  return slugs;
}

const slugs = sidebarSlugs();
const errors = [];

for (const file of walk(docsRoot)) {
  const source = readFileSync(file, 'utf8');
  const route = routeFor(file);
  const displayPath = toPosix(relative(process.cwd(), file));
  const meta = frontmatter(source);

  if (!hasFrontmatterKey(meta, 'title')) {
    errors.push(`${displayPath}: missing title frontmatter`);
  }
  if (!hasFrontmatterKey(meta, 'description')) {
    errors.push(`${displayPath}: missing description frontmatter`);
  }
  if (weakMarkers.test(source)) {
    errors.push(`${displayPath}: contains TODO/TBD/FIXME/coming soon marker`);
  }
  if (route !== '/' && !slugs.has(route)) {
    errors.push(`${displayPath}: route ${route} is not listed in the Starlight sidebar`);
  }
}

if (errors.length > 0) {
  console.error('Documentation page health check failed:');
  for (const error of errors) {
    console.error(`- ${error}`);
  }
  process.exitCode = 1;
} else {
  console.log(`check-page-health.mjs: OK (${slugs.size} sidebar entries)`);
}
