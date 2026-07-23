#!/usr/bin/env node
import { readdirSync, readFileSync } from 'node:fs';
import { dirname, extname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const docsRoot = join(siteRoot, 'src', 'content', 'docs');

const forbidden = [
  'docs/yaml_schema.md',
  'docs/atlas_hcl_schema.md',
  'docs/atlas_project_config.md',
  'docs/public_api.md',
  'docs/capabilities.md',
  'docs/sqlite.md',
  'docs/sqlserver.md',
];
const forbiddenRootDocsURL = 'github.com/stokaro/ptah/blob/master/docs/';

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

const errors = [];
for (const file of walk(docsRoot)) {
  const source = readFileSync(file, 'utf8');
  if (source.includes(forbiddenRootDocsURL)) {
    errors.push(`${toPosix(relative(process.cwd(), file))}: links to root docs on GitHub; publish core information in docs/site instead`);
  }
  for (const target of forbidden) {
    if (source.includes(target)) {
      errors.push(`${toPosix(relative(process.cwd(), file))}: links to ${target}; use the published site reference page instead`);
    }
  }
}

if (errors.length > 0) {
  console.error('Core documentation should be self-contained in the published site:');
  for (const error of errors) {
    console.error(`- ${error}`);
  }
  process.exitCode = 1;
} else {
  console.log(`check-core-doc-links.mjs: OK (${forbidden.length} protected core references)`);
}
