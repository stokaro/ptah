#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const repoRoot = join(siteRoot, '..', '..');
const sourcePath = join(repoRoot, 'docs', 'exit_codes.md');
const sitePath = join(siteRoot, 'src', 'content', 'docs', 'reference', 'exit-codes.md');

function tableRows(source) {
  return source
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.startsWith('| `'));
}

function main() {
  const sourceRows = tableRows(readFileSync(sourcePath, 'utf8'));
  const site = readFileSync(sitePath, 'utf8');
  const missing = sourceRows.filter((row) => !site.includes(row));

  if (missing.length > 0) {
    console.error(`${sitePath} is missing ${missing.length} exit-code reference row(s):`);
    for (const row of missing) {
      console.error(`- ${row}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log(`check-exit-codes.mjs: OK (${sourceRows.length} reference rows)`);
}

main();
