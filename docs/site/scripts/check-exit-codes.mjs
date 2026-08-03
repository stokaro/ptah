#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const repoRoot = join(siteRoot, '..', '..');
const sourcePath = join(repoRoot, 'docs', 'exit_codes.md');
const sitePath = join(siteRoot, 'src', 'content', 'docs', 'reference', 'exit-codes.md');

// A separator row carries no content to mirror: every cell is dashes and
// optional alignment colons.
const separatorRow = /^\|(\s*:?-+:?\s*\|)+$/;

// tableRows returns every content row of every Markdown table in source,
// header rows included.
//
// It deliberately does not require a row to open with a code span. That
// narrower filter silently excluded whole tables whose first column is prose:
// the Diagnostic Prefix table added for stokaro/ptah#1019 opens
// `| Native \`ptah\` |`, so the checker reported OK while comparing none of its
// rows. A checker that decides what to compare from the shape of the first
// cell reports on the rows it happens to recognize, not on the reference.
function tableRows(source) {
  return source
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.startsWith('|') && !separatorRow.test(line));
}

// missingRows returns the reference rows the site page does not reproduce.
function missingRows(source, site) {
  return tableRows(source).filter((row) => !site.includes(row));
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

// selftest proves the checker fails on the omission it exists to catch, and in
// particular on a table whose first cell is prose rather than a code span —
// the case the previous filter passed over in silence.
function selftest() {
  const source = [
    '| Code | Meaning |',
    '| --- | --- |',
    '| `0` | Success. |',
    '',
    '| Surface | Prefix |',
    '| :-- | --: |',
    '| Native `ptah` | `error: ` |',
    '',
  ].join('\n');

  const mirrored = [
    '| Code | Meaning |',
    '| --- | --- |',
    '| `0` | Success. |',
    '| Surface | Prefix |',
    '| --- | --- |',
    '| Native `ptah` | `error: ` |',
  ].join('\n');

  const rows = tableRows(source);
  assert(rows.length === 4, `expected 4 content rows, got ${rows.length}: ${JSON.stringify(rows)}`);
  assert(!rows.some((row) => separatorRow.test(row)), 'separator rows must not be compared');
  assert(rows.includes('| Surface | Prefix |'), 'a prose header row must be compared');
  assert(rows.includes('| Native `ptah` | `error: ` |'), 'a prose-first-cell row must be compared');

  assert(missingRows(source, mirrored).length === 0, 'a fully mirrored page must pass');

  const withoutProseTable = mirrored
    .split('\n')
    .filter((line) => !line.includes('Surface') && !line.includes('Native'))
    .join('\n');
  const droppedProse = missingRows(source, withoutProseTable);
  assert(
    droppedProse.length === 2,
    `expected the 2 dropped prose rows, got ${JSON.stringify(droppedProse)}`,
  );

  const withoutCodeRow = mirrored.replace('| `0` | Success. |\n', '');
  assert(
    missingRows(source, withoutCodeRow).includes('| `0` | Success. |'),
    'a dropped code-span row must still be caught',
  );

  console.log('check-exit-codes.mjs --selftest: OK');
}

function main() {
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }

  const source = readFileSync(sourcePath, 'utf8');
  const site = readFileSync(sitePath, 'utf8');
  const missing = missingRows(source, site);

  if (missing.length > 0) {
    console.error(`${sitePath} is missing ${missing.length} exit-code reference row(s):`);
    for (const row of missing) {
      console.error(`- ${row}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log(`check-exit-codes.mjs: OK (${tableRows(source).length} reference rows)`);
}

main();
