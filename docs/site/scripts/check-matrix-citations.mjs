#!/usr/bin/env node
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const rowsPath = join(scriptDir, 'data', 'feature-matrix-rows.json');
const repoRoot = join(scriptDir, '..', '..', '..');

// A matrix cell's evidence names the files that settle it, and files move. Three
// citations already pointed at nothing: schemaload.go left cmd/internal/,
// clean_live_test.go left internal/schemaclean/, and test_vars_test.go was
// folded into test_test.go. Every claim was still true and still tested -- what
// had been lost was the reader's way of checking it, which is the whole job of
// the evidence field.
//
// Nothing noticed, because build-feature-matrix.mjs --check compares the
// generated page against this file and neither one opens a cited path
// (stokaro/ptah#1812 found these while auditing something else).

// A citation is a path rooted at a directory this repository actually has. That
// is the whole rule, and it is what separates a citation from the example paths
// the prose is full of -- `bucket/x.hcl`, `m/20250801000001_init.sql`,
// `out/public.hcl` name nothing in the tree and are not meant to.
//
// Read from the tree rather than listed, so a new top-level directory is
// covered the day it appears rather than the day someone remembers this file.
const repoDirectories = new Set(
  readdirSync(repoRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && entry.name !== 'node_modules')
    .map((entry) => entry.name),
);

const citation = /\b((?:[A-Za-z0-9_.-]+\/)+[A-Za-z0-9_.-]+\.(?:go|md|mjs|sh|json|yml|yaml|sql|hcl))\b/g;

// Collects the citations in one row that name a path the tree does not have.
export function danglingCitations(row, exists = (path) => existsSync(join(repoRoot, path))) {
  const text = `${row.evidence ?? ''} ${row.note ?? ''}`;
  const dangling = [];
  for (const [, path] of text.matchAll(citation)) {
    const root = path.slice(0, path.indexOf('/'));
    if (!repoDirectories.has(root)) {
      continue;
    }
    if (!exists(path)) {
      dangling.push(path);
    }
  }
  return [...new Set(dangling)];
}

function report(rows) {
  const findings = [];
  for (const [index, row] of rows.entries()) {
    for (const path of danglingCitations(row)) {
      findings.push({ index, feature: row.feature, path });
    }
  }
  return findings;
}

// selftest drives the rule with fixtures, because a check that finds nothing and
// a check that examines nothing print the same thing.
function selftest() {
  const exists = (path) => path === 'internal/real.go';
  const cases = [
    {
      name: 'a citation the tree has',
      row: { evidence: 'internal/real.go holds it' },
      want: [],
    },
    {
      name: 'a citation the tree does not have',
      row: { evidence: 'cmd/gone/vanished_test.go pinned it' },
      want: ['cmd/gone/vanished_test.go'],
    },
    {
      name: 'an example path is not a citation',
      row: { evidence: 'write bucket/x.hcl and m/0001_init.sql' },
      want: [],
    },
    {
      name: 'the note is read as well as the evidence',
      row: { note: 'see cmd/gone/vanished_test.go' },
      want: ['cmd/gone/vanished_test.go'],
    },
    {
      name: 'the same missing path twice is one finding',
      row: { evidence: 'cmd/a/b.go and cmd/a/b.go', note: 'cmd/a/b.go' },
      want: ['cmd/a/b.go'],
    },
  ];

  const failures = [];
  for (const { name, row, want } of cases) {
    const got = danglingCitations(row, exists);
    if (JSON.stringify(got) !== JSON.stringify(want)) {
      failures.push(`${name}: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`);
    }
  }
  if (failures.length > 0) {
    console.error('check-matrix-citations.mjs --selftest: FAILED');
    for (const failure of failures) {
      console.error(`  ${failure}`);
    }
    process.exit(1);
  }
  console.log(
    `check-matrix-citations.mjs --selftest: OK (${cases.length} cases, ${repoDirectories.size} repository directories)`,
  );
}

if (process.argv.includes('--selftest')) {
  selftest();
} else {
  const rows = JSON.parse(readFileSync(rowsPath, 'utf8'));
  const findings = report(rows);
  if (findings.length > 0) {
    console.error('check-matrix-citations.mjs: evidence names paths this tree does not have');
    for (const { index, feature, path } of findings) {
      console.error(`  row ${index} (${feature}): ${path}`);
    }
    process.exit(1);
  }
  console.log(`check-matrix-citations.mjs: OK (${rows.length} rows)`);
}
