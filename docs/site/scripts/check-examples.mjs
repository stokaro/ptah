#!/usr/bin/env node
// Holds every top-level examples directory to one reader contract and keeps the
// examples index derived from those READMEs.
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = join(scriptDir, '..', '..', '..');
const requiredHeadings = [
  'What this example demonstrates',
  'Prerequisites',
  'Run',
  'Expected result',
  'Verify',
  'Cleanup',
  'Learn more',
];
const generatedStart = '<!-- ptah:examples-index:start -->';
const generatedEnd = '<!-- ptah:examples-index:end -->';

function directories(root) {
  return readdirSync(root, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith('.'))
    .map((entry) => entry.name)
    .sort();
}

function section(source, heading) {
  const marker = `## ${heading}`;
  const start = source.split(/\r?\n/).findIndex((line) => line.trim() === marker);
  if (start === -1) return '';
  const lines = source.split(/\r?\n/).slice(start + 1);
  const end = lines.findIndex((line) => line.startsWith('## '));
  return lines.slice(0, end === -1 ? undefined : end).join('\n').trim();
}

function plainPurpose(source) {
  return section(source, 'What this example demonstrates')
    .split(/\n\s*\n/)[0]
    .replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
    .replace(/[`*]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function title(source, fallback) {
  return source.match(/^# (.+)$/m)?.[1].trim() ?? fallback;
}

export function analyze(root) {
  const problems = [];
  const examples = [];
  for (const name of directories(root)) {
    const readmePath = join(root, name, 'README.md');
    let source;
    try {
      source = readFileSync(readmePath, 'utf8');
    } catch {
      problems.push(`${name}: README.md is missing`);
      continue;
    }
    for (const heading of requiredHeadings) {
      if (!section(source, heading)) problems.push(`${name}: section ${JSON.stringify(heading)} is missing or empty`);
    }
    const purpose = plainPurpose(source);
    if (!purpose) problems.push(`${name}: the demonstration summary is empty`);
    examples.push({ name, purpose, title: title(source, name) });
  }
  return { examples, problems };
}

export function renderIndex(examples) {
  const rows = examples.map(({ name, purpose, title }) => `| [${title}](${name}/) | ${purpose} |`);
  return `# Ptah examples

Each example has one run, expected-result, verification, cleanup, and canonical
documentation contract in its own README. The executable gate runs local
examples and mechanically verifies provider-backed examples.

${generatedStart}
| Example | What it demonstrates |
| --- | --- |
${rows.join('\n')}
${generatedEnd}
`;
}

function selftest() {
  const root = mkdtempSync(join(tmpdir(), 'ptah-examples-check-'));
  try {
    const complete = requiredHeadings.map((heading) => `## ${heading}\n\ncontent\n`).join('\n');
    mkdirSync(join(root, 'complete'));
    writeFileSync(join(root, 'complete', 'README.md'), `# Complete\n\n${complete}`);
    mkdirSync(join(root, 'missing'));
    writeFileSync(join(root, 'missing', 'README.md'), '# Missing\n\n## Run\n\ncommand\n');
    const result = analyze(root);
    const rendered = renderIndex(result.examples);
    if (
      result.problems.length !== requiredHeadings.length ||
      !result.problems.some((problem) => problem.includes('Expected result')) ||
      !rendered.includes('[Complete](complete/)')
    ) {
      console.error('check-examples.mjs --selftest: FAILED');
      process.exitCode = 1;
      return;
    }
    console.log('check-examples.mjs --selftest: OK (missing contract sections rejected and index rendered)');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

function main() {
  if (process.argv.includes('--selftest')) {
    selftest();
    return;
  }

  const root = join(repositoryRoot, 'examples');
  const indexPath = join(root, 'README.md');
  const result = analyze(root);
  if (result.problems.length > 0) {
    console.error('check-examples.mjs: FAILED');
    for (const problem of result.problems) console.error(`- ${problem}`);
    process.exitCode = 1;
    return;
  }

  const rendered = renderIndex(result.examples);
  if (process.argv.includes('--write')) {
    writeFileSync(indexPath, rendered);
    console.log(`check-examples.mjs --write: wrote ${relative(repositoryRoot, indexPath)}`);
    return;
  }

  let current = '';
  try { current = readFileSync(indexPath, 'utf8'); } catch { /* reported as stale below */ }
  if (current !== rendered) {
    console.error('check-examples.mjs: examples/README.md is stale; run "npm run examples:write" in docs/site.');
    process.exitCode = 1;
    return;
  }
  console.log(`check-examples.mjs: OK (${result.examples.length} example contracts and generated index)`);
}

main();
