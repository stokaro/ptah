#!/usr/bin/env node
// Keep release-line support facts inside the generated matrix.
//
// docsync proves that the generated block matches capabilityprobe.Cells. This
// check closes the other half: authored prose may explain the policy, but it
// may not restate a count or assign a current support level to a concrete line.
// A second hand-written census is how the page contradicted its own table.

import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, '..', '..', '..');
const pagePath = resolve(scriptDir, '..', 'src', 'content', 'docs', 'databases', 'support-matrix.md');
const begin = '<!-- BEGIN GENERATED VERSION MATRIX -->';
const end = '<!-- END GENERATED VERSION MATRIX -->';
const supportLevel = '(?:certified|legacy-tested|best-effort|known-incompatible)';

const dialectNames = {
  postgres: ['postgres', 'postgresql'],
  mysql: ['mysql'],
  mariadb: ['mariadb'],
  clickhouse: ['clickhouse'],
  oracle: ['oracle'],
  cockroachdb: ['cockroachdb', 'cockroach'],
  yugabytedb: ['yugabytedb', 'yugabyte'],
  spanner: ['spanner'],
  sqlserver: ['sql server', 'sqlserver'],
  sqlite: ['sqlite'],
};

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function splitGenerated(source) {
  const start = source.indexOf(`${begin}\n`);
  const finish = source.indexOf(`\n${end}`, start + begin.length);
  if (start === -1 || finish === -1) throw new Error('support matrix: generated version-matrix markers are missing');
  const bodyStart = start + begin.length + 1;
  return {
    generated: source.slice(bodyStart, finish),
    authored: `${source.slice(0, start)}\n${source.slice(finish + end.length + 1)}`,
  };
}

function rowsFrom(block) {
  const rows = [];
  for (const line of block.split(/\r?\n/)) {
    const matched = line.match(/^\| `([^`]+)` \| ([^|]+?) \| (certified|legacy-tested|best-effort|known-incompatible) \|/);
    if (!matched) continue;
    const renderedLine = matched[2].trim();
    const labeled = renderedLine.match(/^(.+?)\s+\(([^)]+)\)$/);
    rows.push({
      dialect: matched[1],
      line: labeled ? labeled[1] : renderedLine,
      label: labeled ? labeled[2] : null,
      support: matched[3],
    });
  }
  if (rows.length === 0) throw new Error('support matrix: generated block contains no release-line rows');
  return rows;
}

function prose(source) {
  return source
    .replace(/^---\r?\n[\s\S]*?\r?\n---/, ' ')
    .replace(/```[\s\S]*?```/g, ' ')
    .replace(/<!--[^]*?-->/g, ' ')
    .replace(/\s+/g, ' ');
}

function withoutGeneratedMatrix(source) {
  return source.replace(
    new RegExp(`${escapeRegex(begin)}[\\s\\S]*?${escapeRegex(end)}`, 'g'),
    ' ',
  );
}

function checkAuthoredFacts(source, rows) {
  const text = prose(withoutGeneratedMatrix(source));
  const findings = [];

  const counted = new RegExp(
    `\\b(?:zero|one|two|three|four|five|six|seven|eight|nine|ten|\\d+)\\s+` +
      `(?:declared\\s+)?(?:release\\s+)?lines?\\s+(?:are|is|carry|carries|have|has|sit|sits)\\b` +
      `.{0,180}\\b${supportLevel}\\b`,
    'i',
  );
  const census = text.match(counted);
  if (census) findings.push(`authored prose restates a support-level count: ${census[0]}`);

  for (const row of rows) {
    const aliases = dialectNames[row.dialect] ?? [row.dialect];
    const identities = [
      `(?:${aliases.map(escapeRegex).join('|')})\\s+${escapeRegex(row.line)}(?:\\b|(?=\\s|[),.:;]))`,
    ];
    if (row.label) {
      identities.push(`${escapeRegex(row.label)}(?:\\s+\\(${escapeRegex(row.line)}\\))?`);
    }
    const identity = `(?:${identities.join('|')})`;
    const classification = new RegExp(
      `(?:${identity}.{0,180}\\b${supportLevel}\\b|\\b${supportLevel}\\b.{0,180}${identity})`,
      'i',
    );
    const matched = text.match(classification);
    if (matched) {
      findings.push(
        `authored prose assigns a support level to ${row.dialect} ${row.line}; keep that fact in the generated table: ${matched[0]}`,
      );
    }
  }
  return findings;
}

function checkSource(source) {
  const { generated, authored } = splitGenerated(source);
  return checkAuthoredFacts(authored, rowsFrom(generated));
}

function markdownFiles() {
  const output = execFileSync('git', ['-C', repoRoot, 'ls-files', '*.md', '*.mdx'], {
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
  });
  const files = output.split('\n').filter(Boolean);
  if (files.length === 0) throw new Error('support matrix: git found no Markdown files to inspect');
  return files;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function fixture(authored) {
  return `---\ntitle: Matrix\n---\n${authored}\n${begin}\n` +
    '| Dialect | Release line | Support | Capability preset | Probed |\n' +
    '| --- | --- | --- | --- | --- |\n' +
    '| `clickhouse` | 26.3 | certified | `ClickHouse2411` | yes |\n' +
    '| `sqlserver` | 16.0 (SQL Server 2022) | best-effort | `SQLServer2022` | no |\n' +
    `${end}\n`;
}

function selftest() {
  assert(checkSource(fixture('The table is generated.')).length === 0, 'policy prose with no derived facts passes');
  assert(
    checkSource(fixture('| `best-effort` | Not regularly tested. |')).length === 0,
    'a support-level definition is not a census',
  );
  assert(
    checkSource(fixture('Five declared lines are `best-effort`.')).some((finding) => finding.includes('count')),
    'catches a hand-written count',
  );
  assert(
    checkSource(fixture('ClickHouse 26.3 remains `best-effort`.')).some((finding) => finding.includes('clickhouse 26.3')),
    'catches a concrete line classification that contradicts the generated row',
  );
  assert(
    checkSource(fixture('SQL Server 2022 (16.0) is `best-effort`.')).some((finding) => finding.includes('sqlserver 16.0')),
    'catches a labeled release line',
  );
  console.log('check-support-matrix.mjs --selftest: OK (5 assertions)');
}

function main() {
  const argument = process.argv[2];
  if (argument === '--selftest') {
    selftest();
    return;
  }
  if (argument !== undefined) {
    console.error(`usage: node scripts/check-support-matrix.mjs [--selftest] (got ${argument})`);
    process.exitCode = 2;
    return;
  }

  let findings;
  try {
    const page = readFileSync(pagePath, 'utf8');
    const rows = rowsFrom(splitGenerated(page).generated);
    findings = [];
    for (const file of markdownFiles()) {
      const source = readFileSync(resolve(repoRoot, file), 'utf8');
      for (const finding of checkAuthoredFacts(source, rows)) findings.push(`${file}: ${finding}`);
    }
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
    return;
  }
  if (findings.length > 0) {
    console.error('support matrix: authored prose duplicates generated release-line facts');
    for (const finding of findings) console.error(`  ${finding}`);
    process.exitCode = 1;
    return;
  }
  console.log('support matrix: OK (release-line counts and classifications are generated only)');
}

main();
