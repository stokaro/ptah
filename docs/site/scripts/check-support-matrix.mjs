#!/usr/bin/env node
// Keep release-line support facts inside the generated matrix.
//
// docsync proves that the generated block matches capabilityprobe.Cells. This
// check closes the other half: authored prose may explain the policy, but it
// may not restate a count or assign a current support level to a concrete line.
// A second hand-written census is how the page contradicted its own table.

import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, '..', '..', '..');
const pagePath = resolve(scriptDir, '..', 'src', 'content', 'docs', 'databases', 'support-matrix.md');
const waiverPath = resolve(scriptDir, 'data', 'support-fact-waivers.json');
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

function withoutGeneratedMatrix(source) {
  return source.replace(
    new RegExp(`${escapeRegex(begin)}[\\s\\S]*?${escapeRegex(end)}`, 'g'),
    ' ',
  );
}

function inlineText(value) {
  return value
    .replace(/<[^>]+>/g, (tag) =>
      [...tag.matchAll(/\b[\w:-]+\s*=\s*(["'])(.*?)\1/g)].map((match) => match[2]).join(' '),
    )
    .replace(/!\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
    .replace(/[`*_~{}|]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Return logical Markdown blocks. A table row is one block; wrapped prose and
 * list-item continuations stay together; fenced code and comments disappear.
 * MDX component attributes become text so release and level props cannot hide
 * a second support census from the checker.
 */
function authoredBlocks(source) {
  const text = withoutGeneratedMatrix(source)
    .replace(/^---\r?\n[\s\S]*?\r?\n---/, '')
    .replace(/```[\s\S]*?```/g, '\n')
    .replace(/~~~[\s\S]*?~~~/g, '\n')
    .replace(/<!--[^]*?-->/g, '\n');
  const blocks = [];
  let pending = [];
  const flush = () => {
    const normalized = inlineText(pending.join(' '));
    if (normalized) blocks.push(normalized);
    pending = [];
  };

  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim();
    if (line === '') {
      flush();
      continue;
    }
    if (line.startsWith('|')) {
      flush();
      const normalized = inlineText(line);
      if (normalized) blocks.push(normalized);
      continue;
    }
    if (/^(?:[-*+]\s+|\d+[.)]\s+)/.test(line) && pending.length > 0) flush();
    pending.push(line);
  }
  flush();
  return blocks;
}

function matchingWaiver(block, row, level, context) {
  if (!context) return null;
  return context.waivers.find((waiver) =>
    waiver.file === context.file &&
    waiver.dialect === row.dialect &&
    waiver.releaseLine === row.line &&
    waiver.support === level &&
    block.includes(waiver.asOf),
  ) ?? null;
}

function checkAuthoredFacts(source, rows, context = null) {
  const findings = [];

  const counted = new RegExp(
    `\\b(?:zero|one|two|three|four|five|six|seven|eight|nine|ten|\\d+)\\s+` +
      `(?:declared\\s+)?(?:release\\s+)?lines?\\s+(?:are|is|carry|carries|have|has|sit|sits)\\b` +
      `.{0,180}\\b${supportLevel}\\b`,
    'i',
  );
  for (const block of authoredBlocks(source)) {
    const census = block.match(counted);
    if (census) findings.push(`authored prose restates a support-level count: ${census[0]}`);
  }

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
      `(?:${identity}.*\\b${supportLevel}\\b|\\b${supportLevel}\\b.*${identity})`,
      'i',
    );
    for (const block of authoredBlocks(source)) {
      const matched = block.match(classification);
      if (matched) {
        const level = matched[0].match(new RegExp(`\\b(${supportLevel})\\b`, 'i'))?.[1]?.toLowerCase();
        const waiver = level ? matchingWaiver(block, row, level, context) : null;
        if (waiver) {
          context.used.add(waiver.id);
          continue;
        }
        findings.push(
          `authored prose assigns a support level to ${row.dialect} ${row.line}; keep that fact in the generated table: ${matched[0]}`,
        );
        break;
      }
    }
  }
  return findings;
}

function checkSource(source, context = null) {
  const { generated, authored } = splitGenerated(source);
  return checkAuthoredFacts(authored, rowsFrom(generated), context);
}

function validCalendarDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value) &&
    new Date(`${value}T00:00:00Z`).toISOString().slice(0, 10) === value;
}

function waiverProblems(waivers, today = new Date().toISOString().slice(0, 10)) {
  const problems = [];
  const ids = new Set();
  for (const [index, waiver] of waivers.entries()) {
    const where = `waiver ${index + 1}`;
    if (typeof waiver.id !== 'string' || waiver.id.trim() === '') problems.push(`${where}: id is missing`);
    else if (ids.has(waiver.id)) problems.push(`${where}: duplicate id ${waiver.id}`);
    else ids.add(waiver.id);
    if (typeof waiver.file !== 'string' || !waiver.file.endsWith('.md') && !waiver.file.endsWith('.mdx')) {
      problems.push(`${where}: file must name a Markdown file`);
    } else if (!existsSync(resolve(repoRoot, waiver.file))) {
      problems.push(`${where}: file does not exist: ${waiver.file}`);
    }
    if (!Object.hasOwn(dialectNames, waiver.dialect)) problems.push(`${where}: unknown dialect ${JSON.stringify(waiver.dialect)}`);
    if (typeof waiver.releaseLine !== 'string' || waiver.releaseLine.trim() === '') problems.push(`${where}: releaseLine is missing`);
    if (!new RegExp(`^${supportLevel}$`).test(waiver.support)) problems.push(`${where}: invalid support level ${JSON.stringify(waiver.support)}`);
    if (!validCalendarDate(waiver.asOf)) problems.push(`${where}: asOf must be a real YYYY-MM-DD date`);
    else if (waiver.asOf > today) problems.push(`${where}: asOf is in the future`);
    if (typeof waiver.reason !== 'string' || waiver.reason.trim() === '') problems.push(`${where}: reason is missing`);
  }
  return problems;
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
  assert(
    checkSource(fixture('| Release | Support |\n| --- | --- |\n| **ClickHouse 26.3** | `best-effort` |'))
      .some((finding) => finding.includes('clickhouse 26.3')),
    'catches a Markdown table row',
  );
  assert(
    checkSource(fixture('- [ClickHouse **26.3**](https://example.test/release) is\n  `best-effort`.'))
      .some((finding) => finding.includes('clickhouse 26.3')),
    'catches a wrapped list item with inline formatting',
  );
  assert(
    checkSource(fixture('<SupportLine\n  release="SQL Server 2022 (16.0)"\n  level="best-effort"\n/>'))
      .some((finding) => finding.includes('sqlserver 16.0')),
    'catches an MDX component classification',
  );
  assert(
    checkSource(fixture('```text\nClickHouse 26.3 is best-effort.\n```')).length === 0,
    'allows code fixtures',
  );
  assert(
    checkSource(fixture('PostgreSQL 99.9 is `certified` in this fictional example.')).length === 0,
    'allows fictional non-current release lines',
  );
  const historicalContext = {
    file: 'docs/history.md',
    waivers: [{
      id: 'historical-example',
      file: 'docs/history.md',
      dialect: 'clickhouse',
      releaseLine: '26.3',
      support: 'best-effort',
      asOf: '2026-01-15',
      reason: 'Historical release-note evidence.',
    }],
    used: new Set(),
  };
  assert(
    checkSource(fixture('As of 2026-01-15, ClickHouse 26.3 was `best-effort`.'), historicalContext).length === 0 &&
      historicalContext.used.has('historical-example'),
    'allows explicitly dated historical evidence only through the central waiver registry',
  );
  assert(
    waiverProblems([{ ...historicalContext.waivers[0], asOf: '2026-02-30' }], '2026-08-30')
      .some((problem) => problem.includes('real YYYY-MM-DD')),
    'rejects impossible waiver dates',
  );
  console.log('check-support-matrix.mjs --selftest: OK (12 assertions)');
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
    const waiverDocument = JSON.parse(readFileSync(waiverPath, 'utf8'));
    const waivers = waiverDocument.waivers;
    if (!Array.isArray(waivers)) throw new Error('support matrix: waiver registry must contain a waivers array');
    const invalidWaivers = waiverProblems(waivers);
    if (invalidWaivers.length > 0) throw new Error(`support matrix: invalid waiver registry\n  ${invalidWaivers.join('\n  ')}`);
    const used = new Set();
    findings = [];
    for (const file of markdownFiles()) {
      const source = readFileSync(resolve(repoRoot, file), 'utf8');
      const context = { file, waivers, used };
      for (const finding of checkAuthoredFacts(source, rows, context)) findings.push(`${file}: ${finding}`);
    }
    for (const waiver of waivers) {
      if (!used.has(waiver.id)) findings.push(`${waiverPath}: stale or unmatched waiver ${waiver.id}`);
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
