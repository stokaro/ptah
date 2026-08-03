#!/usr/bin/env node
// Regenerates docs/site/src/content/docs/atlas/feature-matrix.md from its
// committed parts: the per-row evidence data, a head, and a tail.
//
// The page is generated so that its summary counts can never drift from its
// body, and so that every row edit happens in the evidence file - which
// records, for each cell, the command that was run or the source cited. The
// first version of this tooling lived outside the repository and was lost to a
// scratch-directory cleanup; that is why it lives here now.
//
// Usage:
//   node scripts/build-feature-matrix.mjs          # rewrite the page
//   node scripts/build-feature-matrix.mjs --check  # fail if the page is stale
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const dataDir = join(scriptDir, 'data');
const pagePath = join(scriptDir, '..', 'src', 'content', 'docs', 'atlas', 'feature-matrix.md');

const SYMBOL = { yes: '✅', partial: '🟡', no: '❌', na: '➖', unknown: '❔' };
const BANNED = /\b(simply|easily|just|seamless|powerful|blazing|effortless|cutting-edge)\b/i;

// Reader-facing section order, folding the audit's fine-grained area labels.
const AREA_MAP = [
  ['Schema sources', ['schema sources', 'go-first modeling']],
  ['Declarative and direct schema changes', ['declarative', 'schema inspection filters']],
  ['Versioned migrations', ['versioned migrations', 'migration directory formats', 'migration import']],
  ['Linting and safety', ['lint', 'migration linting', 'safety gates']],
  ['Testing', ['test frameworks', 'testing framework', 'verification and contracts', 'testing']],
  ['Configuration and dev databases', ['project config', 'atlas project config', 'dev databases', 'configuration and dev databases']],
  ['Databases and schema objects', ['database engines', 'schema object kinds', 'databases and schema objects']],
  ['Go embedding and developer tooling', ['go embedding', 'editor tooling', 'schema visualization', 'api schema export']],
  ['Data and distribution', ['data management', 'oci artifacts', 'data and distribution']],
  ['Atlas Registry and Cloud', ['atlas registry', 'approval and policy']],
];

// A section whose lead row needs more than a cell can carry gets an intro:
// what the verdicts mean for the reader, stated as checkable facts.
const SECTION_INTROS = {
  'Data and distribution': `Ptah's registry story differs from Atlas's in storage, not in function.
Everything artifact distribution needs — publish a migration directory or a
desired schema, pull it elsewhere, pin an exact version, run migrations
straight from the registry — works against any OCI-compliant registry: GHCR,
ECR, GAR, Harbor, Docker Hub, or a self-hosted \`registry:2\`.

For a team this means the registry and credentials already used for container
images also serve schema artifacts; there is no separate account, login verb,
or hosted service to depend on; and a digest pin makes a deployment
reproducible byte for byte. The artifacts are ordinary OCI 1.1 manifests, so
registry-side controls — replication, retention, immutable-tag policy, access
control — come from the registry, not from Ptah. The full workflow is on
[OCI registry artifacts](../../operate/oci-registry/).`,
  'Atlas Registry and Cloud': `These rows are the services Atlas hosts on top of its registry — approvals,
reporting, monitoring, the \`atlas://\` scheme itself. They concern the hosted
service, not artifact storage; the storage function is covered under
[Data and distribution](#data-and-distribution).`,
};

// Every area label a row is allowed to carry. bucket() matches by prefix, so a
// citation appended to an area still routes to a section and the page still
// builds - which is how thirteen rows came to carry another row's evidence in
// their area field. An exact-match roster is what makes that failure loud.
const AREAS = new Set([
  'API schema export',
  'Approval and policy workflows',
  'Atlas Registry and Cloud',
  'Configuration and dev databases',
  'Data and distribution',
  'Data management',
  'Database engines',
  'Declarative / direct schema workflow',
  'Declarative and direct schema changes',
  'Dev databases',
  'Editor tooling',
  'Go embedding',
  'Go embedding and developer tooling',
  'Go-first modeling',
  'Lint analyzers',
  'Lint output and CI',
  'Lint policy and suppression',
  'Linting and safety',
  'Migration directory formats',
  'Migration linting',
  'OCI artifacts',
  'Project config',
  'Safety gates',
  'Schema inspection filters',
  'Schema object kinds',
  'Schema sources',
  'Schema visualization',
  'Test frameworks',
  'Testing framework',
  'Verification and contracts',
  'Versioned migrations',
  'Versioned migrations — directory',
  'Versioned migrations — execution',
  'Versioned migrations — revision state',
  'Versioned migrations — runtime policy',
  'Versioned migrations — safety',
]);

function bucket(area) {
  const text = area.toLowerCase();
  for (const [index, [title, prefixes]] of AREA_MAP.entries()) {
    if (text === title.toLowerCase() || prefixes.some((p) => text.startsWith(p))) {
      return { index, title };
    }
  }
  throw new Error(`unmapped area ${JSON.stringify(area)}: add it to AREA_MAP`);
}

function normalize(feature) {
  return feature
    .toLowerCase()
    .replace(/[`'"()]/g, '')
    .replace(/\b(the|a|an|and|or|for|to|with|as)\b/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function validate(rows) {
  const problems = [];
  const seen = new Map();
  for (const row of rows) {
    if (!AREAS.has(row.area)) {
      problems.push(
        `${row.feature}: area ${JSON.stringify(row.area)} is not a known label - if it is a citation, it belongs in the evidence of the row it settles`,
      );
    }
    if (row.note.length > 200) problems.push(`${row.feature}: note is ${row.note.length} chars`);
    if (BANNED.test(row.note) || BANNED.test(row.feature)) problems.push(`${row.feature}: banned word`);
    for (const key of ['ptah', 'atlas_oss', 'atlas_pro']) {
      if (!(row[key] in SYMBOL)) problems.push(`${row.feature}: bad status ${row[key]}`);
    }
    const key = normalize(row.feature);
    const first = seen.get(key);
    if (first) {
      const clash = ['ptah', 'atlas_oss', 'atlas_pro'].filter((c) => first[c] !== row[c]);
      problems.push(
        clash.length
          ? `${row.feature}: duplicates ${first.feature} and disagrees on ${clash.join(', ')}`
          : `${row.feature}: duplicates ${first.feature}`,
      );
    } else {
      seen.set(key, row);
    }
  }
  if (problems.length > 0) {
    console.error('REFUSING TO BUILD:');
    for (const p of problems) console.error('  ' + p);
    process.exit(1);
  }
}

function summary(rows) {
  const count = (fn) => rows.filter(fn).length;
  const readings = [
    ['Ptah supports it fully', count((r) => r.ptah === 'yes')],
    ['Ptah supports it with a stated limitation', count((r) => r.ptah === 'partial')],
    ['Ptah does not implement it', count((r) => r.ptah === 'no')],
    ['Ptah and Atlas CE both support it', count((r) => r.ptah === 'yes' && r.atlas_oss === 'yes')],
    [
      'Ptah implements it openly where Atlas gates it behind Pro or Cloud',
      count((r) => ['yes', 'partial'].includes(r.ptah) && r.atlas_oss === 'no' && r.atlas_pro === 'yes'),
    ],
    ['Ptah has it and neither Atlas edition does', count((r) => r.ptah === 'yes' && r.atlas_oss === 'no' && r.atlas_pro === 'no')],
    ['Atlas CE has it and Ptah does not, or only in part', count((r) => ['no', 'partial'].includes(r.ptah) && r.atlas_oss === 'yes')],
    [
      'An Atlas column is ❔ — not established by this page\'s evidence',
      count((r) => r.atlas_oss === 'unknown' || r.atlas_pro === 'unknown'),
    ],
  ];
  return [
    '## At a glance',
    '',
    `Across the ${rows.length} capabilities below:`,
    '',
    '| Reading | Count |',
    '| --- | --- |',
    ...readings.map(([label, n]) => `| ${label} | ${n} |`),
    '',
    'Every 🟡 in the Ptah column names its specific limitation, reproduced against a',
    'binary built from this repository; Atlas-column verdicts rest on the cited',
    'Atlas-side sources only. Confirmed gaps are tracked in',
    '[#926 to #942](https://github.com/stokaro/ptah/issues/926) and [#944](https://github.com/stokaro/ptah/issues/944).',
    '',
    'The command surface is counted separately, because it is measured rather than',
    'assessed. The conformance harness inventories every command in the pinned Atlas',
    'CE binary and compares it with the `ptah-compat` surface: 19 of the 37',
    'inventoried commands are open parity targets, and they match on help usage and',
    'flags across 107 observations with one gap — `schema inspect --include`, a',
    'Pro-surface flag the pinned CE binary does not register and Ptah implements',
    'openly ([#951](https://github.com/stokaro/ptah/issues/951)). The remaining 18',
    'are registry, Cloud, or Pro verbs that are not drop-in targets. Ptah implements',
    'seven of them as open capabilities regardless.',
    '',
  ].join('\n');
}

function body(rows) {
  const grouped = new Map();
  const sorted = [...rows].sort((a, b) => {
    const ba = bucket(a.area);
    const bb = bucket(b.area);
    if (ba.index !== bb.index) return ba.index - bb.index;
    return a.feature.toLowerCase().localeCompare(b.feature.toLowerCase());
  });
  for (const row of sorted) {
    const { title } = bucket(row.area);
    if (!grouped.has(title)) grouped.set(title, []);
    grouped.get(title).push(row);
  }
  const out = [];
  const cell = (text) => text.replace(/\|/g, '\\|').trim();
  for (const [title, items] of grouped) {
    out.push(`## ${title}`, '');
    if (SECTION_INTROS[title]) out.push(SECTION_INTROS[title], '');
    out.push('| Capability | Ptah | CE | Pro | Difference |');
    out.push('| --- | :-: | :-: | :-: | --- |');
    for (const row of items) {
      out.push(`| ${cell(row.feature)} | ${SYMBOL[row.ptah]} | ${SYMBOL[row.atlas_oss]} | ${SYMBOL[row.atlas_pro]} | ${cell(row.note)} |`);
    }
    out.push('');
  }
  return out.join('\n');
}

const rows = JSON.parse(readFileSync(join(dataDir, 'feature-matrix-rows.json'), 'utf8'));
validate(rows);
const head = readFileSync(join(dataDir, 'feature-matrix.head.md'), 'utf8');
const tail = readFileSync(join(dataDir, 'feature-matrix.tail.md'), 'utf8');
const page = head + '\n' + summary(rows) + '\n' + body(rows) + '\n' + tail;

if (process.argv.includes('--check')) {
  const current = readFileSync(pagePath, 'utf8');
  if (current !== page) {
    console.error('feature-matrix.md is stale: edit scripts/data/feature-matrix-rows.json and run node scripts/build-feature-matrix.mjs');
    process.exit(1);
  }
  console.log(`build-feature-matrix.mjs --check: OK (${rows.length} rows)`);
} else {
  writeFileSync(pagePath, page);
  console.log(`wrote feature-matrix.md (${rows.length} rows)`);
}
