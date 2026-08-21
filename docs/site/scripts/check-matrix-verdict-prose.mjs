#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const rowsPath = join(scriptDir, 'data', 'feature-matrix-rows.json');

// A matrix cell records how its verdict was settled, and a verdict changes more
// often than the paragraph explaining it. When a row flips, the new reasoning is
// appended and the old justification stays: it is not wrong *about the past*, so
// neither a reviewer nor build-feature-matrix.mjs --check flags it. That check
// compares the generated page against this file; nothing compares a cell against
// itself. Fifteen rows had accumulated a paragraph arguing for a verdict the row
// no longer carried (stokaro/ptah#1873, #1874), and the failure mode is a later
// reader re-measuring a cell and finding it says the answer should be something
// else.
//
// So this governs one thing only: a cell may not assert, in the present tense,
// that its own row carries a verdict other than the one in its `ptah` field.
// History is the whole point of the evidence field and is deliberately allowed --
// "this row read partial until #1802 landed" is the shape that replaces a
// finding here, not a second violation.

// The subject of a verdict claim. Deliberately does NOT include "oss", because
// "so oss stays partial rather than flipping to no" is a true statement about
// the atlas_oss column, which has its own verdict and is not governed here.
const subject = String.raw`(?:the\s+)?(?:verdict|row|cell|Ptah\s+column)`;

const rules = [
  {
    name: 'asserts-partial',
    // "The verdict stays partial", "The row remains partial", "the cell is partial"
    pattern: new RegExp(String.raw`\b${subject}\s+(?:stays|remains|is|reads)\s+(?:a\s+)?partial\b`, 'i'),
    forbiddenFor: (verdict) => verdict !== 'partial',
    says: 'partial',
  },
  {
    name: 'keeps-partial',
    // "What keeps the cell partial is #1716"
    pattern: /\bkeeps?\s+the\s+(?:row|cell)\s+partial\b/i,
    forbiddenFor: (verdict) => verdict !== 'partial',
    says: 'partial',
  },
  {
    name: 'declines-own-form',
    // "Deliberately NOT reclassified to the own-form verdict: #1229 is open ..."
    //
    // Only a contradiction on a row that DID take that verdict. On a `yes` row the
    // same sentence is true and load-bearing: it records why a capability Atlas
    // hosts is nonetheless ordinary local work, which is why row 188 keeps it.
    pattern: /\bDeliberately\s+NOT\s+reclassified\s+to\s+the\s+own-form\s+verdict\b/i,
    forbiddenFor: (verdict) => verdict === 'own',
    says: 'not the own-form verdict',
  },
];

// analyze reports every cell whose prose claims a verdict its row does not carry.
// Takes the parsed rows so the selftest can drive it with fixtures.
export function analyze(rows) {
  const findings = [];

  for (const [index, row] of rows.entries()) {
    const verdict = row.ptah;
    for (const field of ['note', 'evidence']) {
      const text = row[field];
      if (typeof text !== 'string' || text === '') continue;

      // Rules are written not to overlap, but one sentence matching two of them
      // would otherwise be reported twice and read as two defects. Keep the
      // first rule to claim a span and drop anything overlapping it, so a cell
      // with two genuinely separate offending sentences still reports both.
      const claimed = [];
      for (const rule of rules) {
        if (!rule.forbiddenFor(verdict)) continue;
        const hit = rule.pattern.exec(text);
        if (hit === null) continue;
        const start = hit.index;
        const end = start + hit[0].length;
        if (claimed.some((span) => start < span.end && end > span.start)) continue;
        claimed.push({ start, end });
        findings.push({
          row: index,
          feature: row.feature,
          field,
          rule: rule.name,
          message:
            `row ${index} (${row.feature}) carries verdict "${verdict}" but its ${field} says ` +
            `"${hit[0]}", which claims ${rule.says}`,
        });
      }
    }
  }

  return findings;
}

function selftest() {
  const failures = [];

  // Every shape that must NOT fire. The last two are cells that were reported by
  // a looser sweep during stokaro/ptah#1874 and rejected by hand; they are here
  // so a future widening of the rules has to keep clearing them.
  const clean = [
    { feature: 'past tense is the fix, not a violation', ptah: 'yes', note: '', evidence: 'This row read partial until stokaro/ptah#1802 landed, and what had kept the cell partial was the enum nilling.' },
    { feature: 'a genuinely partial row may say so', ptah: 'partial', note: 'The verdict stays partial.', evidence: 'What keeps the cell partial is stokaro/ptah#1630.' },
    { feature: 'atlas_oss has its own verdict', ptah: 'yes', note: '', evidence: 'The pinned CE binary still registers migrate lint, so oss stays partial rather than flipping to no.' },
    { feature: 'declining own-form on a yes row is true', ptah: 'yes', note: '', evidence: 'Deliberately NOT reclassified to the own-form verdict, even though Atlas hosts this one: it needs no service.' },
    { feature: 'no verdict prose at all', ptah: 'own', note: 'Renders locally.', evidence: 'Measured 2026-08-21.' },
  ];
  for (const finding of analyze(clean)) {
    failures.push(`clean fixture produced a finding: ${finding.message}`);
  }

  const violating = [
    { feature: 'stranded partial justification', ptah: 'yes', note: '', evidence: 'The verdict stays partial because the builder still has genuine gaps.' },
    { feature: 'stranded keeps-partial', ptah: 'yes', note: '', evidence: 'What keeps the cell partial is stokaro/ptah#1716.' },
    { feature: 'stranded own-form refusal', ptah: 'own', note: '', evidence: 'Deliberately NOT reclassified to the own-form verdict: #1229 is an open epic.' },
    { feature: 'the note is governed too', ptah: 'yes', note: 'The row remains partial for Spanner alone.', evidence: '' },
    // One sentence, reported once: "row is partial" is the whole defect, and a
    // second overlapping rule must not turn it into two.
    { feature: 'one sentence is one finding', ptah: 'yes', note: '', evidence: 'That is stokaro/ptah#1802, and it is why this row is partial rather than yes.' },
    // Two separate offending sentences in one field must both survive the dedupe.
    { feature: 'two distinct sentences', ptah: 'yes', note: '', evidence: 'The verdict stays partial. Separately, what keeps the cell partial is the enum nilling.' },
  ];
  const expected = [
    { row: 0, rule: 'asserts-partial' },
    { row: 1, rule: 'keeps-partial' },
    { row: 2, rule: 'declines-own-form' },
    { row: 3, rule: 'asserts-partial' },
    { row: 4, rule: 'asserts-partial' },
    { row: 5, rule: 'asserts-partial' },
    { row: 5, rule: 'keeps-partial' },
  ];
  const actual = analyze(violating);
  for (const [index, want] of expected.entries()) {
    const got = actual[index];
    if (!got) {
      failures.push(`missing finding for row ${want.row} (${want.rule})`);
      continue;
    }
    if (got.row !== want.row || got.rule !== want.rule) {
      failures.push(`finding ${index} was row ${got.row}/${got.rule}; wanted row ${want.row}/${want.rule}`);
    }
  }
  if (actual.length !== expected.length) {
    failures.push(`violating fixture produced ${actual.length} findings, wanted ${expected.length}`);
  }

  // A rule that governs nothing reports OK forever, so prove each one can fire.
  const fired = new Set(actual.map((finding) => finding.rule));
  for (const rule of rules) {
    if (!fired.has(rule.name)) failures.push(`rule ${rule.name} never fired in the violating fixture`);
  }

  if (failures.length > 0) {
    console.error('check-matrix-verdict-prose.mjs --selftest: FAILED');
    for (const failure of failures) console.error(`- ${failure}`);
    process.exitCode = 1;
    return;
  }
  console.log(
    `check-matrix-verdict-prose.mjs --selftest: OK (${clean.length} clean rows, ${expected.length} findings, ${rules.length} rules fired)`,
  );
}

function main() {
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }

  const parsed = JSON.parse(readFileSync(rowsPath, 'utf8'));
  const rows = Array.isArray(parsed) ? parsed : parsed.rows;
  if (!Array.isArray(rows) || rows.length === 0) {
    console.error(`check-matrix-verdict-prose.mjs: ${rowsPath} holds no rows`);
    process.exitCode = 1;
    return;
  }

  const findings = analyze(rows);
  if (findings.length > 0) {
    console.error('Matrix verdict-prose check failed:');
    for (const finding of findings) console.error(`- ${finding.message}`);
    console.error(
      '\nA cell may not argue for a verdict its row does not carry. Re-tense the sentence as history' +
        '\n("this row read partial until #1802 landed") rather than deleting the measurement it holds.',
    );
    process.exitCode = 1;
    return;
  }
  console.log(`check-matrix-verdict-prose.mjs: OK (${rows.length} rows, ${rules.length} rules)`);
}

// analyze() is exported so a caller can run the rules over rows it already
// holds, which is how this was proven against the pre-fix history. Importing it
// must not also run the check, so main() is guarded on being the entry point.
if (process.argv[1] === fileURLToPath(import.meta.url)) main();
