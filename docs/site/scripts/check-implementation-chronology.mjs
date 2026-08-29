#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { proseOf, repositoryFiles } from './check-terminology.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(scriptDir, '..', '..', '..');

// Three phrases that have no present-tense reading.
//
// Section 6.2 of docs/STYLE_GUIDE.md lists sixteen spellings this repository
// has produced, and section 6.3 is why fifteen of them stay unchecked: most
// occurrences are a legitimate present-tense sense. `legacy` is `legacy-tested`,
// `no longer` describes a runtime state, `used to` is usually the purpose sense.
// A deny-list over those words reports the language, not the defect.
//
// These three do not have that problem. "X now supports Y" is a release note
// whichever way it is read; "recently added" has no stable meaning a month
// later; and a Ptah capability that arrives in "a later phase" is a promise with
// no owner. Section 16.1 recorded them as the checkable subset and said to add
// the rule once the pages were rewritten (stokaro/ptah#2504).
const phrases = [
  { pattern: /\bnow supports?\b/gi, why: 'a release note; say what it supports' },
  { pattern: /\brecently added\b/gi, why: 'no stable meaning; say what it is, not when it arrived' },
  { pattern: /\b(?:a|the) later phase\b/gi, why: 'a promise with no owner; state the limitation, or link the issue that owns it' },
];

// The files where this history is the subject rather than an intrusion.
//
// Each is a path with a reason, not a convenience: a design record exists to
// say what was decided and when, conformance evidence exists to record what a
// measurement found, a page about divergences that closed is about the change
// itself, and the style guide has to spell the phrases it governs.
//
// The exemption is by PATH because "is the subject Ptah's own roadmap" is not a
// question a regular expression answers. What that misses is a genuine promise
// written on one of these four, and what it buys is a gate nobody has to argue
// with on the pages where the past is the point.
export const exempt = new Map([
  ['docs/STYLE_GUIDE.md', 'states the phrases the rule governs'],
  ['docs/conformance.md', 'conformance evidence: a measurement is dated by nature'],
  ['docs/site/src/content/docs/atlas/retained-divergences.md', 'its subject is which divergences closed'],
]);

const exemptPrefixes = new Map([
  ['docs/adr/', 'a design record says what was decided, and when'],
]);

export function isExempt(path) {
  if (exempt.has(path)) return true;
  for (const prefix of exemptPrefixes.keys()) {
    if (path.startsWith(prefix)) return true;
  }
  return false;
}

/** The Markdown files this rule reads. */
export function corpus(files) {
  return files.filter((path) => /\.mdx?$/.test(path)).filter((path) => !isExempt(path));
}

/** findingsIn reports every governed phrase in one source's prose. */
export function findingsIn(source) {
  const prose = proseOf(source);
  const found = [];
  for (const { pattern, why } of phrases) {
    pattern.lastIndex = 0;
    let match;
    while ((match = pattern.exec(prose)) !== null) {
      const line = prose.slice(0, match.index).split('\n').length;
      found.push({ line, phrase: match[0], why });
    }
  }
  return found.sort((a, b) => a.line - b.line);
}

function selftest() {
  const cases = [
    { name: 'a Ptah release note is reported', source: 'Ptah now supports include columns.', want: ['now supports'] },
    { name: 'a dated adjective is reported', source: 'The recently added flag does this.', want: ['recently added'] },
    { name: 'an unowned promise is reported', source: 'They return when a later phase can supply one.', want: ['a later phase'] },
    { name: 'the singular verb is reported too', source: 'Ptah now support this.', want: ['now support'] },

    // The clean fixtures section 16.1 named. Each is a shape this must not
    // report, and each is a sentence this repository actually writes.
    { name: 'an Atlas subject is not ours to rewrite', source: 'The pinned community binary refuses it and always has.', want: [] },
    { name: 'a runtime state reads in the present', source: 'The lease is no longer held, so the run is claimable.', want: [] },
    { name: 'legacy-tested is a support level', source: 'PostgreSQL 14 is legacy-tested rather than certified.', want: [] },
    { name: 'the purpose sense of used to', source: 'The digest used to address a generation is taken over its inputs.', want: [] },

    // Fenced blocks and code spans are not prose, which is what proseOf is for:
    // an error string a parity test asserts on must not be rewritten by this.
    { name: 'a fenced block is not prose', source: '```text\nptah now supports nothing\n```', want: [] },
    { name: 'a code span is not prose', source: 'The `now supports` phrase is what this reports.', want: [] },
  ];

  const failures = [];
  for (const { name, source, want } of cases) {
    const got = findingsIn(source).map((finding) => finding.phrase.toLowerCase());
    if (JSON.stringify(got) !== JSON.stringify(want)) {
      failures.push(`${name}: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`);
    }
  }

  // The exemptions are asserted too. A path list that stopped matching would
  // widen the gate silently, and a page added under docs/adr/ would be governed
  // by a rule that is not meant to reach it.
  const exemptCases = [
    ['docs/STYLE_GUIDE.md', true],
    ['docs/adr/0002-read-only-agent-mvp-scope-and-transport.md', true],
    ['docs/site/src/content/docs/atlas/retained-divergences.md', true],
    ['docs/site/src/content/docs/operate/ai-agents.md', false],
    ['README.md', false],
  ];
  for (const [path, want] of exemptCases) {
    if (isExempt(path) !== want) {
      failures.push(`isExempt(${path}): got ${isExempt(path)}, want ${want}`);
    }
  }

  if (failures.length > 0) {
    console.error('check-implementation-chronology.mjs --selftest: FAILED');
    for (const failure of failures) console.error(`  ${failure}`);
    process.exit(1);
  }
  console.log(
    `check-implementation-chronology.mjs --selftest: OK (${cases.length} prose cases, ${exemptCases.length} exemption cases)`,
  );
}

if (process.argv.includes('--selftest')) {
  selftest();
} else {
  const files = corpus(repositoryFiles(repoRoot));
  if (files.length === 0) {
    console.error('check-implementation-chronology.mjs: no Markdown files in the corpus; refusing to report a pass');
    process.exit(1);
  }
  const findings = [];
  for (const path of files) {
    let source;
    try {
      source = readFileSync(join(repoRoot, path), 'utf8');
    } catch {
      continue; // A tracked file the working tree does not have.
    }
    for (const finding of findingsIn(source)) {
      findings.push({ path, ...finding });
    }
  }
  if (findings.length > 0) {
    console.error('check-implementation-chronology.mjs: a phrase with no present-tense reading');
    for (const { path, line, phrase, why } of findings) {
      console.error(`  ${path}:${line}: "${phrase}" -- ${why}`);
    }
    process.exit(1);
  }
  console.log(`check-implementation-chronology.mjs: OK (${files.length} files, ${phrases.length} phrases)`);
}
