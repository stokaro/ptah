#!/usr/bin/env node
// Enforces the mechanically checkable rules of docs/STYLE_GUIDE.md.
//
// The style guide is authoritative for every documentation layer, but until now
// it was enforced only by review, so a rule could be broken silently and stay
// broken. This check covers the rules that are objective enough to automate:
// American English, code-fence labels, banned filler, the admonition set, and
// testify in samples. Rules that need editorial judgment (page taxonomy,
// section templates, table design) remain a review responsibility.
//
// Usage:
//   node scripts/check-style.mjs [--selftest]
//
// The check has no npm dependencies on purpose: CI runs it from a bare checkout
// for changes that touch no site page at all.
import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, extname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const repoRoot = join(siteRoot, '..', '..');
const docsRoot = join(siteRoot, 'src', 'content', 'docs');

// docs/STYLE_GUIDE.md defines the banned words, so it necessarily contains
// them. It is the rule source, not prose that has to obey the rule.
const exemptFiles = new Set(['docs/STYLE_GUIDE.md']);

// Directories that never hold governed documentation. Test fixtures are
// excluded because their contents are inputs to a test, not prose a reader
// meets; build output and dependencies are excluded because they are generated.
const skipDirectories = new Set([
  'node_modules',
  'dist',
  'bin',
  'testdata',
  'test-reports',
  'coverage',
  'tmp',
]);

// British spellings that docs/STYLE_GUIDE.md section 4 rules out. Each entry is
// matched as a whole word, case-insensitively, with an optional suffix so that
// "behaviours" and "organised" are caught alongside the stem.
//
// Every stem here must be one that no American word begins with, because the
// suffix is open-ended. That rules out otherwise-tempting entries: "optimis"
// would flag "optimistic", "specialis" would flag "specialist", and "finalis"
// would flag "finalist".
const britishSpellings = [
  ['behaviour', 'behavior'],
  ['colour', 'color'],
  ['cancelled', 'canceled'],
  ['cancelling', 'canceling'],
  ['organis', 'organiz'],
  ['recognis', 'recogniz'],
  ['initialis', 'initializ'],
  ['normalis', 'normaliz'],
  ['serialis', 'serializ'],
  ['synchronis', 'synchroniz'],
  ['authoris', 'authoriz'],
  ['analyse', 'analyze'],
  ['licence', 'license'],
  ['defence', 'defense'],
  ['centre', 'center'],
  ['catalogue', 'catalog'],
  ['dialogue box', 'dialog box'],
  ['favour', 'favor'],
  ['honour', 'honor'],
  ['labelled', 'labeled'],
  ['labelling', 'labeling'],
  ['modelling', 'modeling'],
  ['travelled', 'traveled'],
  ['fulfil ', 'fulfill '],
  ['whilst', 'while'],
  ['amongst', 'among'],
  ['judgement', 'judgment'],
  ['acknowledgement', 'acknowledgment'],
  // Ptah publishes OCI artifacts, so this one is a live risk, not a curiosity.
  ['artefact', 'artifact'],
  ['programme', 'program'],
  ['practise', 'practice'],
  ['dependant', 'dependent'],
  ['enquire', 'inquire'],
  ['analogue', 'analog'],
  ['sceptic', 'skeptic'],
  ['offence', 'offense'],
  ['pretence', 'pretense'],
  ['learnt', 'learned'],
  ['metre', 'meter'],
  ['grey', 'gray'],
  ['flavour', 'flavor'],
  ['neighbour', 'neighbor'],
  ['endeavour', 'endeavor'],
  ['rigour', 'rigor'],
  ['utilis', 'utiliz'],
  ['customis', 'customiz'],
  ['summaris', 'summariz'],
  ['categoris', 'categoriz'],
  ['standardis', 'standardiz'],
  ['prioritis', 'prioritiz'],
  ['visualis', 'visualiz'],
  ['minimis', 'minimiz'],
  ['maximis', 'maximiz'],
];

// docs/STYLE_GUIDE.md section 4: no marketing adjectives.
const bannedFiller = ['simply', 'easily', 'just'];

// docs/STYLE_GUIDE.md section 7 fixes the admonition set.
const allowedAdmonitions = new Set(['note', 'tip', 'caution', 'danger']);

function toPosix(value) {
  return value.split(sep).join('/');
}

function walk(dir, matches) {
  const files = [];
  if (!existsSync(dir)) return files;
  for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      // Dot directories hold tooling, git state, and nested worktrees, none of
      // which are reader-facing documentation.
      if (entry.name.startsWith('.') || skipDirectories.has(entry.name)) continue;
      files.push(...walk(fullPath, matches));
      continue;
    }
    if (entry.isFile() && matches(entry.name)) files.push(fullPath);
  }
  return files;
}

const markdown = (name) => extname(name) === '.md' || extname(name) === '.mdx';
const readme = (name) => name === 'README.md';

// documentationFiles returns every file the style guide governs: the site, the
// repository docs, the example and integration docs, every package README, and
// the contributor entry point that mandates the guide.
function documentationFiles() {
  const files = [
    ...walk(docsRoot, markdown),
    ...walk(join(repoRoot, 'docs'), markdown).filter((file) => !toPosix(file).includes('/docs/site/')),
    ...walk(join(repoRoot, 'examples'), markdown),
    ...walk(join(repoRoot, 'integration'), markdown),
    // Package READMEs are in scope but scattered, so they are discovered rather
    // than listed: a new package must not be able to opt out by existing.
    ...walk(repoRoot, readme),
  ];
  for (const name of ['README.md', 'AGENTS.md']) {
    const path = join(repoRoot, name);
    if (existsSync(path) && statSync(path).isFile()) files.push(path);
  }

  const seen = new Set();
  return files.filter((file) => {
    const key = toPosix(relative(repoRoot, file));
    if (seen.has(key) || exemptFiles.has(key)) return false;
    seen.add(key);
    return true;
  });
}

// splitSource separates prose from fenced code, keeping both arrays aligned to
// the original line numbers so a finding can name the line a reader would edit.
//
// Prose rules must never fire inside code: a column really can be named
// "cancelled". Code rules must never fire outside it: docs/STYLE_GUIDE.md and
// AGENTS.md both name `testify` in prose in order to ban it.
function splitSource(source) {
  const lines = source.split('\n');
  const prose = [];
  const code = [];
  const fenceErrors = [];
  let fence = null;

  for (const [index, line] of lines.entries()) {
    const trimmed = line.trimStart();
    const marker = trimmed.match(/^(`{3,}|~{3,})(.*)$/);

    if (fence) {
      const closes =
        marker && marker[1][0] === fence.char && marker[1].length >= fence.length && marker[2].trim() === '';
      if (closes) {
        fence = null;
        prose.push('');
        code.push('');
        continue;
      }
      // A fence of the other character, or a longer run, is content here.
      prose.push('');
      code.push(line);
      continue;
    }

    if (marker) {
      fence = { char: marker[1][0], length: marker[1].length };
      if (marker[2].trim() === '') {
        fenceErrors.push({ line: index + 1, message: 'fenced code block has no language label' });
      }
      prose.push('');
      code.push('');
      continue;
    }

    // Inline spans are code too, and for the same reason.
    prose.push(line.replace(/`[^`]*`/g, ''));
    code.push('');
  }

  if (fence) {
    fenceErrors.push({ line: lines.length, message: 'fenced code block is never closed' });
  }
  return { prose, code, fenceErrors };
}

// analyze is the single implementation of every rule. checkFile and the
// self-test both call it, so a rule that stops firing here fails the self-test
// instead of quietly passing every file forever.
export function analyze(source) {
  const findings = [];
  const { prose, code, fenceErrors } = splitSource(source);

  for (const [index, line] of prose.entries()) {
    const lineNumber = index + 1;

    for (const [british, american] of britishSpellings) {
      const pattern = new RegExp(`\\b${british.replace(/ /g, '\\s')}[a-z]*`, 'gi');
      for (const match of line.matchAll(pattern)) {
        findings.push({
          line: lineNumber,
          message: `British spelling "${match[0]}"; use American English ("${american.trim()}")`,
        });
      }
    }

    for (const word of bannedFiller) {
      for (const match of line.matchAll(new RegExp(`\\b${word}\\b`, 'gi'))) {
        findings.push({
          line: lineNumber,
          message: `banned filler "${match[0]}"; state the action instead`,
        });
      }
    }
  }

  // The guide bans testify in samples, and a sample is a code block, so this
  // rule reads the code side rather than the prose side.
  for (const [index, line] of code.entries()) {
    if (/\bstretchr\/testify\b/.test(line) || /\btestify\./.test(line)) {
      findings.push({ line: index + 1, message: 'testify appears in a code sample; use quicktest (qt)' });
    }
  }

  for (const [index, line] of prose.entries()) {
    const match = line.match(/^:::([a-zA-Z]+)/);
    if (match && !allowedAdmonitions.has(match[1].toLowerCase())) {
      findings.push({
        line: index + 1,
        message: `admonition ":::${match[1]}" is not one of note, tip, caution, danger`,
      });
    }
  }

  findings.push(...fenceErrors);
  return findings.sort((a, b) => a.line - b.line);
}

function checkFile(file) {
  const displayPath = toPosix(relative(repoRoot, file));
  return analyze(readFileSync(file, 'utf8')).map(
    (finding) => `${displayPath}:${finding.line}: ${finding.message}`,
  );
}

// selftest drives the production analyze() over fixtures, so it fails whenever a
// rule stops firing or starts firing on clean prose. Re-implementing the rules
// here would let a broken checker keep reporting OK.
function selftest() {
  const failures = [];

  const violating = [
    'The behaviour is documented.',
    '',
    'This simply works.',
    '',
    ':::warning',
    'text',
    ':::',
    '',
    '```go',
    'import "github.com/stretchr/testify/require"',
    '```',
    '',
    '```',
    'unlabeled',
    '```',
    '',
    '~~~',
    'tilde fence, also unlabeled',
    '~~~',
  ].join('\n');

  const expected = [
    { line: 1, needle: 'British spelling' },
    { line: 3, needle: 'banned filler' },
    { line: 5, needle: 'admonition' },
    { line: 10, needle: 'testify' },
    { line: 13, needle: 'no language label' },
    { line: 17, needle: 'no language label' },
  ];

  const findings = analyze(violating);
  for (const { line, needle } of expected) {
    if (!findings.some((finding) => finding.line === line && finding.message.includes(needle))) {
      failures.push(`rule "${needle}" did not fire on line ${line}`);
    }
  }

  // Every rule must leave correct prose alone, and must not reach into code.
  // Both halves matter: a check that fires on valid content is as broken as one
  // that never fires, and it is the half that gets a rule deleted.
  const clean = [
    'The behavior is documented and a column may be named `cancelled`.',
    '',
    '```sql',
    'SELECT * FROM orders WHERE status = \'cancelled\';',
    '```',
    '',
    'Never use testify in Ptah tests; use `quicktest` instead.',
    '',
    ':::note',
    'Adjust the value; this sentence contains no banned word.',
    ':::',
    '',
    '```text',
    'plain output',
    '```',
    '',
    // American words that begin with a British stem in the deny-list. Each one
    // was a live false positive during review, so each stays asserted here.
    'An optimistic specialist and a finalist otherwise exercise a raise, and',
    'a concise promise likewise comprises expertise in an enterprise.',
  ].join('\n');

  for (const finding of analyze(clean)) {
    failures.push(`clean fixture produced a finding at line ${finding.line}: ${finding.message}`);
  }

  // An unterminated fence would otherwise swallow the rest of the file and
  // silence every later rule, so it is reported rather than tolerated.
  if (!analyze('```go\nnever closed\n').some((finding) => finding.message.includes('never closed'))) {
    failures.push('unterminated fence was not reported');
  }

  if (failures.length > 0) {
    console.error('check-style.mjs --selftest: FAILED');
    for (const failure of failures) console.error(`- ${failure}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-style.mjs --selftest: OK (${expected.length} rule assertions via analyze())`);
}

function main() {
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }

  const files = documentationFiles();
  const errors = files.flatMap(checkFile);

  if (errors.length > 0) {
    console.error('Documentation style check failed:');
    for (const error of errors) console.error(`- ${error}`);
    console.error('\nSee docs/STYLE_GUIDE.md for the rules these findings reference.');
    process.exitCode = 1;
    return;
  }
  console.log(`check-style.mjs: OK (${files.length} documentation files)`);
}

main();
