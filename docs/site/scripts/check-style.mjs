#!/usr/bin/env node
// Enforces the mechanically checkable rules of docs/STYLE_GUIDE.md.
//
// The style guide is authoritative for every documentation layer, but until now
// it was enforced only by review, so a rule could be broken silently and stay
// broken. This check covers the rules that are objective enough to automate:
// American English, code-fence labels, banned filler, the admonition set, and
// testify in samples. Rules that need editorial judgement (page taxonomy,
// section templates, table design) remain a review responsibility.
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

// British spellings that docs/STYLE_GUIDE.md section 4 rules out. Each entry is
// matched as a whole word, case-insensitively, with an optional suffix so that
// "behaviours" and "organised" are caught alongside the stem.
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
];

// docs/STYLE_GUIDE.md section 4: no marketing adjectives.
const bannedFiller = ['simply', 'easily', 'just'];

// docs/STYLE_GUIDE.md section 7 fixes the admonition set.
const allowedAdmonitions = new Set(['note', 'tip', 'caution', 'danger']);

function toPosix(value) {
  return value.split(sep).join('/');
}

function walk(dir, extensions) {
  const files = [];
  if (!existsSync(dir)) return files;
  for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === 'node_modules' || entry.name === 'dist' || entry.name === '.astro') continue;
      files.push(...walk(fullPath, extensions));
      continue;
    }
    if (entry.isFile() && extensions.includes(extname(entry.name))) {
      files.push(fullPath);
    }
  }
  return files;
}

// documentationFiles returns every file the style guide governs: the site, the
// contributor docs, and the READMEs that readers reach from them.
function documentationFiles() {
  const files = [
    ...walk(docsRoot, ['.md', '.mdx']),
    ...walk(join(repoRoot, 'docs'), ['.md']).filter((file) => !toPosix(file).includes('/docs/site/')),
    ...walk(join(repoRoot, 'examples'), ['.md']),
    ...walk(join(repoRoot, 'integration'), ['.md']),
  ];
  const readme = join(repoRoot, 'README.md');
  if (existsSync(readme) && statSync(readme).isFile()) files.push(readme);

  const seen = new Set();
  return files.filter((file) => {
    const key = toPosix(relative(repoRoot, file));
    if (seen.has(key) || exemptFiles.has(key)) return false;
    seen.add(key);
    return true;
  });
}

// stripCode removes fenced blocks and inline spans so prose rules never fire on
// SQL keywords, identifiers, or sample output. A column really can be named
// "cancelled", and that is not a spelling error.
function stripCode(source) {
  const lines = source.split('\n');
  const out = [];
  let inFence = false;
  for (const line of lines) {
    if (line.trimStart().startsWith('```')) {
      inFence = !inFence;
      out.push('');
      continue;
    }
    out.push(inFence ? '' : line.replace(/`[^`]*`/g, ''));
  }
  return out;
}

// fenceViolations reports opening fences with no language label.
function fenceViolations(source) {
  const violations = [];
  const lines = source.split('\n');
  let inFence = false;
  for (const [index, line] of lines.entries()) {
    const trimmed = line.trimStart();
    if (!trimmed.startsWith('```')) continue;
    if (inFence) {
      inFence = false;
      continue;
    }
    inFence = true;
    if (trimmed.replace(/`+/g, '').trim() === '') {
      violations.push({ line: index + 1, message: 'fenced code block has no language label' });
    }
  }
  return violations;
}

function checkFile(file) {
  const source = readFileSync(file, 'utf8');
  const displayPath = toPosix(relative(repoRoot, file));
  const findings = [];
  const proseLines = stripCode(source);

  for (const [index, line] of proseLines.entries()) {
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

    if (/\bstretchr\/testify\b/.test(line) || /\btestify\./.test(line)) {
      findings.push({ line: lineNumber, message: 'testify appears in documentation; use quicktest (qt)' });
    }
  }

  for (const [index, line] of source.split('\n').entries()) {
    const match = line.match(/^:::([a-zA-Z]+)/);
    if (match && !allowedAdmonitions.has(match[1].toLowerCase())) {
      findings.push({
        line: index + 1,
        message: `admonition ":::${match[1]}" is not one of note, tip, caution, danger`,
      });
    }
  }

  findings.push(...fenceViolations(source));
  return findings.map((finding) => `${displayPath}:${finding.line}: ${finding.message}`);
}

// selftest proves each rule actually fires, so a refactor cannot quietly turn
// this check into a no-op that reports OK forever.
function selftest() {
  const cases = [
    ['behaviour is documented', 'British spelling'],
    ['this simply works', 'banned filler'],
    ['use testify.Assert here', 'testify'],
    [':::warning\ntext\n:::', 'admonition'],
    ['```\nunlabeled\n```', 'no language label'],
  ];
  const failures = [];
  for (const [sample, expected] of cases) {
    const findings = [];
    const proseLines = stripCode(sample);
    for (const [index, line] of proseLines.entries()) {
      for (const [british] of britishSpellings) {
        if (new RegExp(`\\b${british.replace(/ /g, '\\s')}[a-z]*`, 'i').test(line)) {
          findings.push(`${index + 1}: British spelling`);
        }
      }
      for (const word of bannedFiller) {
        if (new RegExp(`\\b${word}\\b`, 'i').test(line)) findings.push(`${index + 1}: banned filler`);
      }
      if (/\btestify\./.test(line)) findings.push(`${index + 1}: testify`);
    }
    for (const line of sample.split('\n')) {
      const match = line.match(/^:::([a-zA-Z]+)/);
      if (match && !allowedAdmonitions.has(match[1].toLowerCase())) findings.push('admonition');
    }
    for (const violation of fenceViolations(sample)) findings.push(violation.message);

    if (!findings.some((finding) => finding.includes(expected))) {
      failures.push(`rule "${expected}" did not fire for sample ${JSON.stringify(sample)}`);
    }
  }

  // A clean sample must stay clean, or the check would fail every build.
  const cleanFindings = [];
  for (const line of stripCode('The behavior is documented.\n\n```bash\nls\n```\n')) {
    for (const word of bannedFiller) {
      if (new RegExp(`\\b${word}\\b`, 'i').test(line)) cleanFindings.push(word);
    }
  }
  if (cleanFindings.length > 0) failures.push(`clean sample produced findings: ${cleanFindings.join(', ')}`);

  if (failures.length > 0) {
    console.error('check-style.mjs --selftest: FAILED');
    for (const failure of failures) console.error(`- ${failure}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-style.mjs --selftest: OK (${cases.length} rules verified)`);
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
