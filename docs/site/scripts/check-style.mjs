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

// docs/STYLE_GUIDE.md section 8: "a cell longer than about two rendered lines
// means the row needs a section with a heading instead."
//
// This ceiling is deliberately well above two lines. It is not the style rule;
// it is the point past which a cell has stopped being a cell. Documentation
// once carried comparison cells of 500 to 3,000 characters, which rendered as
// table rows over a hundred lines tall with a two-word neighbor cell floating
// in the whitespace. Judging "dense but fine" against "should have been a
// section" needs a reader; catching an essay in a grid does not.
//
// check-responsive.mjs enforces the sharper limit on rendered height, which is
// what a narrow column actually does to a long cell.
const maxTableCellChars = 350;

// docs/STYLE_GUIDE.md section 4 asks for paragraphs at or under four
// sentences. This ceiling is the point past which a paragraph has stopped
// being a paragraph and become a wall.
//
// Reference pages grew single paragraphs of 1,700 to 3,300 characters that
// enumerated a dozen flags inline, each wrapped in backticks. A reader cannot
// find one flag in that, and cannot tell where its description ends. The
// remedy is a table or a list, and the guide says so; nothing checked it.
//
// Counted on the rendered text, so a paragraph that is mostly `code spans` is
// measured at what a reader has to wade through, not at its markdown length.
const maxParagraphChars = 900;

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
  // Tables are measured on the raw line: stripping inline code would hide the
  // literal pipe that silently eats a column, and would undercount a cell whose
  // content is mostly code tokens.
  const text = [];
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
        text.push('');
        continue;
      }
      // A fence of the other character, or a longer run, is content here.
      prose.push('');
      code.push(line);
      text.push('');
      continue;
    }

    if (marker) {
      fence = { char: marker[1][0], length: marker[1].length };
      if (marker[2].trim() === '') {
        fenceErrors.push({ line: index + 1, message: 'fenced code block has no language label' });
      }
      prose.push('');
      code.push('');
      text.push('');
      continue;
    }

    // Inline spans are code too, and for the same reason.
    prose.push(line.replace(/`[^`]*`/g, ''));
    code.push('');
    text.push(line);
  }

  if (fence) {
    fenceErrors.push({ line: lines.length, message: 'fenced code block is never closed' });
  }
  return { prose, code, text, fenceErrors };
}

// splitCells splits a markdown table row on unescaped pipes. A cell may legally
// contain `\|`, which is one character of content, not a column boundary.
function splitCells(row) {
  const cells = row.trim().split(/(?<!\\)\|/);
  if (cells[0].trim() === '') cells.shift();
  if (cells.length && cells[cells.length - 1].trim() === '') cells.pop();
  return cells.map((cell) => cell.trim().replace(/\\\|/g, '|'));
}

const isDelimiterRow = (cells) => cells.every((cell) => cell === '' || /^[-: ]+$/.test(cell));

// tableViolations reports cells that have outgrown the table they sit in, and
// rows whose column count does not match the header.
//
// The column-count rule exists because the failure is invisible: GFM inserts
// empty cells for a short row and *silently discards* the excess from a long
// one. One unescaped `|` inside a code span - `split | write` - split a cell in
// two and threw the row's tracking links away, on a published page, with a
// green build. Escape it as `\|`.
//
// Link URLs are excluded from the length measurement: an evidence link is one
// glance for a reader however long its href is.
function tableViolations(lines) {
  const findings = [];
  let columns = null;
  let inTable = false;
  for (const [index, line] of lines.entries()) {
    if (!line.trimStart().startsWith('|')) {
      if (line.trim() === '') {
        inTable = false;
        columns = null;
      }
      continue;
    }
    const cells = splitCells(line);
    if (isDelimiterRow(cells)) {
      inTable = true;
      continue;
    }
    if (!inTable) {
      // The header row, which defines the column count for the rows below it.
      columns = cells.length;
      continue;
    }
    if (columns !== null && cells.length !== columns) {
      findings.push({
        line: index + 1,
        message:
          `table row has ${cells.length} cells but the header has ${columns}; ` +
          (cells.length > columns
            ? 'the extra cells are silently discarded when rendered — escape any literal pipe as \\|'
            : 'the missing cells render empty'),
      });
    }
    for (const cell of cells) {
      const text = cell.replace(/\[([^\]]*)\]\([^)]*\)/g, '$1');
      if (text.length > maxTableCellChars) {
        findings.push({
          line: index + 1,
          message:
            `table cell is ${text.length} characters, over the ${maxTableCellChars} limit; ` +
            'give the row its own section with a heading (docs/STYLE_GUIDE.md section 8)',
        });
      }
    }
  }
  return findings;
}

// A line that starts a block which is not flowing prose. List items and their
// indented continuations matter most: a numbered list reads as a list, and
// joining its items would invent a paragraph nobody wrote.
const blockStart =
  /^\s*(?:[-*+]\s|\d+[.)]\s|>|\||#{1,6}\s|:::|<|\[\^[^\]]+\]:|\[[^\]]+\]:\s|\!\[|\s{2,}\S)/;

// paragraphViolations reports prose blocks that have grown into walls. Blank
// lines, headings, tables, lists, list continuations, admonition markers, HTML,
// and footnote definitions all end a paragraph.
function paragraphViolations(lines) {
  const findings = [];
  let buffer = [];
  let start = 0;

  const flush = () => {
    if (buffer.length === 0) return;
    // Measure what a reader sees: code spans keep their text, not their ticks,
    // and a link is its label rather than its href.
    const rendered = buffer
      .join(' ')
      .replace(/`([^`]*)`/g, '$1')
      .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1');
    if (rendered.length > maxParagraphChars) {
      findings.push({
        line: start,
        message:
          `paragraph is ${rendered.length} rendered characters, over the ${maxParagraphChars} limit; ` +
          'a list or a table reads better than a wall (docs/STYLE_GUIDE.md section 4)',
      });
    }
    buffer = [];
  };

  for (const [index, line] of lines.entries()) {
    if (line.trim() === '' || blockStart.test(line)) {
      flush();
      continue;
    }
    if (buffer.length === 0) start = index + 1;
    buffer.push(line.trim());
  }
  flush();
  return findings;
}

// The symbols a status matrix uses. A contradiction is only meaningful between
// cells drawn from a fixed vocabulary; prose cells legitimately differ.
const STATUS_SYMBOLS = new Set(['✅', '🟡', '❌', '🔷', '➖', '❔']);

const NAME_STOPWORDS = new Set(
  'a an the and or of to in on for with is are it its as by from not no only that this all each per via'.split(' '),
);

function nameTokens(cell) {
  const text = cell.replace(/`([^`]*)`/g, '$1').toLowerCase();
  return new Set(
    (text.match(/[a-z0-9][a-z0-9_.-]*/g) ?? []).filter((w) => w.length > 2 && !NAME_STOPWORDS.has(w)),
  );
}

// The code spans in a capability name are its identity: rows named
// "Verb \`migrate ls\`" and "Verb \`migrate show\`" share
// every prose word and are different capabilities. When both names carry code
// and the code differs, the rows are not comparable.
function codeIdentity(cell) {
  return (cell.match(/`[^`]+`/g) ?? []).sort().join('|');
}

function overlap(a, b) {
  if (a.size === 0 || b.size === 0) return 0;
  let shared = 0;
  for (const token of a) if (b.has(token)) shared += 1;
  return shared / (a.size + b.size - shared);
}

// contradictionViolations reports two rows that name the same capability and
// then disagree about it.
//
// A comparison page is assembled from many rows, and a merge can leave the row
// it was meant to supersede in place. When that happens the page states two
// different verdicts for one capability, which is worse than redundancy: a
// reader cannot tell which is current. Requiring BOTH a high name overlap and a
// differing status keeps this off legitimately-similar rows - `migrations test`
// and `schema test` are different commands that share most of their words.
function contradictionViolations(lines) {
  const rows = [];
  for (const [index, line] of lines.entries()) {
    if (!line.trimStart().startsWith('|')) continue;
    const cells = splitCells(line);
    if (isDelimiterRow(cells) || cells.length < 3) continue;
    const statuses = cells.slice(1).filter((cell) => STATUS_SYMBOLS.has(cell));
    if (statuses.length < 2) continue;
    rows.push({ line: index + 1, name: cells[0], tokens: nameTokens(cells[0]), statuses });
  }

  const findings = [];
  for (let i = 0; i < rows.length; i += 1) {
    for (let j = i + 1; j < rows.length; j += 1) {
      // Short names carry too little signal for overlap to mean identity:
      // "Dry run" under two different command sections is two capabilities.
      if (rows[i].tokens.size < 3 || rows[j].tokens.size < 3) continue;
      const idA = codeIdentity(rows[i].name);
      const idB = codeIdentity(rows[j].name);
      if (idA && idB && idA !== idB) continue;
      if (overlap(rows[i].tokens, rows[j].tokens) < 0.6) continue;
      if (rows[i].statuses.join('') === rows[j].statuses.join('')) continue;
      findings.push({
        line: rows[j].line,
        message:
          `row "${rows[j].name}" states ${rows[j].statuses.join('')} while ` +
          `"${rows[i].name}" on line ${rows[i].line} states ${rows[i].statuses.join('')}; ` +
          'the two name the same capability and disagree — supersede one of them',
      });
    }
  }
  return findings;
}

// A claim that a gap is tracked somewhere else. "Tracked" is a promise that an
// owner exists; these are the spellings that make it without naming one.
const trackingClaim = /\btracked\s+(?:separately|elsewhere|in\s+a\s+separate\b)/i;

// An issue reference, in either spelling the documentation uses: a bare #1234
// or a link to the issues tracker.
const issueReference = /#\d+|\/issues\/\d+/;

// trackingClaimViolations reports prose that says a gap is tracked separately
// without naming the tracker.
//
// A reader who meets "that is tracked separately" concludes the gap has an
// owner and stops worrying about it. When no issue exists, the sentence has
// upgraded an open hole into apparent process, and nothing in the repository
// disagrees with it — which is exactly how one shipped on the atlas.sum
// enumeration in this page. Naming the issue keeps the claim checkable; saying
// plainly that nothing tracks it yet is the honest alternative and passes here
// too, because it makes no tracking claim at all.
//
// Scoped to the paragraph so a table cell or a sentence can carry its own
// reference rather than relying on one elsewhere on the page.
function trackingClaimViolations(lines) {
  const findings = [];
  let buffer = [];
  let start = 0;

  const flush = () => {
    if (buffer.length === 0) return;
    const paragraph = buffer.join(' ');
    if (trackingClaim.test(paragraph) && !issueReference.test(paragraph)) {
      findings.push({
        line: start,
        message:
          'claims a gap is "tracked separately" without naming an issue; ' +
          'cite the issue or say plainly that nothing tracks it yet',
      });
    }
    buffer = [];
  };

  for (const [index, line] of lines.entries()) {
    if (line.trim() === '') {
      flush();
      continue;
    }
    if (buffer.length === 0) start = index + 1;
    buffer.push(line.trim());
  }
  flush();
  return findings;
}

// bareFlagViolations reports a --flag written outside a code span. Astro's
// smartypants pass renders a bare double hyphen as an en dash, so the reader
// sees a typographic dash where a flag was meant and copies a broken token.
// Inside backticks nothing is transformed. Applies to site content only:
// root docs render on GitHub, which leaves hyphens alone.
function bareFlagViolations(lines) {
  const findings = [];
  // Inline code spans may wrap across lines inside one paragraph, so backtick
  // parity carries from line to line and resets on a blank line. A per-line
  // strip mispairs the ticks on such lines and reports flags that are inside
  // a span.
  let inSpan = false;
  for (const [index, line] of lines.entries()) {
    if (line.trim() === '') {
      inSpan = false;
      continue;
    }
    const segments = line.split('`');
    let outside = '';
    for (const [si, segment] of segments.entries()) {
      const open = inSpan ? si % 2 === 1 : si % 2 === 0;
      outside += open ? segment : ' '.repeat(segment.length);
      outside += si < segments.length - 1 ? ' ' : '';
    }
    if (segments.length % 2 === 0) inSpan = !inSpan;
    for (const match of outside.matchAll(/(?<![`\w-])--[a-z][a-z0-9-]*/g)) {
      findings.push({
        line: index + 1,
        message:
          `bare flag "${match[0]}" outside a code span renders with an en dash; wrap it in backticks`,
      });
    }
  }
  return findings;
}

// analyze is the single implementation of every rule. checkFile and the
// self-test both call it, so a rule that stops firing here fails the self-test
// instead of quietly passing every file forever.
export function analyze(source, options = {}) {
  const { siteContent = true } = options;
  const findings = [];
  const { prose, code, text, fenceErrors } = splitSource(source);

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

  findings.push(...tableViolations(text));
  findings.push(...paragraphViolations(text));
  findings.push(...contradictionViolations(text));
  findings.push(...trackingClaimViolations(text));
  if (siteContent) findings.push(...bareFlagViolations(text));
  findings.push(...fenceErrors);
  return findings.sort((a, b) => a.line - b.line);
}

function checkFile(file) {
  const displayPath = toPosix(relative(repoRoot, file));
  const siteContent = toPosix(file).includes('/docs/site/src/content/docs/');
  return analyze(readFileSync(file, 'utf8'), { siteContent }).map(
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
    '',
    '| Area | Detail |',
    '| --- | --- |',
    `| Essay in a grid | ${'word '.repeat(80).trim()} |`,
    '',
    // An unescaped pipe inside a code span. GFM discards the extra cell, so the
    // row loses its last column with no error anywhere.
    '| Gap | Boundary | Tracking |',
    '| --- | --- | --- |',
    '| Exports | HCL/SQL `split | write` file exports | #510 |',
    '',
    // Two rows naming one capability and disagreeing about it. A merge that
    // fails to supersede the old row leaves exactly this shape.
    '| Capability | Ptah | CE |',
    '| --- | --- | --- |',
    '| OCI desired-schema artifacts | ✅ | ❌ |',
    '| Desired-schema artifacts over OCI | ✅ | 🟡 |',
    '',
    `A wall of prose. ${'Every flag is described inline rather than in a list. '.repeat(20)}`,
    '',
    // A contradiction expressed through non-boolean symbols, and an overlap
    // just above the threshold: pins the vocabulary and the 0.6 cut from below.
    '| Capability | Ptah | CE |',
    '| --- | --- | --- |',
    '| Spanner live coverage probes | ➖ | ❔ |',
    '| Spanner nightly coverage probes | ❌ | ❔ |',
    '',
    // A row with FEWER cells than its header exercises the missing-cell branch.
    '| A | B | C |',
    '| --- | --- | --- |',
    '| only | two |',
    '',
    // An empty middle cell must not make the row a delimiter: the long third
    // cell must still be measured.
    '| H1 | H2 | H3 |',
    '| --- | --- | --- |',
    `| left |  | ${'y'.repeat(360)} |`,
    '',
    // A hard-wrapped wall: each source line is short, the joined block is not.
    ...Array.from({ length: 16 }, () => 'This wall is wrapped at ordinary width like real documentation prose.'),
    '',
    // A bare flag outside any code span.
    'Pass --dry-run to preview the plan.',
    '',
    // A gap declared tracked with no issue named anywhere in the paragraph.
    'Gating the two verbs is deferred. That is tracked separately, because it',
    'interacts with the bootstrap flow.',
    '',
    // A contradiction where one verdict is 🔷. The own-form symbol has to be in
    // the status vocabulary: a row carrying an unrecognized symbol drops below
    // the two-status floor and stops being compared with anything, so a stale
    // row beside it would never be reported.
    '| Capability | Ptah | CE |',
    '| --- | --- | --- |',
    '| Registry artifact promotion path | 🔷 | ❌ |',
    '| Artifact promotion path for a registry | ❌ | ❌ |',
  ].join('\n');

  const expected = [
    { line: 1, needle: 'British spelling' },
    { line: 3, needle: 'banned filler' },
    { line: 5, needle: 'admonition' },
    { line: 10, needle: 'testify' },
    { line: 13, needle: 'no language label' },
    { line: 17, needle: 'no language label' },
    { line: 23, needle: 'over the 350 limit' },
    { line: 27, needle: 'cells but the header has 3' },
    { line: 32, needle: 'name the same capability and disagree' },
    { line: 34, needle: 'over the 900 limit' },
    { line: 39, needle: 'name the same capability and disagree' },
    { line: 43, needle: 'missing cells render empty' },
    { line: 47, needle: 'over the 350 limit' },
    { line: 49, needle: 'over the 900 limit' },
    { line: 66, needle: 'bare flag "--dry-run"' },
    { line: 68, needle: 'without naming an issue' },
    { line: 74, needle: 'name the same capability and disagree' },
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
    // Near-identical names carrying the SAME verdict. The rule targets
    // contradictions, so this pair must stay silent; without it, deleting the
    // same-status guard would go unnoticed.
    '| Capability | Ptah | CE |',
    '| --- | --- | --- |',
    '| OCI migration artifacts | ✅ | ❌ |',
    '| OCI migration artifacts and tags | ✅ | ❌ |',
    '',
    // A numbered list and an indented list continuation. Both were measured as
    // one giant paragraph by the first version of the length rule, which would
    // have flagged well-formed lists as walls.
    '1. Use `--include-tables` rather than exporting the entire model by default.',
    '2. Inspect every generated field, including identifiers, audit columns, and',
    '   server-managed values, because an additive database column can be an',
    '   additive but unintended API change and the generated diff is the only',
    '   place that shows it, which is why this review step exists at all.',
    '3. Define authorization, tenant isolation, validation, and assignment rules',
    '   in the implementing service rather than in the exported schema, which',
    '   cannot express them and should not appear to a consumer to express them.',
    '4. Run the target linter or compiler and the consumer compatibility tests',
    '   before publishing the artifact, so a breaking change is caught by a tool',
    '   rather than by the first consumer to upgrade past it in production.',
    '5. Review the generated diff whenever the database model changes, treating',
    '   every added, removed, or retyped field as a deliberate contract change',
    '   that a reviewer has to approve rather than as incidental churn.',
    '',
    // A code span that wraps across two lines carries a flag inside it; the
    // backtick parity must carry over so this never reports.
    'Integrity checks such as `ptah migrations',
    'validate` and `up --verify-sum` still pass on a clean directory.',
    '',
    // Same prose words, different code spans: different capabilities, and the
    // differing verdicts are legitimate.
    '| Capability | Ptah | CE |',
    '| --- | --- | --- |',
    '| Verb `migrate ls` | 🟡 | ❌ |',
    '| Verb `migrate show` | ❌ | ❌ |',
    '',
    // Names too short to compare: two-token labels under different sections.
    '| Capability | Ptah | CE |',
    '| --- | --- | --- |',
    '| Dry run | ✅ | ✅ |',
    '',
    '| Capability | Ptah | CE |',
    '| --- | --- | --- |',
    '| Dry run | ✅ | ❌ |',
    '',
    // A reference-link definition block is not a paragraph wall.
    '[cli-surface]: https://github.com/stokaro/ptah-atlas-conformance/blob/main/cli-surface.md',
    '[gaps]: https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md',
    '[gaps-live]: https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md',
    '[gaps-diff]: https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md',
    '[parity]: https://github.com/stokaro/ptah-atlas-conformance/blob/main/PARITY.md',
    '[features]: https://atlasgo.io/features',
    '[cli-ref]: https://atlasgo.io/cli-reference',
    '[declarative]: https://atlasgo.io/declarative/plan',
    '[versioned-apply]: https://atlasgo.io/versioned/apply',
    '[versioned-down]: https://atlasgo.io/versioned/down',
    '[versioned-import]: https://atlasgo.io/versioned/import',
    '[cloud-deploy]: https://atlasgo.io/cloud/deployment',
    '',
    // Under the limit as a reader sees it, over it as raw markdown. Only the
    // rendered measurement keeps this from being reported.
    'Reference prose is dense with flag names, so the measurement counts what a reader reads rather than the markdown around it: `--flag-00` `--flag-01` `--flag-02` `--flag-03` `--flag-04` `--flag-05` `--flag-06` `--flag-07` `--flag-08` `--flag-09` `--flag-10` `--flag-11` `--flag-12` `--flag-13` `--flag-14` `--flag-15` `--flag-16` `--flag-17` `--flag-18` `--flag-19` `--flag-20` `--flag-21` `--flag-22` `--flag-23` `--flag-24` `--flag-25` `--flag-26` `--flag-27` `--flag-28` `--flag-29` `--flag-30` `--flag-31` `--flag-32` `--flag-33` `--flag-34` `--flag-35` `--flag-36` `--flag-37` `--flag-38` `--flag-39` `--flag-40` `--flag-41` `--flag-42` `--flag-43` `--flag-44` `--flag-45` `--flag-46` `--flag-47` `--flag-48` `--flag-49` `--flag-50` `--flag-51` `--flag-52` `--flag-53` `--flag-54` `--flag-55` `--flag-56` `--flag-57` `--flag-58` `--flag-59` `--flag-60` `--flag-61` `--flag-62` `--flag-63` `--flag-64` `--flag-65` `--flag-66` `--flag-67` `--flag-68` `--flag-69`, and see [the command reference](https://example.com/a/long/documentation/url/that/inflates/raw/markdown) for the rest of them.',
    '',
    // American words that begin with a British stem in the deny-list. Each one
    // was a live false positive during review, so each stays asserted here.
    'An optimistic specialist and a finalist otherwise exercise a raise, and',
    'a concise promise likewise comprises expertise in an enterprise.',
    '',
    // A table whose cells are short, one of which contains an escaped pipe and
    // a long link. Neither may be miscounted as an oversized cell.
    '| Command | Meaning |',
    '| --- | --- |',
    '| `migrate rebase {name \\| version}` | Re-timestamp one migration. |',
    `| Evidence | See [the report](https://example.com/${'p'.repeat(400)}). |`,
    '',
    // Both honest shapes of the tracking rule: one names its issue, the other
    // says outright that nothing tracks the gap. Without this pair the rule
    // could be reduced to "never write the word tracked" and still selftest.
    'Gating the two verbs is tracked separately in',
    '[#1234](https://github.com/stokaro/ptah/issues/1234).',
    '',
    'The two verbs stay divergent. No issue tracks closing that gap yet.',
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
