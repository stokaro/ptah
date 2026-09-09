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
//
// Each is built with `gap` rather than written with a space, because prose
// wraps: at 80 columns the phrase lands with one word at the end of a line and
// the next at the start of the following one, and a pattern holding a literal
// space cannot see it. Nothing in the tree is written that way today, so this
// buys no finding -- it keeps the rule from going quietly out of effect the
// first time somebody reflows a paragraph. internal/countsubjectguard and
// check-style.mjs read a paragraph for the same reason (stokaro/ptah#3143).
const gap = String.raw`(?:[ \t]+|[ \t]*\n[ \t]*)`;

const phrases = [
  { pattern: new RegExp(String.raw`\bnow${gap}supports?\b`, 'gi'), why: 'a release note; say what it supports' },
  { pattern: new RegExp(String.raw`\brecently${gap}added\b`, 'gi'), why: 'no stable meaning; say what it is, not when it arrived' },
  {
    pattern: new RegExp(String.raw`\b(?:a|the)${gap}later${gap}phase\b`, 'gi'),
    why: 'a promise with no owner; state the limitation, or link the issue that owns it',
  },
];

// A clause dating a statement to a Ptah issue -- the fourth shape with no
// present-tense reading, and the one stokaro/ptah#3134 is about. The code is in
// front of the reader and `git log -L` answers the rest, so keep the rule, the
// ablation or the measurement and drop the date; an issue that still owns
// something is cited as "(#N)" rather than dated to.
//
// `after` is deliberately absent. "after #N lands" is a forward reference to
// work with an owner, and no regular expression separates it from the backward
// reading. internal/chronologyguard holds the same four spellings over Go
// comments, which this reader cannot see.
//
// Two spellings, because a reference is written two ways and only one of them
// survives the code-span blanking below: the number in the text, and -- when a
// page writes [`stokaro/ptah#3117`](.../issues/3117) -- the link's target.
const datingClause = {
  patterns: [
    /\b(until|before|since|as of)\s+\[?\s*(?:stokaro\/ptah)?#([0-9]{3,5})\b/gi,
    /\b(until|before|since|as of)\s+\[[^\]\n]{0,160}\]\(https?:\/\/github\.com\/stokaro\/ptah\/(?:issues|pull)\/([0-9]+)/gi,
  ],
  why: 'dates a statement to an issue; say what holds now and cite the issue plainly',
};

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
      found.push({ line, phrase: match[0].replace(/\s+/g, ' '), why });
    }
  }

  // The dating clause is read from the source with fenced blocks removed rather
  // than from proseOf's output. A page writes the reference as
  // [`stokaro/ptah#3117`](https://github.com/stokaro/ptah/issues/3117), and
  // proseOf blanks both the code span and the link target -- every character
  // that identifies the reference lives inside one of the two. A fenced block
  // is still not prose, so a sample of the shape stays quotable.
  const dated = datingText(source);
  // Keyed by where the clause starts, because the two spellings overlap on
  // `until [#982](.../issues/982)`: the number is in the text AND in the target,
  // so both patterns match, at the same offset, and one clause is one finding.
  const dating = new Map();
  for (const pattern of datingClause.patterns) {
    pattern.lastIndex = 0;
    let match;
    while ((match = pattern.exec(dated)) !== null) {
      const line = dated.slice(0, match.index).split('\n').length;
      // The reported phrase is normalized rather than quoted raw. Under the
      // second pattern the raw match carries a blanked code span and a URL,
      // which names the finding worse than the clause itself does.
      const phrase = `${match[1]} #${match[2]}`;
      if (!dating.has(match.index)) dating.set(match.index, { line, phrase, why: datingClause.why });
    }
  }
  found.push(...dating.values());
  return found.sort((a, b) => a.line - b.line);
}

/**
 * datingText is the stream the dating rule reads: the source with fenced blocks
 * removed and code spans blanked, and with link targets left alone.
 *
 * It is not proseOf's output, which blanks the link target too -- and the target
 * is the only part of [`stokaro/ptah#3117`](.../issues/3117) that survives the
 * code-span blanking. Blanking the span is what lets a page quote the banned
 * shape in order to name it, which AGENTS.md and section 6.7 of the style guide
 * both do.
 */
function datingText(source) {
  // A code span may wrap onto the next line, and in prose written to 80 columns
  // most of them do. It may not cross a blank line, which is what keeps an
  // unpaired backtick from blanking the rest of the page.
  //
  // The newlines inside a span are kept. Blanking them too would shorten the
  // text by a line for every wrapped span above a finding, and every line
  // number this gate reports after the first one would name the wrong line.
  return outsideFences(source).replace(/`+[^`]*?`+/g, (span) =>
    span.includes('\n\n') ? span : span.replace(/[^\n]/g, ' '),
  );
}

/** outsideFences blanks the fenced code blocks in one source, keeping line count. */
function outsideFences(source) {
  const lines = source.split('\n');
  let fence = null;
  return lines
    .map((line) => {
      const marker = line.trimStart().match(/^(`{3,}|~{3,})(.*)$/);
      if (fence) {
        if (marker && marker[1][0] === fence.char && marker[1].length >= fence.length && marker[2].trim() === '') {
          fence = null;
        }
        return '';
      }
      if (marker) {
        fence = { char: marker[1][0], length: marker[1].length };
        return '';
      }
      return line;
    })
    .join('\n');
}

function selftest() {
  const cases = [
    { name: 'a Ptah release note is reported', source: 'Ptah now supports include columns.', want: ['now supports'] },
    { name: 'a dated adjective is reported', source: 'The recently added flag does this.', want: ['recently added'] },
    { name: 'an unowned promise is reported', source: 'They return when a later phase can supply one.', want: ['a later phase'] },
    { name: 'the singular verb is reported too', source: 'Ptah now support this.', want: ['now support'] },
    {
      name: 'the phrase wrapped onto the next line',
      source: 'Ptah now\nsupports include columns.',
      want: ['now supports'],
    },
    {
      name: 'an unowned promise wrapped onto the next line',
      source: 'They return when a\nlater phase can supply one.',
      want: ['a later phase'],
    },
    {
      // A word ending one paragraph is not part of the next paragraph's first
      // phrase. Without the boundary the gap would span the blank line and
      // report a sentence nobody wrote.
      name: 'across a paragraph break',
      source: 'The paragraph ends on the word now\n\nsupports arrived with the release.',
      want: [],
    },

    // The dating clause. The reference is written four ways across this site,
    // and the rule has to reach every one: a linked code span, a bare number, a
    // clause the wrapping split from its reference, and the plain spelling.
    {
      // The number lives only in the link target here, because the visible text
      // is a code span and the blanking takes it.
      name: 'a linked reference dated to',
      source: 'It drove the exit code, until [`stokaro/ptah#3117`](https://github.com/stokaro/ptah/issues/3117).',
      want: ['until #3117'],
    },
    { name: 'a bare reference dated to', source: 'It has read a foreign layout since #1013.', want: ['since #1013'] },
    {
      // One clause, matched by both patterns at one offset, reported once.
      name: 'a clause the wrapping split from its reference',
      source: 'The second was missing until\n[#1231](https://github.com/stokaro/ptah/issues/1231).',
      want: ['until #1231'],
    },
    { name: 'the plain spelling', source: 'It was true before stokaro/ptah#934.', want: ['before #934'] },
    {
      // The shape a page names in order to ban it. Every other rule in this
      // tree skips a code span for the same reason.
      name: 'a code span quoting the shape',
      source: 'Never write `until stokaro/ptah#1048` in a comment.',
      want: [],
    },
    {
      // The same, wrapped the way prose written to 80 columns wraps it. This is
      // the shape AGENTS.md writes, and blanking per line leaves it reported.
      name: 'a code span that wraps',
      source: 'Write the ablation, `without this call every write\nwins`, not `before #2391 nothing called\nit`.',
      want: [],
    },

    // A citation is a pointer, not a date: no preposition stands in front of
    // it. `after` is left to the prose rule -- the forward reading names work
    // with an owner, and no pattern separates it from the backward one.
    { name: 'a plain citation', source: 'The refusal is a preflight ([#1086](https://github.com/stokaro/ptah/issues/1086)).', want: [] },
    { name: 'a forward reference with after', source: 'This collapses after [#2725](https://github.com/stokaro/ptah/issues/2725) lands.', want: [] },
    { name: 'a preposition about the code', source: 'The gate runs before the connection is opened.', want: [] },
    { name: 'a preposition about a server', source: 'MariaDB has had SEQUENCE since 10.3.', want: [] },
    { name: 'a fenced sample of the shape', source: '```text\nuntil stokaro/ptah#1048\n```', want: [] },

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

  // A finding names a line, and a reader goes to it. Blanking a wrapped code
  // span without keeping its newlines shortens the text and moves every line
  // after it, so the rule reports a real occurrence at a line that does not
  // hold one -- which is worse than not reporting it, because the reader
  // concludes the gate is wrong.
  const lineCases = [
    { name: 'a finding after a wrapped code span keeps its line', source: 'A `span that\nwraps` here.\n\nIt held until #3116.\n', want: 4 },
    { name: 'a finding after a fenced block keeps its line', source: '```text\na\nb\n```\n\nIt held until #3116.\n', want: 6 },
  ];
  for (const { name, source, want } of lineCases) {
    const got = findingsIn(source).map((finding) => finding.line);
    if (JSON.stringify(got) !== JSON.stringify([want])) {
      failures.push(`${name}: got ${JSON.stringify(got)}, want [${want}]`);
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
  console.log(
    `check-implementation-chronology.mjs: OK (${files.length} files, ${phrases.length + 1} phrases)`,
  );
}
