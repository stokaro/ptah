#!/usr/bin/env node
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const docsRoot = join(siteRoot, 'src', 'content', 'docs');

// Pages whose "Limitations" list is a disposition record rather than prose: a
// reader must be able to tell, per bullet, whether somebody owns the gap or
// whether it is a decision that will not change. A page is opted in by being
// listed here; a listed page that disappears is an error rather than a silent
// pass, because a checker that governs zero files reports OK forever.
const governedPages = ['schema/protobuf.md'];

// A tracking link in the spelling the documentation already uses elsewhere,
// for example [#904](https://github.com/stokaro/ptah/issues/904). The label and
// the target are captured separately so a bullet cannot cite one issue and link
// another.
const trackingLink = /\[#(\d+)\]\(https:\/\/github\.com\/stokaro\/ptah\/issues\/(\d+)\)/;

// The permanence clause. "Permanent" alone is too weak to accept: a bullet has
// to say why, because that reason is what a later reader argues with.
const permanenceClause = /\bis permanent because\b/;

// sectionLines returns the lines of the "## Limitations" section, each with its
// 1-based line number in the file, or null when the page has no such section.
function sectionLines(source) {
  const lines = source.split('\n');
  const start = lines.findIndex((line) => /^##\s+Limitations\s*$/.test(line));
  if (start === -1) return null;

  const collected = [];
  for (let index = start + 1; index < lines.length; index += 1) {
    if (/^##\s/.test(lines[index])) break;
    collected.push({ line: index + 1, text: lines[index] });
  }
  return collected;
}

// bullets groups the section into top-level list items. A nested bullet or an
// indented continuation belongs to the item above it, so a bullet is measured
// as the reader sees it rather than one source line at a time.
function bullets(section) {
  const found = [];
  for (const { line, text } of section) {
    if (/^[-*]\s/.test(text)) {
      found.push({ line, parts: [text.replace(/^[-*]\s+/, '')] });
      continue;
    }
    if (text.trim() === '') continue;
    if (found.length > 0 && /^\s+\S/.test(text)) {
      found[found.length - 1].parts.push(text.trim());
    }
  }
  return found.map((bullet) => ({ line: bullet.line, text: bullet.parts.join(' ') }));
}

// lastSentence returns the closing sentence of a bullet. The disposition has to
// close the bullet: an issue number mentioned halfway through describes the
// gap, while the last sentence is what a reader takes away as its status.
function lastSentence(text) {
  const sentences = text
    .trim()
    .split(/(?<=[.!?])\s+/)
    .map((sentence) => sentence.trim())
    .filter((sentence) => sentence !== '');
  return sentences.length === 0 ? '' : sentences[sentences.length - 1];
}

// analyze is the single implementation of the rule. checkFile and the self-test
// both call it, so a rule that stops firing here fails the self-test rather
// than passing every page in silence.
export function analyze(source) {
  const section = sectionLines(source);
  if (section === null) {
    return [{ line: 1, message: 'governed page has no "## Limitations" section' }];
  }

  const items = bullets(section);
  if (items.length === 0) {
    return [{ line: 1, message: '"## Limitations" section has no bullets' }];
  }

  const findings = [];
  for (const item of items) {
    const closing = lastSentence(item.text);
    const link = trackingLink.exec(closing);
    if (link) {
      if (link[1] !== link[2]) {
        findings.push({
          line: item.line,
          message: `limitation bullet cites #${link[1]} but links issue ${link[2]}`,
        });
      }
      continue;
    }
    if (permanenceClause.test(closing)) continue;
    findings.push({
      line: item.line,
      message:
        'limitation bullet has no disposition; close it with a tracking link ' +
        '[#N](https://github.com/stokaro/ptah/issues/N) or with a sentence saying ' +
        'why it is permanent because …',
    });
  }
  return findings;
}

function checkFile(page) {
  const path = join(docsRoot, page);
  if (!existsSync(path)) {
    return [`docs/site/src/content/docs/${page}: governed page is missing; update governedPages`];
  }
  return analyze(readFileSync(path, 'utf8')).map(
    (finding) => `docs/site/src/content/docs/${page}:${finding.line}: ${finding.message}`,
  );
}

// selftest drives the production analyze() over fixtures that pin each branch:
// a missing disposition, a mismatched citation, a disposition that is not the
// closing sentence, a missing section, an empty section, and clean prose.
function selftest() {
  const failures = [];

  const clean = [
    '## Limitations',
    '',
    '- The source is Go annotations only. Tracked by',
    '  [#1144](https://github.com/stokaro/ptah/issues/1144).',
    '- RLS policies are not emitted. This is permanent because a policy is',
    '  evaluated against a session and a file has none.',
    '',
    '## Next steps',
    '',
    '- Unrelated bullet with no disposition at all.',
  ].join('\n');

  for (const finding of analyze(clean)) {
    failures.push(`clean fixture produced a finding at line ${finding.line}: ${finding.message}`);
  }

  const violating = [
    '## Limitations',
    '',
    '- One file per run, and nothing tracks that.',
    '- Numbers come from the previous file.',
    '  Tracked by [#904](https://github.com/stokaro/ptah/issues/905).',
    '- Comments are copied, see [#1145](https://github.com/stokaro/ptah/issues/1145).',
    '  Review them before publishing the file.',
    '',
  ].join('\n');

  const expected = [
    { line: 3, needle: 'no disposition' },
    { line: 4, needle: 'cites #904 but links issue 905' },
    { line: 6, needle: 'no disposition' },
  ];
  const actual = analyze(violating);
  for (const [index, want] of expected.entries()) {
    const got = actual[index];
    if (!got) {
      failures.push(`missing finding at line ${want.line} (${want.needle})`);
      continue;
    }
    if (got.line !== want.line || !got.message.includes(want.needle)) {
      failures.push(
        `finding ${index} was ${got.line}: ${got.message}; wanted ${want.line} containing "${want.needle}"`,
      );
    }
  }
  if (actual.length !== expected.length) {
    failures.push(`violating fixture produced ${actual.length} findings, wanted ${expected.length}`);
  }

  const noSection = analyze('# Page\n\nProse only.\n');
  if (noSection.length !== 1 || !noSection[0].message.includes('no "## Limitations" section')) {
    failures.push('a governed page without a Limitations section was not reported');
  }

  const emptySection = analyze('## Limitations\n\nProse, no bullets.\n\n## Next steps\n');
  if (emptySection.length !== 1 || !emptySection[0].message.includes('no bullets')) {
    failures.push('an empty Limitations section was not reported');
  }

  if (failures.length > 0) {
    console.error('check-limitations.mjs --selftest: FAILED');
    for (const failure of failures) console.error(`- ${failure}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-limitations.mjs --selftest: OK (${expected.length + 3} assertions via analyze())`);
}

function main() {
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }

  const errors = governedPages.flatMap(checkFile);
  if (errors.length > 0) {
    console.error('Limitation disposition check failed:');
    for (const error of errors) console.error(`- ${error}`);
    console.error(
      '\nEvery bullet in a governed "Limitations" list must close with the issue that tracks it or with the reason it is permanent.',
    );
    process.exitCode = 1;
    return;
  }
  console.log(`check-limitations.mjs: OK (${governedPages.length} governed page(s))`);
}

main();
