#!/usr/bin/env node
// Holds a translated README to the one it was translated from.
//
// A translation goes stale in a way review does not catch: the English page
// gains a flag, the Japanese page keeps the old command, and both files read
// correctly on their own. The prose is a human judgment and stays one. The
// parts that are not prose are not -- a command, a flag, a schema, a block of
// expected output are the same bytes in every language, and a difference there
// is a defect rather than a translation choice.
//
// So this gate compares the fenced blocks, requires the two files to link to
// each other, and holds the product name to the convention the translation
// declares: the reading is given once, at the first mention, and the Latin
// spelling is used everywhere after it.
//
// What it does NOT check is the prose. Nothing here can tell a good translation
// from a bad one, and pretending otherwise would be the worse failure: a gate
// that reports on the half it cannot read teaches a reader to trust it about
// the half it can.
//
// scripts/check-readme-example.sh runs the English example and requires the
// output it promises. Between that and this, the Japanese example is executed
// too: the blocks it publishes are proven identical to the blocks that ran.
//
// Usage:
//   node scripts/check-translations.mjs [--selftest]
//
// No npm dependencies, for the reason check-style.mjs has none: CI runs it from
// a bare checkout on any documentation change.
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { repositoryFiles } from './check-terminology.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(scriptDir, '..', '..', '..');

// A README and the language tag of its translation: README.ja.md is `ja`.
const translatedReadme = /(?:^|\/)README\.([a-z]{2}(?:-[A-Za-z]{2,4})?)\.md$/;

// How each language introduces the product name. The rule is the same in every
// language and the spelling is not, so the spelling is declared here and the
// rule is written once below.
//
// `gloss` is the full first mention -- the Latin name and its reading. `reading`
// is the reading on its own, which may appear only inside the gloss: a page that
// writes it a second time has started using the reading as the name, which is
// the habit this convention exists to prevent.
//
// `label` is what the link to this translation says in the source file. A
// reader who cannot read the page they are on has to recognize the link out of
// it, so it is written in the target language rather than in English.
const languages = {
  ja: { gloss: 'Ptah（プタハ）', reading: 'プタハ', label: '日本語' },
};

/**
 * The fenced blocks of a Markdown source, in order.
 *
 * Both fence characters are read. A block is its language label and its body;
 * indentation inside the body is kept, because a command indented differently
 * is a different command.
 *
 * An unclosed fence is returned as a block with `closed: false` rather than
 * dropped: a translation that lost a closing fence would otherwise swallow the
 * rest of the file and compare as though the blocks after it were never there.
 */
export function fencedBlocks(source) {
  const blocks = [];
  let fence = null;
  let language = '';
  let body = [];

  for (const line of source.split('\n')) {
    const trimmed = line.trimStart();
    if (fence === null) {
      const open = /^(`{3,}|~{3,})(.*)$/.exec(trimmed);
      if (!open) continue;
      fence = open[1][0].repeat(3);
      language = open[2].trim();
      body = [];
      continue;
    }
    if (trimmed.startsWith(fence) && trimmed.slice(fence.length).trim() === '') {
      blocks.push({ language, body: body.join('\n'), closed: true });
      fence = null;
      continue;
    }
    body.push(line);
  }
  if (fence !== null) blocks.push({ language, body: body.join('\n'), closed: false });
  return blocks;
}

/**
 * Every way a Markdown or HTML link can name a file this gate cares about.
 *
 * Both spellings are read because a README uses both: the centered header is
 * HTML, the prose links are Markdown.
 */
function linksTo(source, target) {
  return source.includes(`](${target})`) || source.includes(`href="${target}"`);
}

/**
 * The findings for one source/translation pair.
 *
 * Takes the two sources rather than two paths so the selftest drives this
 * function rather than a copy of it.
 */
export function pairProblems({ sourcePath, sourceText, translationPath, translationText, language }) {
  const problems = [];
  const say = (message) => problems.push(`${translationPath}: ${message}`);

  const sourceBlocks = fencedBlocks(sourceText);
  const translationBlocks = fencedBlocks(translationText);

  for (const [index, block] of translationBlocks.entries()) {
    if (!block.closed) say(`fenced block ${index + 1} is never closed`);
  }

  if (sourceBlocks.length !== translationBlocks.length) {
    say(
      `has ${translationBlocks.length} fenced block(s) and ${sourcePath} has ${sourceBlocks.length}; ` +
        'a command, a schema or a block of expected output is the same in every language',
    );
  }

  for (let index = 0; index < Math.min(sourceBlocks.length, translationBlocks.length); index += 1) {
    const want = sourceBlocks[index];
    const got = translationBlocks[index];
    if (want.language === got.language && want.body === got.body) continue;
    say(
      `fenced block ${index + 1} does not match ${sourcePath}\n` +
        `    ${sourcePath} (${want.language || 'no label'}):\n${indent(want.body)}\n` +
        `    ${translationPath} (${got.language || 'no label'}):\n${indent(got.body)}`,
    );
  }

  if (!linksTo(translationText, baseName(sourcePath))) {
    say(`does not link back to ${sourcePath}; a reader who landed here by accident needs the way out`);
  }
  if (!linksTo(sourceText, baseName(translationPath))) {
    problems.push(
      `${sourcePath}: does not link to ${translationPath}; a translation nothing points at is a page nobody finds`,
    );
  }

  const declared = languages[language];
  if (!declared) {
    say(`is tagged "${language}", which no entry in check-translations.mjs declares a naming convention for`);
    return problems;
  }

  const glosses = occurrences(translationText, declared.gloss);
  const readings = occurrences(translationText, declared.reading);
  if (glosses !== 1) {
    say(
      `writes ${JSON.stringify(declared.gloss)} ${glosses} time(s); it belongs exactly once, ` +
        'at the first mention, and the Latin spelling is used everywhere after it',
    );
  }
  if (readings !== glosses) {
    say(
      `writes ${JSON.stringify(declared.reading)} ${readings} time(s) but ` +
        `${JSON.stringify(declared.gloss)} ${glosses} time(s); the reading appears only inside the ` +
        'first mention, never on its own',
    );
  }
  if (occurrences(sourceText, declared.reading) !== 0) {
    problems.push(`${sourcePath}: carries ${JSON.stringify(declared.reading)}; the reading belongs in the translation`);
  }

  return problems;
}

function baseName(path) {
  return path.slice(path.lastIndexOf('/') + 1);
}

function indent(text) {
  return text
    .split('\n')
    .map((line) => `      ${line}`)
    .join('\n');
}

function occurrences(text, needle) {
  let count = 0;
  let at = text.indexOf(needle);
  while (at !== -1) {
    count += 1;
    at = text.indexOf(needle, at + needle.length);
  }
  return count;
}

/**
 * Every translated README in the tree, paired with the file it translates.
 *
 * Derived from `git ls-files`, so a translation added in this commit is
 * governed by it. Throws when the pattern matches nothing: a gate with an empty
 * corpus reports the success it reports on a healthy tree, and the corpus here
 * is small enough that losing it would be easy to miss.
 */
function pairs(files = repositoryFiles(repoRoot)) {
  const found = [];
  for (const path of files) {
    const match = translatedReadme.exec(path);
    if (!match) continue;
    const sourcePath = path.replace(translatedReadme, (whole) => whole.replace(`.${match[1]}.md`, '.md'));
    found.push({ path, sourcePath, language: match[1] });
  }
  if (found.length === 0) {
    throw new Error(
      'check-translations: no translated README matched; the pattern reaches nothing and this gate would report OK forever',
    );
  }
  return found;
}

function selftest() {
  const failures = [];
  const base = {
    sourcePath: 'README.md',
    translationPath: 'README.ja.md',
    language: 'ja',
  };
  const englishSource = [
    '[日本語](README.ja.md)',
    '',
    '```bash',
    'ptah schema apply --db-url "sqlite://app.db" --auto-approve',
    '```',
    '',
    '```text',
    'Schema apply completed successfully.',
    '```',
  ].join('\n');
  const japaneseSource = [
    '[English](README.md)',
    '',
    'Ptah（プタハ）はスキーマを管理します。Ptah の使い方は次のとおりです。',
    '',
    '```bash',
    'ptah schema apply --db-url "sqlite://app.db" --auto-approve',
    '```',
    '',
    '```text',
    'Schema apply completed successfully.',
    '```',
  ].join('\n');

  const clean = pairProblems({ ...base, sourceText: englishSource, translationText: japaneseSource });
  for (const problem of clean) failures.push(`a correct pair produced a finding: ${problem}`);

  // Each mutant changes one property and must be reported. A flag dropped from
  // the translated command is the defect this gate exists for; the rest hold
  // the parts that would otherwise let it pass on a broken pair.
  const mutants = [
    {
      why: 'a flag dropped from the translated command',
      translationText: japaneseSource.replace(' --auto-approve', ''),
      needle: 'fenced block 1 does not match',
    },
    {
      why: 'a block missing from the translation',
      translationText: japaneseSource.replace(/\n```text\nSchema apply completed successfully\.\n```/, ''),
      needle: 'has 1 fenced block(s) and README.md has 2',
    },
    {
      why: 'a language label changed',
      translationText: japaneseSource.replace('```text', '```console'),
      needle: 'fenced block 2 does not match',
    },
    {
      why: 'a fence left open',
      translationText: `${japaneseSource}\n\n\`\`\`sql\nSELECT 1;`,
      needle: 'is never closed',
    },
    {
      why: 'no link back to the English page',
      translationText: japaneseSource.replace('[English](README.md)', 'English'),
      needle: 'does not link back to README.md',
    },
    {
      why: 'the reading given a second time',
      translationText: japaneseSource.replace('Ptah の使い方', 'プタハ の使い方'),
      needle: 'the reading appears only inside the',
    },
    {
      why: 'the first mention never glossed',
      translationText: japaneseSource.replace('Ptah（プタハ）', 'Ptah'),
      needle: 'time(s); it belongs exactly once',
    },
    {
      why: 'the gloss given twice',
      translationText: japaneseSource.replace('Ptah の使い方', 'Ptah（プタハ）の使い方'),
      needle: 'time(s); it belongs exactly once',
    },
    {
      why: 'a language with no declared convention',
      language: 'zz',
      translationText: japaneseSource,
      needle: 'no entry in check-translations.mjs declares a naming convention',
    },
  ];
  for (const { why, needle, ...overrides } of mutants) {
    const found = pairProblems({ ...base, sourceText: englishSource, ...overrides });
    if (!found.some((problem) => problem.includes(needle))) {
      failures.push(`${why} was not reported (wanted ${JSON.stringify(needle)}, got ${JSON.stringify(found)})`);
    }
  }

  // The source's own two obligations, asserted from the source side.
  const unlinked = pairProblems({
    ...base,
    sourceText: englishSource.replace('[日本語](README.ja.md)', ''),
    translationText: japaneseSource,
  });
  if (!unlinked.some((problem) => problem.includes('does not link to README.ja.md'))) {
    failures.push('an English page that links to no translation was not reported');
  }
  const glossedSource = pairProblems({
    ...base,
    sourceText: `${englishSource}\n\nPtah（プタハ）`,
    translationText: japaneseSource,
  });
  if (!glossedSource.some((problem) => problem.includes('the reading belongs in the translation'))) {
    failures.push('an English page carrying the Japanese reading was not reported');
  }

  // An HTML link counts, because that is how a centered README header writes
  // one. Without this the rule could be narrowed to Markdown and still selftest.
  const htmlLinked = pairProblems({
    ...base,
    sourceText: englishSource.replace('[日本語](README.ja.md)', '<a href="README.ja.md">日本語</a>'),
    translationText: japaneseSource.replace('[English](README.md)', '<a href="README.md">English</a>'),
  });
  for (const problem of htmlLinked) failures.push(`an HTML-linked pair produced a finding: ${problem}`);

  // The corpus refuses to be empty, which is the failure this gate is most
  // likely to reach: one renamed file and it would have nothing to compare.
  let refused = false;
  try {
    pairs(['README.md', 'docs/README.md']);
  } catch {
    refused = true;
  }
  if (!refused) failures.push('a tree with no translated README was not refused');

  const found = pairs(['README.md', 'README.ja.md', 'charts/ptah/README.pt-BR.md']);
  if (found.length !== 2) failures.push(`the pattern found ${found.length} pair(s) in a two-translation tree`);
  if (found[1]?.sourcePath !== 'charts/ptah/README.md') {
    failures.push(`a nested translation resolved to ${found[1]?.sourcePath}`);
  }

  if (failures.length > 0) {
    console.error('check-translations.mjs --selftest: FAILED');
    for (const failure of failures) console.error(`- ${failure}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-translations.mjs --selftest: OK (${mutants.length + 5} assertions via pairProblems())`);
}

function main() {
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }

  const problems = [];
  let compared = 0;
  const found = pairs();

  for (const pair of found) {
    let sourceText;
    try {
      sourceText = readFileSync(join(repoRoot, pair.sourcePath), 'utf8');
    } catch {
      problems.push(`${pair.path}: translates ${pair.sourcePath}, which does not exist`);
      continue;
    }
    const translationText = readFileSync(join(repoRoot, pair.path), 'utf8');
    compared += fencedBlocks(sourceText).length;
    problems.push(
      ...pairProblems({
        sourcePath: pair.sourcePath,
        sourceText,
        translationPath: pair.path,
        translationText,
        language: pair.language,
      }),
    );
  }

  // A pair whose pages carry no fenced block at all compares nothing, and a
  // fence reader that stopped matching looks exactly like that.
  if (compared === 0) {
    console.error('check-translations: found no fenced block to compare across any translated README');
    process.exitCode = 1;
    return;
  }

  if (problems.length > 0) {
    console.error('Translation check failed:');
    for (const problem of problems) console.error(`- ${problem}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-translations.mjs: OK (${found.length} translation(s), ${compared} fenced blocks compared)`);
}

main();
