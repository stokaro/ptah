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

// The product name in Latin script. Every language writes it the same way --
// the gloss introduces the reading once and this spelling carries the page
// after it -- so it sits beside the table rather than inside it.
const latinName = 'Ptah';

/**
 * The fence a line opens or closes, or null when the line is not a fence.
 *
 * `char` and `length` are the delimiter as written, and `info` is the language
 * label after it. A four-backtick fence is how a page shows a triple-backtick
 * block, so a reader that shortened every opener to three characters would end
 * the outer block at the inner one and report the real closing delimiter as an
 * unclosed block -- on two byte-identical files.
 *
 * One reader, because `fencedBlocks` and `proseOnly` have to agree on where
 * code starts and stops. A second copy agrees when it is written and stops
 * agreeing when the first is changed.
 */
function fenceMarker(line) {
  const marker = /^(`{3,}|~{3,})(.*)$/.exec(line.trimStart());
  if (marker === null) return null;
  return { char: marker[1][0], length: marker[1].length, info: marker[2].trim() };
}

/**
 * Whether a marker closes the fence that is open.
 *
 * The same character, at least as long, and carrying no label: a longer run
 * closes a shorter one, a shorter run is content, and a run with a language
 * after it opens a block rather than closing one.
 */
function closesFence(marker, fence) {
  return marker !== null && marker.char === fence.char && marker.length >= fence.length && marker.info === '';
}

/**
 * The fenced blocks of a Markdown source, in order.
 *
 * Both fence characters are read, at the length the opener was written with. A
 * block is its language label and its body; indentation inside the body is
 * kept, because a command indented differently is a different command.
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
    const marker = fenceMarker(line);
    if (fence === null) {
      if (marker === null) continue;
      fence = marker;
      language = marker.info;
      body = [];
      continue;
    }
    if (closesFence(marker, fence)) {
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
 * The source with everything that is not prose replaced by spaces.
 *
 * Offsets are preserved, so a position found here is a position in the
 * original, and the line it falls on is the line a reader would edit.
 *
 * Prose is where a reader meets the product name in a sentence, and that is
 * where the gloss belongs. A reader meets three of its regions some other way,
 * and each is excluded for its own reason:
 *
 *   - code, fenced or inline. A span holds the token, not the name in a
 *     sentence, and this gate requires a fenced block to hold the same bytes in
 *     every language, so a gloss written there would be a defect in the block;
 *   - an HTML attribute value. `alt` describes an image and `href` addresses a
 *     file; neither is a sentence a reader reads in order. The logo `alt` is
 *     the first line of the README, so counting it would demand the one
 *     permitted gloss inside markup and leave the opening sentence unable to
 *     carry it;
 *   - a level-one heading, which names the product rather than saying anything
 *     about it. `Ptah（プタハ）` as a title reads as a different product's name.
 *
 * check-style.mjs reads an attribute value as prose and is right to: a British
 * spelling in alt text is a spelling a reader gets. The question here is where
 * the name is introduced rather than which words are used, and the same bytes
 * answer the two differently.
 */
function proseOnly(source) {
  const blank = (text) => ' '.repeat(text.length);
  const masked = [];
  let fence = null;

  for (const line of source.split('\n')) {
    const marker = fenceMarker(line);
    if (fence !== null) {
      if (closesFence(marker, fence)) fence = null;
      masked.push(blank(line));
      continue;
    }
    if (marker !== null) {
      fence = marker;
      masked.push(blank(line));
      continue;
    }
    if (/^\s*(?:#\s|<h1[\s>])/.test(line)) {
      masked.push(blank(line));
      continue;
    }
    masked.push(line.replace(/`[^`]*`/g, blank).replace(/[A-Za-z-]+\s*=\s*("[^"]*"|'[^']*')/g, blank));
  }
  return masked.join('\n');
}

/**
 * Where `name` first stands on its own in `text`, or -1.
 *
 * A match with a letter or a digit against it is part of a longer word:
 * `PtahSchema` is a Kubernetes kind, not a mention of the product.
 */
function standaloneMention(text, name) {
  const wordCharacter = /[0-9A-Za-z]/;
  let at = text.indexOf(name);
  while (at !== -1) {
    const before = at === 0 ? '' : text[at - 1];
    const after = text[at + name.length] ?? '';
    if (!wordCharacter.test(before) && !wordCharacter.test(after)) return at;
    at = text.indexOf(name, at + name.length);
  }
  return -1;
}

/** The 1-based line an offset falls on. */
function lineOf(text, offset) {
  return text.slice(0, offset).split('\n').length;
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

  // Counting is not ordering. A page that writes `Ptah` and only later
  // `Ptah（プタハ）` satisfies both counts above while giving the reader the
  // reading after they needed it, so the position is compared as well: the
  // gloss has to be the first place the name stands on its own in prose.
  const glossAt = translationText.indexOf(declared.gloss);
  const mentionAt = standaloneMention(proseOnly(translationText), latinName);
  if (glossAt !== -1 && mentionAt !== glossAt) {
    say(
      mentionAt !== -1 && mentionAt < glossAt
        ? `writes ${JSON.stringify(latinName)} on line ${lineOf(translationText, mentionAt)} before ` +
            `${JSON.stringify(declared.gloss)} on line ${lineOf(translationText, glossAt)}; the reading is ` +
            'given at the first mention, so the gloss comes first and the Latin spelling follows it'
        : `writes ${JSON.stringify(declared.gloss)} on line ${lineOf(translationText, glossAt)}, where a ` +
            'reader does not meet it: a code block, an HTML attribute value and the title are not the first ' +
            'mention, so the gloss belongs in the first sentence that names the product',
    );
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
  // The fixture opens the way both READMEs open, with the logo and the title
  // above the first sentence. That shape is what the ordering rule has to
  // accept, so the clean assertion below is made against it rather than
  // against a page with nothing above its first sentence.
  const englishSource = [
    '<p align="center"><img src="logo.svg" alt="The Ptah mark" width="72" height="72"></p>',
    '',
    '<h1 align="center">Ptah</h1>',
    '',
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
    '<p align="center"><img src="logo.svg" alt="Ptah のマーク" width="72" height="72"></p>',
    '',
    '<h1 align="center">Ptah</h1>',
    '',
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
  const japaneseProse = 'Ptah（プタハ）はスキーマを管理します。Ptah の使い方は次のとおりです。';

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
      why: 'the gloss given after a bare mention',
      translationText: japaneseSource.replace(
        japaneseProse,
        'Ptah はスキーマを管理します。Ptah（プタハ）の使い方は次のとおりです。',
      ),
      needle: 'the gloss comes first and the Latin spelling follows it',
    },
    {
      why: 'the gloss written in the title rather than in a sentence',
      translationText: japaneseSource
        .replace('<h1 align="center">Ptah</h1>', '<h1 align="center">Ptah（プタハ）</h1>')
        .replace('Ptah（プタハ）はスキーマを管理します。', 'Ptah はスキーマを管理します。'),
      needle: 'the gloss belongs in the first sentence that names the product',
    },
    {
      why: 'a four-backtick block whose inner sample differs',
      sourceText: `${englishSource}\n\n\`\`\`\`markdown\n\`\`\`bash\nptah migrations up\n\`\`\`\n\`\`\`\``,
      translationText: `${japaneseSource}\n\n\`\`\`\`markdown\n\`\`\`bash\nptah migrations down\n\`\`\`\n\`\`\`\``,
      needle: 'fenced block 3 does not match',
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

  // What the ordering rule does not count. Without these controls the rule
  // could be satisfied by counting nothing at all: every mutant above moves
  // the gloss rather than removing the mentions that precede it, so each one
  // would still be reported by a rule that had gone blind.
  const exemptions = [
    {
      why: 'the product name in the logo alt text',
      translationText: japaneseSource.replace('<h1 align="center">Ptah</h1>', '<h1 align="center">概要</h1>'),
    },
    {
      why: 'the product name in the document title',
      translationText: japaneseSource.replace('alt="Ptah のマーク"', 'alt="プロジェクトのマーク"'),
    },
    {
      why: 'a longer identifier that begins with the product name',
      translationText: japaneseSource.replace(japaneseProse, `PtahSchema は宣言を表します。${japaneseProse}`),
    },
    {
      why: 'the product name inside an inline code span',
      translationText: japaneseSource.replace(japaneseProse, `\`Ptah\` は実行ファイル名です。${japaneseProse}`),
    },
    {
      why: 'the product name inside a fenced block above the first sentence',
      sourceText: `\`\`\`go\n// Ptah reads this struct.\n\`\`\`\n\n${englishSource}`,
      translationText: `\`\`\`go\n// Ptah reads this struct.\n\`\`\`\n\n${japaneseSource}`,
    },
  ];
  for (const { why, ...overrides } of exemptions) {
    const found = pairProblems({ ...base, sourceText: englishSource, ...overrides });
    for (const problem of found) failures.push(`${why} produced a finding: ${problem}`);
  }

  // A four-backtick fence is how a README shows a triple-backtick block. Both
  // files carry it identically, so the pair is clean; a reader that shortened
  // the opener would end the outer block at the inner one and report the real
  // closing delimiter as an unclosed block.
  const nestedFence = ['````markdown', '```bash', 'ptah migrations up', '```', '````'].join('\n');
  const nested = pairProblems({
    ...base,
    sourceText: `${englishSource}\n\n${nestedFence}`,
    translationText: `${japaneseSource}\n\n${nestedFence}`,
  });
  for (const problem of nested) failures.push(`a pair with a four-backtick block produced a finding: ${problem}`);

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
  console.log(
    `check-translations.mjs --selftest: OK (${mutants.length + exemptions.length + 6} assertions via pairProblems())`,
  );
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
