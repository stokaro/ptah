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
// So this gate compares fenced blocks and addresses, requires each language
// to link to all the others near the top, and checks the Japanese pronunciation
// gloss. German and French use the Latin product name throughout.
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

// Native language labels and the optional pronunciation convention. Only
// Japanese needs a gloss; German and French keep the Latin product name.
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
  de: { label: 'Deutsch' },
  fr: { label: 'Français' },
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
 * Every address the page points at, in order: HTML `src` and `href`, and the
 * destination of a Markdown link or image.
 *
 * An address is not prose. A badge URL, an engine page, an asset path and a
 * guide link are the same string in every language, and the two files drift
 * there the same way they drift in a command: the English page is edited and
 * the translation keeps what it had. The fenced-block rule cannot see it,
 * because none of those addresses is inside a fence -- a README writes them in
 * a centered HTML header and in prose links.
 *
 * Two kinds are dropped, because they are the two that SHOULD differ:
 *
 *   - the reciprocal language link, which by definition names the other file;
 *   - an in-page anchor, which addresses a heading, and a translated heading
 *     has a translated anchor.
 *
 * The link TEXT is prose and is not read here. `[インストールガイド](url)` and
 * `[installation guide](url)` are the same link.
 */
export function linkTargets(source, { exclude } = { exclude: '' }) {
  const found = [];
  // Alternation order matters: the HTML attribute is tried first so that a
  // Markdown destination is never matched inside one.
  const pattern = /\b(?:src|href)\s*=\s*"([^"]*)"|\]\(([^)\s]+)(?:\s+[^)]*)?\)/g;
  let match;
  while ((match = pattern.exec(source)) !== null) {
    const target = match[1] ?? match[2];
    if (target === exclude || target.startsWith('#')) continue;
    // The complete language switch differs only in which member is plain text.
    if (/^README(?:\.(?:ja|de|fr))?\.md$/.test(target)) continue;
    found.push(target);
  }
  return found;
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

  const sourceTargets = linkTargets(sourceText, { exclude: baseName(translationPath) });
  const translationTargets = linkTargets(translationText, { exclude: baseName(sourcePath) });

  if (sourceTargets.length !== translationTargets.length) {
    say(
      `points at ${translationTargets.length} address(es) and ${sourcePath} points at ` +
        `${sourceTargets.length}; a link, an image and a badge address the same thing in every language`,
    );
  }

  for (let index = 0; index < Math.min(sourceTargets.length, translationTargets.length); index += 1) {
    if (sourceTargets[index] === translationTargets[index]) continue;
    say(
      `address ${index + 1} does not match ${sourcePath}\n` +
        `    ${sourcePath}:        ${sourceTargets[index]}\n` +
        `    ${translationPath}: ${translationTargets[index]}`,
    );
    // One report per pair. After the first difference the two lists are
    // misaligned, and a finding per remaining address would bury the one a
    // reader has to act on.
    break;
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

  // Latin-script translations keep the product name without a pronunciation gloss.
  if (!declared.gloss) return problems;

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

/** Every README offers every other available translation in its opening. */
export function navigationProblems(text, path, siblings) {
  return siblings.filter((other) => other !== path)
    .filter((other) => !linksTo(text.split('\n').slice(0, 12).join('\n'), baseName(other)))
    .map((other) => `${path}: opening language switch does not link to ${other}`);
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
    // The badge that drifted on master: the English page gained a label and
    // the translation kept the one it had. No fence holds it, so the block
    // rule reports nothing and both pages read correctly on their own.
    {
      why: 'an asset address the translation kept while the source moved on',
      sourceText: englishSource.replace('src="logo.svg"', 'src="logo-v2.svg"'),
      needle: 'address 1 does not match',
    },
    {
      why: 'an address the translation points at and the source does not',
      translationText: `${japaneseSource}\n\n[対応表](https://example.com/support-matrix/)`,
      needle: 'points at 2 address(es) and README.md points at 1',
    },
    {
      why: 'an address the source points at and the translation dropped',
      sourceText: `${englishSource}\n\n[Support matrix](https://example.com/support-matrix/)`,
      needle: 'points at 1 address(es) and README.md points at 2',
    },
  ];
  // Both sides are defaulted, so a row may override either one. Defaulting
  // only the source made a row that mutates the source alone pass `undefined`
  // as the translation and crash in the fence reader, which is a worse failure
  // than the finding it was looking for.
  for (const { why, needle, ...overrides } of mutants) {
    const found = pairProblems({
      ...base,
      sourceText: englishSource,
      translationText: japaneseSource,
      ...overrides,
    });
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
    // What the address rule must not count. Each is a difference the two pages
    // are SUPPOSED to have, and without these the rule could be satisfied by
    // demanding two byte-identical files, which is not a translation.
    {
      why: 'the link text translated while the address stays',
      sourceText: `${englishSource}\n\n[Installation guide](https://example.com/install/)`,
      translationText: `${japaneseSource}\n\n[インストールガイド](https://example.com/install/)`,
    },
    {
      why: 'an in-page anchor pointing at a translated heading',
      sourceText: `${englishSource}\n\n[Install](#install)`,
      translationText: `${japaneseSource}\n\n[インストール](#インストール)`,
    },
    {
      why: 'the reciprocal language link, which names a different file by design',
      sourceText: englishSource,
      translationText: japaneseSource,
    },
  ];
  for (const { why, ...overrides } of exemptions) {
    const found = pairProblems({
      ...base,
      sourceText: englishSource,
      translationText: japaneseSource,
      ...overrides,
    });
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

  // Latin-script translations need no Japanese reading, but keep every code
  // and address rule. Exercise both locales and the four-way switch.
  const siblings = ['README.md', 'README.ja.md', 'README.de.md', 'README.fr.md'];
  for (const language of ['de', 'fr']) {
    const translationPath = `README.${language}.md`;
    const nav = (current) => siblings.filter((path) => path !== current)
      .map((path) => `[${path}](${path})`).join(' · ');
    const en = nav('README.md') + '\n\n```sh\nptah version\n```\n[Guide](https://example.com/guide/)';
    const translated = nav(translationPath) + '\n\n```sh\nptah version\n```\n[Anleitung](https://example.com/guide/)';
    const pair = { sourcePath: 'README.md', sourceText: en, translationPath, translationText: translated, language };
    if (pairProblems(pair).length) failures.push(`${language}: complete translation was refused`);
    for (const [before, after] of [['ptah version', 'ptah --version'], ['example.com/guide/', 'example.com/de/guide/']]) {
      if (!pairProblems({ ...pair, translationText: translated.replace(before, after) }).length) {
        failures.push(`${language}: drift in ${before} was accepted`);
      }
    }
    if (navigationProblems(translated, translationPath, siblings).length) failures.push(`${language}: complete switch was refused`);
    if (!navigationProblems(translated.replace('[README.ja.md](README.ja.md)', ''), translationPath, siblings).length) {
      failures.push(`${language}: missing sibling navigation was accepted`);
    }
  }

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
    `check-translations.mjs --selftest: OK (${mutants.length + exemptions.length + 16} assertions via pairProblems())`,
  );
}

function main() {
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }

  const problems = [];
  let compared = 0;
  let addresses = 0;
  const found = pairs();
  const groups = new Map();
  for (const pair of found) {
    const members = groups.get(pair.sourcePath) ?? [pair.sourcePath];
    members.push(pair.path);
    groups.set(pair.sourcePath, members);
  }
  for (const members of groups.values()) {
    for (const path of members) {
      try {
        problems.push(...navigationProblems(readFileSync(join(repoRoot, path), 'utf8'), path, members));
      } catch {
        problems.push(`${path}: missing README in the language group`);
      }
    }
  }

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
    addresses += linkTargets(sourceText, { exclude: baseName(pair.path) }).length;
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

  // A pair whose pages carry no fenced block and no address compares nothing,
  // and a reader that stopped matching looks exactly like that. Each half has
  // its own floor: a page can legitimately have no code, and one that also had
  // no link would leave both readers unexercised with no finding to show for
  // it. Over this repository both are well above zero.
  if (compared === 0 && addresses === 0) {
    console.error(
      'check-translations: found no fenced block and no address to compare across any translated README',
    );
    process.exitCode = 1;
    return;
  }

  if (problems.length > 0) {
    console.error('Translation check failed:');
    for (const problem of problems) console.error(`- ${problem}`);
    process.exitCode = 1;
    return;
  }
  console.log(
    `check-translations.mjs: OK (${found.length} translation(s), ` +
      `${compared} fenced blocks and ${addresses} addresses compared)`,
  );
}

main();
