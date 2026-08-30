#!/usr/bin/env node
// Holds reader-facing prose to the terminology registry, and holds section 7 of
// docs/STYLE_GUIDE.md to the same registry.
//
// Section 7 has been a terminology registry since it was written: canonical
// names, one usage rule each, and a review-checklist item that says
// "Terminology matches section 7; no retired synonyms introduced". Nothing read
// it. The `direct schema changes` row arrived one commit before this check and
// its rule was already broken in fourteen pages and in `ptah --help`, which is
// the first sentence a reader of the binary meets.
//
// Usage:
//   node scripts/check-terminology.mjs [--write] [--selftest]
//
// The registry is docs/site/scripts/data/terminology.json. Section 7's table is
// generated from it between markers; `--write` regenerates it. Editing the
// table by hand fails this check, the way editing atlas/feature-matrix.md by
// hand fails build-feature-matrix, and for the same reason: a hand-maintained
// restatement of data drifts in the direction nobody notices.
//
// THE PROSE IS A RENDERING, NOT THE SOURCE. Parsing section 7 in place would
// make a writer's formatting load-bearing -- a reflowed cell, an added
// sentence, a Markdown pipe inside a regex, each becoming a parser bug in the
// one file whose whole purpose is to be edited by writers.
//
// WHAT IT READS. Every tracked Markdown file, plus the reader-facing site
// sources that are not Markdown -- the sidebar's labels, the Astro config's
// title, the components' strings. The corpus is DERIVED from the tracked file
// list rather than listed in the registry, so a new page or a new package
// README is governed by existing; check-style.mjs derives its own the same way
// and states the same reason.
//
// Those non-Markdown sources are read AS Markdown, which is exact for the
// quoted string a label is and inexact in one direction worth naming: a value
// written as a backtick template literal reads as a code span and is blanked,
// so it is missed rather than misreported. Silence is the safe half of that
// trade and it is the only half this takes.
//
// WHAT IT CANNOT SEE, said here rather than left to be discovered. The native
// command tree's help text carries the same rules and is checked by
// cmd/internal/terminologyguard, which reads this same registry through Go;
// neither reader covers the other's corpus, and the count assertion in that
// package is what says the Go half is still reading the file at all. Nothing
// here reads HCL, JSON fixtures, or Go comments.
//
// The check has no npm dependencies on purpose: scripts/check-gate-selftests.sh
// runs it in a throwaway worktree that has no node_modules, and the docs job
// runs it from a bare checkout for changes that touch no site page.
import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(scriptDir, '..', '..', '..');
const registryPath = join(scriptDir, 'data', 'terminology.json');

// ---------------------------------------------------------------------------
// The registry
// ---------------------------------------------------------------------------

export function loadRegistry(path = registryPath) {
  const registry = JSON.parse(readFileSync(path, 'utf8'));
  if (!Array.isArray(registry.terms) || registry.terms.length === 0) {
    throw new Error(`check-terminology: ${path} declares no terms; refusing to check a document against an empty registry`);
  }
  return registry;
}

/**
 * Every ban in the registry, each carrying the term it belongs to.
 *
 * A ban is refused here when it could not match anything. That refusal is the
 * twin of the empty-scope refusal in scopedFiles(): a ban with no stems is
 * accepted by every reader, counted in the OK line, and reports nothing
 * forever, which is indistinguishable from a ban the tree satisfies. Half the
 * hole is not closed by closing the other half.
 */
export function bansOf(registry) {
  const bans = registry.terms.flatMap((term) => (term.bans ?? []).map((ban) => ({ ...ban, term })));
  for (const ban of bans) {
    if (!Array.isArray(ban.stems) || ban.stems.length === 0) {
      throw new Error(`check-terminology: ban "${ban.id}" declares no stems; it would match nothing and report OK forever`);
    }
    if ((ban.bannedHeads ?? []).length === 0 && ban.onUnknownHead !== 'report') {
      throw new Error(
        `check-terminology: ban "${ban.id}" names no banned head and allows every head it does not know; ` +
          'it would match nothing and report OK forever',
      );
    }
    if (!ban.message) {
      throw new Error(`check-terminology: ban "${ban.id}" carries no message; a finding a reader cannot act on is not a finding`);
    }
  }
  return bans;
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

// A glob here understands `**`, `*` and nothing else. Written out rather than
// taken from a dependency because this script has none, and stated rather than
// implied because a pattern that silently matches nothing is how a scope comes
// to govern zero files.
function globToRegExp(pattern) {
  let out = '^';
  for (let index = 0; index < pattern.length; index += 1) {
    const char = pattern[index];
    if (char === '*') {
      if (pattern[index + 1] === '*') {
        // `**/` spans any number of directories, including none.
        if (pattern[index + 2] === '/') {
          out += '(?:[^/]+/)*';
          index += 2;
          continue;
        }
        out += '.*';
        index += 1;
        continue;
      }
      out += '[^/]*';
      continue;
    }
    out += char.replace(/[.+?^${}()|[\]\\]/g, '\\$&');
  }
  return new RegExp(`${out}$`);
}

const matchesAny = (path, patterns) => patterns.some((pattern) => globToRegExp(pattern).test(path));

/**
 * Every tracked or newly added file in the repository, POSIX-spelled and
 * relative to the root.
 *
 * From git rather than from a directory walk, for the hazard
 * docs/site/scripts/lib/docroutes.mjs and scripts/list-go-modules.sh both
 * already carry: a walk descends into a linked worktree parked under this one
 * and reports files belonging to a different checkout. `--others
 * --exclude-standard` so a page added but not yet committed is governed too.
 *
 * Throws when git cannot answer. An empty answer is not a result.
 */
export function repositoryFiles(root = repoRoot) {
  let output;
  try {
    output = execFileSync(
      'git',
      ['-C', root, '-c', 'core.quotePath=false', 'ls-files', '--cached', '--others', '--exclude-standard'],
      { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'] },
    );
  } catch (cause) {
    const detail = String(cause.stderr ?? cause.message ?? cause).trim().split('\n')[0];
    throw new Error(`check-terminology: git could not enumerate ${root}: ${detail}`, { cause });
  }
  const files = [...new Set(output.split('\n').map((line) => line.replace(/\r$/, '')).filter(Boolean))].sort();
  if (files.length === 0) {
    throw new Error('check-terminology: git reports no files; refusing to check an empty tree');
  }
  return files;
}

/**
 * Refuses a pattern that matches nothing.
 *
 * A hand-written path in a data file is a claim that was true when it was
 * written, and the way it stops being true is silent: the file moves, the
 * pattern keeps matching zero files, and whatever the pattern was for --
 * governing a page, exempting one -- happens to nobody. This is the same
 * property the terminology-allow marker has, refused when it suppresses
 * nothing, applied to the patterns that decide the corpus.
 */
function requireEachMatches(patterns, files, what) {
  for (const pattern of patterns) {
    const regexp = globToRegExp(pattern);
    if (files.some((file) => regexp.test(file))) continue;
    throw new Error(
      `check-terminology: ${what} pattern ${JSON.stringify(pattern)} matches no file; ` +
        'a pattern that matches nothing is a rule nobody is held to',
    );
  }
}

/**
 * Every file the registry governs, derived rather than listed.
 *
 * A hand-written include list is how the site's own navigation came to be
 * ungoverned while the pages it links were checked, and how a package README
 * could opt out of the vocabulary by existing. So the corpus is every tracked
 * file with a documentation extension, plus the reader-facing site sources
 * that are not Markdown -- the sidebar's labels, the Astro config's site
 * title, the components' own strings -- named as globs that are refused when
 * they match nothing. check-style.mjs derives its corpus the same way and
 * states the same reason: a new page is governed by existing.
 *
 * Throws on an empty result. A corpus that matches nothing reports OK forever,
 * which is the failure this repository names most often: a gate that stopped
 * reading looks exactly like a gate with nothing to report.
 */
export function corpusOf(registry, files) {
  const corpus = registry.corpus ?? {};
  const extensions = corpus.extensions ?? [];
  const also = corpus.also ?? [];
  const exclude = corpus.exclude ?? [];
  if (extensions.length === 0) {
    throw new Error('check-terminology: the registry declares no corpus.extensions; refusing to check an undefined corpus');
  }
  requireEachMatches(also, files, 'corpus.also');
  const selected = files.filter(
    (file) => extensions.some((extension) => file.endsWith(extension)) || matchesAny(file, also),
  );
  requireEachMatches(exclude, selected, 'corpus.exclude');
  const kept = selected.filter((file) => !matchesAny(file, exclude));
  if (kept.length === 0) {
    throw new Error('check-terminology: the corpus matches no file; refusing to report OK on an empty corpus');
  }
  return kept;
}

/**
 * The files one ban governs: the corpus, less what the ban excludes.
 *
 * A ban narrows the corpus and never widens it, so a page cannot be governed
 * by one rule and invisible to another because two lists disagree. Every
 * exclusion is refused when it matches nothing, and the reason it exists is
 * data beside it.
 */
export function scopedFiles(ban, corpus) {
  const exclude = ban.scope?.exclude ?? [];
  requireEachMatches(exclude, corpus, `the scope of ban "${ban.id}"`);
  const scoped = corpus.filter((file) => !matchesAny(file, exclude));
  if (scoped.length === 0) {
    throw new Error(`check-terminology: the scope of ban "${ban.id}" matches no file; refusing to report OK on an empty corpus`);
  }
  return scoped;
}

// ---------------------------------------------------------------------------
// The corpus has to be a corpus CI sees
// ---------------------------------------------------------------------------

// A gate reads what a workflow gives it. This one runs in the Docs workflow.
// The workflow normally runs on every pull request; if a future edit restores
// a `paths:` filter, every corpus file has to be reachable through it.
//
// It is read out of the workflow rather than restated here, because a copy of
// the filter would be the claim it exists to check.
const workflowPath = '.github/workflows/docs.yml';

// A GitHub path filter's `**` spans directories and its `*` does not, which is
// the one difference from globToRegExp above; `**.md` matches README.md at the
// root as well as a README three directories down.
function workflowGlobToRegExp(pattern) {
  let out = '^';
  for (let index = 0; index < pattern.length; index += 1) {
    const char = pattern[index];
    if (char === '*') {
      if (pattern[index + 1] === '*') {
        out += '.*';
        index += 1;
        continue;
      }
      out += '[^/]*';
      continue;
    }
    out += char.replace(/[.+?^${}()|[\]\\]/g, '\\$&');
  }
  return new RegExp(`${out}$`);
}

/**
 * The patterns under the workflow's one `paths:` key, in order.
 *
 * A YAML parser would be a dependency this script does not have, so the block
 * is read by shape: the list items indented under the key, comments skipped,
 * stopping at the first line that is not one. An empty answer is never a
 * result -- run() refuses it rather than concluding that every file is
 * reachable, which is what "no patterns" would otherwise mean.
 */
export function workflowTriggerPaths(source) {
  const lines = source.split('\n');
  const start = lines.findIndex((line) => /^ {4}paths:\s*$/.test(line));
  if (start === -1) return [];
  const patterns = [];
  for (const line of lines.slice(start + 1)) {
    if (/^ {6}#/.test(line)) continue;
    const entry = line.match(/^ {6}- {0,2}'([^']+)'\s*$/) ?? line.match(/^ {6}- {0,2}"([^"]+)"\s*$/) ?? line.match(/^ {6}- {0,2}(\S+)\s*$/);
    if (!entry) break;
    patterns.push(entry[1]);
  }
  return patterns;
}

/** Whether the workflow has an unfiltered pull_request trigger. */
export function workflowRunsForEveryPullRequest(source) {
  const lines = source.split('\n');
  const start = lines.findIndex((line) => /^ {2}pull_request:\s*$/.test(line));
  if (start === -1) return false;
  for (const line of lines.slice(start + 1)) {
    if (/^ {2}\S/.test(line)) break;
    if (/^ {4}paths(?:-ignore)?:\s*$/.test(line)) return false;
  }
  return true;
}

/** Every corpus file the workflow's trigger cannot reach. */
export function unreachableFromWorkflow(corpus, patterns) {
  const matchers = patterns.map(workflowGlobToRegExp);
  return corpus.filter((file) => !matchers.some((matcher) => matcher.test(file)));
}

// ---------------------------------------------------------------------------
// The prose stream
// ---------------------------------------------------------------------------

// Non-prose regions are blanked in place rather than removed, so a byte offset
// still names the line a reader would edit and a phrase can still be matched
// across the line break it wraps on.
function blank(source, start, end) {
  const region = source.slice(start, end).replace(/[^\n]/g, ' ');
  return source.slice(0, start) + region + source.slice(end);
}

// The group's span comes from the `d` flag rather than from searching match[0]
// for the group's text. Searching gets `[a]: a` wrong -- it finds the `a` inside
// the label and blanks that instead of the destination -- and the failure is a
// blanked bracket plus a live URL, which is the wrong half of the line twice.
function blankMatches(source, pattern, group = 0) {
  const withIndices = pattern.flags.includes('d') ? pattern : new RegExp(pattern.source, `${pattern.flags}d`);
  let out = source;
  for (const match of [...source.matchAll(withIndices)]) {
    const span = match.indices[group];
    if (!span) continue;
    out = blank(out, span[0], span[1]);
  }
  return out;
}

/**
 * The prose of a Markdown or MDX source, with everything that is not prose
 * blanked out to spaces.
 *
 * Twenty-two of the 118 occurrences the census measured are URLs, anchors,
 * directory names and code spans -- and nine of them sit on a line that also
 * carries a real violation, because `[Quick start: declarative changes](../quick-start-declarative/)`
 * is one prose half and one URL half. Reporting such a line twice, once for the
 * label and once for the href, is how an author learns to distrust a gate.
 *
 * Command output is dropped with the rest of the fenced blocks, and that
 * exclusion is worth naming: several rules in this registry govern words that
 * appear verbatim in Atlas-parity error strings, where changing the word would
 * break the parity the repository is built on. This is "we do not read fenced
 * blocks", not "we do not read code", and it must not widen into the latter.
 */
export function proseOf(source) {
  let out = source;

  // Fenced blocks, including the fence lines themselves.
  {
    const lines = out.split('\n');
    let offset = 0;
    let fence = null;
    for (const line of lines) {
      const trimmed = line.trimStart();
      const marker = trimmed.match(/^(`{3,}|~{3,})(.*)$/);
      if (fence) {
        const closes = marker && marker[1][0] === fence.char && marker[1].length >= fence.length && marker[2].trim() === '';
        out = blank(out, offset, offset + line.length);
        if (closes) fence = null;
      } else if (marker) {
        fence = { char: marker[1][0], length: marker[1].length };
        out = blank(out, offset, offset + line.length);
      }
      offset += line.length + 1;
    }
  }

  // Frontmatter: `title` and `description` are prose a reader meets. Every
  // other key is machinery, and `slug` in particular is a route.
  {
    const frontmatter = out.match(/^---\n([\s\S]*?)\n---\n/);
    if (frontmatter) {
      const body = frontmatter[1];
      let offset = 4;
      for (const line of body.split('\n')) {
        const key = line.match(/^([A-Za-z_][\w-]*)\s*:/);
        if (!key || !['title', 'description'].includes(key[1])) {
          out = blank(out, offset, offset + line.length);
        }
        offset += line.length + 1;
      }
    }
  }

  // MDX import statements name modules, not things.
  out = blankMatches(out, /^import\s[^\n]*$/gm);

  // HTML comments. The exemption markers are read from the raw source before
  // this runs, so blanking them here costs nothing.
  out = blankMatches(out, /<!--[\s\S]*?-->/g);

  // Inline code spans are code for the same reason a fence is.
  out = blankMatches(out, /`[^`\n]*`/g);

  // Markdown link and image destinations, and reference definitions.
  out = blankMatches(out, /\]\(([^)\n]*)\)/g, 1);
  out = blankMatches(out, /^ {0,3}\[[^\]\n]+\]:[ \t]*(\S+)/gm, 1);

  // Autolinks, bare URLs, and HTML or JSX attributes that carry a location.
  out = blankMatches(out, /<((?:[a-z][a-z0-9+.-]*:\/\/|#|\/)[^>\s]*)>/gi, 1);
  out = blankMatches(out, /[a-z][a-z0-9+.-]*:\/\/[^\s)\]<>"'`]+/gi);
  out = blankMatches(out, /\b(?:href|src|slug|url|to|path|file|id|anchor)\s*=\s*("[^"\n]*"|'[^'\n]*'|\{[^}\n]*\})/gi, 1);

  return out;
}

const lineOf = (source, offset) => source.slice(0, offset).split('\n').length;

// ---------------------------------------------------------------------------
// The matcher
// ---------------------------------------------------------------------------

// A head is the noun the stem heads, taken as up to four words. Word separators
// are whitespace and the hyphen alike, so `declarative-reference-data` and
// `declarative reference data` are one head and not two rules.
//
// The scan stops at punctuation that ends a clause, and at the table pipe, so
// `| Declarative schema changes | The schema you want` heads `schema changes`
// and not the sentence after it. A newline is whitespace, deliberately: the
// single most valuable finding the census produced wraps mid-phrase in
// cmd/root/root.go, where `grep -n 'declarative schema changes'` answers
// nothing.
//
// Emphasis markers are skipped rather than stopping the scan. `a **declarative**
// model` is one noun phrase with bold on one word of it, and a scanner that let
// `**` end the head would read every emphasized term as headless -- silent on
// `a **declarative** workflow`, which is the ban's own target wearing bold.
const headStop = /[.,;:!?()"'|[\]{}<>@#=+\\/]/;
const headSkip = /[\s\-*_~]/;
const headWords = 4;

export function headAfter(text) {
  const words = [];
  let index = 0;
  while (words.length < headWords) {
    while (index < text.length && headSkip.test(text[index])) index += 1;
    if (index >= text.length || headStop.test(text[index])) break;
    const start = index;
    while (index < text.length && /[A-Za-z]/.test(text[index])) index += 1;
    if (index === start) break;
    words.push(text.slice(start, index).toLowerCase());
    // A hyphen inside a word is a separator like a space, but only when a
    // letter follows it; `data-migration-generation` is one compound head.
    if (!(index < text.length && headSkip.test(text[index]))) break;
  }
  return words;
}

// A stem in matching quotes is a MENTION of the word, not a use of it, and the
// pages that explain a rule are full of mentions: `so "declarative" does not
// separate the two groups`, `The phrase "declarative schema changes" is what
// section 7 replaced`. No head-noun rule can tell naming from using, and
// requiring an inline marker on every such sentence would put eleven of them on
// docs/site/CONTENT_INVENTORY.md, whose subject IS the retirement -- the page
// most likely to discuss the vocabulary would be the page the gate is loudest
// on, which is how a gate gets turned off rather than obeyed.
//
// The quote closes around the TERM: at the stem, or at the end of one of the
// head words after it. A quote that runs on past the noun phrase is quoting a
// sentence, and a sentence that uses the label is a use.
const quotePairs = new Map([
  ['"', '"'],
  ["'", "'"],
  ['“', '”'],
  ['‘', '’'],
]);

// An opening quote that follows `=`, or a key at the start of its line, opens a
// VALUE rather than a quotation. `title="Declarative changes"` is the router
// card this ban exists to catch, and `title: "Declarative changes"` is the same
// card written in frontmatter; neither is anybody quoting anybody.
//
// The key has to begin its line. Without that, `Section 7 says: "declarative
// schema changes" is retired` would read as a value and be reported -- prose
// about the rule, on a page allowed to state it, which is the false positive
// this whole mechanism exists to avoid.
export function isValueQuote(prose, quoteAt) {
  let index = quoteAt - 1;
  while (index >= 0 && (prose[index] === ' ' || prose[index] === '\t' || prose[index] === '{')) index -= 1;
  if (index < 0) return false;
  if (prose[index] === '=') return true;
  if (prose[index] !== ':') return false;
  index -= 1;
  while (index >= 0 && (prose[index] === ' ' || prose[index] === '\t')) index -= 1;
  const keyEnd = index;
  while (index >= 0 && /[A-Za-z0-9_-]/.test(prose[index])) index -= 1;
  if (index === keyEnd) return false;
  while (index >= 0 && (prose[index] === ' ' || prose[index] === '\t')) index -= 1;
  return index < 0 || prose[index] === '\n';
}

export function isMention(prose, stemStart, stemEnd) {
  const close = quotePairs.get(prose[stemStart - 1]);
  if (close === undefined) return false;
  if (isValueQuote(prose, stemStart - 1)) return false;

  let index = stemEnd;
  let words = 0;
  while (words <= headWords) {
    if (prose[index] === close) return true;
    if (index >= prose.length) return false;
    if (headSkip.test(prose[index])) {
      index += 1;
      continue;
    }
    if (!/[A-Za-z]/.test(prose[index])) return false;
    while (index < prose.length && /[A-Za-z]/.test(prose[index])) index += 1;
    words += 1;
  }
  return false;
}

function classifyHead(words, ban) {
  const banned = new Set((ban.bannedHeads ?? []).map((head) => head.toLowerCase()));
  const allowed = new Set((ban.allowedHeads ?? []).map((head) => head.toLowerCase()));
  // Longest match wins: `schema apply` is Atlas's page title and `schema
  // changes` is the retired workflow label, and both start with `schema`.
  for (let length = Math.min(words.length, headWords); length >= 1; length -= 1) {
    const head = words.slice(0, length).join(' ');
    if (banned.has(head)) return { verdict: 'banned', head };
    if (allowed.has(head)) return { verdict: 'allowed', head };
  }
  return { verdict: ban.onUnknownHead === 'report' ? 'unknown' : 'allowed', head: words.join(' ') };
}

// An exemption records its reason because the marker is refused without one,
// and it is refused when it matches nothing, so it cannot outlive the sentence
// it was written for.
const markerPattern = /<!--\s*terminology-allow:\s*([^\n-][^\n]*?)\s+--\s+([\s\S]*?)-->/g;
const minimumReason = 20;

// A marker covers the next line carrying prose, resolved against the blanked
// stream so a marker above a code fence covers the sentence after it rather
// than the fence.
export function markersOf(source, prose = proseOf(source)) {
  const proseLines = prose.split('\n');
  return [...source.matchAll(markerPattern)].map((match) => {
    const line = lineOf(source, match.index);
    let covers = line;
    for (let candidate = line; candidate <= proseLines.length; candidate += 1) {
      if (proseLines[candidate - 1] !== undefined && proseLines[candidate - 1].trim() !== '') {
        covers = candidate;
        break;
      }
    }
    return {
      line,
      covers,
      spelling: match[1].trim().toLowerCase().replace(/\s+/g, ' '),
      reason: match[2].trim().replace(/\s+/g, ' '),
    };
  });
}

/**
 * Every finding in one source, as `{ line, message }`.
 *
 * This is the single implementation of the rule. The self-test drives it, so a
 * rule that stops firing fails the self-test instead of quietly passing every
 * file forever.
 */
export function analyze(source, { bans = [], path = '' } = {}) {
  const findings = [];
  const prose = proseOf(source);
  const markers = markersOf(source, prose);
  const used = new Set();

  for (const ban of bans) {
    for (const stem of ban.stems) {
      const pattern = new RegExp(`(^|[^A-Za-z0-9_-])(${stem})(?![A-Za-z])`, 'gi');
      for (const match of prose.matchAll(pattern)) {
        const start = match.index + match[1].length;
        const after = prose.slice(start + match[2].length);
        if (isMention(prose, start, start + match[2].length)) continue;

        const words = headAfter(after);
        // A stem with no head noun is an ordinary adjective or adverb -- "all
        // tests MUST be purely declarative", "roles are managed declaratively"
        // -- and the ban's unit is the compound, not the word. This is the
        // gate's largest deliberate blind spot and it is stated rather than
        // left to be discovered: a bare `Declarative` standing alone as a
        // heading or a card title would not be reported.
        if (words.length === 0) continue;

        const { verdict, head } = classifyHead(words, ban);
        if (verdict === 'allowed') continue;

        const spelling = `${match[2]} ${head}`.trim().toLowerCase();
        const line = lineOf(prose, start);
        const marker = markers.find((entry) => entry.covers === line && entry.spelling === spelling);
        if (marker) {
          used.add(marker);
          continue;
        }

        const detail =
          verdict === 'banned'
            ? `retired spelling "${match[2].toLowerCase()} ${head}": ${ban.message}`
            : `"${match[2].toLowerCase()} ${head}" heads a noun the registry classifies neither way; ` +
              `add it to allowedHeads or bannedHeads of "${ban.id}" in docs/site/scripts/data/terminology.json`;
        const generator = Object.entries(ban.generatedSources ?? {}).find(([file]) => file === path)?.[1];
        findings.push({
          line,
          ban: ban.id,
          enforced: ban.enforced !== false,
          message: generator ? `${detail} -- this file is generated; edit ${generator}` : detail,
        });
      }
    }
  }

  // A marker's own rules, checked whether or not it suppressed anything.
  for (const marker of markers) {
    if (marker.reason.length < minimumReason) {
      findings.push({
        line: marker.line,
        ban: 'marker',
        enforced: true,
        message: `terminology-allow marker for "${marker.spelling}" carries no reason; write why after "--"`,
      });
      continue;
    }
    if (!used.has(marker)) {
      findings.push({
        line: marker.line,
        ban: 'marker',
        enforced: true,
        message: `terminology-allow marker for "${marker.spelling}" suppresses nothing on the line below it; delete it or fix the spelling it names`,
      });
    }
  }

  return findings.sort((a, b) => a.line - b.line);
}

// ---------------------------------------------------------------------------
// Section 7's generated table
// ---------------------------------------------------------------------------

const escapeCell = (value) => value.replace(/\|/g, '\\|');

/**
 * What holds the tree to one row, rendered from the row's own data.
 *
 * Section 16 used to answer this in a sentence, and a sentence about which
 * rows are enforced goes stale the moment a ban is added or a ratchet is
 * flipped. Most rows are held by review, and saying so on the row is the
 * difference between a registry a reader can trust and one whose every row
 * reads as gated because some of them are.
 */
export function heldBy(term) {
  const bans = term.bans ?? [];
  if (bans.length === 0) return 'review';
  const holders = new Set();
  for (const ban of bans) {
    holders.add(ban.enforced === false ? '`check:terminology`, ratcheted' : '`check:terminology`');
    if (ban.enforced !== false && ban.helpText?.tree === 'native') holders.add('`terminologyguard`');
  }
  return [...holders].join('; ');
}

export function renderTable(terms) {
  const rows = terms.map((term) => {
    const cell = [term.meaning, term.rule].filter(Boolean).join(' ');
    return `| ${escapeCell(term.term)} | ${escapeCell(cell)} | ${heldBy(term)} |`;
  });
  return ['| Term | Meaning and usage rule | Held by |', '| --- | --- | --- |', ...rows].join('\n');
}

export function extractBlock(document, begin, end) {
  const beginAt = document.indexOf(begin);
  const endAt = document.indexOf(end);
  if (beginAt === -1 || endAt === -1 || endAt < beginAt) return null;
  return document.slice(beginAt + begin.length, endAt).replace(/^\n/, '').replace(/\n$/, '');
}

export function replaceBlock(document, begin, end, body) {
  const beginAt = document.indexOf(begin);
  const endAt = document.indexOf(end);
  return `${document.slice(0, beginAt + begin.length)}\n${body}\n${document.slice(endAt)}`;
}

// ---------------------------------------------------------------------------
// Running
// ---------------------------------------------------------------------------

function checkGeneratedSection(registry, { write = false } = {}) {
  const { file, begin, end } = registry.markers;
  const path = join(repoRoot, file);
  const document = readFileSync(path, 'utf8');
  const table = renderTable(registry.terms);
  const errors = [];

  if (table.split('\n').length < 3) {
    errors.push(`${file}: the registry rendered no rows; refusing to compare a document against an empty table`);
    return errors;
  }
  // The marker check is not defensive padding. A file whose markers were
  // renamed or lost in a merge would yield an empty block on both sides, the
  // comparison would find them identical, and a gate that compares nothing to
  // nothing reports success at exactly the moment it stopped working. The same
  // reasoning is written at scripts/check-capability-tables.sh.
  const current = extractBlock(document, begin, end);
  if (current === null) {
    errors.push(`${file}: carries no ${begin} / ${end} markers`);
    return errors;
  }

  if (write) {
    writeFileSync(path, replaceBlock(document, begin, end, table));
    console.log(`check-terminology: rewrote ${begin} in ${file}`);
    return errors;
  }

  if (current !== table) {
    errors.push(`${file}: section 7 is out of date with docs/site/scripts/data/terminology.json`);
    errors.push('  run: node docs/site/scripts/check-terminology.mjs --write');
    const currentRows = current.split('\n');
    const wantRows = table.split('\n');
    for (let index = 0; index < Math.max(currentRows.length, wantRows.length); index += 1) {
      if (currentRows[index] === wantRows[index]) continue;
      errors.push(`  -${currentRows[index] ?? '(missing)'}`);
      errors.push(`  +${wantRows[index] ?? '(missing)'}`);
    }
  }

  // A second copy of the table elsewhere would go stale unchecked.
  const carriers = repositoryFiles().filter(
    (candidate) => /\.(md|mdx)$/.test(candidate) && readFileSync(join(repoRoot, candidate), 'utf8').includes(begin),
  );
  if (carriers.length !== 1 || carriers[0] !== file) {
    errors.push(`${begin} appears in files this script does not check: ${carriers.join(', ') || '(none)'}`);
  }
  return errors;
}

// The glossary the site renders is a reader affordance, not this registry, and
// merging them would give a text rule a browser dependency for nothing. One
// invariant crosses the two, in one direction only: a tooltip may not teach a
// spelling the registry retired. Free to write, and it catches the collision
// that would do the most damage -- a retired workflow label in a popover, on
// the reader-facing page.
function checkGlossary(bans) {
  const path = 'docs/site/src/glossary.ts';
  const source = readFileSync(join(repoRoot, path), 'utf8');
  // The module is TypeScript, so it is read as text: every string literal in it
  // is prose a reader can be shown.
  const prose = source.replace(/^\s*(?:import|export interface|\/\*\*|\*|\*\/)[^\n]*$/gm, (line) => ' '.repeat(line.length));
  return analyze(prose, { bans: bans.filter((ban) => ban.enforced !== false), path }).map(
    (finding) => `${path}:${finding.line}: ${finding.message}`,
  );
}

function run({ write }) {
  const registry = loadRegistry();
  // Before anything else, and in every mode. `--write` rewrites section 7 from
  // the registry, so a malformed ban that only the checking path refused would
  // still be rendered into the guide and read as governed.
  const bans = bansOf(registry);
  const errors = checkGeneratedSection(registry, { write });
  if (write) {
    if (errors.length > 0) {
      for (const error of errors) console.error(`- ${error}`);
      process.exitCode = 1;
    }
    return;
  }

  const files = repositoryFiles();
  const corpus = corpusOf(registry, files);
  const advisory = [];

  const workflow = readFileSync(join(repoRoot, workflowPath), 'utf8');
  const trigger = workflowTriggerPaths(workflow);
  if (!workflowRunsForEveryPullRequest(workflow) && trigger.length === 0) {
    errors.push(`${workflowPath}: no unfiltered pull-request trigger or readable path filter was found`);
  } else if (trigger.length > 0) {
    const unreachable = unreachableFromWorkflow(corpus, trigger);
    for (const file of unreachable) {
      errors.push(
        `${file} is in this gate's corpus and no ${workflowPath} pull-request path filter reaches it; ` +
          'a pull request touching only that file would report the same green as one this gate read',
      );
    }
  }

  for (const ban of bans) {
    const scoped = scopedFiles(ban, corpus);
    const findings = [];
    for (const file of scoped) {
      const source = readFileSync(join(repoRoot, file), 'utf8');
      for (const finding of analyze(source, { bans: [ban], path: file })) {
        findings.push(`${file}:${finding.line}: ${finding.message}`);
      }
    }

    if (ban.enforced === false) {
      const ratchet = ban.ratchet ?? 0;
      advisory.push(`  ${ban.id}: ${findings.length} in ${scoped.length} files (ratchet ${ratchet})`);
      if (findings.length > ratchet) {
        errors.push(
          `ban "${ban.id}" is recorded at ${ratchet} findings and the tree now has ${findings.length}. ` +
            'It is not enforced yet, so it ratchets: fix the new ones, or lower nothing and raise nothing.',
        );
        errors.push(...findings.map((finding) => `  ${finding}`));
      }
      continue;
    }
    errors.push(...findings);
  }

  errors.push(...checkGlossary(bans));

  if (errors.length > 0) {
    console.error('check-terminology: FAILED');
    for (const error of errors) console.error(`- ${error}`);
    console.error('\nSee section 7 of docs/STYLE_GUIDE.md for the rule each finding references.');
    process.exitCode = 1;
    return;
  }
  console.log(`check-terminology: OK (${registry.terms.length} terms, ${bans.length} bans, ${corpus.length} files in the corpus)`);
  for (const line of advisory) console.log(line);
}

// ---------------------------------------------------------------------------
// Self-test
// ---------------------------------------------------------------------------

// The self-test drives the production analyze() and renderTable() over fixtures
// that must produce findings and fixtures that must not. Re-implementing the
// rules here would let a broken checker keep reporting OK, which is the failure
// scripts/check-gate-selftests.sh exists to catch on this file.
function selftest() {
  const failures = [];
  const registry = loadRegistry();
  const declarative = bansOf(registry).find((ban) => ban.id === 'declarative-as-workflow-label');
  const desired = bansOf(registry).find((ban) => ban.id === 'desired-state');
  if (!declarative) {
    console.error('check-terminology.mjs --selftest: FAILED\n- the registry no longer declares the declarative ban');
    process.exitCode = 1;
    return;
  }

  // Counted rather than stated. A self-test that announces a number it does not
  // derive is one edit away from claiming coverage it lost.
  let assertions = 0;
  const assert = (ok, complaint) => {
    assertions += 1;
    if (!ok) failures.push(complaint);
  };
  const fires = (source, note) =>
    assert(analyze(source, { bans: [declarative] }).length > 0, `no finding for ${note}: ${source.split('\n')[0]}`);
  const silent = (source, note) => {
    const found = analyze(source, { bans: [declarative] });
    assert(found.length === 0, `false positive for ${note}: ${found[0]?.message}`);
  };

  // Every violation shape the census measured, one fixture each. The bare
  // literal is the least of them: a gate matching `declarative schema changes`
  // finds two of nineteen and reports green.
  fires('| Declarative schema changes | The schema you want |', 'the exact banned phrase in a table row');
  fires('<LinkCard title="Declarative changes" />', 'the router card, where the phrase is shortened');
  fires('This tutorial runs the declarative workflow end to end.', 'the workflow named outright');
  fires('Save the declarative apply plan as a fingerprinted file.', 'an apply plan');
  fires('The declarative quick start uses plain SQL.', 'a page named by the retired label');
  fires('## Declarative and direct schema changes', 'the banned phrase split across a conjunction');
  fires('the same way every other declarative verb takes it', 'a verb');
  fires('Both declarative paths accept desired-state sources.', 'paths');

  // The phrase wraps in cmd/root/root.go, where a line-based grep answers
  // nothing. The stream is matched across the break and the finding names the
  // line the phrase STARTS on.
  {
    const wrapped = 'Use versioned migrations, declarative schema\nchanges, or both.';
    const found = analyze(wrapped, { bans: [declarative] });
    assert(found.length > 0, 'a phrase wrapping across a line break was not reported');
    assert(found[0]?.line === 1, `a wrapped phrase was reported on line ${found[0]?.line}, not where it starts`);
  }

  // An unknown head is a finding. This is what catches the next
  // `Declarative changes` card before it ships, and it is why the two lists are
  // not a convenience.
  {
    const found = analyze('Ptah supports declarative frobnication of tables.', { bans: [declarative] });
    assert(found.some((finding) => finding.message.includes('neither way')), 'an unclassified head was not reported');
  }

  // The legitimate uses. Each one is a sense the census measured and named, and
  // flagging any of them is what gets a gate turned off rather than obeyed.
  silent('## Declarative reference data', 'a capability name');
  silent('Declarative migration and schema tests run each case in order.', "Atlas's own name for its test format");
  silent('Ptah offers declarative database testing against a throwaway database.', 'a capability name');
  silent('PostgreSQL calls this a declaratively partitioned table.', "PostgreSQL's own term");
  silent('Every dialect renders declarative foreign keys.', 'an ordinary compound');
  silent('Skeema is a MySQL/MariaDB declarative schema tool.', "a third party's own description");
  silent('A declarative policy for which destructive changes a planner may emit.', 'ordinary English');
  silent('Run [Quick start](../quick-start-declarative/) to try it.', 'a route in a link destination');
  silent('Create the `ptah-declarative` directory first.', 'a directory name in a code span');
  silent('```bash\nmkdir ptah-declarative\n```', 'a directory name in a fenced block');
  silent('---\nslug: start/quick-start-declarative\n---\n\nThe direct workflow.', 'a route in frontmatter');
  // A reference definition whose label letter also occurs in the destination.
  // Locating the group by searching match[0] blanks the label and leaves the
  // URL live, so this fixture is the one that separates the two spans.
  silent('[a]: ../quick-start-declarative/', 'a reference definition destination');
  silent('See <https://atlasgo.io/declarative/apply> for the Atlas page.', 'an Atlas URL');
  silent('[Declarative schema apply](https://atlasgo.io/declarative/apply) is the Atlas page.', "an Atlas page title");
  silent('All tests MUST be purely declarative.', 'a predicate adjective with no head noun');
  silent('Roles and grants are managed declaratively, within the preset.', 'an adverb with no head noun');

  // Use against mention. The pages that explain the rule are full of mentions,
  // and every half of this group matters: without the firing ones, the mention
  // rule could be widened to "a quote silences everything" and stay green.
  silent('So "declarative" does not separate the two groups.', 'the word quoted as itself');
  silent('If you arrived looking for a "declarative" workflow, this is it.', 'a quoted mention heading a banned noun');
  // The ordinary way to mention a retired MULTI-WORD spelling is to quote the
  // whole spelling, and docs/site/CONTENT_INVENTORY.md -- the artifact this
  // repository mandates for exactly this kind of change -- does it eleven
  // times. A rule that read those as uses would make the page whose subject is
  // the retirement the page the gate is loudest on.
  silent('The phrase "declarative schema changes" is what section 7 replaced.', 'the whole retired spelling quoted as itself');
  silent('Atlas\'s "Declarative workflow" is adapted rather than adopted.', "another product's label quoted as itself");
  silent('Section 7 says: "declarative schema changes" is retired.', 'a quotation a colon introduces mid-sentence');
  // A quote is not a silencer. It has to close around the term; a quote that
  // runs on past the noun phrase is quoting a sentence, and a sentence that
  // uses the label uses it.
  fires('The docs say "declarative workflow steps are documented elsewhere".', 'a quoted sentence that uses the label');
  // A value is not a quotation. These two are the router card and the sidebar
  // entry, which is the text a reader meets rather than anybody quoting
  // anybody.
  fires('<LinkCard title="Declarative changes" href="../quick-start-direct/" />', 'an attribute value wrapped in quotes');
  fires('title: "Declarative changes"', 'a frontmatter value wrapped in quotes');
  fires("  label: 'Declarative schema changes',", 'a sidebar label wrapped in quotes');

  // Emphasis is not a head boundary. A scanner that let `**` end the scan would
  // read every emphasized term as headless, which is the ban's own target
  // wearing bold.
  silent('Ptah also supports a **declarative** model for lookup tables.', 'an emphasized authoring-model sense');
  fires('Ptah runs the **declarative** workflow end to end.', 'the banned label wearing bold');

  // The title frontmatter IS prose, and every link inheriting it does too.
  fires('---\ntitle: "Quick start: declarative changes"\n---\n', 'a frontmatter title');

  // A generated file redirects rather than suppresses: the author must be told
  // where the fix belongs, or they edit the page and the generator overwrites it.
  {
    const generated = Object.keys(declarative.generatedSources ?? {})[0];
    const found = analyze('The declarative workflow.', { bans: [declarative], path: generated });
    assert(
      found.some((finding) => finding.message.includes('this file is generated')),
      'a finding in a generated file did not name the generator',
    );
  }

  // The marker, and both of the properties that stop it outliving its sentence.
  {
    const covered = '<!-- terminology-allow: declarative changes -- names the retired label in order to retire it -->\nThe declarative changes label is what section 7 replaced.';
    assert(analyze(covered, { bans: [declarative] }).length === 0, 'a marker with a reason did not suppress its finding');

    const reasonless = '<!-- terminology-allow: declarative changes -- typo -->\nThe declarative changes label.';
    assert(
      analyze(reasonless, { bans: [declarative] }).some((finding) => finding.message.includes('carries no reason')),
      'a marker with no reason was accepted',
    );

    const stale = '<!-- terminology-allow: declarative changes -- the sentence this marked was rewritten long ago -->\nThe direct schema changes workflow.';
    assert(
      analyze(stale, { bans: [declarative] }).some((finding) => finding.message.includes('suppresses nothing')),
      'a marker matching nothing was accepted',
    );
  }

  // The advisory ban is data, and it has to keep matching or its ratchet is a
  // number about nothing.
  if (desired) {
    assert(
      analyze('Load the desired state and report every problem.', { bans: [desired] }).length > 0,
      'the advisory desired-state ban reported nothing on its own banned spelling',
    );
    assert(
      analyze('Load the desired schema and report every problem.', { bans: [desired] }).length === 0,
      'the advisory desired-state ban fired on the canonical spelling',
    );
  }

  // The two lists must stay disjoint, or the longest-match rule decides a head
  // by which set is consulted first, which is not a decision anyone made.
  for (const ban of bansOf(registry)) {
    const banned = new Set((ban.bannedHeads ?? []).map((head) => head.toLowerCase()));
    for (const head of (ban.allowedHeads ?? []).map((entry) => entry.toLowerCase())) {
      assert(!banned.has(head), `ban "${ban.id}" lists the head "${head}" as both banned and allowed`);
    }
  }

  // The generated table has to render every term, or section 7 would be checked
  // against a rendering shorter than the registry.
  {
    const table = renderTable(registry.terms);
    const rows = table.split('\n').length - 2;
    assert(rows === registry.terms.length, `renderTable produced ${rows} rows for ${registry.terms.length} terms`);
    assert(table.includes('| direct schema changes |'), 'renderTable dropped the direct schema changes row');
  }

  // Everything that could quietly govern nothing is refused rather than
  // reported OK. Each of these is a way a gate stops reading without saying so.
  const refuses = (thunk, complaint) => {
    let refused = false;
    try {
      thunk();
    } catch {
      refused = true;
    }
    assert(refused, complaint);
  };

  refuses(() => scopedFiles({ id: 'fixture', scope: { exclude: ['**/*.md'] } }, ['README.md']), 'a scope excluding every file was not refused');
  refuses(
    () => scopedFiles({ id: 'fixture', scope: { exclude: ['no/such/page.md'] } }, ['README.md']),
    'an exclusion matching no file was not refused',
  );
  refuses(() => corpusOf({ corpus: { extensions: ['.rst'] } }, ['README.md']), 'a corpus matching no file was not refused');
  refuses(() => corpusOf({ corpus: {} }, ['README.md']), 'a corpus declaring no extensions was not refused');
  refuses(
    () => corpusOf({ corpus: { extensions: ['.md'], also: ['docs/site/src/no-such-source.mjs'] } }, ['README.md']),
    'a corpus.also pattern matching no file was not refused',
  );
  refuses(
    () => corpusOf({ corpus: { extensions: ['.md'], exclude: ['no/such/page.md'] } }, ['README.md']),
    'a corpus.exclude pattern matching no file was not refused',
  );

  // The corpus has to be a corpus CI sees. The checked-in workflow is
  // unfiltered; the reachability rule remains covered for any future filter.
  {
    const workflow = readFileSync(join(repoRoot, workflowPath), 'utf8');
    assert(workflowRunsForEveryPullRequest(workflow), `${workflowPath} no longer runs on every pull request`);
    const patterns = ['docs/**', '**.md'];
    assert(
      unreachableFromWorkflow(['TESTING.md', 'docs/site/src/sidebar.mjs'], patterns).length === 0,
      'a root record and a site source were not reachable from the workflow trigger',
    );
    assert(
      unreachableFromWorkflow(['cmd/root/root.go'], patterns).length === 1,
      'a file outside every trigger pattern was reported reachable',
    );
    assert(
      unreachableFromWorkflow(['TESTING.md'], ['docs/**']).length === 1,
      'the reachability rule passed a file the given filter cannot reach',
    );
  }

  // A ban that could match nothing is the same failure written in the registry
  // instead of in a path list: three readers accept it, the OK line counts it,
  // and it reports nothing forever.
  const oneBan = (ban) => ({ terms: [{ id: 'fixture', term: 'fixture', bans: [{ id: 'fixture', message: 'x', ...ban }] }] });
  refuses(() => bansOf(oneBan({ stems: [], bannedHeads: ['file'] })), 'a ban with no stems was not refused');
  refuses(() => bansOf(oneBan({ bannedHeads: ['file'] })), 'a ban with no stems key was not refused');
  refuses(
    () => bansOf(oneBan({ stems: ['sum'], bannedHeads: [], onUnknownHead: 'allow' })),
    'a ban that bans no head and allows every unknown one was not refused',
  );
  refuses(
    () => bansOf({ terms: [{ id: 'f', term: 'f', bans: [{ id: 'f', stems: ['sum'], bannedHeads: ['file'] }] }] }),
    'a ban with no message was not refused',
  );
  assert(bansOf(oneBan({ stems: ['sum'], bannedHeads: [], onUnknownHead: 'report' })).length === 1, 'a report-on-unknown ban with no banned head was refused');

  // The generated table says what holds each row, so section 16 does not have
  // to say it in a sentence that goes stale when a ban is added.
  assert(heldBy({ term: 'x' }) === 'review', 'a row with no ban claimed a gate holds it');
  assert(heldBy({ bans: [{ enforced: false }] }).includes('ratcheted'), 'a ratcheted row did not say so');
  assert(
    heldBy({ bans: [{ helpText: { tree: 'native' } }] }).includes('terminologyguard'),
    'a row the Go reader also holds did not name it',
  );

  if (failures.length > 0) {
    console.error('check-terminology.mjs --selftest: FAILED');
    for (const failure of failures) console.error(`- ${failure}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-terminology.mjs --selftest: OK (${assertions} assertions through analyze(), renderTable() and scopedFiles())`);
}

function main() {
  const args = process.argv.slice(2);
  if (args.includes('--selftest')) {
    selftest();
    return;
  }
  run({ write: args.includes('--write') });
}

// Only when this file is the program. Its helpers -- proseOf above all -- are
// exported so another gate reads prose the same way rather than deciding again
// what a code span is, and an unguarded main would run this whole check as a
// side effect of that import, with the importer's flags.
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main();
}
