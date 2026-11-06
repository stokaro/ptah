#!/usr/bin/env node
// Holds every top-level examples directory to one reader contract and keeps the
// examples index derived from those READMEs.
//
// It also holds a page that quotes an example to the example's files. A README
// names such a page with a `<!-- ptah:quoted-by <path> -->` line, the path
// relative to the repository root. On that page, every fenced block whose meta
// carries `title="<file>"` must be a run of whole lines of that file of the
// example, and every `go` block must carry such a title. A tutorial whose code
// drifted from the example it teaches then fails here, in the change that
// moved one without the other.
import { existsSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = join(scriptDir, '..', '..', '..');
const requiredHeadings = [
  'What this example demonstrates',
  'Prerequisites',
  'Run',
  'Expected result',
  'Verify',
  'Cleanup',
  'Learn more',
];
const generatedStart = '<!-- ptah:examples-index:start -->';
const generatedEnd = '<!-- ptah:examples-index:end -->';

function directories(root) {
  return readdirSync(root, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith('.'))
    .map((entry) => entry.name)
    .sort();
}

function section(source, heading) {
  const marker = `## ${heading}`;
  const start = source.split(/\r?\n/).findIndex((line) => line.trim() === marker);
  if (start === -1) return '';
  const lines = source.split(/\r?\n/).slice(start + 1);
  const end = lines.findIndex((line) => line.startsWith('## '));
  return lines.slice(0, end === -1 ? undefined : end).join('\n').trim();
}

function plainPurpose(source) {
  return section(source, 'What this example demonstrates')
    .split(/\n\s*\n/)[0]
    .replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
    .replace(/[`*]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function title(source, fallback) {
  return source.match(/^# (.+)$/m)?.[1].trim() ?? fallback;
}

export function analyze(root) {
  const problems = [];
  const examples = [];
  for (const name of directories(root)) {
    const readmePath = join(root, name, 'README.md');
    let source;
    try {
      source = readFileSync(readmePath, 'utf8');
    } catch {
      problems.push(`${name}: README.md is missing`);
      continue;
    }
    for (const heading of requiredHeadings) {
      if (!section(source, heading)) problems.push(`${name}: section ${JSON.stringify(heading)} is missing or empty`);
    }
    const purpose = plainPurpose(source);
    if (!purpose) problems.push(`${name}: the demonstration summary is empty`);
    examples.push({ name, purpose, title: title(source, name) });
  }
  return { examples, problems };
}

const quotedByMarker = /<!--\s*ptah:quoted-by\s+(\S+)\s*-->/g;

// fencedBlocks returns each fenced block of a Markdown source with its
// language, its meta string and its body, without the fence lines.
export function fencedBlocks(source) {
  const blocks = [];
  const lines = source.replace(/\r\n/g, '\n').split('\n');
  for (let index = 0; index < lines.length; index += 1) {
    const opening = lines[index].match(/^(\s*)(`{3,}|~{3,})([^`\s]*)\s*(.*)$/);
    if (!opening) continue;
    const [, indent, fence, language, meta] = opening;
    const body = [];
    let end = index + 1;
    for (; end < lines.length; end += 1) {
      if (lines[end].trim() === fence) break;
      body.push(indent && lines[end].startsWith(indent) ? lines[end].slice(indent.length) : lines[end]);
    }
    blocks.push({ language, meta, body: body.join('\n'), line: index + 1 });
    index = end;
  }
  return blocks;
}

// quotedExcerptProblems reads every README under examplesRoot for quoted-by
// markers and checks each named page, resolved against repositoryRoot.
export function quotedExcerptProblems(examplesRoot, repositoryRoot) {
  const problems = [];
  let pages = 0;
  for (const name of directories(examplesRoot)) {
    const readmePath = join(examplesRoot, name, 'README.md');
    if (!existsSync(readmePath)) continue;
    for (const [, pagePath] of readFileSync(readmePath, 'utf8').matchAll(quotedByMarker)) {
      pages += 1;
      const absolutePage = join(repositoryRoot, pagePath);
      if (!existsSync(absolutePage)) {
        problems.push(`${name}: README.md names ${pagePath}, which does not exist`);
        continue;
      }
      let checked = 0;
      for (const block of fencedBlocks(readFileSync(absolutePage, 'utf8'))) {
        const title = block.meta.match(/(?:^|\s)title="([^"]+)"/)?.[1];
        if (!title) {
          if (block.language === 'go') problems.push(`${pagePath}:${block.line}: a go block names no example file in title="..."`);
          continue;
        }
        const file = join(examplesRoot, name, title);
        if (!existsSync(file)) {
          problems.push(`${pagePath}:${block.line}: title="${title}" names no file of examples/${name}`);
          continue;
        }
        checked += 1;
        const text = readFileSync(file, 'utf8').replace(/\r\n/g, '\n');
        // Framed by newlines, so a block matches whole lines only.
        if (!`\n${text}${text.endsWith('\n') ? '' : '\n'}`.includes(`\n${block.body}\n`)) {
          problems.push(`${pagePath}:${block.line}: the block titled ${title} is not a run of whole lines of examples/${name}/${title}`);
        }
      }
      if (checked === 0) problems.push(`${pagePath}: quotes no file of examples/${name}`);
    }
  }
  return { pages, problems };
}

export function renderIndex(examples) {
  const rows = examples.map(({ name, purpose, title }) => `| [${title}](${name}/) | ${purpose} |`);
  return `# Ptah examples

Each example has one run, expected-result, verification, cleanup, and canonical
documentation contract in its own README. The executable gate runs local
examples and mechanically verifies provider-backed examples.

${generatedStart}
| Example | What it demonstrates |
| --- | --- |
${rows.join('\n')}
${generatedEnd}
`;
}

function selftest() {
  const root = mkdtempSync(join(tmpdir(), 'ptah-examples-check-'));
  try {
    const complete = requiredHeadings.map((heading) => `## ${heading}\n\ncontent\n`).join('\n');
    mkdirSync(join(root, 'complete'));
    writeFileSync(join(root, 'complete', 'README.md'), `# Complete\n\n${complete}`);
    mkdirSync(join(root, 'missing'));
    writeFileSync(join(root, 'missing', 'README.md'), '# Missing\n\n## Run\n\ncommand\n');
    const result = analyze(root);
    const rendered = renderIndex(result.examples);
    if (
      result.problems.length !== requiredHeadings.length ||
      !result.problems.some((problem) => problem.includes('Expected result')) ||
      !rendered.includes('[Complete](complete/)')
    ) {
      console.error('check-examples.mjs --selftest: FAILED');
      process.exitCode = 1;
      return;
    }

    // A quoting page: one faithful excerpt, one that drifted, one go block
    // without a title, and one title that names no file.
    const repository = join(root, 'repository');
    mkdirSync(join(repository, 'examples', 'quoted'), { recursive: true });
    writeFileSync(join(repository, 'examples', 'quoted', 'README.md'), '<!-- ptah:quoted-by page.md -->\n');
    writeFileSync(join(repository, 'examples', 'quoted', 'main.go'), 'package main\n\nfunc main() {\n\tprintln("one")\n}\n');
    const page = [
      '```go title="main.go"', 'func main() {', '\tprintln("one")', '}', '```',
      '```go title="main.go"', 'func main() {', '\tprintln("two")', '}', '```',
      '```go', 'package main', '```',
      '```text title="missing.go"', 'x', '```',
    ].join('\n');
    writeFileSync(join(repository, 'page.md'), page);
    const quoted = quotedExcerptProblems(join(repository, 'examples'), repository);
    const expected = ['page.md:6: the block titled main.go', 'page.md:11: a go block names no example file', 'page.md:14: title="missing.go" names no file'];
    if (quoted.pages !== 1 || quoted.problems.length !== expected.length || !expected.every((text, index) => quoted.problems[index].startsWith(text))) {
      console.error(`check-examples.mjs --selftest: FAILED (quoted excerpts: ${JSON.stringify(quoted.problems)})`);
      process.exitCode = 1;
      return;
    }
    writeFileSync(join(repository, 'page.md'), 'No code here.\n');
    const empty = quotedExcerptProblems(join(repository, 'examples'), repository);
    if (empty.problems.length !== 1 || !empty.problems[0].includes('quotes no file')) {
      console.error('check-examples.mjs --selftest: FAILED (a quoting page with no excerpt was accepted)');
      process.exitCode = 1;
      return;
    }
    console.log('check-examples.mjs --selftest: OK (missing contract sections rejected, index rendered, quoted excerpts held to their files)');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

function main() {
  if (process.argv.includes('--selftest')) {
    selftest();
    return;
  }

  const root = join(repositoryRoot, 'examples');
  const indexPath = join(root, 'README.md');
  const result = analyze(root);
  const quoted = quotedExcerptProblems(root, repositoryRoot);
  const problems = [...result.problems, ...quoted.problems];
  if (problems.length > 0) {
    console.error('check-examples.mjs: FAILED');
    for (const problem of problems) console.error(`- ${problem}`);
    process.exitCode = 1;
    return;
  }

  const rendered = renderIndex(result.examples);
  if (process.argv.includes('--write')) {
    writeFileSync(indexPath, rendered);
    console.log(`check-examples.mjs --write: wrote ${relative(repositoryRoot, indexPath)}`);
    return;
  }

  let current = '';
  try { current = readFileSync(indexPath, 'utf8'); } catch { /* reported as stale below */ }
  if (current !== rendered) {
    console.error('check-examples.mjs: examples/README.md is stale; run "npm run examples:write" in docs/site.');
    process.exitCode = 1;
    return;
  }
  console.log(`check-examples.mjs: OK (${result.examples.length} example contracts, generated index, ${quoted.pages} quoting page${quoted.pages === 1 ? '' : 's'})`);
}

main();
