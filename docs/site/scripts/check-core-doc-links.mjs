#!/usr/bin/env node
// Fails when a published site page sends the reader to a repository document
// instead of publishing the information itself.
//
// The site is the product's documentation. A page that links to
// `docs/capabilities.md` on GitHub hands a reader a file with no navigation, no
// version selector and no search, and it hides a gap: the information was never
// published, and the link is what makes that look intentional.
//
// Two spellings are refused. A repository-relative path to one of the protected
// core documents, and any link into `blob/master/docs/` on GitHub, which is the
// same handoff written as a URL.
import { readdirSync, readFileSync } from 'node:fs';
import { dirname, extname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const docsRoot = join(siteRoot, 'src', 'content', 'docs');

const forbidden = [
  'docs/yaml_schema.md',
  'docs/atlas_hcl_schema.md',
  'docs/atlas_project_config.md',
  'docs/public_api.md',
  'docs/capabilities.md',
  'docs/sqlite.md',
  'docs/sqlserver.md',
];
const forbiddenRootDocsURL = 'github.com/stokaro/ptah/blob/master/docs/';

function toPosix(value) {
  return value.split(sep).join('/');
}

function walk(dir) {
  const files = [];
  for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...walk(fullPath));
      continue;
    }
    if (entry.isFile() && ['.md', '.mdx'].includes(extname(entry.name))) {
      files.push(fullPath);
    }
  }
  return files;
}

// analyze takes the pages already read -- `{ path, source }` -- so the rules can
// be driven over fixtures that never touch the filesystem, and returns one
// problem per violation.
//
// The empty-corpus refusal is the first rule rather than a detail. This gate
// discovers its own inputs by walking a directory, so a walk that finds nothing
// -- a moved content root, a renamed extension -- produces zero problems and
// prints the same success it prints on a clean tree. The count in that success
// line was the protected-reference count, which is a constant: it read as
// evidence of work done while the corpus was empty.
export function analyze(pages) {
  if (pages.length === 0) {
    return ['no documentation pages were scanned; a gate that read nothing reports the same success as one that found nothing'];
  }

  const problems = [];
  for (const page of pages) {
    if (page.source.includes(forbiddenRootDocsURL)) {
      problems.push(`${page.path}: links to root docs on GitHub; publish core information in docs/site instead`);
    }
    for (const target of forbidden) {
      if (page.source.includes(target)) {
        problems.push(`${page.path}: links to ${target}; use the published site reference page instead`);
      }
    }
  }
  return problems;
}

function selftest() {
  const assert = (condition, message) => {
    if (!condition) throw new Error(message);
  };

  const page = (source) => [{ path: 'reference/example.md', source }];

  // The control. Without it every assertion below could be a gate that rejects
  // whatever it is given. The prose names the protected subjects and links to
  // the published pages that carry them, which is exactly what the rule asks a
  // page to do.
  assert(
    analyze(page('See [capabilities](/reference/capabilities/) and the [public API](/extend/public-api/).\n')).length === 0,
    'a page linking to the published references is accepted',
  );

  // Each half of the rule, one at a time, so a fixture cannot pass on the other
  // half's account.
  const relativeLink = analyze(page('Read [the capability matrix](../../../docs/capabilities.md).\n'));
  assert(relativeLink.length === 1, `a repository-relative link to a protected document is refused: ${JSON.stringify(relativeLink)}`);
  assert(relativeLink[0].includes('docs/capabilities.md'), 'the refusal names the document that was linked');

  const gitHubLink = analyze(page('Read [it](https://github.com/stokaro/ptah/blob/master/docs/anything.md).\n'));
  assert(gitHubLink.length === 1, `a GitHub link into the repository docs directory is refused: ${JSON.stringify(gitHubLink)}`);
  assert(gitHubLink[0].includes('root docs on GitHub'), 'the URL refusal says which rule fired');

  // The URL rule covers the whole directory rather than the protected list, so
  // a document that is not on the list is still refused when it is reached this
  // way. Asserted because the two rules are easy to collapse into one.
  assert(
    analyze(page('https://github.com/stokaro/ptah/blob/master/docs/STYLE_GUIDE.md\n'))[0].includes('root docs on GitHub'),
    'the URL rule is not limited to the protected list',
  );

  // A page carrying both spellings reports both. A gate that stopped at the
  // first would leave the second to a later run that nobody makes.
  assert(
    analyze(page('docs/public_api.md and https://github.com/stokaro/ptah/blob/master/docs/sqlite.md\n')).length >= 2,
    'every violation on a page is reported, not only the first',
  );

  // The floor. This is the failure the count in the success line used to hide.
  const empty = analyze([]);
  assert(empty.length === 1, 'an empty corpus is refused');
  assert(empty[0].includes('read nothing'), 'the refusal says the gate scanned nothing');

  console.log('check-core-doc-links.mjs --selftest: OK');
}

function main() {
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }

  const files = walk(docsRoot);
  const problems = analyze(
    files.map((file) => ({
      path: toPosix(relative(process.cwd(), file)),
      source: readFileSync(file, 'utf8'),
    })),
  );

  if (problems.length > 0) {
    console.error('Core documentation should be self-contained in the published site:');
    for (const problem of problems) {
      console.error(`- ${problem}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log(`check-core-doc-links.mjs: OK (${forbidden.length} protected core references across ${files.length} pages)`);
}

main();
