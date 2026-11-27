#!/usr/bin/env node
// Keep repository instructions small, discoverable, and linked to tracked files.
// Read Git's index so parked worktrees cannot enlarge or satisfy the corpus.
import { execFileSync, spawnSync } from 'node:child_process';
import { lstatSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, posix, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptPath = fileURLToPath(import.meta.url);
const repoRoot = resolve(dirname(scriptPath), '../../..');
const rootBudget = 12 * 1024;
const claudeBudget = 1024;
const skillBudget = 8 * 1024;
const adapterBudget = 2 * 1024;
// A narrowed discovery pattern must not pass after finding just one skill.
// The corpus covers Go, schemas, providers, compatibility, validation, docs, and review.
const minimumSkills = 5;
const canonicalPattern = /^\.agents\/skills\/([^/]+)\/SKILL\.md$/;
const adapterPattern = /^\.claude\/skills\/([^/]+)\/SKILL\.md$/;

function outsideFences(source) {
  let fence = null;
  return source.split('\n').map((line) => {
    const match = /^\s*(`{3,}|~{3,})/.exec(line);
    if (match) {
      if (fence === null) fence = match[1];
      else if (match[1][0] === fence[0] && match[1].length >= fence.length) fence = null;
      return '';
    }
    return fence === null ? line : '';
  }).join('\n');
}

function imports(source) {
  const text = outsideFences(source).replace(/(`+)[^\n]*?\1/g, '');
  return [...text.matchAll(/(?:^|[\s([{])@([^\s)\]}>,;]+)/g)].map((match) => match[1]);
}

// Only the simple scalar frontmatter used by these repository skills is accepted.
// Reject folded/multiline values instead of guessing what another agent parses.
function metadata(source, path, findings) {
  const match = /^---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/.exec(source);
  if (!match) {
    findings.push(`${path}: missing frontmatter`);
    return null;
  }
  const fields = {};
  for (const line of match[1].split(/\r?\n/)) {
    const field = /^(name|description): (.+)$/.exec(line);
    if (!field || fields[field[1]] !== undefined) {
      findings.push(`${path}: use one simple name and description field`);
      return null;
    }
    let value = field[2].trim();
    try {
      if (value.startsWith('"')) value = JSON.parse(value);
      else if (/^'(?:[^']|'')*'$/.test(value)) value = value.slice(1, -1).replaceAll("''", "'");
      else if (/^[>|'"[\]{}&*!#%@`]|:\s|\s#|^(?:true|false|null|~|[\d.+-]+)$/i.test(value)) throw new Error('not a plain scalar');
    } catch {
      findings.push(`${path}: ${field[1]} must be a simple string scalar`);
      return null;
    }
    fields[field[1]] = value;
  }
  if (typeof fields.name !== 'string' || fields.name.length > 64 || !/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(fields.name)) {
    findings.push(`${path}: invalid skill name`);
  }
  if (typeof fields.description !== 'string' || /[\r\n]/.test(fields.description) || fields.description.trim().length < 20 || fields.description.length > 1024) {
    findings.push(`${path}: description must be a useful single-line string of 20 to 1024 characters`);
  }
  if (source.slice(match[0].length).trim().length < 80) findings.push(`${path}: instruction body is empty or only a stub`);
  return fields;
}

function localTargets(path, source, findings) {
  const text = outsideFences(source);
  const links = [...text.matchAll(/\[[^\]\n]*\]\((<[^>\n]+>|[^\s)]+)(?:\s+["'][^\n]*?["'])?\)/g)].map((match) => match[1]);
  links.push(...[...text.matchAll(/^ {0,3}\[(?!\^)[^\]\n]+\]:\s*(<[^>\n]+>|\S+)/gm)].map((match) => match[1]));
  return links.flatMap((link) => {
    const raw = link.replace(/^<|>$/g, '');
    if (/^(?:[a-z][a-z0-9+.-]*:|\/\/|#)/i.test(raw)) return [];
    let target;
    try {
      target = decodeURIComponent(raw.split(/[?#]/)[0]);
    } catch {
      findings.push(`${path}: invalid link ${raw}`);
      return [];
    }
    const resolved = posix.normalize(target.startsWith('/') ? target.slice(1) : posix.join(posix.dirname(path), target));
    if (resolved === '..' || resolved.startsWith('../')) {
      findings.push(`${path}: local link escapes the repository: ${raw}`);
      return [];
    }
    return [resolved];
  });
}

function inspect(root) {
  const tracked = execFileSync('git', ['ls-files', '-z'], { cwd: root, encoding: 'utf8' }).split('\0').filter(Boolean);
  const trackedSet = new Set(tracked);
  const paths = tracked.filter((path) => /(?:^|\/)(?:AGENTS|CLAUDE)\.md$/.test(path) || path.startsWith('.agents/') || path.startsWith('.claude/skills/'));
  const findings = [];
  const sources = new Map();
  for (const path of paths.filter((name) => name.endsWith('.md'))) {
    try {
      if (!lstatSync(join(root, path)).isFile()) throw new Error('not a regular Markdown file');
      sources.set(path, readFileSync(join(root, path), 'utf8'));
    } catch (error) {
      findings.push(`${path}: cannot read tracked guidance: ${error.message}`);
    }
  }
  for (const path of ['AGENTS.md', 'CLAUDE.md']) {
    if (!sources.get(path)?.trim()) findings.push(`${path}: tracked nonempty entrypoint is required`);
  }
  if (Buffer.byteLength(sources.get('AGENTS.md') ?? '') > rootBudget) findings.push(`AGENTS.md: exceeds ${rootBudget} byte budget`);
  if (Buffer.byteLength(sources.get('CLAUDE.md') ?? '') > claudeBudget) findings.push(`CLAUDE.md: exceeds ${claudeBudget} byte budget`);
  if (imports(sources.get('AGENTS.md') ?? '').length !== 0) findings.push('AGENTS.md: do not eagerly import other instructions; route with Markdown links');
  const claudeImports = imports(sources.get('CLAUDE.md') ?? '');
  if (claudeImports.length !== 1 || claudeImports[0] !== 'AGENTS.md') findings.push('CLAUDE.md: import only @AGENTS.md once; do not eagerly import skills');

  const links = new Map();
  for (const [path, source] of sources) {
    const targets = localTargets(path, source, findings);
    links.set(path, targets);
    for (const target of targets) {
      const inTree = trackedSet.has(target) || tracked.some((file) => file.startsWith(`${target}/`));
      try {
        if (!inTree) throw new Error('not tracked');
        lstatSync(join(root, target));
      } catch {
        findings.push(`${path}: local link target is missing or untracked: ${target}`);
      }
    }
  }

  const skills = paths.filter((path) => canonicalPattern.test(path));
  if (skills.length < minimumSkills) findings.push(`found ${skills.length} canonical skills, below the corpus floor of ${minimumSkills}`);
  for (const path of skills) {
    const name = canonicalPattern.exec(path)[1];
    const source = sources.get(path);
    if (source === undefined) continue;
    if (Buffer.byteLength(source) > skillBudget) findings.push(`${path}: exceeds ${skillBudget} byte budget`);
    const fields = metadata(source, path, findings);
    if (fields?.name !== name) findings.push(`${path}: name must match its skill directory`);
    if (!links.get('AGENTS.md')?.includes(path)) findings.push(`AGENTS.md: does not link canonical skill ${name}`);
    const adapterPath = `.claude/skills/${name}/SKILL.md`;
    const adapter = sources.get(adapterPath);
    if (adapter === undefined) {
      findings.push(`${path}: missing tracked Claude adapter`);
      continue;
    }
    if (Buffer.byteLength(adapter) > adapterBudget) findings.push(`${adapterPath}: exceeds ${adapterBudget} byte adapter budget; keep policy in the canonical skill`);
    const adapterFields = metadata(adapter, adapterPath, findings);
    if (adapterFields?.name !== fields?.name || adapterFields?.description !== fields?.description) {
      findings.push(`${adapterPath}: metadata differs from the canonical skill`);
    }
    if (!links.get(adapterPath)?.includes(path)) findings.push(`${adapterPath}: missing canonical skill link`);
  }
  for (const path of paths.filter((name) => adapterPattern.test(name))) {
    const name = adapterPattern.exec(path)[1];
    if (!trackedSet.has(`.agents/skills/${name}/SKILL.md`)) findings.push(`${path}: orphan Claude adapter`);
  }
  return { findings, skills: skills.length, files: sources.size };
}

function selftest() {
  const temp = mkdtempSync(join(tmpdir(), 'ptah-agent-guidance-'));
  const failures = [];
  let assertions = 0;
  try {
    const fixture = new Map([
      ['AGENTS.md', '# Agent guidance\n\n'],
      ['CLAUDE.md', '@AGENTS.md\n'],
      ['docs/guide.md', '# A tracked reference\n'],
    ]);
    for (let i = 0; i < minimumSkills; i += 1) {
      const name = `ptah-task-${i}`;
      const frontmatter = `---\nname: ${name}\ndescription: Use for the matching Ptah task and validate its observable result.\n---\n\n`;
      const canonical = `.agents/skills/${name}/SKILL.md`;
      fixture.set(canonical, `${frontmatter}# Task instructions\n\nRead [the guide](../../../docs/guide.md), inspect the changed behavior, and verify the result through its public entry point.\n`);
      fixture.set(`.claude/skills/${name}/SKILL.md`, `${frontmatter}# Skill adapter\n\nRead [the canonical skill](../../../${canonical}) and follow its instructions. Load only references relevant to the current task.\n`);
      fixture.set('AGENTS.md', `${fixture.get('AGENTS.md')}[${name}](${canonical})\n`);
    }
    execFileSync('git', ['init', '-q'], { cwd: temp });
    const writeFixture = (files) => {
      for (const path of fixture.keys()) rmSync(join(temp, path), { force: true });
      for (const [path, content] of files) {
        mkdirSync(dirname(join(temp, path)), { recursive: true });
        writeFileSync(join(temp, path), content);
      }
      execFileSync('git', ['add', '-A'], { cwd: temp });
    };
    const skill = '.agents/skills/ptah-task-0/SKILL.md';
    const adapter = '.claude/skills/ptah-task-0/SKILL.md';
    const cases = [
      ['complete fixture', (files) => files, null],
      ['empty corpus', (files) => new Map([...files].filter(([path]) => !path.startsWith('.agents/skills/'))), 'below the corpus floor'],
      ['partially shrunk consistent corpus', (files) => {
        files.delete(skill);
        files.delete(adapter);
        return files.set('AGENTS.md', files.get('AGENTS.md').replace('[ptah-task-0](.agents/skills/ptah-task-0/SKILL.md)', ''));
      }, 'below the corpus floor'],
      ['oversized root', (files) => files.set('AGENTS.md', `${files.get('AGENTS.md')}${'x'.repeat(rootBudget)}`), 'AGENTS.md: exceeds'],
      ['oversized Claude entrypoint', (files) => files.set('CLAUDE.md', `@AGENTS.md\n${'x'.repeat(claudeBudget)}`), 'CLAUDE.md: exceeds'],
      ['oversized skill', (files) => files.set(skill, `${files.get(skill)}${'x'.repeat(skillBudget)}`), 'byte budget'],
      ['missing metadata', (files) => files.set(skill, '# Instructions without frontmatter'), 'missing frontmatter'],
      ['invalid name', (files) => files.set(skill, files.get(skill).replace('name: ptah-task-0', 'name: Bad_Name')), 'invalid skill name'],
      ['multiline description', (files) => files.set(skill, files.get(skill).replace('description: Use', 'description: |\n  Use')), 'simple string scalar'],
      ['decoded multiline description', (files) => files.set(skill, files.get(skill).replace(/description: .+/, `description: ${JSON.stringify('Use for the matching Ptah task.\nValidate its observable result.')}`)), 'single-line string'],
      ['duplicate metadata', (files) => files.set(skill, files.get(skill).replace('name: ptah-task-0', 'name: ptah-task-0\nname: ptah-task-0')), 'one simple name'],
      ['unrouted skill', (files) => files.set('AGENTS.md', files.get('AGENTS.md').replace('[ptah-task-0](.agents/skills/ptah-task-0/SKILL.md)', '')), 'does not link canonical skill'],
      ['missing adapter', (files) => { files.delete(adapter); return files; }, 'missing tracked Claude adapter'],
      ['metadata drift', (files) => files.set(adapter, files.get(adapter).replace('matching Ptah task', 'different Ptah task')), 'metadata differs'],
      ['wrong adapter link', (files) => files.set(adapter, files.get(adapter).replace('../../../.agents/skills/ptah-task-0/SKILL.md', '../../../docs/guide.md')), 'missing canonical skill link'],
      ['orphan adapter', (files) => { files.delete(skill); return files; }, 'orphan Claude adapter'],
      ['eager import', (files) => files.set('CLAUDE.md', '@AGENTS.md\n@.agents/skills/ptah-task-0/SKILL.md\n'), 'import only @AGENTS.md'],
      ['eager root import', (files) => files.set('AGENTS.md', `${files.get('AGENTS.md')}\nRead (@.agents/skills/ptah-task-0/SKILL.md).\n`), 'do not eagerly import other instructions'],
      ['imports in examples', (files) => files.set('AGENTS.md', `${files.get('AGENTS.md')}\n\`@example.md\`\n\n\`\`\`text\n@another-example.md\n\`\`\`\n`), null],
      ['missing link', (files) => files.set(skill, `${files.get(skill)}\n[missing](missing.md)\n`), 'local link target is missing'],
      ['reference link', (files) => files.set(skill, `${files.get(skill)}\n[more]: missing.md\n`), 'local link target is missing'],
      ['stub body', (files) => files.set(skill, files.get(skill).split('# Task instructions')[0] + '# Task\n'), 'only a stub'],
      ['copied adapter policy', (files) => files.set(adapter, `${files.get(adapter)}${'x'.repeat(adapterBudget)}`), 'adapter budget'],
    ];
    for (const [name, change, expected] of cases) {
      writeFixture(change(new Map(fixture)));
      const result = inspect(temp);
      assertions += 1;
      if (expected === null ? result.findings.length !== 0 : !result.findings.some((finding) => finding.includes(expected))) {
        failures.push(`${name}: ${JSON.stringify(result.findings)}`);
      }
    }
    writeFixture(fixture);
    const parked = join(temp, '.claude/worktrees/parked/AGENTS.md');
    mkdirSync(dirname(parked), { recursive: true });
    writeFileSync(parked, 'x'.repeat(rootBudget * 2));
    assertions += 1;
    if (inspect(temp).findings.length !== 0) failures.push('untracked parked worktree changed the corpus');

    // Exercise the real entrypoint against an isolated Git checkout, then break
    // a required target. A passing helper with a disconnected CLI is insufficient.
    const fixtureScript = join(temp, 'docs/site/scripts/check-agent-guidance.mjs');
    mkdirSync(dirname(fixtureScript), { recursive: true });
    writeFileSync(fixtureScript, readFileSync(scriptPath));
    const good = spawnSync(process.execPath, [fixtureScript], { encoding: 'utf8' });
    assertions += 1;
    if (good.status !== 0 || !good.stdout.includes('check-agent-guidance.mjs: OK')) failures.push(`entrypoint refused a valid fixture: ${good.stderr}`);
    const unknown = spawnSync(process.execPath, [fixtureScript, '--unknown'], { encoding: 'utf8' });
    assertions += 1;
    if (unknown.status !== 1 || !unknown.stderr.includes('unknown arguments')) failures.push('entrypoint accepted an unknown argument');
    rmSync(join(temp, 'docs/guide.md'));
    const bad = spawnSync(process.execPath, [fixtureScript], { encoding: 'utf8' });
    assertions += 1;
    if (bad.status !== 1 || !bad.stderr.includes('local link target is missing')) failures.push('entrypoint did not fail for a removed target');
  } finally {
    rmSync(temp, { recursive: true, force: true });
  }
  if (failures.length) {
    console.error('check-agent-guidance.mjs --selftest: FAILED');
    for (const failure of failures) console.error(`- ${failure}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-agent-guidance.mjs --selftest: OK (${assertions} assertions, including the real entrypoint)`);
}

function main() {
  if (process.argv.length > 3 || (process.argv[2] !== undefined && process.argv[2] !== '--selftest')) {
    throw new Error('unknown arguments; use --selftest or no arguments');
  }
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }
  const { findings, skills, files } = inspect(repoRoot);
  if (findings.length) {
    console.error('check-agent-guidance.mjs: FAILED');
    for (const finding of findings) console.error(`- ${finding}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-agent-guidance.mjs: OK (${skills} canonical skills, ${files} tracked instruction files)`);
}

try {
  main();
} catch (error) {
  console.error(`check-agent-guidance.mjs: FAILED: ${error.message}`);
  process.exitCode = 1;
}
