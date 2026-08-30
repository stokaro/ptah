#!/usr/bin/env node
// Hold reader-facing schema-source claims to their declared page contract.
// Command behavior remains owned by docs/source-support.json; this checker
// verifies how workflow pages introduce and enumerate that behavior.

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const repoRoot = join(siteRoot, '..', '..');
const inventoryPath = join(siteRoot, 'content-inventory.json');
const supportPath = join(repoRoot, 'docs', 'source-support.json');
const defaultManifest = JSON.parse(readFileSync(supportPath, 'utf8'));

const goOnlyCommands = new Map([
  ['/schema/visualize/', { command: 'ptah viz', owner: 'cmd/viz', alternatives: ['/schema/document/'] }],
  ['/schema/serve/', {
    command: 'ptah schema serve', owner: 'cmd/internal/schemaserve',
    alternatives: ['/schema/document/', '/direct/compare-and-drift/'],
  }],
]);

const goOnlyFrontends = new Map([
  ['/schema/go-annotations/', { owner: 'internal/schemaload', alternatives: ['/schema/work-with-a-source/'] }],
]);

const commandSpecificCommands = new Map([
  ['/testing/migrations-and-schema/', ['ptah schema test']],
  ['/schema/validate-and-format/', ['ptah schema validate']],
]);

const authoredReaderTypes = new Set([
  'landing', 'tutorial', 'how-to', 'concept', 'reference', 'troubleshooting', 'status',
]);
const neutralSelector = /--(?:schema-file|schema-cmd|source-db-url|config|from|to)\b/;
const schemaTestOverloadedRoot = /--root-dir(?:=|\s+)(?:['"]?)(?:[^\s'"]+\.(?:sql|ya?ml|hcl|dbml)|[a-z][a-z0-9+.-]*:\/\/[^\s'"]+)(?:['"]?)(?:\s|$)/i;
const expressivenessOverclaim = /\b(?:all|every) (?:schema )?sources? (?:are|is) (?:fully |exactly )?(?:equivalent|interchangeable)|\bsame expressiveness\b/i;
const supportedStatuses = new Set(['verified', 'supported-missing-command-test', 'supported-untested']);
const acceptedStatuses = new Set([...supportedStatuses, 'conditional']);

function bodyOf(source) {
  return source.replace(/^---\r?\n[\s\S]*?\r?\n---\r?\n/, '');
}

function regexpEscape(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function commandBases(manifest) {
  return [...new Set(manifest.entries
    .map((entry) => entry.command.match(/^(ptah(?:\s+[^\s-]+){1,2})/)?.[1] ?? null)
    .filter(Boolean))]
    .sort((left, right) => right.length - left.length);
}

function sourceCommandPattern(manifest) {
  return new RegExp(`\\b(?:${commandBases(manifest).map(regexpEscape).join('|')})\\b`);
}

function commandBlocks(source, manifest = defaultManifest) {
  const commands = [];
  const pattern = sourceCommandPattern(manifest);
  for (const fence of bodyOf(source).matchAll(/```(?:bash|console|powershell|sh)\r?\n([\s\S]*?)```/g)) {
    const lines = fence[1].split(/\r?\n/);
    for (let index = 0; index < lines.length; index += 1) {
      if (!pattern.test(lines[index])) continue;
      const command = [lines[index]];
      while (/[\\`]\s*$/.test(command.at(-1) ?? '') && index + 1 < lines.length) {
        command.push(lines[index + 1]);
        index += 1;
      }
      commands.push(command.join('\n'));
    }
  }
  return commands;
}

export function requiresSourceMode(page, source, manifest = defaultManifest) {
  return page.generated !== true && authoredReaderTypes.has(page.type) && commandBlocks(source, manifest).length > 0;
}

export function sourceModeDeclarationProblems(page, source, manifest = defaultManifest) {
  if (!requiresSourceMode(page, source, manifest) || page.sourceMode != null) return [];
  return [`${page.path}: authored ${page.type} page has a runnable schema-source command but no sourceMode`];
}

function firstProse(source, limit = 1800) {
  return bodyOf(source)
    .replace(/^import .*$/gm, '')
    .replace(/^export .*$/gm, '')
    .replace(/```[\s\S]*?```/g, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, limit);
}

function normalizeRoute(route) {
  const value = route.endsWith('/') ? route : `${route}/`;
  return value.startsWith('/') ? value : `/${value}`;
}

function openingLinkRoutes(source, currentRoute) {
  const opening = bodyOf(source).slice(0, 3000);
  const hrefs = [
    ...[...opening.matchAll(/\[[^\]]+\]\(([^)\s]+)(?:\s+['"][^'"]*['"])?\)/g)].map((match) => match[1]),
    ...[...opening.matchAll(/\bhref=['"]([^'"]+)['"]/g)].map((match) => match[1]),
  ];
  return hrefs
    .filter((href) => !/^(?:[a-z][a-z0-9+.-]*:|#)/i.test(href))
    .map((href) => normalizeRoute(new URL(href, `https://ptah.invalid${currentRoute}`).pathname));
}

function supportedLabels(manifest, command) {
  const labels = new Map(manifest.sources.map((source) => [source.id, source.label]));
  return manifest.entries
    .filter((entry) => entry.command === command && supportedStatuses.has(entry.status))
    .map((entry) => labels.get(entry.source))
    .filter(Boolean);
}

function supportedEntries(manifest, command) {
  return manifest.entries.filter((entry) => entry.command === command && supportedStatuses.has(entry.status));
}

function declaredSourceCommands(source) {
  const commands = [];
  const markerPattern = /<!--\s*source-support-command:\s*([^>]+?)\s*-->|\{\/\*\s*source-support-command:\s*([\s\S]*?)\s*\*\/\}/g;
  for (const marker of source.matchAll(markerPattern)) commands.push((marker[1] ?? marker[2]).trim());
  return commands;
}

function normalizeCell(value) {
  return value
    .replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
    .replace(/[`*_]/g, '')
    .trim();
}

function tableCells(line) {
  const trimmed = line.trim().replace(/^\|/, '').replace(/\|$/, '');
  return trimmed.split('|').map(normalizeCell);
}

function optionNames(value) {
  return [...new Set([...value.matchAll(/--[a-z0-9-]+/gi)].map((match) => match[0]))];
}

function significantTokens(value) {
  const stop = new Set([
    'about', 'after', 'also', 'and', 'are', 'because', 'before', 'being', 'can', 'each', 'every',
    'exactly', 'from', 'have', 'into', 'named', 'not', 'only', 'selected', 'source', 'that', 'their',
    'the', 'these', 'they', 'this', 'through', 'when', 'where', 'which', 'with', 'without',
  ]);
  return new Set((value.toLowerCase().match(/[a-z0-9][a-z0-9-]*/g) ?? [])
    .filter((token) => token.length >= 3 && !stop.has(token)));
}

function sharesSignificantToken(left, right) {
  const expected = significantTokens(right);
  for (const token of significantTokens(left)) if (expected.has(token)) return true;
  return false;
}

function expectedSelectorOptions(command, source) {
  if (command === 'ptah schema test') {
    if (source.id === 'go-annotations') return ['--root-dir'];
    if (source.id === 'live-database') return ['--source-db-url'];
    return ['--schema-file'];
  }
  return optionNames(source.spelling ?? '');
}

function commandName(invocation, manifest) {
  const base = commandBases(manifest).find((candidate) =>
    new RegExp(`\\b${regexpEscape(candidate)}\\b`).test(invocation));
  if (!base) return null;
  const candidates = [...new Set(manifest.entries
    .map((entry) => entry.command)
    .filter((command) => command === base || command.startsWith(`${base} --`)))];
  for (const candidate of candidates.sort((left, right) => right.length - left.length)) {
    const suffix = candidate.slice(base.length).trim();
    if (suffix === '' || invocation.replace(/\s+/g, ' ').includes(suffix)) return candidate;
  }
  return base;
}

function selectedSourceID(command, invocation) {
  if (/--schema-cmd\b/.test(invocation)) return 'external-program';
  if (/--config\b/.test(invocation) && /--allow-external-schema\b/.test(invocation)) return 'configured-external';
  if (command === 'ptah schema test' && /--source-db-url\b/.test(invocation)) return 'live-database';
  if (/--schema-file\b/.test(invocation)) {
    if (/oci:\/\//i.test(invocation)) return 'oci-artifact';
    if (/\.ya?ml\b/i.test(invocation)) return 'yaml-file';
    if (/\.hcl\b/i.test(invocation)) return 'hcl-file';
    if (/\.dbml\b/i.test(invocation)) return 'dbml-file';
    if (/\.sql\b/i.test(invocation)) return 'sql-file';
    return null;
  }
  if (/--root-dir\b/.test(invocation)) {
    return 'go-annotations';
  }
  if (command === 'ptah viz') return 'go-annotations';
  if (command === 'ptah schema inspect' && /--db-url\b/.test(invocation)) return 'live-database';
  return null;
}

function commandInvocationProblems(source, manifest, label) {
  const problems = [];
  for (const invocation of commandBlocks(source, manifest)) {
    const command = commandName(invocation, manifest);
    if (!command) continue;
    const entries = manifest.entries.filter((entry) => entry.command === command);
    if (entries.length === 0) {
      problems.push(`${label}: ${command} is missing from docs/source-support.json`);
      continue;
    }
    const sourceID = selectedSourceID(command, invocation);
    if (sourceID === null) continue;
    const entry = entries.find((candidate) => candidate.source === sourceID);
    if (!entry || !acceptedStatuses.has(entry.status)) {
      problems.push(`${label}: ${command} does not accept ${sourceID} according to docs/source-support.json`);
    }
  }
  return problems;
}

export function declaredSourceListProblems(source, manifest) {
  const problems = [];
  const markerPattern = /<!--\s*source-support-command:\s*([^>]+?)\s*-->|\{\/\*\s*source-support-command:\s*([\s\S]*?)\s*\*\/\}/g;
  for (const marker of source.matchAll(markerPattern)) {
    const command = (marker[1] ?? marker[2]).trim();
    const tail = source.slice(marker.index + marker[0].length);
    const lines = tail.split(/\r?\n/);
    const headerIndex = lines.findIndex((line) => /^\s*\|\s*Source\s*\|/i.test(line));
    if (headerIndex === -1 || !/^\s*\|(?:\s*:?-+:?\s*\|)+\s*$/.test(lines[headerIndex + 1] ?? '')) {
      problems.push(`${command}: source-support marker must be followed by a Source table`);
      continue;
    }
    const headers = tableCells(lines[headerIndex]);
    const selectorIndex = headers.findIndex((header) => /selector|flags|root-dir value/i.test(header));
    const limitationIndex = headers.findIndex((header) => /condition|limitation/i.test(header));
    if (selectorIndex === -1) {
      problems.push(`${command}: Source table must have a selector or flags column`);
      continue;
    }
    if (limitationIndex === -1) {
      problems.push(`${command}: Source table must have a source-specific condition or limitation column`);
      continue;
    }
    const declared = [];
    const rows = new Map();
    for (const line of lines.slice(headerIndex + 2)) {
      if (!/^\s*\|/.test(line)) break;
      const cells = tableCells(line);
      declared.push(cells[0] ?? '');
      rows.set(cells[0] ?? '', cells);
    }
    const entries = supportedEntries(manifest, command);
    const sourceByID = new Map(manifest.sources.map((item) => [item.id, item]));
    const expected = supportedLabels(manifest, command);
    if (entries.length === 0) {
      problems.push(`${command}: no verified source rows exist in docs/source-support.json`);
      continue;
    }
    const missing = expected.filter((label) => !declared.includes(label));
    const extra = declared.filter((label) => !expected.includes(label));
    if (missing.length > 0 || extra.length > 0) {
      problems.push(
        `${command}: source table disagrees with docs/source-support.json` +
        `${missing.length > 0 ? `; missing ${missing.join(', ')}` : ''}` +
        `${extra.length > 0 ? `; unexpected ${extra.join(', ')}` : ''}`,
      );
    }
    for (const entry of entries) {
      const sourceDefinition = sourceByID.get(entry.source);
      if (!sourceDefinition) {
        problems.push(`${command}: source ${entry.source} has no declaration in docs/source-support.json`);
        continue;
      }
      const cells = rows.get(sourceDefinition.label);
      if (!cells) continue;
      const selector = cells[selectorIndex] ?? '';
      const missingOptions = expectedSelectorOptions(command, sourceDefinition)
        .filter((option) => !selector.includes(option));
      if (missingOptions.length > 0) {
        problems.push(`${command}: ${sourceDefinition.label} selector must include ${missingOptions.join(', ')}`);
      }
      const limitation = cells[limitationIndex] ?? '';
      if (limitation.trim() === '') {
        problems.push(`${command}: ${sourceDefinition.label} must state its source-specific limitation`);
      } else {
        if (!sharesSignificantToken(limitation, sourceDefinition.expressiveness ?? '')) {
          problems.push(`${command}: ${sourceDefinition.label} limitation is not tied to the manifest expressiveness note`);
        }
        if (entry.limitations && !sharesSignificantToken(limitation, entry.limitations)) {
          problems.push(`${command}: ${sourceDefinition.label} limitation is not tied to the command-specific manifest limitation`);
        }
      }
    }
  }
  return problems;
}

export function pageContractProblems(page, source, manifest, options = {}) {
  const problems = [];
  const label = `${page.path} (${page.route})`;
  const intro = firstProse(source);
  const opening = firstProse(source, 700);

  for (const invocation of commandBlocks(source, manifest)) {
    if (commandName(invocation, manifest) === 'ptah schema test' && schemaTestOverloadedRoot.test(invocation)) {
      problems.push(`${label}: ptah schema test reader workflows must use --schema-file or --source-db-url instead of overloaded --root-dir`);
    }
  }

  if (page.sourceMode === 'source-neutral') {
    if (/\bGo (?:entities|models?)\b/i.test(`${page.title} ${page.description}`)) {
      problems.push(`${label}: source-neutral title or description frames the desired schema as Go`);
    }
    if (/(?<!no )(?<!not )\b(?:requires?|must use) (?:a )?Go(?: toolchain| source| annotations?)?\b/i.test(opening)) {
      problems.push(`${label}: source-neutral introduction says Go is required`);
    }
    const first = commandBlocks(source, manifest)[0];
    if (!first) {
      problems.push(`${label}: source-neutral page has no runnable source-consuming command`);
    } else if (!neutralSelector.test(first) &&
        !(/\bptah schema inspect\b/.test(first) && /--db-url\b/.test(first))) {
      problems.push(`${label}: first source-consuming command has no non-Go primary path`);
    }
    if (expressivenessOverclaim.test(bodyOf(source))) {
      problems.push(`${label}: source transport is described as equal expressiveness`);
    }
  }

  if (page.sourceMode === 'go-only') {
    if (!/\b(?:Go annotations only|only Go annotations|Go-only|Go-specific)\b/i.test(intro)) {
      problems.push(`${label}: Go-only limitation must appear near the beginning`);
    }
    if (!/source-neutral/i.test(intro) || !/\]\([^)]+\)/.test(intro)) {
      problems.push(`${label}: Go-only introduction must link to a source-neutral alternative`);
    }
    const contract = goOnlyCommands.get(page.route);
    const frontend = goOnlyFrontends.get(page.route);
    if (!contract && !frontend) {
      problems.push(`${label}: Go-only page has no command-support contract`);
    } else if (contract) {
      if (!(page.sourceOfTruth ?? []).includes(contract.owner)) {
        problems.push(`${label}: sourceOfTruth must name ${contract.owner}`);
      }
      const sources = supportedLabels(manifest, contract.command);
      if (sources.length !== 1 || sources[0] !== 'Go annotations') {
        problems.push(`${label}: ${contract.command} is not Go-only in docs/source-support.json`);
      }
    } else if (!(page.sourceOfTruth ?? []).includes(frontend.owner)) {
      problems.push(`${label}: sourceOfTruth must name ${frontend.owner}`);
    }
    const alternativeContract = contract ?? frontend;
    const liveNeutralRoutes = options.sourceNeutralRoutes ?? new Set();
    const canonicalAlternatives = (alternativeContract?.alternatives ?? [])
      .filter((route) => liveNeutralRoutes.has(route));
    const linked = openingLinkRoutes(source, page.route);
    if (canonicalAlternatives.length === 0 || !canonicalAlternatives.some((route) => linked.includes(route))) {
      problems.push(`${label}: Go-only introduction must link to its canonical source-neutral alternative`);
    }
  }

  if (page.sourceMode === 'static-file-only') {
    const first = commandBlocks(source, manifest)[0];
    if (!first || !/--schema-file\b/.test(first) || /--root-dir\b/.test(first)) {
      problems.push(`${label}: static-file-only page must use --schema-file in its first source-consuming command`);
    }
  }

  if (page.sourceMode === 'external-program-only') {
    const first = commandBlocks(source, manifest)[0];
    if (!first || !/--schema-cmd\b/.test(first)) {
      problems.push(`${label}: external-program-only page must use --schema-cmd in its first source-consuming command`);
    }
  }

  if (page.sourceMode === 'oci-artifact-only') {
    const first = commandBlocks(source, manifest)[0];
    if (!first || !/--schema-file\b/.test(first) || !/\bOCI\b|oci:\/\//i.test(intro)) {
      problems.push(`${label}: OCI-artifact-only page must introduce OCI and consume it through --schema-file`);
    }
  }

  if (page.sourceMode === 'live-database-only') {
    if (!/\b(?:live database|database URL)\b|--db-url\b/i.test(intro)) {
      problems.push(`${label}: live-database-only page must state its database prerequisite early`);
    }
    if (/\bschema file (?:can|may) (?:replace|substitute for) (?:the )?live (?:catalog|database)\b/i.test(bodyOf(source))) {
      problems.push(`${label}: schema file is presented as a substitute for the live catalog`);
    }
  }

  if (page.sourceMode === 'command-specific') {
    if (!/\b(?:accepts?|inputs?|sources?|desired schema)\b/i.test(intro)) {
      problems.push(`${label}: command-specific page must explain its accepted inputs near the beginning`);
    }
    if (!Array.isArray(page.sourceOfTruth) || page.sourceOfTruth.length === 0) {
      problems.push(`${label}: command-specific page must name implementation ownership`);
    }
    const expectedCommands = commandSpecificCommands.get(page.route);
    if (expectedCommands) {
      const declaredCommands = declaredSourceCommands(source);
      for (const command of expectedCommands) {
        if (!manifest.entries.some((entry) => entry.command === command)) {
          problems.push(`${label}: ${command} is missing from docs/source-support.json`);
        }
        if (!declaredCommands.includes(command)) {
          problems.push(`${label}: accepted inputs for ${command} must use a source-support marker`);
        }
      }
    } else {
      problems.push(...commandInvocationProblems(source, manifest, label));
    }
  }

  problems.push(...declaredSourceListProblems(source, manifest).map((problem) => `${label}: ${problem}`));
  return problems;
}

function fixturePage(sourceMode, overrides = {}) {
  return {
    path: 'fixture.md', route: '/fixture/', title: 'Apply a desired schema',
    description: 'Apply a desired schema.', sourceMode, sourceOfTruth: ['cmd/schema'],
    ...overrides,
  };
}

function selftest() {
  const manifest = {
    sources: [
      { id: 'sql-file', label: 'SQL file', spelling: '--schema-file schema.sql', expressiveness: 'The Ptah DDL parser subset.' },
      { id: 'go-annotations', label: 'Go annotations', spelling: '--root-dir ./models', expressiveness: 'The native Go annotation model.' },
    ],
    entries: [
      { command: 'ptah migrations generate', source: 'sql-file', status: 'verified' },
      { command: 'ptah migrations generate', source: 'go-annotations', status: 'verified' },
      { command: 'ptah schema render', source: 'sql-file', status: 'verified' },
      { command: 'ptah schema test', source: 'sql-file', status: 'verified' },
      { command: 'ptah viz', source: 'go-annotations', status: 'verified' },
    ],
  };
  const sqlFirst = 'Use a static source.\n\n```bash\nptah migrations generate --schema-file schema.sql --db-url sqlite://app.db\n```\n';
  const sourceNeutralRoutes = new Set(['/schema/document/']);
  if (pageContractProblems(fixturePage('source-neutral'), sqlFirst, manifest).length !== 0) {
    throw new Error('source-neutral SQL-first page failed');
  }
  const rootOnly = 'Use the schema.\n\n```bash\nptah migrations generate --root-dir ./models --db-url sqlite://app.db\n```\n';
  if (!pageContractProblems(fixturePage('source-neutral'), rootOnly, manifest).some((problem) => problem.includes('non-Go primary path'))) {
    throw new Error('source-neutral root-dir-only page passed');
  }
  const disguisedRoot = 'Use a static schema.\n\n```bash\nptah schema render --root-dir schema.sql --dialect sqlite\n```\n';
  if (!pageContractProblems(fixturePage('source-neutral'), disguisedRoot, manifest).some((problem) => problem.includes('non-Go primary path'))) {
    throw new Error('static --root-dir was accepted outside schema test');
  }
  const overloadedSchemaTest = 'Test a static schema.\n\n```bash\nptah schema test --root-dir schema.sql --dir ./tests\n```\n';
  if (!pageContractProblems(fixturePage('source-neutral'), overloadedSchemaTest, manifest)
    .some((problem) => problem.includes('instead of overloaded --root-dir'))) {
    throw new Error('schema test overloaded --root-dir passed as the primary neutral selector');
  }
  const goEarly = 'This command reads Go annotations only. Use the [source-neutral export](../document/).\n\n```bash\nptah viz --root-dir ./models\n```\n';
  if (pageContractProblems(
    fixturePage('go-only', { route: '/schema/visualize/', sourceOfTruth: ['cmd/viz'] }),
    goEarly, manifest, { sourceNeutralRoutes },
  ).length !== 0) {
    throw new Error('Go-only page with an early limitation failed');
  }
  const goBuried = `${'The command renders models. '.repeat(100)}\n\nGo annotations only. Use the [source-neutral export](../document/).`;
  if (!pageContractProblems(
    fixturePage('go-only', { route: '/schema/visualize/', sourceOfTruth: ['cmd/viz'] }),
    goBuried, manifest, { sourceNeutralRoutes },
  ).some((problem) => problem.includes('near the beginning'))) {
    throw new Error('Go-only page with a buried limitation passed');
  }
  const unrelatedAlternative = 'Go annotations only. Use the source-neutral [installation guide](../../start/install/).';
  if (!pageContractProblems(
    fixturePage('go-only', { route: '/schema/visualize/', sourceOfTruth: ['cmd/viz'] }),
    unrelatedAlternative, manifest, { sourceNeutralRoutes },
  ).some((problem) => problem.includes('canonical source-neutral alternative'))) {
    throw new Error('Go-only page with an unrelated link passed as an alternative');
  }
  const staleList = '<!-- source-support-command: ptah migrations generate -->\n\n| Source | Flags | Limitation |\n| --- | --- | --- |\n| SQL file | `--schema-file` | DDL parser subset. |\n';
  if (!declaredSourceListProblems(staleList, manifest).some((problem) => problem.includes('missing Go annotations'))) {
    throw new Error('manual source list inconsistent with the manifest passed');
  }
  const labelsOnly = '<!-- source-support-command: ptah migrations generate -->\n\n' +
    '| Source | Selector | Limitation |\n| --- | --- | --- |\n' +
    '| SQL file | `--root-dir schema.sql` | All sources are interchangeable. |\n' +
    '| Go annotations | `--root-dir ./models` | Go annotation model. |\n';
  const labelsOnlyProblems = declaredSourceListProblems(labelsOnly, manifest);
  if (!labelsOnlyProblems.some((problem) => problem.includes('SQL file selector must include --schema-file')) ||
      !labelsOnlyProblems.some((problem) => problem.includes('manifest expressiveness note'))) {
    throw new Error('source table with correct labels but wrong selector or limitation passed');
  }
  const commandSpecific = 'The command accepts these desired-schema sources.\n\n' +
    '<!-- source-support-command: ptah migrations generate -->\n\n' +
    '| Source | Selector | Limitation |\n| --- | --- | --- |\n' +
    '| SQL file | `--schema-file schema.sql` | Ptah DDL parser subset. |\n' +
    '| Go annotations | `--root-dir ./models` | Native Go annotation model. |\n';
  const commandPage = fixturePage('command-specific', { route: '/fixture/', sourceOfTruth: ['cmd/migrate'] });
  commandSpecificCommands.set('/fixture/', ['ptah migrations generate']);
  if (pageContractProblems(commandPage, commandSpecific, manifest).length !== 0) {
    throw new Error('command-specific page with explicit constraints failed');
  }
  commandSpecificCommands.delete('/fixture/');
  const discoveredPage = { ...fixturePage(undefined), type: 'how-to', generated: false };
  if (sourceModeDeclarationProblems(discoveredPage, sqlFirst).length !== 1) {
    throw new Error('new authored source-consuming page without sourceMode passed');
  }
  const shReference = { ...fixturePage(undefined), type: 'reference', generated: false };
  const shSource = '```sh\nptah schema diff --from current.sql --to desired.sql --dev-url sqlite://dev.db\n```\n';
  if (sourceModeDeclarationProblems(shReference, shSource).length !== 1) {
    throw new Error('authored reference page in an sh fence bypassed sourceMode discovery');
  }
  console.log('check-source-page-contracts.mjs --selftest: OK (12 contract fixtures)');
}

function main() {
  if (process.argv.includes('--selftest')) {
    selftest();
    return;
  }

  const inventory = JSON.parse(readFileSync(inventoryPath, 'utf8'));
  const manifest = JSON.parse(readFileSync(supportPath, 'utf8'));
  const problems = [];
  const contractPages = [];
  const sourceNeutralRoutes = new Set(inventory.pages
    .filter((page) => page.sourceMode === 'source-neutral')
    .map((page) => page.route));
  for (const page of inventory.pages) {
    const source = readFileSync(join(repoRoot, page.path), 'utf8');
    const declarationProblems = sourceModeDeclarationProblems(page, source);
    if (declarationProblems.length > 0) {
      problems.push(...declarationProblems);
      continue;
    }
    if (page.sourceMode === null) continue;
    contractPages.push(page);
    problems.push(...pageContractProblems(page, source, manifest, { sourceNeutralRoutes }));
  }

  if (problems.length > 0) {
    console.error('check-source-page-contracts.mjs: FAILED');
    for (const problem of problems) console.error(`  ${problem}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-source-page-contracts.mjs: OK (${contractPages.length} discovered page contracts)`);
}

main();
