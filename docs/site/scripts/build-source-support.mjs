#!/usr/bin/env node

import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, '..', '..', '..');
const outputPath = join(repoRoot, 'docs', 'source-support.json');
const verifiedAt = '2026-09-02';

const exportMetadataStatuses = new Set([
  'supported',
  'unsupported-design',
  'declared-format',
  'component-dependent',
  'not-applicable',
]);

function exportMetadata(status, detail) {
  return { status, detail };
}

const sources = [
  { id: 'sql-file', label: 'SQL file', spelling: '--schema-file schema.sql', transport: 'local file', expressiveness: 'The Ptah DDL parser subset for the selected dialect; SQL has no portable API export-metadata spelling.', exportMetadata: exportMetadata('unsupported-design', 'SQL DDL cannot carry Ptah API names, type overrides, or exposure without overloading comments or unrelated storage syntax.') },
  { id: 'yaml-file', label: 'YAML file', spelling: '--schema-file schema.yaml', transport: 'local file', expressiveness: 'Ptah YAML schema objects, API export metadata, and documented dialect overrides.', exportMetadata: exportMetadata('supported', 'Tables and columns carry the documented Ptah API export-metadata keys.') },
  { id: 'hcl-file', label: 'HCL file', spelling: '--schema-file schema.hcl', transport: 'local file', expressiveness: 'The documented Atlas-compatible HCL subset plus named Ptah extensions, including API export metadata.', exportMetadata: exportMetadata('supported', 'Ptah table and column attributes carry API export metadata through canonical HCL round trips.') },
  { id: 'dbml-file', label: 'DBML file', spelling: '--schema-file schema.dbml', transport: 'local file', expressiveness: 'DBML tables, columns, keys, indexes, relationships, and supported notes; DBML has no lossless API export-metadata spelling.', exportMetadata: exportMetadata('unsupported-design', 'Ptah refuses attempted API export metadata instead of overloading DBML notes or presentation settings.') },
  { id: 'go-annotations', label: 'Go annotations', spelling: '--root-dir ./models', transport: 'local Go source tree', expressiveness: 'The native annotation model, including API export metadata and Ptah-specific objects that narrower interchange formats may omit.', exportMetadata: exportMetadata('supported', 'Table and field annotations carry every API export-metadata attribute.') },
  { id: 'external-program', label: 'External program', spelling: '--schema-cmd ./load-schema --schema-format sql', transport: 'explicit child process', expressiveness: 'Exactly the SQL, HCL, or YAML format declared by --schema-format.', exportMetadata: exportMetadata('declared-format', 'YAML and HCL payloads carry API export metadata; SQL payloads do not.') },
  { id: 'configured-external', label: 'Configured external source', spelling: '--config ptah.yaml --allow-external-schema', transport: 'opt-in configured child process', expressiveness: 'Exactly the SQL, HCL, or YAML format declared by the selected external_schema block.', exportMetadata: exportMetadata('declared-format', 'YAML and HCL payloads carry API export metadata; SQL payloads do not.') },
  { id: 'oci-artifact', label: 'OCI artifact', spelling: '--schema-file oci://registry.example/app:v1', transport: 'OCI 1.1 artifact', expressiveness: 'The lossless canonical Ptah HCL document stored in the artifact.', exportMetadata: exportMetadata('supported', 'Canonical HCL preserves API export metadata across push and pull.') },
  { id: 'live-database', label: 'Live database', spelling: '--db-url sqlite://app.db', transport: 'database connection', expressiveness: 'Objects and attributes the selected database reader can measure from that server.', exportMetadata: exportMetadata('not-applicable', 'Database catalogs do not store Ptah API export metadata.') },
  { id: 'migration-directory', label: 'Migration directory', spelling: '--migrations-dir ./migrations', transport: 'local directory or command-specific URL', expressiveness: 'The schema produced by ordered replay on a disposable dev database.', exportMetadata: exportMetadata('not-applicable', 'Migration replay reconstructs database state, which does not contain Ptah API export metadata.') },
  { id: 'composite-source', label: 'Composite source', spelling: '--schema-file schema/tables.sql --schema-file schema/views.hcl', transport: 'repeated and mixed accepted sources', expressiveness: 'The union of complete selected sources; API metadata follows each component format and conflicting definitions fail instead of silently choosing one.', exportMetadata: exportMetadata('component-dependent', 'YAML, HCL, and Go components can own metadata; SQL and DBML components cannot act as metadata overlays.') },
];

const localFiles = ['sql-file', 'yaml-file', 'hcl-file', 'dbml-file'];
const allSourceIds = sources.map((source) => source.id);
const statuses = new Set([
  'verified',
  'supported-missing-command-test',
  'conditional',
  'unsupported-design',
  'unsupported-gap',
  'not-applicable',
]);

function desiredCommand(command, help, owner, evidence, options = {}) {
  return {
    command,
    help,
    owner,
    evidence,
    defaultStatus: 'not-applicable',
    groups: {
      verified: options.verified ?? [],
      'supported-missing-command-test': options.missing ?? [],
      conditional: options.conditional ?? [],
      'unsupported-design': options.design ?? [],
      'unsupported-gap': options.gap ?? [],
    },
    composableSources: options.composableSources ?? [],
    limitations: options.limitations ?? {},
    invocations: options.invocations ?? {},
    suffix: options.suffix ?? '',
  };
}

const fullDesiredVerified = [...localFiles, 'go-annotations', 'external-program', 'configured-external'];
const commands = [
  desiredCommand('ptah schema render', ['schema', 'render'], 'internal/cli/generate', ['internal/cli/generate/generate_test.go', 'scripts/check-source-equivalence.sh'], {
    verified: fullDesiredVerified,
    missing: ['oci-artifact', 'composite-source'],
    design: ['live-database', 'migration-directory'],
    composableSources: [...localFiles, 'go-annotations', 'external-program', 'configured-external', 'composite-source'],
    suffix: '--dialect sqlite',
  }),
  desiredCommand('ptah schema validate', ['schema', 'validate'], 'internal/cli/schema/validate.go', ['internal/cli/schema/dbml_source_cli_test.go'], {
    verified: ['sql-file', 'dbml-file'],
    missing: ['yaml-file', 'hcl-file', 'go-annotations', 'oci-artifact', 'composite-source'],
    design: ['external-program', 'configured-external', 'live-database', 'migration-directory'],
    composableSources: [...localFiles, 'go-annotations', 'composite-source'],
    suffix: '--dialect sqlite',
  }),
  desiredCommand('ptah schema compare', ['schema', 'compare'], 'internal/cli/compare', ['internal/cli/compare/compare_test.go'], {
    verified: ['sql-file', 'external-program', 'composite-source'],
    missing: ['yaml-file', 'hcl-file', 'dbml-file', 'go-annotations', 'configured-external', 'oci-artifact'],
    conditional: ['live-database'],
    design: ['migration-directory'],
    composableSources: [...localFiles, 'go-annotations', 'external-program', 'configured-external', 'composite-source'],
    invocations: { 'live-database': '--schema-file desired.sql --db-url sqlite://current.db' },
    limitations: { 'live-database': 'The live database is the current state, not the desired-schema source.' },
    suffix: '--db-url sqlite://current.db',
  }),
  desiredCommand('ptah schema drift', ['schema', 'drift'], 'internal/cli/drift', ['internal/cli/drift/drift_test.go'], {
    verified: ['sql-file', 'external-program'],
    missing: ['yaml-file', 'hcl-file', 'dbml-file', 'go-annotations', 'configured-external', 'oci-artifact', 'composite-source'],
    conditional: ['live-database'],
    design: ['migration-directory'],
    composableSources: [...localFiles, 'go-annotations', 'external-program', 'configured-external', 'composite-source'],
    invocations: { 'live-database': '--schema-file desired.sql --db-url sqlite://current.db --exit-code=false' },
    limitations: { 'live-database': 'The live database is the measured state, not the declaration.' },
    suffix: '--db-url sqlite://current.db --exit-code=false',
  }),
  desiredCommand('ptah schema diff', ['schema', 'diff'], 'internal/cli/schema/diff.go', ['internal/cli/schema/diff_test.go'], {
    verified: [...localFiles, 'live-database', 'migration-directory'],
    conditional: ['composite-source'],
    design: ['go-annotations', 'external-program', 'configured-external'],
    gap: ['oci-artifact'],

    invocations: {
      'sql-file': '--from current.sql --to desired.sql --dev-url sqlite://dev.db',
      'yaml-file': '--from current.yaml --to desired.yaml --dev-url sqlite://dev.db',
      'hcl-file': '--from current.hcl --to desired.hcl --dev-url sqlite://dev.db',
      'dbml-file': '--from current.dbml --to desired.dbml --dev-url sqlite://dev.db',
      'live-database': '--from sqlite://current.db --to sqlite://desired.db',
      'migration-directory': '--from file://migrations --to desired.hcl --dev-url sqlite://dev.db',
      'composite-source': '--from base.sql --from views.hcl --to desired.hcl --dev-url sqlite://dev.db',
    },
    limitations: {
      'composite-source': 'Repeated sources on one side must all be the same source kind.',
      'oci-artifact': '--from and --to refuse an oci:// URL; they accept local schema files, migration directories, database URLs, and env:// references.',
    },
  }),
  desiredCommand('ptah schema plan', ['schema', 'plan'], 'internal/cli/schema/plan.go', ['internal/cli/schema/plan_test.go'], {
    verified: ['sql-file'],
    missing: ['yaml-file', 'hcl-file', 'dbml-file', 'go-annotations', 'oci-artifact', 'composite-source'],
    conditional: ['live-database'],
    design: ['migration-directory'],
    gap: ['external-program', 'configured-external'],
    composableSources: [...localFiles, 'go-annotations', 'composite-source'],
    invocations: { 'live-database': '--schema-file desired.sql --db-url sqlite://target.db --dry-run' },
    limitations: {
      'live-database': 'The live database is the target whose fingerprint the saved plan records.',
      'external-program': 'The command registers no --schema-cmd.',
      'configured-external': 'The command registers no --allow-external-schema.',
    },
    suffix: '--db-url sqlite://target.db --dry-run',
  }),
  desiredCommand('ptah schema apply', ['schema', 'apply'], 'internal/cli/schema/apply.go', ['internal/cli/schema/apply_test.go'], {
    verified: ['sql-file'],
    missing: ['yaml-file', 'hcl-file', 'dbml-file', 'go-annotations', 'oci-artifact', 'composite-source'],
    conditional: ['live-database', 'migration-directory'],
    gap: ['external-program', 'configured-external'],
    composableSources: [...localFiles, 'go-annotations', 'composite-source'],
    invocations: {
      'migration-directory': '--to file://migrations --dev-url sqlite://dev.db --db-url sqlite://target.db',
      'live-database': '--to sqlite://desired.db --db-url sqlite://target.db --dry-run',
    },
    limitations: {
      'live-database': 'A live desired source uses --to; --db-url remains the mutation target.',
      'migration-directory': 'A migration directory uses --to and requires a destructive disposable --dev-url.',
      'external-program': 'The command registers no --schema-cmd.',
      'configured-external': 'The command registers no --allow-external-schema.',
    },
    suffix: '--db-url sqlite://target.db --dry-run',
  }),
  desiredCommand('ptah schema inspect', ['schema', 'inspect'], 'internal/cli/schema/inspect.go', ['internal/cli/schema/inspect_dbml_test.go', 'internal/cli/schema/inspect_oci_test.go'], {
    verified: ['sql-file', 'oci-artifact', 'live-database'],
    missing: ['yaml-file', 'hcl-file', 'dbml-file', 'migration-directory'],
    design: ['go-annotations', 'external-program', 'configured-external', 'composite-source'],
    invocations: {
      'live-database': '--db-url sqlite://app.db --format json',
      'migration-directory': '--migrations-dir ./migrations --dev-url sqlite://dev.db --format json',
    },
    limitations: { 'migration-directory': 'Replay resets the required disposable dev database.' },
    suffix: '--dev-url sqlite://dev.db --format json',
  }),
  ...['openapi-v3', 'graphql', 'protobuf'].map((target) =>
    desiredCommand(`ptah schema export --to ${target}`, ['schema', 'export'], 'internal/cli/schema', [
      'internal/cli/schema/export_source_test.go',
      ...(target === 'protobuf' ? ['scripts/check-source-workflows.sh'] : []),
    ], {
      verified: target === 'graphql'
        ? [...localFiles, 'go-annotations', 'composite-source']
        : ['sql-file', 'yaml-file', 'hcl-file', 'go-annotations'],
      missing: target === 'graphql'
        ? ['oci-artifact']
        : ['dbml-file', 'oci-artifact', 'composite-source'],
      design: ['external-program', 'configured-external', 'live-database', 'migration-directory'],
      composableSources: [...localFiles, 'go-annotations', 'composite-source'],
    })),
  ...['markdown', 'html'].map((target) =>
    desiredCommand(`ptah schema export --to ${target}`, ['schema', 'export'], 'internal/cli/schema', ['internal/cli/schema/export_dbml_test.go', 'internal/schemaexport'], {
      verified: ['go-annotations'],
      missing: [...localFiles, 'oci-artifact', 'composite-source'],
      design: ['external-program', 'configured-external', 'live-database', 'migration-directory'],
      composableSources: [...localFiles, 'go-annotations', 'composite-source'],
    })),
  desiredCommand('ptah schema export --to dbml', ['schema', 'export'], 'internal/cli/schema', ['internal/cli/schema/export_dbml_test.go', 'internal/dbmlrender'], {
    verified: ['go-annotations'],
    missing: [...localFiles, 'oci-artifact', 'composite-source'],
    design: ['external-program', 'configured-external', 'live-database', 'migration-directory'],
    composableSources: [...localFiles, 'go-annotations', 'composite-source'],
    limitations: Object.fromEntries([
      'yaml-file',
      'hcl-file',
      'go-annotations',
      'external-program',
      'configured-external',
      'oci-artifact',
      'composite-source',
    ].map((id) => [
      id,
      'DBML export succeeds only when selected tables and columns carry no API export metadata; metadata-bearing schemas are refused before output.',
    ])),
  }),
  desiredCommand('ptah schema export --to hcl', ['schema', 'export'], 'internal/cli/schema', ['internal/cli/schema/export_source_test.go'], {
    verified: ['go-annotations'],
    design: [...localFiles, 'external-program', 'configured-external', 'oci-artifact', 'live-database', 'migration-directory', 'composite-source'],
    limitations: Object.fromEntries(allSourceIds.map((id) => [id, id === 'go-annotations' ? '' : 'HCL export rewrites Go annotations and deliberately accepts only --root-dir.'])),
  }),
  desiredCommand('ptah schema test', ['schema', 'test'], 'internal/cli/schema/test.go', ['internal/cli/schema/test_test.go', 'internal/cli/schema/test_source_test.go'], {
    verified: [...localFiles, 'go-annotations', 'live-database', 'oci-artifact'],
    missing: ['composite-source'],
    design: ['external-program', 'configured-external', 'migration-directory'],
    // One selector per source kind, and each names what it takes: --root-dir a
    // directory of Go annotations, --schema-file a file or OCI artifact, and
    // --source-db-url a live database. --db-url stays the throwaway target.
    invocations: Object.fromEntries([
      ...localFiles.map((id) => [id, `--schema-file schema.${id.split('-')[0]} --dir ./tests`]),
      ['go-annotations', '--root-dir ./models --dir ./tests'],
      ['live-database', '--source-db-url sqlite://source.db --db-url sqlite://throwaway.db --dir ./tests'],
      ['oci-artifact', '--schema-file oci://registry.example/app:v1 --dir ./tests'],
      ['composite-source', '--schema-file tables.sql --schema-file views.sql --dir ./tests'],
    ]),
    limitations: { 'live-database': 'The destination must be throwaway; a non-SQLite source requires an explicit matching --db-url.' },
    suffix: '--dir ./tests',
  }),
  desiredCommand('ptah schema lineage', ['schema', 'lineage'], 'internal/cli/schema/lineage.go', ['internal/cli/schema/lineage_test.go'], {
    verified: ['sql-file'],
    missing: ['yaml-file', 'hcl-file', 'dbml-file', 'go-annotations', 'oci-artifact', 'live-database', 'composite-source'],
    gap: ['external-program', 'configured-external'],
    composableSources: [...localFiles, 'go-annotations', 'composite-source'],
    limitations: { 'external-program': 'The command registers no --schema-cmd.', 'configured-external': 'The command registers no --allow-external-schema.' },
    suffix: '--dialect postgres --format json',
  }),
  desiredCommand('ptah schema push', ['schema', 'push'], 'internal/cli/schemapush', ['internal/cli/schemapush/schemapush_test.go'], {
    missing: [...localFiles, 'go-annotations', 'composite-source'],
    design: ['oci-artifact', 'live-database', 'migration-directory'],
    gap: ['external-program', 'configured-external'],
    composableSources: [...localFiles, 'go-annotations', 'composite-source'],
    limitations: {
      'oci-artifact': 'OCI is the destination of this command, not an input source.',
      'external-program': 'The command registers no --schema-cmd.',
      'configured-external': 'The command registers no --allow-external-schema.',
    },
    suffix: 'oci://registry.example/app:v1',
  }),
  desiredCommand('ptah migrations plan', ['migrations', 'plan'], 'internal/cli/migrate', ['scripts/check-source-workflows.sh', 'internal/cli/migrate/source_oci_test.go'], {
    verified: [...localFiles, 'go-annotations', 'external-program', 'configured-external', 'oci-artifact', 'composite-source'],
    conditional: ['live-database'],
    design: ['migration-directory'],
    composableSources: [...localFiles, 'go-annotations', 'external-program', 'configured-external', 'composite-source'],
    invocations: { 'live-database': '--schema-file desired.sql --db-url sqlite://current.db' },
    limitations: { 'live-database': 'The live database supplies the current state through --db-url.' },
    suffix: '--db-url sqlite://current.db',
  }),
  desiredCommand('ptah migrations generate', ['migrations', 'generate'], 'internal/cli/migrate', ['scripts/check-source-workflows.sh', 'internal/cli/migrate/source_oci_test.go'], {
    verified: [...localFiles, 'go-annotations', 'external-program', 'configured-external', 'oci-artifact', 'composite-source'],
    conditional: ['live-database', 'migration-directory'],
    composableSources: [...localFiles, 'go-annotations', 'external-program', 'configured-external', 'composite-source'],
    invocations: {
      'live-database': '--schema-file desired.sql --db-url sqlite://current.db --migrations-dir ./migrations --name canonical',
      'migration-directory': '--replay --migrations-dir ./migrations --dev-url sqlite://dev.db --schema-file desired.sql',
    },
    limitations: {
      'live-database': 'The live database supplies the current state through --db-url.',
      'migration-directory': 'With --replay, the migration directory supplies current state and is reset on --dev-url.',
    },
    suffix: '--db-url sqlite://current.db --migrations-dir ./migrations --name canonical',
  }),
  desiredCommand('ptah viz', ['viz'], 'internal/cli/viz',
    ['internal/cli/viz/viz_test.go', 'scripts/check-source-equivalence.sh'], {
      verified: [...localFiles, 'go-annotations', 'composite-source'],
      missing: ['oci-artifact'],
      design: ['external-program', 'configured-external', 'live-database', 'migration-directory'],
      composableSources: [...localFiles, 'go-annotations', 'oci-artifact', 'composite-source'],
      limitations: {
        'external-program': 'The command registers no --schema-cmd; render the program output to a file first.',
        'configured-external': 'The command reads no project configuration for an external source.',
        'live-database': 'A diagram is drawn from a desired schema. Introspect the database first, or inspect it into a schema file.',
        'migration-directory': 'A migration directory is a script rather than a desired schema.',
      },
    }),
  desiredCommand('ptah schema serve', ['schema', 'serve'], 'internal/cli/internal/schemaserve', ['internal/cli/internal/schemaserve/schemaserve_test.go'], {
    verified: [...localFiles, 'go-annotations', 'composite-source'],
    conditional: ['live-database'],
    design: ['external-program', 'configured-external', 'oci-artifact', 'migration-directory'],
    limitations: {
      'external-program': 'The command registers no --schema-cmd; render the program output to a file first.',
      'configured-external': 'The command reads no project configuration for an external source.',
      'oci-artifact': 'The page re-reads its source on every request, so a registry artifact would be pulled on that schedule or shown stale. Use ptah schema drift, which pulls once and answers once.',
      'migration-directory': 'A migration directory is a script rather than a desired schema.',
      'live-database': 'The live database is the read-only comparison target.',
    },
    invocations: { 'live-database': '--root-dir ./models --db-url sqlite://current.db' },
    suffix: '--db-url sqlite://current.db',
  }),
  desiredCommand('ptah introspect', ['introspect'], 'internal/cli/introspect', ['internal/cli/introspect/introspect_internal_test.go'], {
    verified: ['live-database'],
    design: allSourceIds.filter((id) => id !== 'live-database'),
    limitations: { 'live-database': 'This is an output conversion to Go annotations, not a general desired-schema consumer.' },
    suffix: '--out ./models',
  }),
  desiredCommand('stokaro/ptah-action', null, '.github/actions/ptah', ['.github/actions/ptah/run.sh'], {
    verified: [...localFiles, 'go-annotations', 'external-program', 'configured-external', 'oci-artifact', 'composite-source'],
    conditional: ['live-database'],
    design: ['migration-directory'],
    invocations: {
      'go-annotations': 'uses: stokaro/ptah-action with input dir: ./models',
      'sql-file': 'uses: stokaro/ptah-action with input schema-file: ./schema.sql',
      'yaml-file': 'uses: stokaro/ptah-action with input schema-file: ./schema.yaml',
      'hcl-file': 'uses: stokaro/ptah-action with input schema-file: ./schema.hcl',
      'dbml-file': 'uses: stokaro/ptah-action with input schema-file: ./schema.dbml',
      'oci-artifact': 'uses: stokaro/ptah-action with input schema-file: oci://ghcr.io/acme/app-schema:v1',
      'external-program': 'uses: stokaro/ptah-action with inputs schema-cmd: go run ./loader and schema-format: sql',
      'configured-external': 'uses: stokaro/ptah-action with inputs schema-cmd and schema-format',
      'composite-source': 'uses: stokaro/ptah-action with schema-file and dir taking one value per line',
      'live-database': 'uses: stokaro/ptah-action with inputs dir: ./models and db-url: sqlite://app.db',
    },
    limitations: {
      'live-database': 'The database is the current-state target.',
    },
  }),
];

function statusFor(command, sourceId) {
  let found = command.defaultStatus;
  for (const [status, sourceIds] of Object.entries(command.groups)) {
    if (sourceIds.includes(sourceId)) {
      if (found !== command.defaultStatus) throw new Error(`${command.command}/${sourceId} appears in more than one status group`);
      found = status;
    }
  }
  return found;
}

function invocationFor(command, source, status) {
  if (command.invocations[source.id]) return `${command.command} ${command.invocations[source.id]}`;
  if (status.startsWith('unsupported') || status === 'not-applicable') return null;
  return `${command.command} ${source.spelling}${command.suffix ? ` ${command.suffix}` : ''}`;
}

export function buildManifest() {
  const entries = [];
  for (const command of commands) {
    for (const source of sources) {
      const status = statusFor(command, source.id);
      entries.push({
        command: command.command,
        source: source.id,
        status,
        invocation: invocationFor(command, source, status),
        implementationOwner: command.owner,
        evidence: command.evidence,
        limitations: command.limitations[source.id] ?? '',
        composable: command.composableSources.includes(source.id),
        externalExecutionRequiresOptIn: source.id === 'configured-external',
      });
    }
  }
  return {
    schemaVersion: 2,
    verifiedAt,
    statuses: [...statuses],
    sources,
    entries,
  };
}

export function manifestProblems(manifest, { binary } = {}) {
  const problems = [];
  if (manifest.schemaVersion !== 2) problems.push('schemaVersion must be 2');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(manifest.verifiedAt ?? '')) problems.push('verifiedAt must be YYYY-MM-DD');
  if (!Array.isArray(manifest.sources) || manifest.sources.length !== sources.length) {
    problems.push(`sources has ${manifest.sources?.length ?? 'no'} rows, want ${sources.length}`);
  } else {
    for (const source of manifest.sources) {
      if (!source.transport || !source.expressiveness) problems.push(`${source.id}: transport and expressiveness are required`);
      if (!exportMetadataStatuses.has(source.exportMetadata?.status)) {
        problems.push(`${source.id}: exportMetadata.status must name a supported state`);
      }
      if (!source.exportMetadata?.detail) problems.push(`${source.id}: exportMetadata.detail is required`);
    }
  }
  const wanted = commands.length * sources.length;
  if (!Array.isArray(manifest.entries) || manifest.entries.length !== wanted) {
    problems.push(`entries has ${manifest.entries?.length ?? 'no'} rows, want ${wanted}`);
    return problems;
  }
  const seen = new Set();
  const helpCache = new Map();
  for (const entry of manifest.entries) {
    const key = `${entry.command}\u0000${entry.source}`;
    if (seen.has(key)) problems.push(`duplicate entry ${entry.command}/${entry.source}`);
    seen.add(key);
    if (!statuses.has(entry.status)) problems.push(`${entry.command}/${entry.source}: unknown status ${entry.status}`);
    if (!existsSync(join(repoRoot, entry.implementationOwner))) problems.push(`${entry.command}/${entry.source}: missing owner ${entry.implementationOwner}`);
    for (const evidence of entry.evidence ?? []) {
      if (!existsSync(join(repoRoot, evidence))) problems.push(`${entry.command}/${entry.source}: missing evidence ${evidence}`);
    }
    const usable = !entry.status.startsWith('unsupported') && entry.status !== 'not-applicable';
    if (usable && !entry.invocation) problems.push(`${entry.command}/${entry.source}: usable entry has no invocation`);
    if (!usable && entry.invocation !== null) problems.push(`${entry.command}/${entry.source}: unsupported entry must have a null invocation`);
    if (entry.source === 'configured-external' && !entry.externalExecutionRequiresOptIn) {
      problems.push(`${entry.command}/${entry.source}: configured execution must require opt-in`);
    }

    if (binary && usable && entry.command.startsWith('ptah ')) {
      const declaration = commands.find((candidate) => candidate.command === entry.command);
      const cacheKey = declaration.help.join(' ');
      let help = helpCache.get(cacheKey);
      if (!help) {
        help = execFileSync(binary, [...declaration.help, '--help'], { encoding: 'utf8' });
        helpCache.set(cacheKey, help);
      }
      for (const flag of entry.invocation.match(/--[a-z][a-z-]*/g) ?? []) {
        if (!help.includes(flag)) problems.push(`${entry.command}/${entry.source}: ${flag} is absent from built help`);
      }
    }
  }
  return problems;
}

function selftest() {
  const manifest = buildManifest();
  if (manifestProblems(manifest).length !== 0) throw new Error(manifestProblems(manifest).join('\n'));
  const duplicate = structuredClone(manifest);
  duplicate.entries[1] = structuredClone(duplicate.entries[0]);
  if (!manifestProblems(duplicate).some((problem) => problem.includes('duplicate entry'))) throw new Error('duplicate entry passed');
  const missingInvocation = structuredClone(manifest);
  const usable = missingInvocation.entries.find((entry) => entry.status === 'verified');
  usable.invocation = null;
  if (!manifestProblems(missingInvocation).some((problem) => problem.includes('usable entry has no invocation'))) {
    throw new Error('usable entry without an invocation passed');
  }
  const missingExpressiveness = structuredClone(manifest);
  missingExpressiveness.sources[0].expressiveness = '';
  if (!manifestProblems(missingExpressiveness).some((problem) => problem.includes('transport and expressiveness'))) {
    throw new Error('source without an expressiveness boundary passed');
  }
  const missingExportMetadata = structuredClone(manifest);
  delete missingExportMetadata.sources[0].exportMetadata;
  if (!manifestProblems(missingExportMetadata).some((problem) => problem.includes('exportMetadata.status'))) {
    throw new Error('source without an export-metadata boundary passed');
  }
  const invalidExportMetadata = structuredClone(manifest);
  invalidExportMetadata.sources[0].exportMetadata.status = 'maybe';
  if (!manifestProblems(invalidExportMetadata).some((problem) => problem.includes('exportMetadata.status'))) {
    throw new Error('source with an invalid export-metadata state passed');
  }
  console.log(`build-source-support.mjs --selftest: OK (${manifest.entries.length} complete command/source cells)`);
}

function main() {
  const arguments_ = process.argv.slice(2);
  if (arguments_.includes('--selftest')) {
    selftest();
    return;
  }
  const write = arguments_.includes('--write');
  const binaryIndex = arguments_.indexOf('--binary');
  const binary = binaryIndex === -1 ? undefined : arguments_[binaryIndex + 1];
  const allowed = new Set(['--write', '--binary', binary]);
  if (arguments_.some((argument) => !allowed.has(argument)) || binaryIndex !== -1 && !binary) {
    console.error('usage: node docs/site/scripts/build-source-support.mjs [--write] [--binary <ptah>]');
    process.exitCode = 2;
    return;
  }
  const expected = buildManifest();
  const problems = manifestProblems(expected, { binary });
  if (problems.length > 0) throw new Error(problems.join('\n'));
  const rendered = `${JSON.stringify(expected, null, 2)}\n`;
  if (write) {
    writeFileSync(outputPath, rendered);
    console.log(`source support: wrote ${outputPath}`);
    return;
  }
  if (!existsSync(outputPath) || readFileSync(outputPath, 'utf8') !== rendered) {
    console.error('source support: generated manifest is stale; run with --write');
    process.exitCode = 1;
    return;
  }
  console.log(`source support: OK (${commands.length} commands, ${sources.length} sources, ${expected.entries.length} cells)`);
}

main();
