/**
 * Terms whose definition belongs where the term is used.
 *
 * One map rather than prose repeated per use site, so the same word cannot come
 * to mean two things on two pages. A term is added here when a table shows it as
 * a glyph or a number whose meaning lives in a paragraph somewhere else on the
 * page (stokaro/ptah#1454).
 *
 * Definitions are one or two sentences. Anything longer is a section with a
 * heading, and a tooltip is the wrong shape for it.
 */
export interface GlossaryEntry {
  /** The definition shown in the tooltip. */
  readonly definition: string;
  /**
   * Where the fact comes from, when something in the repository pins it. A
   * definition that names a test or a generated file is one a reader can check
   * and one that goes stale loudly.
   */
  readonly source?: string;
}

export const glossary: Record<string, GlossaryEntry> = {
  measured: {
    definition:
      'The row was executed against a server on this exact release line, and the answer here is what that server said.',
    source: 'core/platform/capability/capability_measured_lines_test.go',
  },
  carried: {
    definition:
      'No server on this line answered for the key. The value is inherited from the preset the line resolves to, and the reason is recorded beside it.',
    source: 'core/platform/capability/capability_measured_lines_test.go',
  },
  certified: {
    definition:
      'A release line the capability probe runs against on every pull request, so its preset is checked by a server rather than by a paragraph.',
    source: 'internal/capabilityprobe/cells.go',
  },
  'best-effort': {
    definition:
      'A release line Ptah supports without a server in the probe matrix. Its preset is transcribed from measurement rather than re-measured on every run.',
    source: 'internal/capabilityprobe/cells.go',
  },
  'legacy-tested': {
    definition:
      'A release line past its upstream end of life that Ptah still tests. End of life lowers the support level, not the behavior.',
    source: 'internal/capabilityprobe/cells.go',
  },
  'known-incompatible': {
    definition:
      'A release line with a named technical incompatibility. Upstream end of life by itself does not make a line incompatible.',
    source: 'core/platform/capability/support.go',
  },
  'desired-schema': {
    definition:
      'The schema state Ptah is asked to reach, assembled from one or more schema sources before it is compared with a database.',
    source: 'core/schemamodel/types.go',
  },
  'schema-source': {
    definition:
      'One input that contributes to a desired schema, such as Go annotations, HCL, YAML, SQL, a database, or an external loader.',
    source: 'core/schemasource/schemasource.go',
  },
  'migration-directory': {
    definition:
      'An ordered set of migration files plus its integrity metadata. Ptah plans, validates, applies, and rolls back the directory as history.',
    source: 'migration/migrationfile',
  },
  revision: {
    definition:
      'The migration history state recorded in the target database: which version was applied, when, and with which integrity metadata.',
    source: 'migration/migrator/revision_table.go',
  },
  drift: {
    definition:
      'A difference between the desired schema and the state Ptah reads from the target database.',
    source: 'migration/schemadiff',
  },
  'dev-database': {
    definition:
      'A disposable database Ptah may reset and replay into while materializing schema state or planning a change. It is not the migration target.',
    source: 'migration/internal/shadowdb',
  },
  'shadow-database': {
    definition:
      'A disposable database used to verify that migrations reproduce the intended schema and that rollback behaves as planned.',
    source: 'migration/shadow',
  },
  'throwaway-database': {
    definition:
      'A database supplied specifically for destructive tests or examples, with no state the operator needs to preserve.',
    source: 'cmd/schema',
  },
  dialect: {
    definition:
      'The SQL and database family Ptah parses and renders for. A dialect selects a family; capabilities decide which constructs one target accepts.',
    source: 'core/platform/constants.go',
  },
  capability: {
    definition:
      'A measured or declared property of a database target that gates one schema construct or behavior independently of the dialect name.',
    source: 'core/platform/capability/capability.go',
  },
  'release-line': {
    definition:
      'A version family Ptah declares as one support and capability measurement cell, such as PostgreSQL 17 or MySQL 8.4.',
    source: 'internal/capabilityprobe/cells.go',
  },
  generation: {
    definition:
      'One immutable identity for an embedding model, preprocessing contract, target layout, and the vectors produced under that contract.',
    source: 'cmd/inference',
  },
  'candidate-generation': {
    definition:
      'A generation being prepared, backfilled, indexed, and verified while queries still read the active generation.',
    source: 'cmd/inference',
  },
  cutover: {
    definition:
      'The explicit, digest-approved state transition that moves the query pointer from the active generation to a verified candidate.',
    source: 'cmd/inference',
  },
  conformance: {
    definition:
      'External measurement of Ptah against a pinned comparison surface. Registered commands or internal tests alone are not conformance evidence.',
    source: 'stokaro/ptah-atlas-conformance',
  },
  contour: {
    definition:
      'One measured corpus of the conformance repository, with its own report and its own budget file: the offline fixtures, the live round-trip, the Atlas CE differential, the migrate runtime, and the CLI surface. Each is gated on its own.',
    source: 'stokaro/ptah-atlas-conformance: gaps.md, gaps-live.md, gaps-diff.md, gaps-migrate-runtime.md, cli-surface.md',
  },
  'regression-budget': {
    definition:
      'A gate that fails when a report exceeds the committed budget for its contour, or when a waiver no longer matches a finding. Green means nothing regressed, not that the contour is covered.',
    source: 'stokaro/ptah-atlas-conformance: make budget, gap-budget.txt',
  },
  'full-conformance': {
    definition:
      'A gate that regenerates its report and fails if any non-OK observation remains, waived findings included. It is the yardstick rather than the merge gate.',
    source: 'stokaro/ptah-atlas-conformance: make gate, .github/workflows/full-conformance.yml',
  },
};

/** Terms in the order a legend should list them. */
export const glossaryTerms = Object.keys(glossary);
