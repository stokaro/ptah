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
};

/** Terms in the order a legend should list them. */
export const glossaryTerms = Object.keys(glossary);
