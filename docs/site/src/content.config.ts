import { defineCollection } from 'astro:content';
import { z } from 'astro/zod';
import { docsLoader, i18nLoader } from '@astrojs/starlight/loaders';
import { docsSchema, i18nSchema } from '@astrojs/starlight/schema';
import { fileURLToPath } from 'node:url';
import { dispositions, pageTypes, sourceModes, validatePageMetadata } from './lib/content-metadata.mjs';

const repositoryRoot = fileURLToPath(new URL('../../../', import.meta.url));

const pageMetadata = z.object({
  type: z.enum(pageTypes as [string, ...string[]]),
  audience: z.array(z.string().min(1)).min(1),
  readerQuestion: z.string().min(1),
  goal: z.string().min(1),
  sourceOfTruth: z.array(z.string().min(1)).min(1),
  owns: z.array(z.string().min(1)).optional(),
  generated: z.boolean(),
  generator: z.string().min(1).optional(),
  editSource: z.string().min(1).optional(),
  lastVerified: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
  evidence: z.array(z.string().min(1)).min(1).optional(),
  searchAliases: z.array(z.string().min(1)).optional(),
  overlaps: z.array(z.string().min(1)),
  disposition: z.enum(dispositions as [string, ...string[]]),
  sourceMode: z.enum(sourceModes as [string, ...string[]]).optional(),
  // Keep the retired key visible only so Astro rejects it instead of silently
  // stripping an unknown frontmatter field.
  lengthWaiver: z.never().optional(),
  quickstart: z.boolean().optional(),
}).superRefine((data, context) => {
  for (const problem of validatePageMetadata(data, { repositoryRoot })) {
    context.addIssue({ code: 'custom', path: problem.path, message: problem.message });
  }
});

export const collections = {
  docs: defineCollection({
    loader: docsLoader(),
    schema: docsSchema({ extend: pageMetadata }),
  }),
  // Starlight's interface strings, where the design writes them differently:
  // src/content/i18n/en.json (stokaro/ptah#2893).
  i18n: defineCollection({
    loader: i18nLoader(),
    schema: i18nSchema(),
  }),
};
