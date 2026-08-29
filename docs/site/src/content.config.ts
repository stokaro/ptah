import { defineCollection } from 'astro:content';
import { z } from 'astro/zod';
import { docsLoader } from '@astrojs/starlight/loaders';
import { docsSchema } from '@astrojs/starlight/schema';
import { dispositions, pageTypes, validatePageMetadata } from './lib/content-metadata.mjs';

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
  lengthWaiver: z.string().min(1).optional(),
  quickstart: z.boolean().optional(),
}).superRefine((data, context) => {
  for (const problem of validatePageMetadata(data)) {
    context.addIssue({ code: 'custom', path: problem.path, message: problem.message });
  }
});

export const collections = {
  docs: defineCollection({
    loader: docsLoader(),
    schema: docsSchema({ extend: pageMetadata }),
  }),
};
