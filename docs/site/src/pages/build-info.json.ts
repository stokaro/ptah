import type { APIRoute } from 'astro';
import { execFileSync } from 'node:child_process';
import { sourceRefForVersion } from '../lib/source-ref.mjs';

export const prerender = true;

function sourceCommit(): string {
  const supplied = process.env.DOCS_SOURCE_COMMIT?.trim();
  if (supplied) return supplied;

  try {
    return execFileSync('git', ['rev-parse', 'HEAD'], {
      cwd: process.cwd(),
      encoding: 'utf8',
    }).trim();
  } catch (error) {
    throw new Error(
      `build-info.json requires DOCS_SOURCE_COMMIT or a Git checkout: ${error instanceof Error ? error.message : error}`,
    );
  }
}

const documentationVersion = process.env.DOCS_VERSION?.trim() || 'edge';
const builtAt = process.env.DOCS_BUILT_AT?.trim() || new Date().toISOString();
const payload = {
  documentation_version: documentationVersion,
  source_ref: sourceRefForVersion(documentationVersion),
  source_commit: sourceCommit(),
  built_at: builtAt,
};

export const GET: APIRoute = () => new Response(`${JSON.stringify(payload, null, 2)}\n`, {
  headers: {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-cache',
  },
});
