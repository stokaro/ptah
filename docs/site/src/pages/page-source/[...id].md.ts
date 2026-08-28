import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';
import { readFile } from 'node:fs/promises';
import { pageMarkdown } from '../../lib/page-context.mjs';

export const prerender = true;

export async function getStaticPaths() {
  const entries = await getCollection('docs');
  const paths = [];

  for (const entry of entries) {
    if (!entry.filePath || entry.data.template === 'splash') continue;

    const source = await readFile(entry.filePath, 'utf8');
    const pagePath = entry.id === 'index' ? '' : `${entry.id}/`;
    const canonicalUrl = new URL(`${import.meta.env.BASE_URL}${pagePath}`, import.meta.env.SITE).href;
    paths.push({
      params: { id: entry.id },
      props: {
        markdown: pageMarkdown({
          title: entry.data.title,
          description: entry.data.description,
          canonicalUrl,
          source,
        }),
      },
    });
  }

  return paths;
}

export const GET: APIRoute = ({ props }) =>
  new Response(props.markdown, {
    headers: {
      'Content-Type': 'text/markdown; charset=utf-8',
    },
  });
