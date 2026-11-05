import type { APIRoute } from 'astro';
import { renderEcosystemSvg } from '../../lib/ecosystem-map.mjs';

// The full-size and download targets of the ecosystem map. They are built from
// the declaration that draws the inline map, so no committed render can fall
// behind it. A file opened on its own has no stylesheet around it, so each one
// embeds the colors of one theme.
export const prerender = true;

export function getStaticPaths() {
  return [{ params: { theme: 'light' } }, { params: { theme: 'dark' } }];
}

export const GET: APIRoute = ({ params }) =>
  new Response(renderEcosystemSvg({ theme: params.theme }), {
    headers: { 'Content-Type': 'image/svg+xml; charset=utf-8' },
  });
