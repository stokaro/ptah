import { createServer } from 'node:http';
import { existsSync, readFileSync, statSync } from 'node:fs';
import { extname, join } from 'node:path';

const mimeTypes = {
  '.css': 'text/css; charset=utf-8',
  '.html': 'text/html; charset=utf-8',
  '.ico': 'image/x-icon',
  '.js': 'text/javascript; charset=utf-8',
  '.jpg': 'image/jpeg',
  '.json': 'application/json; charset=utf-8',
  '.pagefind': 'application/octet-stream',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.wasm': 'application/wasm',
  '.webp': 'image/webp',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.xml': 'application/xml; charset=utf-8',
};

export function detectBase(distRoot) {
  const home = join(distRoot, 'index.html');
  if (!existsSync(home)) return '';
  const match = readFileSync(home, 'utf8').match(/(?:href|src)="([^"]*)\/_astro\//);
  return match ? match[1] : '';
}

// startBuiltSite serves one version's build under its base. `route`, when
// given, answers first: it receives the decoded path and the method, and
// returns { status, body, type } for a request it owns or undefined for one it
// does not. The deploy puts files beside the version directories -- the version
// index, the version picker -- and a check that reads them from the page it
// renders serves them here, the way the Pages root does.
export function startBuiltSite(distRoot, base = detectBase(distRoot), route = undefined) {
  const server = createServer((request, response) => {
    let url = decodeURIComponent((request.url ?? '/').split('?')[0]);
    const routed = route?.(url, request.method ?? 'GET');
    if (routed) {
      response.writeHead(routed.status, routed.type ? { 'content-type': routed.type } : {});
      response.end(request.method === 'HEAD' ? undefined : routed.body);
      return;
    }
    if (base && url.startsWith(base)) url = url.slice(base.length) || '/';

    let filePath = join(distRoot, url);
    if (existsSync(filePath) && statSync(filePath).isDirectory()) {
      filePath = join(filePath, 'index.html');
    }
    if (!existsSync(filePath) || !statSync(filePath).isFile()) {
      response.writeHead(404);
      response.end('not found');
      return;
    }

    response.writeHead(200, {
      'content-type': mimeTypes[extname(filePath)] ?? 'application/octet-stream',
    });
    response.end(readFileSync(filePath));
  });

  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      resolve({ base, server, port: server.address().port });
    });
  });
}

// mimeType is the content type the server above gives a path.
export function mimeType(path) {
  return mimeTypes[extname(path)] ?? 'application/octet-stream';
}

export async function loadChromium(checkName) {
  try {
    const { chromium } = await import('playwright');
    return chromium;
  } catch {
    const install = 'npm i -D playwright && npx playwright install chromium';
    const message = `${checkName}: playwright is not installed; run "${install}"`;
    if (process.env.CI) throw new Error(message);
    console.log(`${message} (skipped outside CI)`);
    return null;
  }
}
