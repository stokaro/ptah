#!/usr/bin/env node
// Render committed Mermaid sources. SVG text labels work in GitHub's image sandbox.
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const siteRoot = join(dirname(fileURLToPath(import.meta.url)), '..');
const check = process.argv.includes('--check');
if (process.platform !== 'linux') {
  throw new Error('Journey SVGs use Linux text metrics. Set PTAH_DOCKER_CONTEXT and run npm run diagrams:docker (add -- --check to verify).');
}
// Embed the font so image viewers need no installed fonts. Linux owns layout metrics.
const font = readFileSync(join(siteRoot, 'src/fonts/instrument-sans-var-latin.woff2')).toString('base64');
const fontCSS = `@font-face{font-family:PtahDiagram;src:url(data:font/woff2;base64,${font}) format('woff2');font-weight:100 900;}`;
const browser = await chromium.launch();
try {
  for (const name of ['product-journeys', 'inference-generation-lifecycle']) {
    const source = readFileSync(join(siteRoot, 'diagrams', `${name}.mmd`), 'utf8');
    for (const theme of ['light', 'dark']) {
      const page = await browser.newPage();
      await page.setContent(`<style>${fontCSS}</style><main></main>`);
      await page.evaluate(() => document.fonts.load('18px PtahDiagram'));
      await page.addScriptTag({ path: join(siteRoot, 'node_modules/mermaid/dist/mermaid.min.js') });
      const svg = await page.evaluate(async ({ source, name, theme, fontCSS }) => {
        const dark = theme === 'dark';
        window.mermaid.initialize({
          startOnLoad: false,
          securityLevel: 'strict',
          theme: 'base',
          look: 'classic',
          markdownAutoWrap: false,
          fontFamily: 'PtahDiagram, Arial, sans-serif',
          htmlLabels: false,
          deterministicIds: true,
          deterministicIDSeed: name,
          flowchart: { htmlLabels: false, curve: 'linear', nodeSpacing: 32, rankSpacing: 42, padding: 16, wrappingWidth: 280, useMaxWidth: false },
          themeCSS: `
            .node.active rect { fill: ${dark ? '#14382b' : '#dcfce7'}; stroke: ${dark ? '#4ade80' : '#15803d'}; }
            .node.candidate rect { fill: ${dark ? '#3d2e16' : '#fef3c7'}; stroke: ${dark ? '#fbbf24' : '#b45309'}; }
            .node.destructive rect { fill: ${dark ? '#421c23' : '#fee2e2'}; stroke: ${dark ? '#f87171' : '#b91c1c'}; }
          `,
          themeVariables: {
            darkMode: dark,
            primaryColor: dark ? '#172f43' : '#e0f2fe',
            primaryTextColor: dark ? '#f1f5f9' : '#0f172a',
            primaryBorderColor: dark ? '#38bdf8' : '#0284c7',
            lineColor: dark ? '#7dd3fc' : '#0369a1',
            clusterBkg: dark ? '#111827' : '#f8fafc',
            clusterBorder: dark ? '#64748b' : '#cbd5e1',
            titleColor: dark ? '#f1f5f9' : '#0f172a',
            edgeLabelBackground: dark ? '#111827' : '#f8fafc',
            fontSize: '18px',
          },
        });
        const { svg } = await window.mermaid.render(`ptah-${name}`, source);
        document.querySelector('main').innerHTML = svg;
        const root = document.querySelector('main svg');
        // Mermaid's text-only edge labels need an opaque backing in image renders.
        for (const label of root.querySelectorAll('.edgeLabel .label')) {
          const bounds = label.getBBox();
          const backing = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
          backing.setAttribute('x', bounds.x - 4);
          backing.setAttribute('y', bounds.y - 2);
          backing.setAttribute('width', bounds.width + 8);
          backing.setAttribute('height', bounds.height + 4);
          backing.setAttribute('style', `fill:${dark ? '#111827' : '#f8fafc'};opacity:1`);
          label.prepend(backing);
        }
        return root.outerHTML.replace('<style>', `<style>${fontCSS}`);
      }, { source, name, theme, fontCSS });
      await page.close();
      emit(`${name}-${theme}.svg`, svg);
    }
  }
} finally {
  await browser.close();
}
function emit(name, svg) {
  const path = join(siteRoot, 'src/assets', name);
  const content = `${svg.replace(/-?\d+\.\d{4,}/g, (value) => String(Number(Number(value).toFixed(3))))}\n`;
  if (check) {
    if (readFileSync(path, 'utf8') !== content) throw new Error(`${name} is stale; run npm run diagrams:write`);
  } else writeFileSync(path, content);
}
console.log(`Journey diagrams ${check ? 'match their Mermaid sources' : 'rendered'} (light and dark).`);
