#!/usr/bin/env node
// Drives the FAQ's tag rail in a browser.
//
// check-faq-groups.mjs holds the page and src/faq-tags.mjs to each other and
// stops at the markup, so a page that lost the filter altogether passes it.
// What this one checks is what a reader would notice and a build log would not.
// The rail names every tag the data carries and says how many questions are
// behind it. Selecting one leaves exactly those questions, a second widens the
// result, and a label typed into the field under the rail reaches them too. The
// contents rail navigates inside the result instead of throwing it away, while
// a link to a question the filter hid still opens it. A tag picked out of the
// folded tail keeps a control that releases it. The chips under an answer name
// that question's own tags. The empty state says only what the reader did.
//
//   node scripts/check-faq-page.mjs [--dist <dir>] [--selftest]
//
// Requires Playwright's chromium. Without it this skips and says what to
// install; with CI=1 it fails instead, because a green check that measured
// nothing is worse than a red one.

import { createServer } from 'node:http';
import { existsSync, readFileSync, statSync } from 'node:fs';
import { dirname, extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { questionTags, tagLabels } from '../src/faq-tags.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');

const mimeTypes = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.json': 'application/json',
  '.svg': 'image/svg+xml',
  '.woff2': 'font/woff2',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
};

function detectBase(distRoot) {
  const home = join(distRoot, 'index.html');
  if (!existsSync(home)) return '';
  const match = readFileSync(home, 'utf8').match(/(?:href|src)="([^"]*)\/_astro\//);
  return match ? match[1] : '';
}

function startServer(distRoot, base) {
  const server = createServer((request, response) => {
    let url = decodeURIComponent((request.url ?? '/').split('?')[0]);
    if (base && url.startsWith(base)) url = url.slice(base.length) || '/';
    let filePath = join(distRoot, url);
    if (existsSync(filePath) && statSync(filePath).isDirectory()) filePath = join(filePath, 'index.html');
    if (!existsSync(filePath) || !statSync(filePath).isFile()) {
      response.writeHead(404);
      response.end('not found');
      return;
    }
    response.writeHead(200, { 'content-type': mimeTypes[extname(filePath)] ?? 'application/octet-stream' });
    response.end(readFileSync(filePath));
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve({ server, port: server.address().port }));
  });
}

// What the rail owes the data: one button per tag a question carries, under the
// label the data gives it, counting the questions behind it. Computed here
// rather than written down, so a tag added to src/faq-tags.mjs is a tag this
// check expects to see.
export function expectedRail(tags) {
  const counts = new Map();
  for (const carried of Object.values(tags.questionTags)) {
    for (const tag of carried) counts.set(tag, (counts.get(tag) ?? 0) + 1);
  }
  return new Map(
    [...counts.entries()].map(([tag, count]) => [tag, { count, label: tags.tagLabels[tag] ?? tag }]),
  );
}

// The questions a selection reaches: any of the chosen tags, which is what the
// rail's own group label promises a reader who picks a second one.
export function questionsCarrying(selected, questionTags) {
  return Object.keys(questionTags).filter((anchor) =>
    questionTags[anchor].some((tag) => selected.includes(tag)),
  );
}

// railProblems reads the buttons the page rendered against the tags the data
// carries. A page that lost the component renders none of them, which is the
// hole this file was written for.
export function railProblems(rail, expected) {
  const problems = [];
  const shown = new Map(rail.map((button) => [button.tag, button]));
  for (const [tag, want] of expected) {
    const button = shown.get(tag);
    if (!button) {
      problems.push(`the rail offers no button for "${tag}", which ${want.count} question(s) carry`);
      continue;
    }
    if (button.label !== want.label) {
      problems.push(
        `the rail calls "${tag}" ${JSON.stringify(button.label)} and the data calls it ` +
          JSON.stringify(want.label),
      );
    }
    if (button.count !== want.count) {
      problems.push(`the rail puts ${button.count} behind "${tag}" and ${want.count} question(s) carry it`);
    }
  }
  for (const button of rail) {
    if (!expected.has(button.tag)) {
      problems.push(`the rail offers "${button.tag}", which no question carries`);
    }
  }
  return problems;
}

// filterProblems judges what each interaction left on the page. A reading with
// `atLeast` is a term typed into the field, which may match prose as well; one
// without it is a selection, which is exactly the questions carrying the tags.
export function filterProblems(readings, questionTags) {
  const problems = [];
  for (const reading of readings) {
    const want = questionsCarrying(reading.select, questionTags);
    // A reading naming no tag the data carries can no longer discriminate: it
    // would pass over a filter that showed anything at all.
    if (want.length === 0) {
      problems.push(`${reading.what} names no tag any question carries, so it measures nothing`);
      continue;
    }
    const shown = new Set(reading.shown);
    const missing = want.filter((anchor) => !shown.has(anchor));
    if (missing.length > 0) problems.push(`${reading.what} leaves out ${missing.join(', ')}`);
    if (!reading.atLeast) {
      const extra = reading.shown.filter((anchor) => !want.includes(anchor));
      if (extra.length > 0) problems.push(`${reading.what} also shows ${extra.join(', ')}`);
    }
  }
  return problems;
}

// behaviorProblems judges what each control owes the reader, which no count on
// the page can show.
export function behaviorProblems(readings) {
  const problems = [];
  const { railLink, deepLink, fold, empty, chips } = readings;

  if (!railLink.kept) {
    problems.push(
      'following the contents rail cleared the filter, so the only navigation a filtered page ' +
        'offers is the one that destroys it',
    );
  }
  if (!railLink.arrived) {
    problems.push('following the contents rail did not reach the group it names');
  }
  if (!deepLink.opened) {
    problems.push('a link to a question the filter hid left it hidden, so a shared link reads as broken');
  }

  if (fold.hidden) {
    problems.push(
      'folding the tag tail hid a tag that is still selected, leaving the page filtered by a facet ' +
        'with no control on screen to release it',
    );
  }
  if (!fold.stillFiltering) {
    problems.push('folding the tag tail dropped a selection the page is still filtered by');
  }

  if (/\btags?\b/i.test(empty.withoutTags)) {
    problems.push(`with no tag picked the empty state says ${JSON.stringify(empty.withoutTags)}`);
  }
  if (!/\btags?\b/i.test(empty.withTags)) {
    problems.push(
      `with a tag picked the empty state says ${JSON.stringify(empty.withTags)}, which does not ` +
        'tell the reader the tags are why',
    );
  }

  if (chips.got.join(' ') !== chips.want.join(' ')) {
    problems.push(
      `the chips under ${chips.anchor} name ${chips.got.join(', ') || 'nothing'} and the question ` +
        `carries ${chips.want.join(', ')}`,
    );
  }
  return problems;
}

function selftest() {
  const tags = {
    questionTags: { one: ['alpha'], two: ['alpha', 'beta'], three: ['beta'] },
    tagLabels: { alpha: 'Alpha', beta: 'Beta' },
  };
  const expected = expectedRail(tags);
  if (expected.get('alpha').count !== 2 || expected.get('beta').label !== 'Beta') {
    throw new Error(`expectedRail read ${JSON.stringify([...expected])}`);
  }

  const goodRail = [
    { tag: 'alpha', label: 'Alpha', count: 2 },
    { tag: 'beta', label: 'Beta', count: 2 },
  ];
  if (railProblems(goodRail, expected).length !== 0) {
    throw new Error('railProblems refused a rail that matches the data');
  }
  const railCases = [
    ['a page carrying no rail at all', [], 'offers no button for "alpha"'],
    ['a tag the rail dropped', goodRail.slice(0, 1), 'offers no button for "beta"'],
    [
      'a count that drifted',
      [{ tag: 'alpha', label: 'Alpha', count: 9 }, goodRail[1]],
      'puts 9 behind "alpha"',
    ],
    [
      'a label that drifted',
      [{ tag: 'alpha', label: 'Older name', count: 2 }, goodRail[1]],
      'calls "alpha" "Older name"',
    ],
    [
      'a button no question is behind',
      [...goodRail, { tag: 'gamma', label: 'Gamma', count: 1 }],
      'offers "gamma", which no question carries',
    ],
  ];
  for (const [name, rail, want] of railCases) {
    const found = railProblems(rail, expected);
    if (!found.some((problem) => problem.includes(want))) {
      throw new Error(`railProblems read ${name} as ${JSON.stringify(found)}`);
    }
  }

  const goodReadings = [
    { what: 'the Alpha tag', select: ['alpha'], shown: ['one', 'two'] },
    { what: 'Alpha and Beta', select: ['alpha', 'beta'], shown: ['one', 'two', 'three'] },
    { what: '"Beta" typed into the field', select: ['beta'], shown: ['two', 'three', 'one'], atLeast: true },
  ];
  if (filterProblems(goodReadings, tags.questionTags).length !== 0) {
    throw new Error('filterProblems refused a filter that holds');
  }
  const filterCases = [
    [
      'a selection that lost a question',
      [{ what: 'the Alpha tag', select: ['alpha'], shown: ['one'] }],
      'leaves out two',
    ],
    [
      'a selection that kept one it should not',
      [{ what: 'the Alpha tag', select: ['alpha'], shown: ['one', 'two', 'three'] }],
      'also shows three',
    ],
    [
      'a second tag that narrowed instead of widening',
      [{ what: 'Alpha and Beta', select: ['alpha', 'beta'], shown: ['two'] }],
      'leaves out one, three',
    ],
    [
      'a term that reaches none of the questions carrying the label it names',
      [{ what: '"Beta" typed into the field', select: ['beta'], shown: [], atLeast: true }],
      'leaves out two, three',
    ],
    [
      'a reading whose tag no question carries',
      [{ what: 'the Gamma tag', select: ['gamma'], shown: ['one'] }],
      'names no tag any question carries',
    ],
  ];
  for (const [name, readings, want] of filterCases) {
    const found = filterProblems(readings, tags.questionTags);
    if (!found.some((problem) => problem.includes(want))) {
      throw new Error(`filterProblems read ${name} as ${JSON.stringify(found)}`);
    }
  }

  const goodBehavior = {
    railLink: { kept: true, arrived: true },
    deepLink: { opened: true },
    fold: { hidden: false, stillFiltering: true },
    empty: {
      withoutTags: 'No question matches "zzzz". Try the site search, or open Troubleshooting.',
      withTags: 'No question matches "zzzz" in the tags you picked. Clear the tags to search the whole page.',
    },
    chips: { anchor: 'two', got: ['alpha', 'beta'], want: ['alpha', 'beta'] },
  };
  if (behaviorProblems(goodBehavior).length !== 0) {
    throw new Error(`behaviorProblems refused a page that holds: ${behaviorProblems(goodBehavior).join('; ')}`);
  }
  const behaviorCases = [
    ['railLink.kept', (one) => { one.railLink.kept = false; }, 'cleared the filter'],
    ['railLink.arrived', (one) => { one.railLink.arrived = false; }, 'did not reach the group'],
    ['deepLink.opened', (one) => { one.deepLink.opened = false; }, 'reads as broken'],
    ['fold.hidden', (one) => { one.fold.hidden = true; }, 'hid a tag that is still selected'],
    ['fold.stillFiltering', (one) => { one.fold.stillFiltering = false; }, 'dropped a selection'],
    [
      'an empty state claiming tags nobody picked',
      (one) => { one.empty.withoutTags = 'No question matches "zzzz" in the tags you picked.'; },
      'with no tag picked the empty state says',
    ],
    [
      'an empty state that hides the tags',
      (one) => { one.empty.withTags = 'No question matches "zzzz".'; },
      'does not tell the reader the tags are why',
    ],
    ['the chips under an answer', (one) => { one.chips.got = []; }, 'name nothing and the question carries'],
  ];
  for (const [name, spoil, want] of behaviorCases) {
    const bad = structuredClone(goodBehavior);
    spoil(bad);
    const found = behaviorProblems(bad);
    if (found.length !== 1 || !found[0].includes(want)) {
      throw new Error(`behaviorProblems read a withdrawn ${name} as ${JSON.stringify(found)}`);
    }
  }

  console.log(
    'check-faq-page.mjs --selftest: OK (what the rail owes the data, what a selection leaves on ' +
      'the page, and what each control owes the reader)',
  );
}

// What the page is showing, read the way the filter writes it: the flag goes on
// the heading's wrapper, so a question is on screen when its wrapper is not
// hidden.
const readPage = (page) =>
  page.evaluate(() => ({
    rail: [...document.querySelectorAll('[data-faq-tag]')].map((button) => ({
      tag: button.dataset.faqTag,
      label: button.textContent.replace(/\s*\d+\s*$/, '').trim(),
      count: Number(button.querySelector('.faq-tag__n')?.textContent),
      hidden: button.hidden,
      pressed: button.getAttribute('aria-pressed') === 'true',
    })),
    shown: [...document.querySelectorAll('.sl-markdown-content h3[id]')]
      .filter((heading) => !(heading.closest('.sl-heading-wrapper') ?? heading).hidden)
      .map((heading) => heading.id),
    groups: [...document.querySelectorAll('.sl-markdown-content h2[id]')]
      .filter((heading) => !(heading.closest('.sl-heading-wrapper') ?? heading).hidden)
      .map((heading) => heading.id),
    count: document.querySelector('[data-faq-count]')?.textContent ?? '',
    empty: document.querySelector('[data-faq-empty]')?.hidden
      ? ''
      : (document.querySelector('[data-faq-empty]')?.textContent ?? ''),
  }));

// Back to a page nothing has been done to, hash included: a hash left over from
// the reading before it would be applied again on load.
const reset = async (page, origin) => {
  await page.goto(`${origin}faq/`, { waitUntil: 'load' });
  await page.waitForSelector('[data-faq-tag]');
};

// measure drives the page and reports readings. Nothing is judged here: the
// functions above own that, and they are the ones the self-test exercises.
async function measure(browser, origin) {
  const context = await browser.newContext();
  const page = await context.newPage();
  const thrown = [];
  page.on('pageerror', (error) => thrown.push(String(error)));
  try {
    await reset(page, origin);
    const atRest = await readPage(page);
    const readings = [];

    // Which tag leads the rail and which sits in the folded tail is the rail's
    // own order, so both are read off the page rather than named here. A rail
    // short enough to fit its lead has no tail and no control to fold it.
    const lead = atRest.rail.find((button) => !button.hidden);
    const tail = atRest.rail.find((button) => button.hidden);
    const typed = tail ?? lead;

    await page.locator(`[data-faq-tag="${lead.tag}"]`).click();
    const selected = await readPage(page);
    readings.push({ what: `the ${lead.label} tag`, select: [lead.tag], shown: selected.shown });

    // A second tag widens the result. Taken from the tags whose questions are
    // not already on screen, so the reading cannot pass by comparing a set with
    // itself.
    const widens = (button) =>
      button.tag !== lead.tag &&
      !questionsCarrying([button.tag], questionTags).every((anchor) => selected.shown.includes(anchor));
    const second = atRest.rail.find(widens) ?? atRest.rail.find((button) => button.tag !== lead.tag);
    if (second.hidden) await page.locator('[data-faq-more]').click();
    await page.locator(`[data-faq-tag="${second.tag}"]`).click();
    readings.push({
      what: `the ${lead.label} and ${second.label} tags`,
      select: [lead.tag, second.tag],
      shown: (await readPage(page)).shown,
    });

    // The contents rail lists the groups the filter left, so following one is
    // navigation inside the result rather than a request to leave it.
    await reset(page, origin);
    await page.locator(`[data-faq-tag="${lead.tag}"]`).click();
    const filtered = await readPage(page);
    const group = filtered.groups.at(-1);
    await page.locator(`starlight-toc a[href="#${group}"]`).click();
    await page.waitForTimeout(300);
    const afterRailLink = await readPage(page);
    const railLink = {
      kept: afterRailLink.count === filtered.count && afterRailLink.shown.length === filtered.shown.length,
      arrived: (await page.evaluate(() => location.hash)) === `#${group}`,
    };

    // A link to a question the filter hid is a link somebody shared, and it has
    // to open the question.
    const hiddenQuestion = Object.keys(questionTags).find((anchor) => !filtered.shown.includes(anchor));
    await page.evaluate((anchor) => {
      location.hash = `#${anchor}`;
    }, hiddenQuestion);
    await page.waitForTimeout(300);
    const deepLink = { opened: (await readPage(page)).shown.includes(hiddenQuestion) };

    // A label the rail shows, typed into the field right under it.
    await reset(page, origin);
    await page.fill('#faq-filter-input', typed.label);
    await page.waitForTimeout(150);
    readings.push({
      what: `${JSON.stringify(typed.label)} typed into the field`,
      select: [typed.tag],
      shown: (await readPage(page)).shown,
      atLeast: true,
    });

    // A tag picked out of the folded tail keeps a control that releases it.
    const fold = { hidden: false, stillFiltering: true };
    if (tail) {
      await reset(page, origin);
      await page.locator('[data-faq-more]').click();
      await page.locator(`[data-faq-tag="${tail.tag}"]`).click();
      await page.locator('[data-faq-more]').click();
      await page.waitForTimeout(150);
      const refolded = (await readPage(page)).rail.find((button) => button.tag === tail.tag);
      fold.hidden = refolded.hidden;
      fold.stillFiltering = refolded.pressed;
    } else {
      console.warn('check-faq-page.mjs: the rail folds no tags, so folding one away was not measured');
    }

    // The chips under an answer name that question's own tags, and select from
    // there: the second way into the same filter.
    await reset(page, origin);
    const anchor = Object.keys(questionTags).find((one) => questionTags[one].length > 1);
    const got = await page.evaluate((id) => {
      const heading = document.getElementById(id);
      const wrapper = heading.closest('.sl-heading-wrapper') ?? heading;
      const found = [];
      for (let node = wrapper.nextElementSibling; node; node = node.nextElementSibling) {
        if (node.querySelector?.('h2[id], h3[id]')) break;
        for (const chip of node.querySelectorAll?.('[data-faq-chip]') ?? []) found.push(chip.dataset.faqChip);
      }
      return found;
    }, anchor);
    const chips = { anchor, got, want: questionTags[anchor] };
    await page.locator(`[data-faq-chip="${questionTags[anchor][0]}"]`).first().click();
    await page.waitForTimeout(200);
    readings.push({
      what: `the ${tagLabels[questionTags[anchor][0]]} chip under an answer`,
      select: [questionTags[anchor][0]],
      shown: (await readPage(page)).shown,
    });

    // The empty state, with and without a tag picked.
    await reset(page, origin);
    await page.fill('#faq-filter-input', 'zzzz');
    await page.waitForTimeout(150);
    const withoutTags = (await readPage(page)).empty;
    await page.locator(`[data-faq-tag="${lead.tag}"]`).click();
    await page.waitForTimeout(150);
    const withTags = (await readPage(page)).empty;

    return {
      rail: atRest.rail,
      readings,
      behavior: { railLink, deepLink, fold, empty: { withoutTags, withTags }, chips },
      thrown,
    };
  } finally {
    await context.close();
  }
}

// Without JavaScript the rail is still markup and every question is still on
// the page, which is the fallback a page meant to be read owes a crawler, a
// screen reader, and a reader on a slow connection.
async function measureWithoutScript(browser, origin) {
  const context = await browser.newContext({ javaScriptEnabled: false });
  const page = await context.newPage();
  try {
    await page.goto(`${origin}faq/`, { waitUntil: 'load' });
    return await page.evaluate(() => ({
      rail: document.querySelectorAll('[data-faq-tag]').length,
      questions: document.querySelectorAll('.sl-markdown-content h3[id]').length,
    }));
  } finally {
    await context.close();
  }
}

async function main() {
  if (process.argv.includes('--selftest')) {
    selftest();
    return;
  }

  const distIndex = process.argv.indexOf('--dist');
  const distRoot = distIndex >= 0 ? process.argv[distIndex + 1] : join(siteRoot, 'dist');
  if (!existsSync(distRoot)) {
    console.error('check-faq-page.mjs: dist/ is missing; build the site first');
    process.exit(1);
  }

  let chromium;
  try {
    ({ chromium } = await import('playwright'));
  } catch {
    const message =
      'check-faq-page.mjs: playwright is not installed (npm i -D playwright && npx playwright install chromium)';
    if (process.env.CI) {
      console.error(message);
      process.exit(1);
    }
    console.warn(`${message}; skipping`);
    return;
  }

  const expected = expectedRail({ questionTags, tagLabels });
  const base = detectBase(distRoot);
  const { server, port } = await startServer(distRoot, base);
  const origin = `http://127.0.0.1:${port}${base}/`;
  const browser = await chromium.launch();
  const problems = [];

  try {
    const quiet = await measureWithoutScript(browser, origin);
    if (quiet.rail !== expected.size) {
      problems.push(`without JavaScript the page renders ${quiet.rail} of ${expected.size} tag buttons`);
    }
    if (quiet.questions !== Object.keys(questionTags).length) {
      problems.push(
        `without JavaScript the page carries ${quiet.questions} of ` +
          `${Object.keys(questionTags).length} questions`,
      );
    }

    // A page with no rail has no controls to drive, and driving it would spend
    // a selector's timeout to report that a selector timed out.
    if (quiet.rail === 0) {
      problems.push('the FAQ page carries no tag rail, so none of its controls were driven');
    } else {
      const measured = await measure(browser, origin);
      problems.push(...railProblems(measured.rail, expected));
      problems.push(...filterProblems(measured.readings, questionTags));
      problems.push(...behaviorProblems(measured.behavior));
      if (measured.thrown.length > 0) problems.push(`the filter threw: ${measured.thrown.join('; ')}`);
    }
  } finally {
    await browser.close();
    server.close();
  }

  if (problems.length > 0) {
    console.error(`check-faq-page.mjs: ${problems.length} problem(s):\n- ${problems.join('\n- ')}`);
    process.exit(1);
  }
  console.log(
    `check-faq-page.mjs: OK (${expected.size} tags on the rail, driven with and without the filter's script)`,
  );
}

await main();
