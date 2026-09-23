// What a rendered page's version picker reads as, for the checks that render
// one: check-version-picker.mjs on the edge build, and
// check-release-page-actions.mjs on each overlaid release.
//
// The picker is two files at the Pages root and a mount point in the page, so
// a check has to serve those files the way the deploy does. pickerRoute serves
// them from the sources publish-root-assets.mjs declares, which are the bytes
// the deploy copies to the root.
import { readFileSync } from 'node:fs';

import { ROOT_ASSETS, sourcePath } from '../publish-root-assets.mjs';
import { mimeType } from './built-site.mjs';

export const PICKER_SCRIPT = 'version-picker.js';
export const PICKER_STYLESHEET = 'version-picker.css';
export const PICKER_LABEL = 'Select documentation version';

// pickerRoute answers the root paths a version's page reaches for: the two
// picker files, and whatever `extra` maps a path to, such as a version index
// or another version's page. It is the `route` argument of startBuiltSite.
export function pickerRoute(extra = () => undefined) {
  const files = new Map();
  for (const asset of ROOT_ASSETS) {
    if (asset.name === PICKER_SCRIPT || asset.name === PICKER_STYLESHEET) {
      files.set(`/${asset.name}`, sourcePath(asset));
    }
  }
  if (files.size !== 2) {
    throw new Error(`ROOT_ASSETS declares ${files.size} of the picker's two files; the deploy would not publish it`);
  }
  return (path, method) => {
    const source = files.get(path);
    if (source) return { status: 200, body: readFileSync(source), type: mimeType(path) };
    return extra(path, method);
  };
}

// readPicker waits for the script to replace the mount point, then reads the
// mount point and the control. A mount point the script never replaced is read
// as it stands, and mountProblems reports it.
export async function readPicker(page, { hydrate = true } = {}) {
  if (hydrate) {
    await page
      .locator('header [data-ptah-version-picker-ready] select')
      .first()
      .waitFor({ state: 'attached', timeout: 10_000 })
      .catch(() => {});
  }
  return page.evaluate(
    ([script, stylesheet]) => {
      const mounts = [...document.querySelectorAll('header [data-ptah-version-picker]')];
      const mount = mounts[0];
      const select = mount?.querySelector('select');
      const ends = (url, name) => new URL(url, location.href).pathname.endsWith(`/${name}`);
      return {
        mounts: mounts.length,
        current: mount?.getAttribute('data-current') ?? null,
        root: mount?.getAttribute('data-root') ?? null,
        versions: mount?.getAttribute('data-versions') ?? null,
        stylesheets: [...document.querySelectorAll('link[rel="stylesheet"]')]
          .map((link) => link.getAttribute('href'))
          .filter((href) => ends(href, stylesheet)),
        scripts: [...document.querySelectorAll('script[src]')]
          .map((element) => element.getAttribute('src'))
          .filter((src) => ends(src, script)),
        text: mount?.textContent?.trim() ?? null,
        hydrated: Boolean(select),
        selected: select?.value ?? null,
        options: select ? [...select.options].map((option) => option.value) : [],
        appearance: select ? getComputedStyle(select).appearance : null,
        label: select?.labels?.[0]?.textContent?.trim() ?? null,
      };
    },
    [PICKER_SCRIPT, PICKER_STYLESHEET],
  );
}

// mountProblems judges one reading of a page that belongs to `version`, served
// under a Pages root at `root`. `hydrated` says whether the script was allowed
// to run: without it, the mount point shows the version as text.
export function mountProblems(reading, { version, root = '/', hydrated = true }) {
  const problems = [];
  if (reading.mounts !== 1) problems.push(`the header carries ${reading.mounts} version picker mount points, want 1`);
  if (reading.current !== version) problems.push(`the mount point names ${reading.current}, want ${version}`);
  if (reading.root !== root) problems.push(`the mount point names the root ${reading.root}, want ${root}`);
  if (reading.versions !== `${root}versions.json`) {
    problems.push(`the mount point reads the version index at ${reading.versions}, want ${root}versions.json`);
  }
  if (JSON.stringify(reading.stylesheets) !== JSON.stringify([`${root}${PICKER_STYLESHEET}`])) {
    problems.push(`the page loads ${JSON.stringify(reading.stylesheets)} for the picker's stylesheet, want ${root}${PICKER_STYLESHEET}`);
  }
  if (JSON.stringify(reading.scripts) !== JSON.stringify([`${root}${PICKER_SCRIPT}`])) {
    problems.push(`the page loads ${JSON.stringify(reading.scripts)} for the picker, want ${root}${PICKER_SCRIPT}`);
  }
  if (!hydrated) {
    if (reading.hydrated) problems.push('the picker ran with scripting disabled');
    if (reading.text !== version) problems.push(`without scripting the mount point reads ${JSON.stringify(reading.text)}, want ${version}`);
    return problems;
  }
  if (!reading.hydrated) {
    problems.push('the picker did not replace its mount point');
    return problems;
  }
  if (reading.selected !== version) problems.push(`the picker selects ${reading.selected}, want ${version}`);
  if (reading.appearance !== 'none') problems.push(`the picker's select has appearance ${reading.appearance}; the root stylesheet did not apply`);
  if (reading.label !== PICKER_LABEL) problems.push(`the picker's label reads ${JSON.stringify(reading.label)}, want ${JSON.stringify(PICKER_LABEL)}`);
  return problems;
}

// A reading mountProblems accepts, for the self-tests that mutate it.
export function mountFixture(version, { root = '/', hydrated = true } = {}) {
  return {
    mounts: 1,
    current: version,
    root,
    versions: `${root}versions.json`,
    stylesheets: [`${root}${PICKER_STYLESHEET}`],
    scripts: [`${root}${PICKER_SCRIPT}`],
    text: version,
    hydrated,
    selected: hydrated ? version : null,
    options: hydrated ? [version] : [],
    appearance: hydrated ? 'none' : null,
    label: hydrated ? PICKER_LABEL : null,
  };
}
