/* The documentation version picker.
 *
 * One copy of this file is served at the Pages root, beside versions.json, and
 * every documentation version loads it from there. A version carries only a
 * mount point (src/components/VersionPicker.astro), and the release UI overlay
 * puts that mount point into every released version it rebuilds. So a change
 * here reaches edge and every overlaid release on the next deploy, without
 * rebuilding anything from its tag.
 *
 * The mount point is
 *
 *   <div class="ptah-version-picker" data-ptah-version-picker
 *        data-current="v0.7.0" data-root="/" data-versions="/versions.json">
 *     <span class="ptah-version-picker__current">v0.7.0</span>
 *   </div>
 *
 * data-current is the version the page belongs to, data-root is the Pages root
 * path every version directory sits under, and data-versions is the version
 * index to read. The text inside is what a reader sees before this script runs,
 * or if it never does; this script replaces it with the control.
 *
 * Plain script, no build step and no imports: this file is published as it is
 * written, and old releases load it through a tag that none of them can update.
 */
(function () {
  'use strict';

  var MOUNT = '[data-ptah-version-picker]';
  var READY = 'data-ptah-version-picker-ready';
  var mountCount = 0;

  // The page path below the version directory, so a reader on
  // /v0.7.0/versioned/generate/ lands on /v0.8.0/versioned/generate/.
  function pageBelow(versionBase) {
    var path = window.location.pathname;
    if (path.indexOf(versionBase) === 0) path = path.slice(versionBase.length);
    return path.replace(/^\/+/, '');
  }

  function option(slug, label, selected) {
    var element = document.createElement('option');
    element.value = slug;
    element.textContent = label || slug;
    element.selected = selected;
    return element;
  }

  // The index lists edge first and then releases newest first. The current
  // version is selected; a version the index does not list (an old build, or a
  // preview) is kept at the top so the control never claims to be elsewhere.
  function fill(select, versions, current) {
    select.textContent = '';
    var listed = false;
    for (var index = 0; index < versions.length; index += 1) {
      var version = versions[index];
      if (!version || typeof version.slug !== 'string') continue;
      if (version.slug === current) listed = true;
      select.appendChild(option(version.slug, version.label, version.slug === current));
    }
    if (!listed) select.insertBefore(option(current, current, true), select.firstChild);
  }

  function load(select, url, current) {
    if (!url || !window.fetch) return;
    window
      .fetch(url, { headers: { Accept: 'application/json' } })
      .then(function (response) {
        return response.ok ? response.json() : null;
      })
      .then(function (index) {
        var versions = index && Array.isArray(index.versions) ? index.versions : [];
        if (versions.length > 0) fill(select, versions, current);
      })
      .catch(function () {
        // Without the index the control keeps the current version alone,
        // which is true and does nothing wrong.
      });
  }

  // The same page in the target version when it exists there, and that
  // version's home page when it does not.
  function go(root, target, page) {
    var targetBase = root + target + '/';
    var candidate = targetBase + page;
    if (!window.fetch) {
      window.location.assign(targetBase);
      return;
    }
    window
      .fetch(candidate, { method: 'HEAD' })
      .then(function (response) {
        return response.ok ? candidate : targetBase;
      })
      .catch(function () {
        return targetBase;
      })
      .then(function (destination) {
        window.location.assign(destination);
      });
  }

  function mount(element) {
    if (element.hasAttribute(READY)) return;
    var current = element.getAttribute('data-current');
    var root = element.getAttribute('data-root');
    if (!current || !root) return;
    element.setAttribute(READY, '');
    mountCount += 1;

    var id = 'ptah-version-picker-' + mountCount;
    var label = document.createElement('label');
    label.className = 'sr-only';
    label.htmlFor = id;
    label.textContent = 'Select documentation version';

    var select = document.createElement('select');
    select.id = id;
    select.appendChild(option(current, current, true));

    var caret = document.createElement('span');
    caret.className = 'ptah-version-picker__caret';
    caret.setAttribute('aria-hidden', 'true');
    caret.textContent = '▾';

    element.textContent = '';
    element.appendChild(label);
    element.appendChild(select);
    element.appendChild(caret);

    var versionBase = root + current + '/';
    select.addEventListener('change', function () {
      var target = select.value;
      if (!target || target === current) return;
      go(root, target, pageBelow(versionBase));
    });

    load(select, element.getAttribute('data-versions'), current);
  }

  function start() {
    var elements = document.querySelectorAll(MOUNT);
    for (var index = 0; index < elements.length; index += 1) mount(elements[index]);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }
})();
