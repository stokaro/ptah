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
 * or if it never does; this script replaces it with a button that opens a
 * panel of versions.
 *
 * Each version in the panel is a link, so it can be opened in a new tab. It
 * points at the same page in that version when the page exists there, and at
 * that version's home page when it does not.
 *
 * Plain script, no build step and no imports: this file is published as it is
 * written, and old releases load it through a tag that none of them can update.
 */
(function () {
  'use strict';

  var MOUNT = '[data-ptah-version-picker]';
  var READY = 'data-ptah-version-picker-ready';
  var EDGE = 'edge';
  var mountCount = 0;

  function element(tag, className, text) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  // The page path below the version directory, so a reader on
  // /v0.7.0/versioned/generate/ lands on /v0.8.0/versioned/generate/.
  function pageBelow(versionBase) {
    var path = window.location.pathname;
    if (path.indexOf(versionBase) === 0) path = path.slice(versionBase.length);
    return path.replace(/^\/+/, '');
  }

  // The index lists edge first and then releases newest first. A version the
  // index does not list (an old build, or a preview) is kept at the top, so
  // the panel never claims the page belongs to another version.
  function entries(index, current) {
    var versions = index && Array.isArray(index.versions) ? index.versions : [];
    var list = [];
    var listed = false;
    for (var i = 0; i < versions.length; i += 1) {
      var version = versions[i];
      if (!version || typeof version.slug !== 'string') continue;
      if (version.slug === current) listed = true;
      list.push({
        slug: version.slug,
        label: version.label || version.slug,
        released: typeof version.released === 'string' ? version.released : '',
        latest: Boolean(index.latest) && version.slug === index.latest,
      });
    }
    if (!listed) list.unshift({ slug: current, label: current, released: '', latest: false });
    return list;
  }

  function Picker(mount) {
    this.mount = mount;
    this.current = mount.getAttribute('data-current');
    this.root = mount.getAttribute('data-root');
    this.indexUrl = mount.getAttribute('data-versions');
    this.page = pageBelow(this.root + this.current + '/');
    this.versions = entries(null, this.current);
    this.targets = {};
    this.id = 'ptah-version-picker-' + (mountCount += 1);
    this.build();
    this.load();
  }

  Picker.prototype.build = function () {
    var self = this;
    var trigger = element('button', 'ptah-version-picker__trigger');
    trigger.type = 'button';
    trigger.setAttribute('aria-haspopup', 'dialog');
    trigger.setAttribute('aria-expanded', 'false');
    trigger.setAttribute('aria-controls', this.id);
    trigger.appendChild(element('span', 'sr-only', 'Documentation version: '));
    trigger.appendChild(element('span', 'ptah-version-picker__value', this.current));
    var caret = element('span', 'ptah-version-picker__caret', '▾');
    caret.setAttribute('aria-hidden', 'true');
    trigger.appendChild(caret);
    this.trigger = trigger;

    var panel = element('div', 'ptah-version-picker__panel');
    panel.id = this.id;
    panel.hidden = true;
    panel.tabIndex = -1;
    panel.setAttribute('role', 'dialog');
    panel.setAttribute('aria-label', 'Documentation versions');

    var search = element('div', 'ptah-version-picker__search');
    var icon = element('span', 'ptah-version-picker__search-icon');
    icon.setAttribute('aria-hidden', 'true');
    search.appendChild(icon);
    var input = element('input', 'ptah-version-picker__input');
    input.type = 'search';
    input.placeholder = 'Filter versions';
    input.autocomplete = 'off';
    input.spellcheck = false;
    input.setAttribute('aria-label', 'Filter versions');
    input.setAttribute('aria-controls', this.id + '-list');
    search.appendChild(input);
    panel.appendChild(search);
    this.input = input;

    this.list = element('div', 'ptah-version-picker__list');
    this.list.id = this.id + '-list';
    panel.appendChild(this.list);

    this.empty = element('p', 'ptah-version-picker__empty');
    this.empty.hidden = true;
    panel.appendChild(this.empty);

    this.status = element('p', 'sr-only');
    this.status.setAttribute('aria-live', 'polite');
    panel.appendChild(this.status);

    var hints = element('p', 'ptah-version-picker__hints');
    hints.setAttribute('aria-hidden', 'true');
    [['↑↓', 'move'], ['↵', 'open'], ['esc', 'close']].forEach(function (pair) {
      var hint = element('span');
      hint.appendChild(element('kbd', '', pair[0]));
      hint.appendChild(document.createTextNode(' ' + pair[1]));
      hints.appendChild(hint);
    });
    panel.appendChild(hints);
    this.panel = panel;

    this.mount.textContent = '';
    this.mount.appendChild(trigger);
    document.body.appendChild(panel);
    this.render();

    trigger.addEventListener('click', function () {
      if (self.isOpen()) self.close(false);
      else self.open(true);
    });
    trigger.addEventListener('keydown', function (event) {
      if (event.key === 'ArrowDown' && !self.isOpen()) {
        event.preventDefault();
        self.open(true);
      }
    });
    input.addEventListener('input', function () {
      self.filter();
    });
    panel.addEventListener('keydown', function (event) {
      self.keydown(event);
    });
    panel.addEventListener('click', function (event) {
      self.choose(event);
    });
    document.addEventListener('pointerdown', function (event) {
      if (self.isOpen() && !panel.contains(event.target) && !trigger.contains(event.target)) self.close(false);
    });
    panel.addEventListener('focusout', function (event) {
      var next = event.relatedTarget;
      if (next && !panel.contains(next) && next !== trigger) self.close(false);
    });
    // A phone's keyboard resizes the viewport when the filter takes focus,
    // so a resize moves the panel rather than closing it.
    window.addEventListener('resize', function () {
      if (self.isOpen()) self.place();
    });
  };

  Picker.prototype.load = function () {
    var self = this;
    if (!this.indexUrl || !window.fetch) return;
    window
      .fetch(this.indexUrl, { headers: { Accept: 'application/json' } })
      .then(function (response) {
        return response.ok ? response.json() : null;
      })
      .then(function (index) {
        if (!index) return;
        self.versions = entries(index, self.current);
        self.render();
        if (self.isOpen()) self.resolve();
      })
      .catch(function () {
        // Without the index the panel lists the current version alone, which
        // is true and leads nowhere wrong.
      });
  };

  // The page in `slug`, or that version's home page when the page does not
  // exist there. Asked once per version and remembered.
  Picker.prototype.target = function (slug) {
    var root = this.root + slug + '/';
    var candidate = root + this.page;
    if (this.targets[slug]) return this.targets[slug];
    if (!this.page || !window.fetch) {
      this.targets[slug] = Promise.resolve(root);
    } else {
      this.targets[slug] = window
        .fetch(candidate, { method: 'HEAD' })
        .then(function (response) {
          return response.ok ? candidate : root;
        })
        .catch(function () {
          return root;
        });
    }
    return this.targets[slug];
  };

  Picker.prototype.row = function (version) {
    var link = element('a', 'ptah-version-picker__option');
    link.href = this.root + version.slug + '/' + this.page;
    link.setAttribute('data-version', version.slug);
    var isCurrent = version.slug === this.current;
    if (isCurrent) link.setAttribute('aria-current', 'page');

    var mark = element('span', 'ptah-version-picker__mark', isCurrent ? '✓' : '');
    mark.setAttribute('aria-hidden', 'true');
    link.appendChild(mark);
    link.appendChild(element('span', 'ptah-version-picker__slug', version.label));
    if (version.latest) link.appendChild(element('span', 'ptah-version-picker__badge', 'latest'));
    var meta = version.slug === EDGE ? 'tracks master' : version.released;
    if (meta) {
      var detail = element('span', 'ptah-version-picker__meta', meta);
      if (version.released) {
        var time = element('time', '', version.released);
        time.dateTime = version.released;
        detail.textContent = '';
        detail.appendChild(time);
      }
      link.appendChild(detail);
    }
    link.setAttribute('data-search', [version.slug, version.label, version.released, version.latest ? 'latest' : '', version.slug === EDGE ? 'master' : ''].join(' ').toLowerCase());

    var item = element('div', 'ptah-version-picker__item');
    item.appendChild(link);
    return { item: item, link: link, group: version.slug === EDGE ? 'development' : 'releases' };
  };

  // Each link starts at the same page in its version and is corrected once
  // that version answers. Asked when the panel opens rather than on every page
  // view, since most readers never open it.
  Picker.prototype.resolve = function () {
    var self = this;
    this.links.forEach(function (link) {
      var slug = link.getAttribute('data-version');
      if (slug === self.current) return;
      self.target(slug).then(function (href) {
        link.href = href;
      });
    });
  };

  Picker.prototype.render = function () {
    this.list.textContent = '';
    this.links = [];
    this.groups = [];
    var groups = { development: null, releases: null };
    var titles = { development: 'In development', releases: 'Releases' };
    for (var i = 0; i < this.versions.length; i += 1) {
      var row = this.row(this.versions[i]);
      if (!groups[row.group]) {
        var section = element('div', 'ptah-version-picker__group');
        section.setAttribute('role', 'group');
        var title = element('p', 'ptah-version-picker__group-title', titles[row.group]);
        title.id = this.id + '-' + row.group;
        section.setAttribute('aria-labelledby', title.id);
        section.appendChild(title);
        this.list.appendChild(section);
        groups[row.group] = section;
        this.groups.push(section);
      }
      groups[row.group].appendChild(row.item);
      this.links.push(row.link);
    }
    this.filter();
  };

  Picker.prototype.visible = function () {
    return this.links.filter(function (link) {
      return !link.parentNode.hidden;
    });
  };

  Picker.prototype.filter = function () {
    var query = this.input.value.trim().toLowerCase();
    var shown = 0;
    for (var i = 0; i < this.links.length; i += 1) {
      var match = !query || this.links[i].getAttribute('data-search').indexOf(query) !== -1;
      this.links[i].parentNode.hidden = !match;
      if (match) shown += 1;
    }
    for (var g = 0; g < this.groups.length; g += 1) {
      this.groups[g].hidden = !this.groups[g].querySelector('.ptah-version-picker__item:not([hidden])');
    }
    this.empty.hidden = shown > 0;
    this.empty.textContent = shown > 0 ? '' : 'No version matches “' + this.input.value.trim() + '”';
    this.status.textContent = query ? shown + (shown === 1 ? ' version' : ' versions') : '';
  };

  Picker.prototype.isOpen = function () {
    return !this.panel.hidden;
  };

  // Below the button, aligned with its start, and kept inside the viewport.
  Picker.prototype.place = function () {
    var box = this.trigger.getBoundingClientRect();
    var width = this.panel.offsetWidth;
    var left = Math.min(box.left, window.innerWidth - width - 8);
    this.panel.style.left = Math.max(8, left) + 'px';
    this.panel.style.top = box.bottom + 6 + 'px';
    this.panel.style.setProperty('--ptah-version-picker-room', window.innerHeight - box.bottom - 16 + 'px');
  };

  // `search` puts the caret in the filter. A touch screen gets the panel
  // without it, so opening the list does not also raise the keyboard.
  Picker.prototype.open = function (search) {
    this.panel.hidden = false;
    this.trigger.setAttribute('aria-expanded', 'true');
    this.place();
    this.resolve();
    var current = this.panel.querySelector('[aria-current="page"]');
    if (current) current.scrollIntoView({ block: 'nearest' });
    var coarse = window.matchMedia && window.matchMedia('(pointer: coarse)').matches;
    if (search && !coarse) this.input.focus();
    else this.panel.focus();
  };

  Picker.prototype.close = function (refocus) {
    if (!this.isOpen()) return;
    this.panel.hidden = true;
    this.trigger.setAttribute('aria-expanded', 'false');
    this.input.value = '';
    this.filter();
    if (refocus) this.trigger.focus();
  };

  Picker.prototype.keydown = function (event) {
    var links = this.visible();
    var at = links.indexOf(document.activeElement);
    var inInput = document.activeElement === this.input;
    if (event.key === 'Escape') {
      event.preventDefault();
      if (inInput && this.input.value) {
        this.input.value = '';
        this.filter();
      } else {
        this.close(true);
      }
    } else if (event.key === 'ArrowDown') {
      event.preventDefault();
      if (links.length) links[at < 0 ? 0 : Math.min(at + 1, links.length - 1)].focus();
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      if (at <= 0) this.input.focus();
      else links[at - 1].focus();
    } else if ((event.key === 'Home' || event.key === 'End') && !inInput && links.length) {
      event.preventDefault();
      links[event.key === 'Home' ? 0 : links.length - 1].focus();
    } else if (event.key === 'Enter' && inInput && links.length) {
      event.preventDefault();
      links[0].click();
    } else if (!inInput && event.key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey) {
      // Typing on the list goes to the filter, as it would in a menu.
      this.input.focus();
    }
  };

  // A plain click waits for the version's target to be known, so it never
  // lands on a page that does not exist; a click that opens a new tab or
  // window keeps the link's own behavior.
  Picker.prototype.choose = function (event) {
    var link = event.target.closest ? event.target.closest('a.ptah-version-picker__option') : null;
    if (!link) return;
    if (event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    var slug = link.getAttribute('data-version');
    if (slug === this.current) {
      this.close(true);
      return;
    }
    this.target(slug).then(function (href) {
      window.location.assign(href);
    });
  };

  function mount(node) {
    if (node.hasAttribute(READY)) return;
    if (!node.getAttribute('data-current') || !node.getAttribute('data-root')) return;
    node.setAttribute(READY, '');
    new Picker(node);
  }

  function start() {
    var nodes = document.querySelectorAll(MOUNT);
    for (var i = 0; i < nodes.length; i += 1) mount(nodes[i]);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }
})();
