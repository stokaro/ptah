// Enhance image links while retaining their ordinary href as a no-script and
// modified-click fallback. No image or report is loaded until the reader opens it.
const IMAGE = /\.(?:svg|png|webp|jpe?g|gif|avif)$/i;
const DOCUMENT = /\.html?$/i;
const PADDING = 16;

class GraphicPreview extends HTMLElement {
  connectedCallback() {
    this.events = new AbortController();
    this.dialog = this.querySelector('dialog');
    this.viewport = this.querySelector('[data-preview-viewport]');
    this.canvas = this.querySelector('[data-preview-canvas]');
    this.image = this.querySelector('[data-preview-image]');
    this.frame = this.querySelector('[data-preview-document]');
    this.controls = this.querySelector('[data-preview-controls]');
    this.caption = this.querySelector('[data-preview-caption]');
    this.status = this.querySelector('[data-preview-status]');
    this.error = this.querySelector('[data-preview-error]');
    this.help = this.querySelector('[data-preview-help]');
    this.output = this.querySelector('[data-preview-scale]');
    this.pointers = new Map();
    this.scale = 1;

    this.listen(document, 'click', (event) => this.openLink(event));
    this.listen(this.dialog, 'click', (event) => {
      const action = event.target.closest('[data-preview-control]')?.dataset.previewControl;
      if (action === 'close') this.dialog.close();
      else if (action) this.control(action);
      else if (event.target === this.dialog && this.outsideDown && this.outside(event)) this.dialog.close();
    });
    this.listen(this.dialog, 'pointerdown', (event) => { this.outsideDown = this.outside(event); });
    this.listen(this.dialog, 'close', () => this.finish());
    this.listen(this.dialog, 'keydown', (event) => {
      if (event.key === 'Tab') this.trapTab(event);
      if (this.dialog.dataset.kind !== 'image' || event.ctrlKey || event.metaKey || event.altKey) return;
      const action = { '+': 'in', '=': 'in', '-': 'out', '0': 'actual', f: 'fit', w: 'width' }[event.key];
      if (action) { event.preventDefault(); this.control(action); }
    });
    this.listen(this.image, 'load', () => {
      if (!this.dialog.open || this.dialog.dataset.kind !== 'image' || !this.image.hasAttribute('src') || !this.image.naturalWidth) return;
      this.status.textContent = '';
      this.image.hidden = false;
      this.originalSize ??= { width: this.image.naturalWidth, height: this.image.naturalHeight };
      this.fit('fit');
    });
    this.listen(this.image, 'error', () => {
      if (this.dialog.open && this.dialog.dataset.kind === 'image' && this.image.hasAttribute('src')) this.failed();
    });
    this.listen(this.frame, 'load', () => {
      if (!this.dialog.open || !this.frame.hasAttribute('src')) return;
      this.status.textContent = '';
      // A key pressed inside an iframe does not reach the parent dialog.
      this.frame.contentDocument?.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') { event.preventDefault(); this.dialog.close(); }
        if (event.key === 'Tab') {
          const controls = this.focusable(this.frame.contentDocument);
          const active = this.frame.contentDocument.activeElement;
          if (!controls.length || (event.shiftKey ? active === controls[0] : active === controls.at(-1))) {
            event.preventDefault();
            this.querySelector('[data-preview-control="close"]').focus();
          }
        }
      }, { signal: this.loading.signal });
    });
    this.listen(this.viewport, 'pointerdown', (event) => {
      if (event.button !== 0 || this.image.hidden) return;
      this.viewport.focus({ preventScroll: true });
      this.pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
      this.viewport.setPointerCapture(event.pointerId);
      this.viewport.dataset.dragging = '';
    });
    this.listen(this.viewport, 'pointermove', (event) => this.move(event));
    for (const type of ['pointerup', 'pointercancel', 'lostpointercapture']) {
      this.listen(this.viewport, type, (event) => {
        this.pointers.delete(event.pointerId);
        if (!this.pointers.size) delete this.viewport.dataset.dragging;
      });
    }
    this.listen(this.viewport, 'wheel', (event) => {
      if (!event.ctrlKey || this.image.hidden) return;
      event.preventDefault();
      this.zoom(this.scale * Math.exp(-event.deltaY * 0.002), { x: event.clientX, y: event.clientY });
    }, { passive: false });
    this.listen(this.viewport, 'dblclick', (event) => {
      if (this.scale < 1) this.zoom(1, { x: event.clientX, y: event.clientY });
      else this.fit('fit');
    });
    this.observer = new ResizeObserver(() => {
      cancelAnimationFrame(this.resizing);
      this.resizing = requestAnimationFrame(() => {
        if (!this.dialog.open || this.image.hidden) return;
        if (this.mode === 'fit' || this.mode === 'width') this.fit(this.mode);
        else this.render();
      });
    });
    this.observer.observe(this.viewport);
    this.enhanceImages();
  }

  disconnectedCallback() {
    if (this.dialog.open) { this.dialog.close(); this.finish(); }
    this.events.abort();
    this.loading?.abort();
    this.observer.disconnect();
    cancelAnimationFrame(this.resizing);
  }

  listen(target, type, callback, options = {}) {
    target.addEventListener(type, callback, { ...options, signal: this.events.signal });
  }

  focusable(root) {
    return [...root.querySelectorAll('button:not(:disabled), a[href], iframe, [tabindex="0"], input:not(:disabled), select:not(:disabled), textarea:not(:disabled)')]
      .filter((element) => element.getClientRects().length > 0);
  }

  trapTab(event) {
    const controls = this.focusable(this.dialog);
    const active = document.activeElement;
    if (event.shiftKey ? active === controls[0] : active === controls.at(-1)) {
      event.preventDefault();
      (event.shiftKey ? controls.at(-1) : controls[0]).focus();
    }
  }

  enhanceImages() {
    const article = document.querySelector('.sl-markdown-content');
    if (!article) return;
    for (const image of article.querySelectorAll('img')) {
      if (!image.alt || image.closest('.ptah-output-gallery')) continue;
      let link = image.closest('a');
      if (!link) {
        const source = new URL(image.currentSrc || image.src, location.href);
        if (source.origin !== location.origin || !IMAGE.test(source.pathname)) continue;
        const media = image.closest('picture') ?? image;
        const parent = media.parentElement;
        link = document.createElement('a');
        link.href = image.currentSrc || image.src;
        link.className = 'ptah-graphic-preview-trigger';
        link.dataset.previewInlineImage = '';
        media.replaceWith(link);
        link.appendChild(media);
        // Keep the wide measure a standalone Markdown image had before wrapping.
        if (parent.tagName === 'P' && parent.children.length === 1 && !parent.textContent.trim()) parent.classList.add('ptah-wide-content');
      }
      const url = new URL(link.href, location.href);
      if (!link.hasAttribute('download') && url.origin === location.origin &&
          (IMAGE.test(url.pathname) || (DOCUMENT.test(url.pathname) && link.closest('[data-product-preview]')))) {
        this.mark(link, image.alt);
      }
    }
    for (const link of article.querySelectorAll('[data-preview-action="full-size"]')) {
      const url = new URL(link.href, location.href);
      if (url.origin === location.origin && (IMAGE.test(url.pathname) || DOCUMENT.test(url.pathname))) this.mark(link);
    }
  }

  mark(link, alt) {
    link.dataset.graphicPreview = '';
    link.setAttribute('aria-haspopup', 'dialog');
    link.setAttribute('aria-controls', 'ptah-graphic-preview-dialog');
    if (link.hasAttribute('data-preview-inline-image')) link.setAttribute('aria-label', `Preview: ${alt}`);
    this.dialog.id = 'ptah-graphic-preview-dialog';
  }

  openLink(event) {
    if (event.defaultPrevented || event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
    const link = event.target.closest('a[data-graphic-preview]');
    if (!link || typeof this.dialog.showModal !== 'function') return;
    const figure = link.closest('[data-preview-variant]') ?? link.closest('[data-product-preview]');
    const image = link.querySelector('img') ?? figure?.querySelector('img');
    const url = new URL(link.href, location.href);
    const kind = DOCUMENT.test(url.pathname) ? 'document' : 'image';
    let source = url.href;
    const themedVector = image?.closest('picture') && /\.svg$/i.test(url.pathname);
    if (kind === 'image' && image && (link.hasAttribute('data-preview-inline-image') || url.href === image.src || themedVector)) {
      source = image.currentSrc || image.src;
    }
    event.preventDefault();
    this.opener = link;
    this.position = { left: window.scrollX, top: window.scrollY };
    const root = document.documentElement;
    this.previousOverflow = root.style.overflow;
    this.previousGutter = root.style.scrollbarGutter;
    root.style.scrollbarGutter = 'stable';
    root.style.overflow = 'hidden';
    this.loading = new AbortController();
    this.maximumFit = /\.svg$/i.test(new URL(source).pathname) ? 8 : 1;
    this.originalSize = undefined;
    this.dialog.dataset.kind = kind;
    this.controls.hidden = kind !== 'image';
    this.viewport.hidden = kind !== 'image';
    this.frame.hidden = kind !== 'document';
    this.image.hidden = true;
    this.error.hidden = true;
    this.help.hidden = kind !== 'image';
    this.caption.textContent = image?.alt ?? figure?.querySelector('figcaption')?.textContent?.trim() ?? 'Generated output';
    this.image.alt = image?.alt ?? 'Full-size output';
    this.querySelector('[data-preview-original]').href = url.href;
    this.status.textContent = kind === 'image' ? 'Loading image…' : 'Loading output…';
    this.disable(true);
    this.dialog.showModal();
    if (kind === 'image') this.loadImage(source);
    else this.loadDocument(source);
  }

  async loadImage(source) {
    const signal = this.loading.signal;
    try {
      if (/\.svg$/i.test(new URL(source).pathname)) {
        const response = await fetch(source, { signal });
        if (!response.ok) throw new Error('Image is unavailable');
        const text = await response.text();
        if (signal.aborted) return;
        const svg = new DOMParser().parseFromString(text, 'image/svg+xml').documentElement;
        const box = svg.getAttribute('viewBox')?.trim().split(/[\s,]+/).map(Number);
        const relative = (value) => !value || value === 'auto' || value.includes('%');
        // Responsive SVGs have no intrinsic pixel size. Browsers use a small
        // fallback; their viewBox gives 100% a readable coordinate size instead.
        if (relative(svg.getAttribute('width')) && relative(svg.getAttribute('height')) &&
            box?.length === 4 && box.every(Number.isFinite) && box[2] > 0 && box[3] > 0) {
          this.originalSize = { width: box[2], height: box[3] };
        }
      }
      if (!signal.aborted) this.image.src = source;
    } catch {
      if (!signal.aborted) this.failed();
    }
  }

  async loadDocument(source) {
    const signal = this.loading.signal;
    try {
      const response = await fetch(source, { method: 'HEAD', signal });
      if (!response.ok) throw new Error('Output is unavailable');
      if (!signal.aborted) this.frame.src = source;
    } catch {
      if (!signal.aborted) this.failed();
    }
  }

  failed() {
    this.image.hidden = true;
    this.viewport.hidden = true;
    this.frame.hidden = true;
    this.error.hidden = false;
    this.help.hidden = true;
    this.status.textContent = '';
    this.disable(true);
  }

  disable(disabled) {
    for (const button of this.controls.querySelectorAll('button')) button.disabled = disabled;
  }

  fit(mode) {
    if (!this.originalSize?.width || !this.originalSize?.height || this.image.hidden) return;
    this.mode = mode;
    // Recheck after rendering: a long image can introduce a vertical scrollbar.
    for (let pass = 0; pass < 2; pass += 1) {
      const width = Math.max(1, this.viewport.clientWidth - PADDING * 2) / this.originalSize.width;
      const height = Math.max(1, this.viewport.clientHeight - PADDING * 2) / this.originalSize.height;
      this.scale = mode === 'width' ? Math.min(8, width) : Math.min(this.maximumFit, width, height);
      this.render();
    }
    this.viewport.scrollTo(0, 0);
  }

  render() {
    const width = this.originalSize.width * this.scale;
    const height = this.originalSize.height * this.scale;
    this.image.style.width = `${width}px`;
    this.image.style.height = `${height}px`;
    this.canvas.style.width = `${Math.max(this.viewport.clientWidth, width + PADDING * 2)}px`;
    this.canvas.style.height = `${Math.max(this.viewport.clientHeight, height + PADDING * 2)}px`;
    this.output.value = `${this.scale < 0.1 ? (this.scale * 100).toFixed(1) : Math.round(this.scale * 100)}%`;
    this.disable(false);
    for (const button of this.controls.querySelectorAll('[data-preview-control]')) {
      const selected = { fit: this.mode === 'fit', width: this.mode === 'width', actual: this.mode === 'manual' && this.scale === 1 }[button.dataset.previewControl];
      if (selected !== undefined) button.setAttribute('aria-pressed', String(selected));
    }
  }

  center() {
    const box = this.viewport.getBoundingClientRect();
    return { x: box.left + this.viewport.clientWidth / 2, y: box.top + this.viewport.clientHeight / 2 };
  }

  zoom(scale, anchor = this.center(), nextAnchor = anchor) {
    if (this.image.hidden || !this.originalSize) return;
    const fit = Math.min(1, this.viewport.clientWidth / this.originalSize.width, this.viewport.clientHeight / this.originalSize.height);
    const next = Math.max(Math.min(fit / 2, 0.1), Math.min(8, scale));
    const before = this.image.getBoundingClientRect();
    const point = { x: (anchor.x - before.left) / this.scale, y: (anchor.y - before.top) / this.scale };
    this.mode = 'manual';
    this.scale = next;
    this.render();
    const after = this.image.getBoundingClientRect();
    this.viewport.scrollBy(after.left + point.x * next - nextAnchor.x, after.top + point.y * next - nextAnchor.y);
  }

  control(action) {
    if (this.image.hidden) return;
    if (action === 'fit' || action === 'width') this.fit(action);
    else if (action === 'actual') this.zoom(1);
    else if (action === 'in') this.zoom(this.scale * 1.25);
    else if (action === 'out') this.zoom(this.scale / 1.25);
  }

  move(event) {
    const old = this.pointers.get(event.pointerId);
    if (!old) return;
    const before = [...this.pointers.values()];
    this.pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
    const after = [...this.pointers.values()];
    if (before.length === 1) this.viewport.scrollBy(old.x - event.clientX, old.y - event.clientY);
    else {
      const center = (points) => ({ x: (points[0].x + points[1].x) / 2, y: (points[0].y + points[1].y) / 2 });
      const distance = (points) => Math.hypot(points[0].x - points[1].x, points[0].y - points[1].y);
      if (distance(before) > 0) this.zoom(this.scale * distance(after) / distance(before), center(before), center(after));
    }
  }

  outside(event) {
    const box = this.dialog.getBoundingClientRect();
    return event.clientX < box.left || event.clientX > box.right || event.clientY < box.top || event.clientY > box.bottom;
  }

  finish() {
    this.loading?.abort();
    this.image.removeAttribute('src');
    this.frame.removeAttribute('src');
    this.pointers.clear();
    delete this.viewport.dataset.dragging;
    document.documentElement.style.overflow = this.previousOverflow;
    document.documentElement.style.scrollbarGutter = this.previousGutter;
    this.opener?.focus({ preventScroll: true });
    if (this.position) window.scrollTo({ ...this.position, behavior: 'instant' });
  }
}

if (!customElements.get('ptah-graphic-preview')) customElements.define('ptah-graphic-preview', GraphicPreview);
