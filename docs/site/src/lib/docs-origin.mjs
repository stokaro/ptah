// Where the documentation is published, declared once.
//
// The site is served at the apex of its own domain, so a page's address is the
// origin and the version -- there is no path segment between them. Both halves
// live here and everything else derives from them: `astro.config.mjs` builds
// `site` and `base`, the version generator builds the apex stub, and the gates
// build the addresses they fetch and the pattern they extract with.
//
// Deriving them is the point. The address is reachable from a shell script, a
// workflow and a Markdown page too, and each of those spells it literally
// because none of them can import a module. That is the whole reason a partial
// move is possible at all, and it is what
// `scripts/check-docs-origin.sh` exists to refuse: it holds every
// spelling in the tree against this file. See stokaro/ptah#2884 for what a
// half-moved address published.
//
// This module is plain ESM with no imports, so astro.config.mjs, the .mjs gates
// and the version generator can all read it. Anything that cannot -- a shell
// script, a workflow, a Markdown page -- names the origin literally and is held
// to it by that gate.

// Origin is the scheme and host the documentation is served from, with no
// trailing slash.
export const Origin = 'https://docs.ptah.run';

// BasePath is the site-root-relative prefix every page of one version lives
// under, with leading and trailing slashes.
//
// Just the version: the site is served at the apex of its own domain, so there
// is nothing between the root and it. It is a function rather than a constant
// because the version is chosen per build.
export function BasePath(version) {
	return `/${version}/`;
}

// RootURL is the absolute URL of a file published at the site root, beside the
// version folders rather than inside one -- the install scripts, versions.json.
export function RootURL(name) {
	return `${Origin}/${name}`;
}

// SiteOrigin is the project's own site, and the address a reader is given for
// the installers: `curl -fsSL https://ptah.run/install.sh | sh`. That site's
// deploy fetches docs/site/public/install.sh and install.ps1 from the master
// branch of this repository, so what a reader runs is what this tree holds.
export const SiteOrigin = 'https://ptah.run';

// InstallURL is the advertised address of one installer.
//
// The documentation root keeps serving the same files at RootURL: every command
// published before the move names that address, and the retired host's redirect
// lands there. What changed is which address the pages give a reader.
export function InstallURL(name) {
	return `${SiteOrigin}/${name}`;
}

// PageURL is the absolute URL of one page of one version.
//
// Route is written the way the site's own routes are, without a leading slash
// and with a trailing one.
export function PageURL(version, route) {
	return `${Origin}${BasePath(version)}${route}`;
}
