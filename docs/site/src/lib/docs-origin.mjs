// Where the documentation is published, declared once.
//
// Two facts used to be spelled in many places and in two shapes: an origin
// (`https://stokaro.github.io`) and a path prefix (`/ptah`), the second because
// the site was a GitHub project page and every URL it built carried the
// repository name. It is served from its own domain now, so the prefix is gone
// -- and the move left the constants behind, because there was no one place to
// change. `docs.ptah.run/` redirected to `/ptah/v0.3.0/`, which 404s, and the
// page a reader reached by hand asked for its stylesheets under `/ptah/`
// (stokaro/ptah#2884).
//
// So the origin lives here and everything else is derived from it. A gate
// refuses the old host anywhere in the tree, because 96 occurrences is what
// made a partial move possible in the first place: see
// scripts/check-docs-origin.sh.
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

// PageURL is the absolute URL of one page of one version.
//
// Route is written the way the site's own routes are, without a leading slash
// and with a trailing one.
export function PageURL(version, route) {
	return `${Origin}${BasePath(version)}${route}`;
}
