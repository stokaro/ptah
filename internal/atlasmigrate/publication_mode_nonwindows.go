//go:build !windows

package atlasmigrate

import "ptah.run/internal/pathguard"

func platformPublicationMode(
	d *pathguard.OpenedDirectory,
	stagedName string,
) (publicationMode, error) {
	return detectPublicationModeWithLink(d, stagedName, d.Link)
}
