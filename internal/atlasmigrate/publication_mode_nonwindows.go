//go:build !windows

package atlasmigrate

import "go.5x5.cz/ptah/internal/pathguard"

func platformPublicationMode(
	d *pathguard.OpenedDirectory,
	stagedName string,
) (publicationMode, error) {
	return detectPublicationModeWithLink(d, stagedName, d.Link)
}
