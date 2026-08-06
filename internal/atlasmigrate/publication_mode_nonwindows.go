//go:build !windows

package atlasmigrate

import "go.5x5.cz/ptah/internal/pathguard"

func platformPublicationMode(
	root *pathguard.OpenedDirectory,
	stagedName string,
) (publicationMode, error) {
	return detectPublicationModeWithLink(root, stagedName, root.Link)
}
