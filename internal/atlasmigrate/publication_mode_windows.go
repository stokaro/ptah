//go:build windows

package atlasmigrate

import "go.5x5.cz/ptah/internal/pathguard"

func platformPublicationMode(
	*pathguard.OpenedDirectory,
	string,
) (publicationMode, error) {
	return publicationModeWriteThroughMove, nil
}
