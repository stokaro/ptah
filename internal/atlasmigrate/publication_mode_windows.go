//go:build windows

package atlasmigrate

import "ptah.run/internal/pathguard"

func platformPublicationMode(*pathguard.OpenedDirectory, string) (publicationMode, error) {
	return publicationModeWriteThroughMove, nil
}
