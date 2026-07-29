//go:build !windows

package atlasmigrate

import "os"

func platformPublicationMode(stagedPath string) (publicationMode, error) {
	return detectPublicationModeWithLink(stagedPath, os.Link)
}
