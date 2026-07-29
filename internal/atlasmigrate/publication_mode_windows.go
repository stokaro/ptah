//go:build windows

package atlasmigrate

func platformPublicationMode(string) (publicationMode, error) {
	return publicationModeWriteThroughMove, nil
}
