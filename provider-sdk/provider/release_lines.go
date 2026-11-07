package provider

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// validateReleaseLines checks the release lines an engine declares. A line
// that claims testing names the image that runs it, and a line the provider
// keeps without running it says why.
func validateReleaseLines(engine *protocol.Engine) error {
	seen := make(map[string]struct{})
	for _, line := range engine.GetReleaseLines() {
		name := line.GetLine()
		if name == "" || strings.TrimSpace(name) != name || strings.IndexFunc(name, unicode.IsSpace) >= 0 || !utf8.ValidString(name) {
			return fmt.Errorf("%w: invalid release line", ErrProtocol)
		}
		if _, found := seen[name]; found {
			return fmt.Errorf("%w: release line %s is declared twice", ErrProtocol, name)
		}
		seen[name] = struct{}{}
		if err := validateReleaseLineSupport(line); err != nil {
			return err
		}
	}
	return nil
}

func validateReleaseLineSupport(line *protocol.ReleaseLine) error {
	name := line.GetLine()
	for _, text := range []string{line.GetLabel(), line.GetNote(), line.GetUnprobed(), line.GetImage()} {
		if !utf8.ValidString(text) {
			return fmt.Errorf("%w: release line %s carries invalid text", ErrProtocol, name)
		}
	}
	if line.GetUnprobed() != "" && strings.TrimSpace(line.GetUnprobed()) == "" {
		return fmt.Errorf("%w: release line %s has a blank reason", ErrProtocol, name)
	}
	switch line.GetSupport() {
	case protocol.ReleaseLine_CERTIFIED, protocol.ReleaseLine_LEGACY_TESTED:
		if line.GetImage() == "" || line.GetUnprobed() != "" {
			return fmt.Errorf("%w: release line %s claims testing without an image that runs it", ErrProtocol, name)
		}
	case protocol.ReleaseLine_BEST_EFFORT:
		if line.GetUnprobed() == "" && line.GetImage() != "" {
			return fmt.Errorf("%w: release line %s has an image and no level that claims it", ErrProtocol, name)
		}
	case protocol.ReleaseLine_KNOWN_INCOMPATIBLE:
		if line.GetImage() != "" || line.GetUnprobed() != "" {
			return fmt.Errorf("%w: release line %s is incompatible and cannot be exercised", ErrProtocol, name)
		}
	default:
		return fmt.Errorf("%w: release line %s has no support level", ErrProtocol, name)
	}
	return nil
}
