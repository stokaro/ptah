package provider

import (
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// LinkedReleaseLinesFeature enables ReleaseLine.linked: the version of the
// engine's library the provider executable links, for an engine that runs in
// the provider's process, such as DuckDB or SQLite. A CERTIFIED or
// LEGACY_TESTED line names that version in place of an image, since the
// provider's own tests run the engine it links. It is a two-way feature: the
// provider and the engine list it. A host that does not list it receives the
// line as BEST_EFFORT with a reason that names the linked version. Contract
// 25.30.
const LinkedReleaseLinesFeature = "engine.linked-release-lines"

// validateReleaseLines checks the release lines an engine declares. A line
// that claims testing names what runs it, an image or the linked engine, and
// a line the provider keeps without running it says why.
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
	for _, text := range []string{line.GetLabel(), line.GetNote(), line.GetUnprobed(), line.GetImage(), line.GetLinked()} {
		if !utf8.ValidString(text) {
			return fmt.Errorf("%w: release line %s carries invalid text", ErrProtocol, name)
		}
	}
	if line.GetUnprobed() != "" && strings.TrimSpace(line.GetUnprobed()) == "" {
		return fmt.Errorf("%w: release line %s has a blank reason", ErrProtocol, name)
	}
	if line.GetImage() != "" && line.GetLinked() != "" {
		return fmt.Errorf("%w: release line %s names both an image and a linked engine", ErrProtocol, name)
	}
	evidence := line.GetImage() != "" || line.GetLinked() != ""
	switch line.GetSupport() {
	case protocol.ReleaseLine_CERTIFIED, protocol.ReleaseLine_LEGACY_TESTED:
		if !evidence || line.GetUnprobed() != "" {
			return fmt.Errorf("%w: release line %s claims testing without an image or a linked engine that runs it", ErrProtocol, name)
		}
	case protocol.ReleaseLine_BEST_EFFORT:
		if line.GetUnprobed() == "" && evidence {
			return fmt.Errorf("%w: release line %s has an image or a linked engine and no level that claims it", ErrProtocol, name)
		}
	case protocol.ReleaseLine_KNOWN_INCOMPATIBLE:
		if evidence || line.GetUnprobed() != "" {
			return fmt.Errorf("%w: release line %s is incompatible and cannot be exercised", ErrProtocol, name)
		}
	default:
		return fmt.Errorf("%w: release line %s has no support level", ErrProtocol, name)
	}
	return nil
}

// validateLinkedReleaseLines checks the lines that name a linked engine: they
// need the feature on the engine and the provider, an engine that lists the
// feature names one, one line at most names it, since an executable links
// one version of a library, and the version is the line's: equal to it or
// starting with it and a dot, without white space.
func validateLinkedReleaseLines(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), LinkedReleaseLinesFeature)
	if listed && !slices.Contains(description.GetFeatures(), LinkedReleaseLinesFeature) {
		return fmt.Errorf("%w: linked release lines require the provider's feature", ErrProtocol)
	}
	var linked []string
	for _, line := range engine.GetReleaseLines() {
		version := line.GetLinked()
		if version == "" {
			continue
		}
		if strings.IndexFunc(version, unicode.IsSpace) >= 0 || (version != line.GetLine() && !strings.HasPrefix(version, line.GetLine()+".")) {
			return fmt.Errorf("%w: release line %s links engine version %q, which is not of the line", ErrProtocol, line.GetLine(), version)
		}
		linked = append(linked, line.GetLine())
	}
	if listed != (len(linked) != 0) {
		return fmt.Errorf("%w: an engine names a linked engine exactly when it lists %s", ErrProtocol, LinkedReleaseLinesFeature)
	}
	if len(linked) > 1 {
		return fmt.Errorf("%w: release lines %s each name a linked engine, and an executable links one", ErrProtocol, strings.Join(linked, ", "))
	}
	return nil
}

// negotiateLinkedReleaseLines gives a host that does not list
// LinkedReleaseLinesFeature each line without its linked version: a line that
// claimed testing by it alone becomes BEST_EFFORT, with a reason that names
// the version, since such a host can see no evidence for the claim.
func negotiateLinkedReleaseLines(engine *protocol.Engine, hostFeatures []string) {
	if slices.Contains(hostFeatures, LinkedReleaseLinesFeature) {
		return
	}
	for _, line := range engine.GetReleaseLines() {
		version := line.GetLinked()
		if version == "" {
			continue
		}
		line.Linked = ""
		if line.GetImage() == "" && (line.GetSupport() == protocol.ReleaseLine_CERTIFIED || line.GetSupport() == protocol.ReleaseLine_LEGACY_TESTED) {
			line.Support = protocol.ReleaseLine_BEST_EFFORT
			line.Unprobed = "the provider runs the engine it links, version " + version + ", which this host reads no evidence of"
		}
	}
}
