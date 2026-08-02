package atlasmigrateimport

import (
	"io/fs"
	"math"
	"path"
	"regexp"
	"strconv"
	"strings"
)

// LegacyFlywayVersion pairs the Atlas version a pre-#982 Ptah build recorded
// for a covered Flyway file with the version this build assigns the same file.
type LegacyFlywayVersion struct {
	// Source is the source file name, relative to the migration directory.
	Source string
	// Legacy is the Atlas version Ptah v0.1.0 through v0.1.2 assigned.
	Legacy int64
	// Current is the Atlas version this build assigns.
	Current int64
}

// legacyFlywayFileRe is the file-name rule the pre-#982 importer selected with.
// It is frozen here on purpose: this package no longer has such a rule, and the
// point of the copy is to reconstruct what an OLDER binary did, so it must not
// follow any future change to the live selection.
var legacyFlywayFileRe = regexp.MustCompile(`(?i)^([vbru])([0-9][0-9._]*)?__(.+)\.sql$`)

const (
	// legacyFlywayMaxComponents and legacyFlywayComponentBase are the pre-#982
	// encoding's shape: major*100^2 + minor*100 + patch over the components with
	// trailing zeros trimmed.
	legacyFlywayMaxComponents = 3
	legacyFlywayComponentBase = 100
)

// LegacyFlywayAtlasVersions reports, for every file this build still covers,
// the Atlas version a pre-#982 Ptah build would have recorded for it and the
// version this build assigns — but only where the two differ.
//
// It exists because #982 changed migration IDENTITY, not only the file names
// `migrate import` writes. A database migrated by Ptah v0.1.0 through v0.1.2
// through `?format=flyway` carries revision rows keyed on the old encoding, and
// to this build those versions match no file at all: every migration reads as
// pending and runs a second time. Where the body is DDL that fails loudly;
// where it is a backfill or a seed it succeeds and silently duplicates the
// data.
//
// The reconstruction is deliberately conservative — it reports a pairing only
// for a file the OLD importer would also have converted, which means:
//
//   - top level only, since the old importer read one directory level;
//   - a V or B prefix in any case, since its regexp was case-insensitive, while
//     R was refused outright and U skipped;
//   - a non-empty version token starting with a digit, and a non-empty
//     description, since its regexp demanded both;
//   - a version the old base-100 encoding could actually represent.
//
// Anything else means the old build either refused the whole directory or never
// executed that file, so no revision row of its making can exist.
func LegacyFlywayAtlasVersions(fsys fs.FS) ([]LegacyFlywayVersion, error) {
	covered, err := flywayCoveredFiles(fsys)
	if err != nil {
		return nil, err
	}
	current, err := flywayConvertedVersions(covered)
	if err != nil {
		// A directory this build cannot convert has no current version to
		// compare against; the conversion error is the caller's real answer.
		return nil, err
	}

	var out []LegacyFlywayVersion
	for i, file := range covered {
		legacy, ok := legacyFlywayAtlasVersion(file)
		if !ok || legacy == current[i] {
			continue
		}
		out = append(out, LegacyFlywayVersion{Source: file.name, Legacy: legacy, Current: current[i]})
	}
	return out, nil
}

// legacyFlywayAtlasVersion reproduces the pre-#982 version assignment for one
// covered file, reporting false when that build would not have converted it.
func legacyFlywayAtlasVersion(file flywaySumFile) (int64, bool) {
	if path.Dir(file.name) != "." {
		return 0, false
	}
	match := legacyFlywayFileRe.FindStringSubmatch(file.name)
	if match == nil {
		return 0, false
	}
	switch strings.ToUpper(match[1]) {
	case "V", "B":
	default:
		// U was skipped and R was a hard refusal, so neither produced a row.
		return 0, false
	}
	components, ok := legacyFlywayComponents(match[2])
	if !ok {
		return 0, false
	}
	return legacyFlywayEncode(components)
}

// legacyFlywayComponents reproduces the old parseFlywayVersion: split on the
// interchangeable '.' and '_' DROPPING empty parts, every part a non-negative
// integer, and not every part zero.
func legacyFlywayComponents(raw string) ([]int, bool) {
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == '.' || r == '_' })
	if len(parts) == 0 {
		return nil, false
	}
	components := make([]int, len(parts))
	allZero := true
	for i, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return nil, false
		}
		if value != 0 {
			allZero = false
		}
		components[i] = value
	}
	if allZero {
		return nil, false
	}
	return components, true
}

// legacyFlywayEncode reproduces the old flywayVersion.atlasVersion: trim
// trailing zero components, then fold into a fixed-width base-100 number.
func legacyFlywayEncode(components []int) (int64, bool) {
	end := len(components)
	for end > 0 && components[end-1] == 0 {
		end--
	}
	canonical := components[:end]
	if len(canonical) == 0 || len(canonical) > legacyFlywayMaxComponents {
		return 0, false
	}
	padded := make([]int, legacyFlywayMaxComponents)
	copy(padded, canonical)
	for i := 1; i < legacyFlywayMaxComponents; i++ {
		if padded[i] >= legacyFlywayComponentBase {
			return 0, false
		}
	}
	var value int64
	for _, component := range padded {
		if value > (math.MaxInt64-int64(component))/legacyFlywayComponentBase {
			return 0, false
		}
		value = value*legacyFlywayComponentBase + int64(component)
	}
	if value <= 0 {
		return 0, false
	}
	return value, true
}
