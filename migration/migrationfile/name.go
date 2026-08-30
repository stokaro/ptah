package migrationfile

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// Migration file naming pattern: NNNNNNNNNN_description.up.sql or NNNNNNNNNN_description.down.sql.
// The dot before the direction is literal: a description merely ending in
// "up"/"down" (cleanup, setup, teardown, ...) is not a migration file.
var fileNameRe = regexp.MustCompile(`^(\d{10})_(.*)\.(down|up)(\.sql)$`)

type atlasParseMode int

const (
	atlasParseExplicit atlasParseMode = iota
	atlasParseAuto
)

// File is the parsed identity of one migration file: what its name — and, for
// files found by [Discover], its path — says about it before any content is
// read.
type File struct {
	// Path is the slash-separated path of the file relative to the walked
	// filesystem root. [Discover] fills it; the name parsers, which see only
	// a file name, leave it empty.
	Path string
	// Version is the numeric version the name carries. An Atlas repeatable
	// file with no numeric prefix (R__name.sql) leaves it 0 and carries its
	// identity in [File.RevisionVersion].
	Version int64
	// Name is the humanized description: underscore (and, in Atlas names,
	// dot) separators become spaces and words are Title-Cased, so
	// "create_users_table" reads back as "Create Users Table". The raw
	// file-name token lives in RevisionDescription for Atlas files.
	Name string
	// Direction is "up" or "down". An Atlas name without a direction suffix
	// is "up".
	Direction string
	// Extension is the ".sql" suffix the name carried, dot included.
	Extension string
	// Format records which naming family parsed the name: [DirFormatPtah] or
	// [DirFormatAtlas], never [DirFormatAuto].
	Format               DirFormat
	atlasRevisionVersion string
	// RevisionDescription is the raw, unnormalized description token an Atlas
	// file name carries, exactly as the revision table records it. Ptah files
	// leave it empty.
	RevisionDescription string
	// Repeatable marks Flyway-style repeatable migrations imported by Atlas.
	// Atlas records them under an opaque string token such as "R" or "2R".
	Repeatable bool
	// IsCheckpoint marks a checkpoint migration whose up body is the full
	// cumulative schema at its version. A fresh database bootstraps from the
	// newest checkpoint instead of replaying pre-checkpoint history; an
	// already-migrated database ignores it. The Ptah marker is spelled
	// NNNNNNNNNN_description.checkpoint.(up|down).sql and is recognized here
	// at name-parse time. Atlas-format checkpoints instead carry a first-line
	// `-- atlas:checkpoint` file directive, which name parsing cannot see;
	// the migrator's FSMigrationProvider detects it from file content when
	// loading, so this
	// field stays false for Atlas files parsed by name alone.
	IsCheckpoint bool
}

// RevisionVersion returns the token this file uses as its revision-table
// identity. Native Ptah files and ordinary Atlas files return the decimal
// numeric version. Atlas repeatable files return Atlas's opaque R-suffixed
// token.
func (f File) RevisionVersion() string {
	if f.atlasRevisionVersion != "" {
		return f.atlasRevisionVersion
	}
	return strconv.FormatInt(f.Version, 10)
}

// ParseFileName parses a migration filename into its components
// Expected format: NNNNNNNNNN_description.up.sql or NNNNNNNNNN_description.down.sql
// where NNNNNNNNNN is a 10-digit version number
func ParseFileName(filename string) (*File, error) {
	// A checkpoint file carries a ".checkpoint" marker between the description
	// and the direction (NNNN_desc.checkpoint.up.sql). Strip it before applying
	// the ordinary name regex so existing up/down parsing is untouched, and
	// remember it on the parsed file.
	isCheckpoint, filename := stripCheckpointMarker(filename)

	matches := fileNameRe.FindStringSubmatch(filename)

	if matches == nil || len(matches) != 5 {
		return nil, errors.New("invalid migration file name format")
	}

	version, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return nil, err
	}

	// Check if the name component is empty
	if matches[2] == "" {
		return nil, errors.New("migration name cannot be empty")
	}

	name := strings.ReplaceAll(matches[2], "_", " ")
	// Capitalize name
	name = cases.Title(language.English).String(name)

	direction := matches[3]
	extension := matches[4]

	return &File{
		Version:      version,
		Name:         name,
		Direction:    direction,
		Extension:    extension,
		Format:       DirFormatPtah,
		IsCheckpoint: isCheckpoint,
	}, nil
}

// stripCheckpointMarker removes the ".checkpoint" marker that a checkpoint
// migration carries immediately before its direction, returning whether the
// marker was present and the filename with the marker removed so the ordinary
// migration name regex can parse it. NNNN_desc.checkpoint.up.sql becomes
// NNNN_desc.up.sql; a filename without the marker is returned unchanged.
func stripCheckpointMarker(filename string) (bool, string) {
	for _, direction := range []string{"up", "down"} {
		marker := ".checkpoint." + direction + ".sql"
		if stem, ok := strings.CutSuffix(filename, marker); ok {
			return true, stem + "." + direction + ".sql"
		}
	}
	return false, filename
}

// ParseAtlasFileName parses an Atlas versioned migration file name.
// Expected format: version.sql, version_description.sql, or numeric migration
// names emitted by Atlas importers such as version_description.up.sql,
// version_description.down.sql, and version.tool.sql.
func ParseAtlasFileName(filename string) (*File, error) {
	return parseAtlasFileName(filename, atlasParseExplicit)
}

// ParseAtlasFileNameForAutoDetection parses Atlas names accepted by
// auto-detection. It is stricter than explicit Atlas parsing so legacy
// suffixless Ptah-looking files keep surfacing as unrecognized in auto mode,
// while accepting short numeric names used by Atlas-imported migration tools.
func ParseAtlasFileNameForAutoDetection(filename string) (*File, error) {
	return parseAtlasFileName(filename, atlasParseAuto)
}

func parseAtlasFileName(filename string, mode atlasParseMode) (*File, error) {
	stem, ok := strings.CutSuffix(filename, ".sql")
	if !ok {
		return nil, errors.New("invalid Atlas migration file name format")
	}

	if migrationFile, ok := parseAtlasRepeatableStem(stem); ok {
		return migrationFile, nil
	}

	direction := "up"
	for _, suffix := range []string{".up", ".down"} {
		if strings.HasSuffix(stem, suffix) {
			direction = strings.TrimPrefix(suffix, ".")
			stem = strings.TrimSuffix(stem, suffix)
			break
		}
	}

	versionDigits, rawName, ok := splitAtlasStem(stem)
	if !ok {
		return nil, errors.New("invalid Atlas migration file name format")
	}
	if mode == atlasParseAuto && len(versionDigits) == 10 && direction == "up" {
		return nil, errors.New("Atlas auto-detection rejects Ptah-width suffixless migration names")
	}

	version, err := strconv.ParseInt(versionDigits, 10, 64)
	if err != nil {
		return nil, err
	}
	if version <= 0 {
		return nil, errors.New("migration version must be greater than zero")
	}

	name := rawName
	if name == "" {
		name = versionDigits
	}
	name = strings.NewReplacer("_", " ", ".", " ").Replace(name)
	name = cases.Title(language.English).String(name)

	return &File{
		Version:              version,
		Name:                 name,
		Direction:            direction,
		Extension:            ".sql",
		Format:               DirFormatAtlas,
		atlasRevisionVersion: strconv.FormatInt(version, 10),
		RevisionDescription:  rawName,
	}, nil
}

func parseAtlasRepeatableStem(stem string) (*File, bool) {
	repeatable, ok := atlasRepeatable(stem)
	if !ok {
		return nil, false
	}
	return &File{
		Version:              repeatable.version,
		Name:                 repeatable.name,
		Direction:            "up",
		Extension:            ".sql",
		Format:               DirFormatAtlas,
		atlasRevisionVersion: repeatable.revisionVersion,
		RevisionDescription:  repeatable.rawName,
		Repeatable:           true,
	}, true
}

type atlasRepeatableParts struct {
	version         int64
	revisionVersion string
	rawName         string
	name            string
}

func atlasRepeatable(stem string) (atlasRepeatableParts, bool) {
	if strings.HasSuffix(stem, ".up") || strings.HasSuffix(stem, ".down") {
		return atlasRepeatableParts{}, false
	}
	switch {
	case strings.HasPrefix(stem, "R__"):
		rawName := strings.TrimPrefix(stem, "R__")
		if rawName == "" {
			return atlasRepeatableParts{}, false
		}
		return atlasRepeatableParts{
			revisionVersion: "R",
			rawName:         rawName,
			name:            titleAtlasName(rawName),
		}, true
	case strings.Contains(stem, "R_"):
		prefix, rawName, _ := strings.Cut(stem, "R_")
		if prefix == "" || !allDigits(prefix) || rawName == "" {
			return atlasRepeatableParts{}, false
		}
		version, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil {
			return atlasRepeatableParts{}, false
		}
		return atlasRepeatableParts{
			version:         version,
			revisionVersion: prefix + "R",
			rawName:         rawName,
			name:            titleAtlasName(rawName),
		}, true
	default:
		return atlasRepeatableParts{}, false
	}
}

func titleAtlasName(name string) string {
	name = strings.NewReplacer("_", " ", ".", " ").Replace(name)
	return cases.Title(language.English).String(name)
}

func allDigits(value string) bool {
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return value != ""
}

func splitAtlasStem(stem string) (versionDigits, rawName string, ok bool) {
	i := 0
	for i < len(stem) && stem[i] >= '0' && stem[i] <= '9' {
		i++
	}
	if i == 0 {
		return "", "", false
	}
	if i == len(stem) {
		return stem, "", true
	}
	if stem[i] != '_' && stem[i] != '.' {
		return "", "", false
	}
	return stem[:i], strings.TrimLeft(stem[i:], "_."), true
}

// FileName produces a Ptah migration file name in the shape
// %010d_description.direction.sql. The description is normalized to the stem
// the name grammar accepts — lowercased, spaces become underscores, and every
// remaining character outside [a-z0-9_] is stripped — so the result
// round-trips through [ParseFileName], which title-cases it back into
// [File.Name], provided the normalized description is non-empty (a
// description with no [a-z0-9_] characters produces a name ParseFileName
// refuses). Direction is used verbatim and must be "up" or "down" for the
// result to parse as a migration file.
func FileName(version int64, description, direction string) string {
	return fmt.Sprintf("%010d_%s.%s.sql", version, normalizeDescription(description), direction)
}

// CheckpointFileName returns the file name for one direction of
// a checkpoint migration: NNNNNNNNNN_description.checkpoint.(up|down).sql. It
// mirrors FileName but carries the checkpoint marker that
// ParseFileName recognizes, so a written checkpoint file round-trips.
func CheckpointFileName(version int64, description, direction string) string {
	return fmt.Sprintf("%010d_%s.checkpoint.%s.sql", version, normalizeDescription(description), direction)
}

// normalizeDescription lower-cases the description, replaces spaces
// with underscores, and strips any remaining non [a-z0-9_] characters, matching
// the snake_case stem the file name regex accepts.
func normalizeDescription(description string) string {
	desc := strings.ToLower(description)
	desc = strings.ReplaceAll(desc, " ", "_")
	return regexp.MustCompile(`[^a-z0-9_]`).ReplaceAllString(desc, "")
}

// NextVersion returns a version number for a new migration: the current time
// in Unix seconds. Two calls within the same second return the same value, so
// a writer creating several migrations in one run must ensure distinct
// versions itself.
func NextVersion() int64 {
	return time.Now().Unix()
}
