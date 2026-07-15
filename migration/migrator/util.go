package migrator

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"sort"
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

// Atlas migration file naming pattern: version_description.sql. Atlas
// versioned directories commonly use 14-digit timestamp versions. Requiring
// more than Ptah's 10-digit version width keeps legacy suffixless Ptah-looking
// files from being auto-classified as Atlas migrations.
var atlasFileNameRe = regexp.MustCompile(`^(\d{11,})_(.+)(\.sql)$`)

// MigrationDirFormat selects how a filesystem migration directory is parsed.
type MigrationDirFormat string

const (
	// MigrationDirFormatAuto prefers Ptah files when present and otherwise
	// falls back to Atlas single-file migrations.
	MigrationDirFormatAuto MigrationDirFormat = "auto"
	// MigrationDirFormatPtah parses NNNNNNNNNN_description.(up|down).sql pairs.
	MigrationDirFormatPtah MigrationDirFormat = "ptah"
	// MigrationDirFormatAtlas parses Atlas version_description.sql files.
	MigrationDirFormatAtlas MigrationDirFormat = "atlas"
)

// ParseMigrationDirFormat normalizes a migration directory format value.
func ParseMigrationDirFormat(value string) (MigrationDirFormat, error) {
	switch MigrationDirFormat(strings.ToLower(strings.TrimSpace(value))) {
	case "", MigrationDirFormatAuto:
		return MigrationDirFormatAuto, nil
	case MigrationDirFormatPtah:
		return MigrationDirFormatPtah, nil
	case MigrationDirFormatAtlas:
		return MigrationDirFormatAtlas, nil
	default:
		return "", fmt.Errorf("unknown migration directory format %q: expected auto, ptah, or atlas", value)
	}
}

// MigrationFile represents the parsed components of a migration file name
type MigrationFile struct {
	Path      string
	Version   int64
	Name      string
	Direction string
	Extension string
	Format    MigrationDirFormat
}

// ParseMigrationFileName parses a migration filename into its components
// Expected format: NNNNNNNNNN_description.up.sql or NNNNNNNNNN_description.down.sql
// where NNNNNNNNNN is a 10-digit version number
func ParseMigrationFileName(filename string) (*MigrationFile, error) {
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

	return &MigrationFile{
		Version:   version,
		Name:      name,
		Direction: direction,
		Extension: extension,
		Format:    MigrationDirFormatPtah,
	}, nil
}

// ParseAtlasMigrationFileName parses an Atlas versioned migration file name.
// Expected format: version_description.sql. Atlas does not use paired .up.sql
// and .down.sql files; these files are forward migrations.
func ParseAtlasMigrationFileName(filename string) (*MigrationFile, error) {
	matches := atlasFileNameRe.FindStringSubmatch(filename)
	if matches == nil || len(matches) != 4 {
		return nil, errors.New("invalid Atlas migration file name format")
	}

	if strings.HasSuffix(matches[2], ".up") || strings.HasSuffix(matches[2], ".down") {
		return nil, errors.New("Atlas migration file name must not use Ptah direction suffixes")
	}

	version, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return nil, err
	}
	if version <= 0 {
		return nil, errors.New("migration version must be greater than zero")
	}

	name := strings.ReplaceAll(matches[2], "_", " ")
	name = cases.Title(language.English).String(name)

	return &MigrationFile{
		Version:   version,
		Name:      name,
		Direction: "up",
		Extension: matches[3],
		Format:    MigrationDirFormatAtlas,
	}, nil
}

// ValidateMigrationFileName validates that a filename follows the expected migration pattern
func ValidateMigrationFileName(filename string) bool {
	_, err := ParseMigrationFileName(filename)
	return err == nil
}

// GenerateMigrationFileName generates a migration filename from components
func GenerateMigrationFileName(version int64, description, direction string) string {
	// Convert description to snake_case
	desc := strings.ToLower(description)
	desc = strings.ReplaceAll(desc, " ", "_")
	desc = regexp.MustCompile(`[^a-z0-9_]`).ReplaceAllString(desc, "")

	return fmt.Sprintf("%010d_%s.%s.sql", version, desc, direction)
}

// GetNextMigrationVersion generates the next migration version number
// This is a simple implementation that uses the current timestamp
func GetNextMigrationVersion() int64 {
	return time.Now().Unix()
}

// GroupMigrationFiles groups migration files by version, returning a map
// where each version maps to a struct containing up and down migration files
func GroupMigrationFiles(files []MigrationFile) map[int64]MigrationPair {
	groups := make(map[int64]MigrationPair)

	for _, file := range files {
		pair := groups[file.Version]
		switch file.Direction {
		case "up":
			pair.Up = &file
		case "down":
			pair.Down = &file
		}
		groups[file.Version] = pair
	}

	return groups
}

// MigrationPair represents a pair of up and down migration files for a version
type MigrationPair struct {
	Up   *MigrationFile
	Down *MigrationFile
}

// IsComplete returns true if both up and down migrations are present
func (mp MigrationPair) IsComplete() bool {
	return mp.Up != nil && mp.Down != nil
}

// HasUp returns true if the up migration is present
func (mp MigrationPair) HasUp() bool {
	return mp.Up != nil
}

// HasDown returns true if the down migration is present
func (mp MigrationPair) HasDown() bool {
	return mp.Down != nil
}

// GetVersion returns the version number (assumes both up and down have same version)
func (mp MigrationPair) GetVersion() int64 {
	if mp.Up != nil {
		return mp.Up.Version
	}
	if mp.Down != nil {
		return mp.Down.Version
	}
	return 0
}

// GetDescription returns the description (assumes both up and down have same description)
func (mp MigrationPair) GetDescription() string {
	if mp.Up != nil {
		return mp.Up.Name
	}
	if mp.Down != nil {
		return mp.Down.Name
	}
	return ""
}

// ValidateMigrationPairs validates that all migration pairs are complete
// Returns a list of versions that are missing either up or down migrations
func ValidateMigrationPairs(pairs map[int64]MigrationPair) []int64 {
	var incomplete []int64

	for version, pair := range pairs {
		if !pair.IsComplete() {
			incomplete = append(incomplete, version)
		}
	}

	slicesSort(incomplete)
	return incomplete
}

// FindMigrationGaps finds gaps in migration version sequences
// Returns a list of missing version numbers in the sequence
func FindMigrationGaps(versions []int64) []int64 {
	if len(versions) == 0 {
		return nil
	}

	slicesSort(versions)
	var gaps []int64

	for i := 1; i < len(versions); i++ {
		current := versions[i]
		previous := versions[i-1]

		// Check for gaps (this is a simple implementation)
		// In practice, you might want more sophisticated gap detection
		if current-previous > 1 {
			for v := previous + 1; v < current; v++ {
				gaps = append(gaps, v)
			}
		}
	}

	return gaps
}

// DiscoverMigrationFiles walks fsys and returns files matching the requested
// migration directory format. In auto mode Ptah files win when present, so
// existing directories with stray single .sql files keep their old behavior.
func DiscoverMigrationFiles(fsys fs.FS, format MigrationDirFormat) ([]MigrationFile, error) {
	format, err := normalizeMigrationDirFormat(format)
	if err != nil {
		return nil, err
	}

	var sqlFiles []string
	var ptahFiles []MigrationFile
	var atlasFiles []MigrationFile
	err = fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.EqualFold(path.Ext(p), ".sql") {
			return nil
		}

		sqlFiles = append(sqlFiles, p)
		base := path.Base(p)
		if migrationFile, err := ParseMigrationFileName(base); err == nil {
			migrationFile.Path = p
			ptahFiles = append(ptahFiles, *migrationFile)
		}
		if migrationFile, err := ParseAtlasMigrationFileName(base); err == nil {
			migrationFile.Path = p
			atlasFiles = append(atlasFiles, *migrationFile)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan migrations directory: %w", err)
	}

	files := selectMigrationFiles(format, ptahFiles, atlasFiles)
	if len(files) == 0 && len(sqlFiles) > 0 {
		return nil, fmt.Errorf("no migration files matched format %q; unrecognized SQL files: %s", format, strings.Join(sqlFiles, ", "))
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Version != files[j].Version {
			return files[i].Version < files[j].Version
		}
		return files[i].Path < files[j].Path
	})
	return files, nil
}

func normalizeMigrationDirFormat(format MigrationDirFormat) (MigrationDirFormat, error) {
	if format == "" {
		return MigrationDirFormatAuto, nil
	}
	return ParseMigrationDirFormat(string(format))
}

func selectMigrationFiles(format MigrationDirFormat, ptahFiles, atlasFiles []MigrationFile) []MigrationFile {
	switch format {
	case MigrationDirFormatPtah:
		return ptahFiles
	case MigrationDirFormatAtlas:
		return atlasFiles
	default:
		if len(ptahFiles) > 0 {
			return ptahFiles
		}
		return atlasFiles
	}
}

func slicesSort(values []int64) {
	slices.Sort(values)
}
