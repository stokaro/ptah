package importer

import (
	"fmt"
	"io/fs"
	"regexp"
	"strconv"
)

// golangMigrateFileRE matches golang-migrate's <version>_<name>.<up|down>.sql
// file names. Version is an integer counter or a timestamp; both are valid Ptah
// versions.
var golangMigrateFileRE = regexp.MustCompile(`^(\d+)_(.+)\.(up|down)\.sql$`)

// golangMigrateParser imports directories written by golang-migrate, whose file
// layout is closest to Ptah's own — mostly filename mapping and up/down pairing.
type golangMigrateParser struct{}

func (golangMigrateParser) Name() string { return "golang-migrate" }

func (golangMigrateParser) Detect(fsys fs.FS) bool {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if match := golangMigrateFileRE.FindStringSubmatch(entry.Name()); match != nil && match[3] == "up" {
			return true
		}
	}
	return false
}

func (golangMigrateParser) Parse(fsys fs.FS) ([]SourceMigration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read source directory: %w", err)
	}

	byVersion := make(map[int64]*SourceMigration)
	hasUp := make(map[int64]bool)
	hasDown := make(map[int64]bool)
	var order []int64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		match := golangMigrateFileRE.FindStringSubmatch(entry.Name())
		if match == nil {
			continue // ignore non-migration files (README, .gitkeep, ...)
		}
		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid golang-migrate version in %q: %w", entry.Name(), err)
		}
		name, direction := match[2], match[3]
		content, err := fs.ReadFile(fsys, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", entry.Name(), err)
		}

		migration := byVersion[version]
		if migration == nil {
			migration = &SourceMigration{Version: version, Name: name}
			byVersion[version] = migration
			order = append(order, version)
		}
		// golang-migrate pairs up/down by an identical <version>_<name> prefix; a
		// name mismatch across the two directions is a malformed pair.
		if migration.Name != name {
			return nil, fmt.Errorf("golang-migrate version %d has mismatched names %q and %q", version, migration.Name, name)
		}
		// Two files whose numeric versions are equal but whose text differs (for
		// example 000001_x and 1_x) must not silently merge with last-writer-wins.
		switch direction {
		case "up":
			if hasUp[version] {
				return nil, fmt.Errorf("golang-migrate version %d has two up files (differently formatted version numbers)", version)
			}
			migration.UpSQL = string(content)
			hasUp[version] = true
		case "down":
			if hasDown[version] {
				return nil, fmt.Errorf("golang-migrate version %d has two down files (differently formatted version numbers)", version)
			}
			migration.DownSQL = string(content)
			hasDown[version] = true
		}
	}

	if len(byVersion) == 0 {
		return nil, fmt.Errorf("no golang-migrate migration files (<version>_<name>.up.sql) found")
	}

	migrations := make([]SourceMigration, 0, len(order))
	for _, version := range order {
		if !hasUp[version] {
			return nil, fmt.Errorf("golang-migrate version %d (%q) has a down file but no .up.sql", version, byVersion[version].Name)
		}
		migrations = append(migrations, *byVersion[version])
	}
	return migrations, nil
}
