package migrationfile

import (
	"fmt"
	"strings"
)

// AtlasTxtar is the parsed view of an Atlas txtar migration archive: one file
// whose `-- atlas:txtar` first line introduces named sections for the up SQL,
// an optional down SQL, and embedded check files.
type AtlasTxtar struct {
	// MigrationSQL is the migration.sql section: the executable up SQL.
	MigrationSQL string
	// MigrationLineOffset maps line numbers in MigrationSQL back to the
	// source file.
	MigrationLineOffset int
	// DownSQL is the down.sql section; meaningful only when HasDown is true.
	DownSQL string
	// CheckFiles are the checks.sql and checks/*.sql sections, in file order.
	CheckFiles []AtlasTxtarCheckFile
	// HasDown reports whether the archive carries a down.sql section.
	HasDown bool
}

// AtlasTxtarCheckFile is one embedded check file of an Atlas txtar archive.
type AtlasTxtarCheckFile struct {
	// Name is the section name, such as "checks.sql" or "checks/users.sql".
	Name string
	// SQL is the section's content.
	SQL string
}

// The Atlas txtar file vocabulary: the directive that marks an archive and the
// section names its SQL is filed under.
const (
	AtlasTxtarDirective        = "-- atlas:txtar"
	AtlasTxtarMigrationSection = "migration.sql"
	AtlasTxtarDownSection      = "down.sql"
	AtlasTxtarChecksSection    = "checks.sql"
)

// ParseAtlasTxtar reports whether sql is an Atlas txtar archive and, when it
// is, splits it into its sections. A file that carries the directive but not a
// well-formed archive — a misplaced directive, a duplicate section, SQL before
// the first section marker, or a missing migration.sql — returns an error with
// isTxtar still true, so a caller does not fall back to reading the raw bytes
// as plain SQL.
func ParseAtlasTxtar(filename, sql string) (AtlasTxtar, bool, error) {
	isTxtar, misplacedTxtar := classifyAtlasTxtarDirective(sql)
	if misplacedTxtar {
		return AtlasTxtar{}, true, fmt.Errorf(
			"invalid Atlas txtar migration %s: %s must be the first non-empty line",
			filename,
			AtlasTxtarDirective,
		)
	}
	if !isTxtar {
		return AtlasTxtar{}, false, nil
	}

	sections := make(map[string]*strings.Builder)
	var checkSectionNames []string
	var currentSection string
	var migrationLineOffset int
	sawSection := false
	for lineNumber, line := range strings.SplitAfter(sql, "\n") {
		section, isMarker := parseAtlasTxtarSectionMarker(line)
		if !isMarker {
			if !sawSection {
				trimmed := strings.TrimSpace(line)
				if trimmed == "" || strings.HasPrefix(trimmed, "--") {
					continue
				}
				return AtlasTxtar{}, true, fmt.Errorf("invalid Atlas txtar migration %s: SQL appears before the first txtar section", filename)
			}

			if builder := sections[currentSection]; builder != nil {
				builder.WriteString(line)
			}
			continue
		}

		sawSection = true
		currentSection = ""
		if !isAtlasTxtarSQLSection(section) && !isAtlasTxtarCheckSection(section) {
			continue
		}
		if _, exists := sections[section]; exists {
			return AtlasTxtar{}, true, fmt.Errorf("invalid Atlas txtar migration %s: duplicate %s section", filename, section)
		}
		sections[section] = &strings.Builder{}
		currentSection = section
		if section == AtlasTxtarMigrationSection {
			migrationLineOffset = lineNumber + 1
		}
		if isAtlasTxtarCheckSection(section) {
			checkSectionNames = append(checkSectionNames, section)
		}
	}

	migrationSection := sections[AtlasTxtarMigrationSection]
	if migrationSection == nil {
		return AtlasTxtar{}, true, fmt.Errorf("invalid Atlas txtar migration %s: missing migration.sql section", filename)
	}
	parsed := AtlasTxtar{
		MigrationSQL:        migrationSection.String(),
		MigrationLineOffset: migrationLineOffset,
	}
	for _, section := range checkSectionNames {
		parsed.CheckFiles = append(parsed.CheckFiles, AtlasTxtarCheckFile{
			Name: section,
			SQL:  sections[section].String(),
		})
	}
	if downSection := sections[AtlasTxtarDownSection]; downSection != nil {
		parsed.DownSQL = downSection.String()
		parsed.HasDown = true
	}
	return parsed, true, nil
}

func classifyAtlasTxtarDirective(sql string) (isTxtar, misplaced bool) {
	sawContent := false
	sawDirective := false
	sawSection := false
	for line := range strings.SplitSeq(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !sawContent {
			sawContent = true
			if trimmed == AtlasTxtarDirective {
				return true, false
			}
		}
		if trimmed == AtlasTxtarDirective {
			sawDirective = true
			if sawSection {
				return false, true
			}
			continue
		}
		_, isMarker := parseAtlasTxtarSectionMarker(line)
		if isMarker {
			sawSection = true
		}
		if sawDirective && sawSection {
			return false, true
		}
	}
	return false, false
}

func parseAtlasTxtarSectionMarker(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "-- ") || !strings.HasSuffix(trimmed, " --") {
		return "", false
	}
	section := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "-- "), " --"))
	if isAtlasTxtarSQLSection(section) || looksAtlasTxtarFileSection(section) {
		return section, true
	}
	return "", false
}

func isAtlasTxtarSQLSection(section string) bool {
	return section == AtlasTxtarMigrationSection || section == AtlasTxtarDownSection
}

func isAtlasTxtarCheckSection(section string) bool {
	if section == AtlasTxtarChecksSection {
		return true
	}
	name, ok := strings.CutPrefix(section, "checks/")
	return ok && name != "" && strings.HasSuffix(name, ".sql")
}

func looksAtlasTxtarFileSection(section string) bool {
	if len(strings.Fields(section)) != 1 {
		return false
	}
	return strings.ContainsAny(section, `./\`)
}
