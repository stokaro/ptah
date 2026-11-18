package migrationfile

import (
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
)

// Discover walks fsys and returns the files matching the requested migration
// directory format, ordered ascending by Version and deterministically ordered
// within a version, so two runs over the same filesystem return the same
// order. An empty format behaves as [DirFormatAuto]. In auto mode Ptah files
// win when present, so a directory holding a Ptah history beside a stray
// single-file .sql still selects the Ptah files.
//
// A directory with no .sql files at all returns an empty result and no error.
// A directory whose candidate .sql files match no name grammar the format
// accepts is an error rather than an empty result, so a misnamed history is
// refused rather than silently run as nothing. That refusal covers only files
// that could have run: an Atlas-governed directory holding nothing outside
// atlas.sum coverage (next paragraph) returns an empty result and no error.
//
// When atlas.sum governs the directory the candidate set is narrowed to exactly
// the files that sum covers; see [atlasSumGovernsSelection] for why that is a
// correctness rule rather than a compatibility preference.
//
// In a directory read in the Ptah format, a file named R__<identity>.sql is a
// repeatable migration, which [DiscoverRepeatables] returns and this function
// leaves out.
func Discover(fsys fs.FS, format DirFormat) ([]File, error) {
	discovered, err := discover(fsys, format, rReadAsAtlas)
	if err != nil {
		return nil, err
	}
	return discovered.files, nil
}

// rReading is what a walk makes of a file named R__<identity>.sql in a
// directory read in the Atlas format.
type rReading uint8

const (
	// rReadAsAtlas reads it as the Atlas format does: the one-time version R.
	rReadAsAtlas rReading = iota
	// rReadAsRepeatable reads it as a repeatable migration, and never as a
	// version.
	rReadAsRepeatable
)

// discovered is what one walk of a migration directory found: the versioned
// files of the format it resolved to, and the repeatable migrations, which a
// directory read in the Ptah format has, and a directory read in the Atlas
// format has when [DiscoverAtlasRepeatables] asks for them.
type discovered struct {
	files       []File
	repeatables []RepeatableFile
}

// classifySQLFiles reads each SQL file name as a Ptah versioned file and, when
// parseAtlas is not nil, as an Atlas file through it.
func classifySQLFiles(sqlFiles []string, parseAtlas func(string) (*File, error)) (ptahFiles, atlasFiles []File) {
	for _, p := range sqlFiles {
		base := path.Base(p)
		if migrationFile, err := ParseFileName(base); err == nil {
			migrationFile.Path = p
			ptahFiles = append(ptahFiles, *migrationFile)
		}
		if parseAtlas == nil {
			continue
		}
		if migrationFile, err := parseAtlas(base); err == nil {
			migrationFile.Path = p
			atlasFiles = append(atlasFiles, *migrationFile)
		}
	}
	return ptahFiles, atlasFiles
}

// discover is the walk behind [Discover], [DiscoverRepeatables] and
// [DiscoverAtlasRepeatables]. One walk answers both, so the versioned files and
// the repeatable files of a directory are always read under the same format and
// the same atlas.sum narrowing. reading says what a file that claims the
// repeatable prefix is under the Atlas format.
func discover(fsys fs.FS, format DirFormat, reading rReading) (discovered, error) {
	atlasRepeatables := reading == rReadAsRepeatable
	format, err := normalizeDirFormat(format)
	if err != nil {
		return discovered{}, err
	}

	var sqlFiles []string
	hasAtlasSum := false
	err = fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}

		// Top-level only, and deliberately so. The predicate that decides
		// which integrity file governs a directory -- migratesum's hasFile --
		// looks at the root, so accepting a nested sub/atlas.sum here would
		// make the two disagree.
		//
		// Measured when they did: a directory holding a top-level ptah pair, a
		// nested pair and sub/atlas.sum applied version 1 only, exit 0, with
		// the success banner and no warning. The author's second migration
		// silently never ran -- the same silent-drop shape this change exists
		// to remove, pointed the other way.
		if p == "atlas.sum" {
			hasAtlasSum = true
			return nil
		}
		if path.Base(p) == "atlas.sum" {
			return nil
		}
		if !strings.EqualFold(path.Ext(p), ".sql") {
			return nil
		}
		sqlFiles = append(sqlFiles, p)
		return nil
	})
	if err != nil {
		return discovered{}, fmt.Errorf("failed to scan migrations directory: %w", err)
	}

	if atlasSumGovernsSelection(format, hasAtlasSum) {
		sqlFiles = retainAtlasSumCovered(sqlFiles)
	}

	// The Atlas reading of a file name: none under the Ptah format, strict
	// under the Atlas format or beside a top-level atlas.sum, and the
	// auto-detection reading otherwise.
	var parseAtlas func(string) (*File, error)
	switch {
	case format == DirFormatPtah:
	case format == DirFormatAtlas || hasAtlasSum:
		parseAtlas = ParseAtlasFileName
	default:
		parseAtlas = ParseAtlasFileNameForAutoDetection
	}
	versioned := sqlFiles
	if atlasRepeatables {
		versioned = withoutRepeatableClaims(sqlFiles)
	}
	ptahFiles, atlasFiles := classifySQLFiles(versioned, parseAtlas)
	files := selectFiles(format, ptahFiles, atlasFiles)
	var repeatables []RepeatableFile
	if atlasRepeatables || readsRepeatables(format, ptahFiles) {
		repeatables, err = collectRepeatables(sqlFiles)
		if err != nil {
			return discovered{}, err
		}
	}
	if len(files) == 0 && len(repeatables) == 0 && len(sqlFiles) > 0 {
		return discovered{}, fmt.Errorf("no migration files matched format %q; unrecognized SQL files: %s", format, strings.Join(sqlFiles, ", "))
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Version != files[j].Version {
			return files[i].Version < files[j].Version
		}
		return files[i].Path < files[j].Path
	})
	return discovered{files: files, repeatables: repeatables}, nil
}

// readsRepeatables reports whether a walk resolved to the Ptah format, the
// only one whose directories hold repeatable migrations: it was asked for by
// name, or auto mode found Ptah versioned files and selected them, as
// [selectFiles] does.
func readsRepeatables(format DirFormat, ptahFiles []File) bool {
	switch format {
	case DirFormatPtah:
		return true
	case DirFormatAuto:
		return len(ptahFiles) > 0
	default:
		return false
	}
}

// atlasSumGovernsSelection reports whether the directory's integrity file is
// atlas.sum, which is also the condition migratesum.ComputeWithFormat uses to
// pick the Atlas hasher. The two must agree: whichever file the sum is written
// into decides which files it covers, and therefore which files may run.
//
// The ptah format is excluded outright rather than by absence of an atlas.sum,
// mirroring that same dispatch — a ptah.sum is computed over
// [Discover] itself, so its coverage follows the selection instead
// of constraining it.
func atlasSumGovernsSelection(format DirFormat, hasAtlasSum bool) bool {
	if format == DirFormatPtah {
		return false
	}
	return format == DirFormatAtlas || hasAtlasSum
}

// retainAtlasSumCovered keeps only the paths atlas.sum can cover: top-level
// files whose name ends in exactly ".sql".
//
// This is what stops Ptah executing SQL no checksum protects (stokaro/ptah#976).
// An Atlas-format sum is built from a shallow, case-sensitive `*.sql` glob
// (migratesum.computeAtlas, atlasmigrateimport.SumFileNames), so a migration in
// a subdirectory or spelled `.SQL` is structurally outside it. Discovering such
// a file anyway made the executed set a strict superset of the covered set: the
// file ran, `migrate validate` still reported the directory clean, and editing
// it afterwards changed what ran without changing any hash. Narrowing the
// candidates here converges the two sets rather than bolting a refusal on top,
// the same remedy stokaro/ptah#982 applied to the imported layouts.
//
// The uppercase half is the same bug seen from the other side: `.SQL` was
// collected and then rejected by the Atlas name parser, so a directory holding
// only `1_a.SQL` was refused as "unrecognized SQL files" while `1_a.sql`
// alongside `2_c.SQL` was silently accepted. One rule replaces both outcomes
// with the one the sum implies.
//
// Narrowing the set silently would only be half the fix, so the command layer
// names the files it declined; it derives them from
// atlasmigrateimport.SumFileNames rather than from a copy of this rule, and a
// test pins the two selections equal so neither can drift into the asymmetry
// this function exists to remove.
func retainAtlasSumCovered(sqlFiles []string) []string {
	covered := make([]string, 0, len(sqlFiles))
	for _, p := range sqlFiles {
		if atlasSumCoversFile(p) {
			covered = append(covered, p)
		}
	}
	return covered
}

// atlasSumCoversFile reports whether name, a slash-separated path relative to
// the migration directory root, belongs to the file set an Atlas-format
// atlas.sum covers: top level only, spelled exactly ".sql".
func atlasSumCoversFile(name string) bool {
	return !strings.Contains(name, "/") && strings.HasSuffix(name, ".sql")
}

func normalizeDirFormat(format DirFormat) (DirFormat, error) {
	if format == "" {
		return DirFormatAuto, nil
	}
	return ParseDirFormat(string(format))
}

func selectFiles(format DirFormat, ptahFiles, atlasFiles []File) []File {
	switch format {
	case DirFormatPtah:
		return ptahFiles
	case DirFormatAtlas:
		return atlasFiles
	default:
		if len(ptahFiles) > 0 {
			return ptahFiles
		}
		return atlasFiles
	}
}
