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
func Discover(fsys fs.FS, format DirFormat) ([]File, error) {
	format, err := normalizeDirFormat(format)
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("failed to scan migrations directory: %w", err)
	}

	if atlasSumGovernsSelection(format, hasAtlasSum) {
		sqlFiles = retainAtlasSumCovered(sqlFiles)
	}

	var ptahFiles []File
	var atlasFiles []File
	for _, p := range sqlFiles {
		base := path.Base(p)
		if migrationFile, err := ParseFileName(base); err == nil {
			migrationFile.Path = p
			ptahFiles = append(ptahFiles, *migrationFile)
		}
		if format == DirFormatPtah {
			continue
		}
		if format == DirFormatAtlas || hasAtlasSum {
			if migrationFile, err := ParseAtlasFileName(base); err == nil {
				migrationFile.Path = p
				atlasFiles = append(atlasFiles, *migrationFile)
			}
			continue
		}
		if migrationFile, err := ParseAtlasFileNameForAutoDetection(base); err == nil {
			migrationFile.Path = p
			atlasFiles = append(atlasFiles, *migrationFile)
		}
	}

	files := selectFiles(format, ptahFiles, atlasFiles)
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
