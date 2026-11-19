package migrationfile

import (
	"fmt"
	"strings"
)

// DuplicateFileError is the refusal of a directory holding more than one file
// for one direction of one version, such as 0000000002_a.up.sql beside
// 0000000002_b.up.sql. Keeping any of them would leave the others unapplied,
// and nothing in the directory says which one the author meant.
//
// [Discover] returns it for a directory read in the Ptah format, where a
// file's version and direction are its migration's identity. A directory read
// in the Atlas format is refused with it when its migrations load, where a
// revision's identity is known.
type DuplicateFileError struct {
	// Format is the format the directory was read in.
	Format DirFormat
	// Direction is "up" or "down".
	Direction string
	// Version is the version the files carry, as [File.RevisionVersion]
	// writes it.
	Version string
	// Files are the files that carry it, as paths relative to the directory,
	// in the order the walk met them.
	Files []string
}

// Error names the version, the direction and every file that carries them.
func (e *DuplicateFileError) Error() string {
	format := ""
	if e.Format == DirFormatAtlas {
		format = "Atlas "
	}
	files := strings.Join(e.Files, " and ")
	if len(e.Files) > 2 {
		files = strings.Join(e.Files[:len(e.Files)-1], ", ") + " and " + e.Files[len(e.Files)-1]
	}
	return fmt.Sprintf("duplicate %s%s migration for version %s: %s", format, e.Direction, e.Version, files)
}

// refuseDuplicatePtahFiles refuses the first version and direction that more
// than one file of a Ptah-format selection carries, naming every file that
// carries it. files are in walk order, so the files are named in the order
// their names sort.
func refuseDuplicatePtahFiles(files []File) error {
	type identity struct {
		version   int64
		direction string
	}
	carriers := make(map[identity][]string, len(files))
	var order []identity
	for _, file := range files {
		if file.Format != DirFormatPtah {
			continue
		}
		key := identity{version: file.Version, direction: file.Direction}
		if _, seen := carriers[key]; !seen {
			order = append(order, key)
		}
		carriers[key] = append(carriers[key], file.Path)
	}
	for _, key := range order {
		if paths := carriers[key]; len(paths) > 1 {
			return &DuplicateFileError{
				Format: DirFormatPtah, Direction: key.direction, Version: fmt.Sprint(key.version), Files: paths,
			}
		}
	}
	return nil
}
