// Package schemaroot answers which directories bound the row files a desired
// state may make a command read, and reads a declaration's file through them.
package schemaroot

import (
	"cmp"
	"errors"
	"os"
	"path/filepath"

	"ptah.run/core/manageddata"
	"ptah.run/core/schemamodel"
)

// Of returns the projects a command is operating in, for bounding the row files
// a declaration names.
//
// A declared row set points at a file, and a desired state is not always one the
// reader wrote, so the path it names is confined rather than followed anywhere.
// The boundary is what the operator pointed the command at: every --root-dir when
// there is one, since each of them is merged into the composite schema, and
// otherwise the working directory, which is what every other relative path on
// this surface is measured against.
//
// It is the project rather than the directory holding the schema because
// `schema export` writes a row path back out of the directory it exports into,
// and a boundary that refused Ptah's own output would be one no caller could
// keep.
//
// A root that cannot be made absolute is left out, because the process cannot
// name its own directory. With no root left a declared row file is refused where
// it is read, and a command whose schema declares no rows still runs.
func Of(rootDirs []string) []string {
	roots := make([]string, 0, len(rootDirs))
	for _, dir := range rootDirs {
		if dir == "" {
			continue
		}
		if absolute, err := filepath.Abs(dir); err == nil {
			roots = append(roots, absolute)
		}
	}
	if len(roots) > 0 {
		return roots
	}
	working, err := os.Getwd()
	if err != nil {
		return nil
	}
	return []string{working}
}

// Load reads the row file md names with load, through the first of roots that
// holds it. A file every root refuses answers with the refusal of the first, and
// no roots at all is a refusal too, since a row file is never read unbounded.
//
// It takes the loader rather than choosing the root itself, so the question of
// where a declaration's path leads has one answer, in the loader that reads it.
func Load[T any](
	roots []string,
	md schemamodel.ManagedData,
	load func(rootDir string, md schemamodel.ManagedData) (T, error),
) (T, error) {
	if len(roots) == 0 {
		return load("", md)
	}
	var refusal error
	for _, root := range roots {
		rows, err := load(root, md)
		if !errors.Is(err, manageddata.ErrOutsideRoot) {
			return rows, err
		}
		refusal = cmp.Or(refusal, err)
	}
	var none T
	return none, refusal
}
