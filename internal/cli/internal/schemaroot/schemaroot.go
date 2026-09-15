// Package schemaroot answers which directory bounds the files a desired state
// may make a command read.
package schemaroot

import (
	"os"
	"path/filepath"
)

// Of returns the project a command is operating in, for bounding the row files a
// declaration names.
//
// A declared row set points at a file, and a desired state is not always one the
// reader wrote, so the path it names is confined rather than followed anywhere.
// The boundary is the project the operator pointed the command at: the first
// --root-dir when there is one, and otherwise the working directory, which is
// what every other relative path on this surface is measured against.
//
// It is the project rather than the directory holding the schema because
// `schema export` writes a row path back out of the directory it exports into,
// and a boundary that refused Ptah's own output would be one no caller could
// keep.
//
// An unreadable working directory yields an empty root, which bounds nothing.
// Refusing to plan because the process cannot name its own directory would
// trade a read nobody asked about for a command that does not run.
func Of(rootDirs []string) string {
	for _, dir := range rootDirs {
		if dir == "" {
			continue
		}
		absolute, err := filepath.Abs(dir)
		if err != nil {
			return ""
		}
		return absolute
	}
	working, err := os.Getwd()
	if err != nil {
		return ""
	}
	return working
}
