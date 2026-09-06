package docs_test

import (
	"fmt"
	"io/fs"

	"ptah.run/docs"
)

// ExampleFS reads one embedded guide with the standard filesystem helpers. FS
// is a plain embed.FS, so anything that consumes an fs.FS — fs.ReadFile here,
// or fs.WalkDir, fs.Glob, http.FS — works on it directly, offline, with no
// operator configuration. The paths inside it are the repository's own docs/
// layout and move when the documentation is reorganized, so the example prints
// a structural fact rather than content bytes or a path listing.
func ExampleFS() {
	page, err := fs.ReadFile(docs.FS, "capabilities.md")
	if err != nil {
		fmt.Println("read:", err)
		return
	}

	fmt.Println("capabilities.md is embedded and non-empty:", len(page) > 0)

	// Output:
	// capabilities.md is embedded and non-empty: true
}
