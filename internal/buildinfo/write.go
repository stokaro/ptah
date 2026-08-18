package buildinfo

import (
	"fmt"
	"io"
)

// Write prints build metadata in Ptah's stable CLI format.
func Write(w io.Writer, info Info) {
	fmt.Fprintf(w, "Version: %s\n", info.Version)
	fmt.Fprintf(w, "Commit: %s\n", info.Commit)
	fmt.Fprintf(w, "Date: %s\n", info.Date)
	fmt.Fprintf(w, "Go: %s\n", info.Go)
	fmt.Fprintf(w, "Platform: %s\n", info.Platform)
}
