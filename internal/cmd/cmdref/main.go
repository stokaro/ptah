// Command cmdref prints one command-reference rendering for a contributor who
// wants to see it without running the documentation sync.
//
// The renderings themselves live in internal/cmdrefviews, which the sync
// imports too, so the two cannot answer differently (stokaro/ptah#2510).
package main

import (
	"fmt"
	"os"

	"go.5x5.cz/ptah/internal/cmdrefviews"
)

const usage = `usage: cmdref native|compat|strict|flags

  native   the ptah command table
  compat   the ptah-compat command table
  strict   what PTAH_ATLAS_STRICT_COMPAT=1 removes from ptah-compat
  flags    the whole flag reference page, both binaries
`

func main() {
	if len(os.Args) != 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	rendered, err := cmdrefviews.Render(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n%s", err, usage)
		os.Exit(1)
	}
	fmt.Print(rendered)
}
