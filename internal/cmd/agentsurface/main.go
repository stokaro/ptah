// Command agentsurface prints the agent-exposure classification of every Ptah
// verb as markdown, for docs/agent-surface.md.
//
// It walks the command tree in process rather than parsing `--help` output, so
// the inventory is a measurement of the binary this repository builds and not
// of a transcript somebody pasted (stokaro/ptah#1484).
package main

import (
	"flag"
	"fmt"
	"os"

	"ptah.run/cmd/root"
	"ptah.run/internal/agentsurface"
)

func main() {
	safe := flag.Bool("database-safe", false, "print only the verbs that cannot change a database")
	flag.Parse()

	leaves := agentsurface.Walk(root.NewRootCommand())
	if *safe {
		fmt.Print(agentsurface.DatabaseSafeMarkdown(leaves))
		return
	}
	if len(leaves) == 0 {
		fmt.Fprintln(os.Stderr, "agentsurface: the command tree has no runnable verbs")
		os.Exit(1)
	}
	fmt.Print(agentsurface.Markdown(leaves))
}
