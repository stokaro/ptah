// Command workflowimagepins prints container images that GitHub Actions
// workflows actually start.
package main

import (
	"fmt"
	"os"

	"go.5x5.cz/ptah/internal/workflowimagepins"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: workflowimagepins WORKFLOW_DIR")
		os.Exit(2)
	}
	images, err := workflowimagepins.Directory(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "workflowimagepins: %v\n", err)
		os.Exit(1)
	}
	for _, image := range images {
		fmt.Println(image)
	}
}
