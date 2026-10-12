package main

import (
	"fmt"
	"io"
	"log"
	"os"
)

func main() {
	if err := run(os.Stdout, os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

// run writes the demo the first argument names to w, and the usage when there
// is no argument or it names no demo.
func run(w io.Writer, args []string) error {
	fmt.Fprintln(w, "SQL AST Generator Demo")
	fmt.Fprintln(w, "=====================")
	fmt.Fprintln(w)

	if len(args) < 1 {
		printUsage(w)
		return nil
	}

	switch args[0] {
	case "demo":
		return DemonstrateASTApproach(w)
	case "compare":
		CompareApproaches(w)
		return nil
	case "advanced":
		return ShowAdvancedFeatures(w)
	default:
		printUsage(w)
		return nil
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: go run main.go [command]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Available commands:")
	fmt.Fprintln(w, "  demo      - Show basic AST-based SQL generation examples")
	fmt.Fprintln(w, "  compare   - Compare old string-based vs new AST-based approaches")
	fmt.Fprintln(w, "  advanced  - Show advanced AST features")
}
