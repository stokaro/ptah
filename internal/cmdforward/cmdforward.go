// Package cmdforward names what a forwarding command records about the native
// command it runs.
//
// The compatibility surface builds many of its verbs as forwarders: a cobra
// command that disables flag parsing and hands its words to a native command it
// constructs when it runs. Until then nothing in the tree links the two, so
// code that reads a tree without executing it cannot find the flags a forwarded
// verb accepts. The forwarder records the target's path in an annotation, and
// this package is the one place both ends read the key from.
package cmdforward

import "github.com/spf13/cobra"

// Annotation is the cobra annotation key a forwarding command carries. Its
// value is the path of the native command it runs, without the program name:
// "migrations down".
const Annotation = "ptah.run/forwards-to"

// Target returns the native command path cmd forwards to, or "" when cmd is not
// a forwarder.
func Target(cmd *cobra.Command) string {
	return cmd.Annotations[Annotation]
}
