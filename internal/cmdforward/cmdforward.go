// Package cmdforward records what a forwarding command runs, so code that reads
// a command tree without executing it can find out.
//
// The compatibility surface builds many of its verbs as forwarders: a cobra
// command that disables flag parsing and hands its words to a native command a
// factory builds when it runs. Until then nothing in the tree links the two, and
// the native command a forwarder runs is not the one in the native tree: the
// factory builds it detached, without the flags an ancestor or the native root
// would add. So the forwarder registers its factory here, and a reader builds
// the command the forwarder would run and reads its flags.
//
// The registry holds a forwarder weakly. A test binary builds hundreds of
// compatibility trees, and an entry that kept its forwarder reachable would keep
// the forwarder's whole tree with it for the life of the process.
package cmdforward

import (
	"runtime"
	"strings"
	"sync"
	"weak"

	"github.com/spf13/cobra"
)

// Annotation is the cobra annotation key a forwarding command carries. Its
// value is the path of the native command it runs, without the program name:
// "migrations down".
const Annotation = "ptah.run/forwards-to"

// RefusedAnnotation is the annotation key naming, comma-separated, the native
// flags a forwarder refuses before it forwards, although its target registers
// them.
const RefusedAnnotation = "ptah.run/forward-refuses"

// factories maps a registered forwarder, held weakly, to the factory that
// builds its target. An entry leaves the map when its forwarder is collected.
var factories sync.Map

// Register records that forwarder runs the native command at path, which
// factory builds each time the forwarder runs.
func Register(forwarder *cobra.Command, path string, factory func() *cobra.Command) {
	annotate(forwarder, Annotation, path)
	key := weak.Make(forwarder)
	factories.Store(key, factory)
	runtime.AddCleanup(forwarder, func(key weak.Pointer[cobra.Command]) { factories.Delete(key) }, key)
}

// Refuse records native flags, by long name, that forwarder refuses before it
// forwards.
func Refuse(forwarder *cobra.Command, flags ...string) {
	if len(flags) > 0 {
		annotate(forwarder, RefusedAnnotation, strings.Join(flags, ","))
	}
}

func annotate(cmd *cobra.Command, key, value string) {
	if cmd.Annotations == nil {
		cmd.Annotations = make(map[string]string)
	}
	cmd.Annotations[key] = value
}

// Target returns the native command path cmd forwards to, or "" when cmd is not
// a forwarder.
func Target(cmd *cobra.Command) string {
	return cmd.Annotations[Annotation]
}

// Refused returns the native flags cmd refuses before it forwards.
func Refused(cmd *cobra.Command) []string {
	if value := cmd.Annotations[RefusedAnnotation]; value != "" {
		return strings.Split(value, ",")
	}
	return nil
}

// Build returns a new instance of the command forwarder runs, built by the
// factory it registered, or nil when forwarder registered none.
func Build(forwarder *cobra.Command) *cobra.Command {
	stored, ok := factories.Load(weak.Make(forwarder))
	if !ok {
		return nil
	}
	factory, ok := stored.(func() *cobra.Command)
	if !ok {
		return nil
	}
	return factory()
}
