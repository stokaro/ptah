package platform

import (
	"fmt"
	"sync"
	"sync/atomic"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// process is the registry every package-level function of this package reads.
// It is a package-level variable rather than something an init function or a
// caller installs: Go initializes it before any importing package, so a
// package-level variable elsewhere that normalizes a dialect name reads the
// built-in descriptors, whatever the initialization order of the packages
// that do not import each other.
var process = newProcessRegistry(builtindialects.Descriptors())

// processRegistry serializes registrations and hands readers a snapshot with
// one atomic load.
type processRegistry struct {
	mu      sync.Mutex
	current atomic.Pointer[snapshot]
}

// snapshot is one registry and a copy of each of its descriptors, keyed by
// name, for this package's own predicates. [Lookup] copies the whole
// descriptor on every call, and a predicate such as [IsPostgresFamily] runs
// once per column and index on the render and plan paths, so the predicates
// read these copies instead. Nothing modifies them, and a registration makes
// a new snapshot.
type snapshot struct {
	registry *descriptor.Registry
	byName   map[string]descriptor.Dialect
}

func newSnapshot(registry *descriptor.Registry) *snapshot {
	dialects := registry.Dialects()
	byName := make(map[string]descriptor.Dialect, len(dialects))
	for _, dialect := range dialects {
		byName[dialect.Name] = dialect
	}
	return &snapshot{registry: registry, byName: byName}
}

// described returns the descriptor of the dialect a spelling selects, folded
// as [NormalizeDialect] folds it, from the current snapshot. The caller only
// reads it.
func described(dialect string) (descriptor.Dialect, bool) {
	current := process.current.Load()
	found, ok := current.byName[current.registry.Normalize(dialect)]
	return found, ok
}

func newProcessRegistry(seed []descriptor.Dialect) *processRegistry {
	registry, err := descriptor.NewRegistry(seed...)
	if err != nil {
		// The seed is data compiled into the binary, and its own tests build
		// it; an invalid one is a defect in this module, not an input.
		panic(fmt.Sprintf("platform: the built-in dialect descriptors are invalid: %v", err))
	}
	process := &processRegistry{}
	process.current.Store(newSnapshot(registry))
	return process
}

// Dialects returns the process registry: the built-in dialects, followed by
// every dialect [Register] has added, in the order they were added. The
// registry is immutable; a later Register produces a new one and leaves this
// one as it was.
func Dialects() *descriptor.Registry {
	return process.current.Load().registry
}

// Lookup returns a copy of the descriptor of the dialect a spelling selects,
// folded as [NormalizeDialect] folds it.
func Lookup(dialect string) (descriptor.Dialect, bool) {
	return Dialects().Lookup(dialect)
}

// Register adds the descriptors of dialects Ptah does not ship to the process
// registry, which every package-level function of this package reads. It is
// for the composition root of a program that selects such a dialect, and it
// runs before that program asks about the dialect: no built-in code knows
// the name, so nothing reads it earlier.
//
// Registration only adds. A descriptor equal to one already registered is
// accepted and changes nothing, so registering a provider's descriptors twice
// is safe. One that claims a spelling a different descriptor owns fails with
// [descriptor.ErrConflict], and a malformed one with
// [descriptor.ErrInvalidDescriptor]; on failure nothing is added. A spelling
// therefore keeps the answer it was first given for the life of the process.
// Register is safe for concurrent use, with itself and with every reader.
//
// A test must not call Register to describe a dialect it invents: the
// registration lasts for the rest of the test binary. Build a registry with
// [descriptor.NewRegistry] and call the function that takes a descriptor
// instead.
func Register(dialects ...descriptor.Dialect) error {
	process.mu.Lock()
	defer process.mu.Unlock()
	next, err := process.current.Load().registry.With(dialects...)
	if err != nil {
		return fmt.Errorf("register dialects: %w", err)
	}
	process.current.Store(newSnapshot(next))
	return nil
}
