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
	current atomic.Pointer[descriptor.Registry]
}

func newProcessRegistry(seed []descriptor.Dialect) *processRegistry {
	registry, err := descriptor.NewRegistry(seed...)
	if err != nil {
		// The seed is data compiled into the binary, and its own tests build
		// it; an invalid one is a defect in this module, not an input.
		panic(fmt.Sprintf("platform: the built-in dialect descriptors are invalid: %v", err))
	}
	process := &processRegistry{}
	process.current.Store(registry)
	return process
}

// Dialects returns the process registry: the built-in dialects, followed by
// every dialect [Register] has added, in the order they were added. The
// registry is immutable; a later Register produces a new one and leaves this
// one as it was.
func Dialects() *descriptor.Registry {
	return process.current.Load()
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
	next, err := process.current.Load().With(dialects...)
	if err != nil {
		return fmt.Errorf("register dialects: %w", err)
	}
	process.current.Store(next)
	return nil
}
