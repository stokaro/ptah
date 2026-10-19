// Package descriptortable keeps an answer derived from each dialect
// descriptor of the process registry, for a package that asks about a
// dialect on a path where copying its descriptor would cost more than the
// work it decides.
//
// platform.Lookup copies the whole descriptor on every call. A [Table]
// derives its answer once per descriptor, and an [Index] one answer from all
// of them, and each serves it until a registration replaces the process
// registry, which makes the answers for the older registry stale rather than
// wrong; the next call derives them anew.
package descriptortable

import (
	"sync/atomic"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
)

// Table holds the answers derive gives for the descriptors of one registry.
// The zero value is not usable; build one with [New]. A Table is safe for
// concurrent use.
type Table[T any] struct {
	derive  func(descriptor.Dialect) T
	current atomic.Pointer[answers[T]]
}

type answers[T any] struct {
	registry *descriptor.Registry
	byName   map[string]T
	unknown  T
}

// New returns a table that derives each answer with derive. derive reads
// only the descriptor it is given, and the answers it returns are shared by
// every caller, so a caller must not modify one.
func New[T any](derive func(descriptor.Dialect) T) *Table[T] {
	return &Table[T]{derive: derive}
}

// Get returns the answer for the dialect a spelling selects, folded as
// platform.NormalizeDialect folds it. An empty or unknown dialect gets the
// answer derive gives for the zero descriptor.
func (t *Table[T]) Get(dialect string) T {
	registry := platform.Dialects()
	current := t.current.Load()
	if current == nil || current.registry != registry {
		current = t.build(registry)
		t.current.Store(current)
	}
	if answer, found := current.byName[registry.Normalize(dialect)]; found {
		return answer
	}
	return current.unknown
}

func (t *Table[T]) build(registry *descriptor.Registry) *answers[T] {
	dialects := registry.Dialects()
	byName := make(map[string]T, len(dialects))
	for _, described := range dialects {
		byName[described.Name] = t.derive(described)
	}
	return &answers[T]{registry: registry, byName: byName, unknown: t.derive(descriptor.Dialect{})}
}

// Index holds one answer derived from every descriptor of one registry, for a
// question about the registry as a whole, such as which dialect claims a
// name. The zero value is not usable; build one with [NewIndex]. An Index is
// safe for concurrent use.
type Index[T any] struct {
	derive  func([]descriptor.Dialect) T
	current atomic.Pointer[indexAnswer[T]]
}

type indexAnswer[T any] struct {
	registry *descriptor.Registry
	answer   T
}

// NewIndex returns an index that derives its answer with derive, which is
// given every descriptor of the registry in the order they were added. The
// answer is shared by every caller, so a caller must not modify it.
func NewIndex[T any](derive func(dialects []descriptor.Dialect) T) *Index[T] {
	return &Index[T]{derive: derive}
}

// Get returns the answer for the process registry, derived once per
// registry.
func (i *Index[T]) Get() T {
	registry := platform.Dialects()
	current := i.current.Load()
	if current == nil || current.registry != registry {
		current = &indexAnswer[T]{registry: registry, answer: i.derive(registry.Dialects())}
		i.current.Store(current)
	}
	return current.answer
}
