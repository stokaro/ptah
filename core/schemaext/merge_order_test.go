package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// mergeRuns is how many times a merge is repeated. Go randomizes map
// iteration, so a merge that names the duplicate it meets first in a map
// names a different one of eight within a few runs.
const mergeRuns = 100

// onceNamed is a declaration of a model that states nothing about a second
// declaration, under name.
func onceNamed(name string) schemaext.Object {
	return schemaext.Object{Ref: objectidentity.ID{Kind: objectidentity.Kind((&declaredOnce{}).Kind()),
		Name: objectidentity.Part{Source: name, Normalized: name}}, Value: &declaredOnce{}}
}

// repeatedObjects are two collections that hold the same eight identities,
// listed in opposite orders.
func repeatedObjects() (held, other schemaext.Objects) {
	names := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	forward := make([]schemaext.Object, 0, len(names))
	backward := make([]schemaext.Object, 0, len(names))
	for i := range names {
		forward = append(forward, onceNamed(names[i]))
		backward = append(backward, onceNamed(names[len(names)-1-i]))
	}
	return must.Must(schemaext.NewObjects(forward...)), must.Must(schemaext.NewObjects(backward...))
}

// TestMerge_NamesTheFirstRepeatedObject refuses a merge that repeats several
// identities with one message on every run: the one naming the first
// repeated identity in CompareRefs order.
func TestMerge_NamesTheFirstRepeatedObject(t *testing.T) {
	c := qt.New(t)
	held, other := repeatedObjects()
	messages := make(map[string]int)

	for range mergeRuns {
		merged, err := held.Merge(other)
		c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
		c.Assert(merged.Len(), qt.Equals, 0)
		messages[err.Error()]++
	}

	c.Assert(messages, qt.DeepEquals, map[string]int{"duplicate feature identity: object example.org/once a": mergeRuns})
}

// TestMergeDeclarations_NamesTheFirstRepeatedObject is the same property for
// a merge of declarations, whose model refuses a second declaration.
func TestMergeDeclarations_NamesTheFirstRepeatedObject(t *testing.T) {
	c := qt.New(t)
	held, other := repeatedObjects()
	messages := make(map[string]int)

	for range mergeRuns {
		merged, err := held.MergeDeclarations(other)
		c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
		c.Assert(merged.Len(), qt.Equals, 0)
		messages[err.Error()]++
	}

	c.Assert(messages, qt.DeepEquals, map[string]int{"duplicate feature identity: object example.org/once a": mergeRuns})
}
