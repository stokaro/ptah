package descriptor_test

import (
	"fmt"
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtindialects"
)

// TestDialect_CloneSharesNothing holds Clone to its contract over every
// bundled descriptor: no slice and no map reachable from the copy, however
// deep, is the original's. The copy is made whole and its references are
// replaced after, so a reference the replacement misses is shared with the
// registry.
func TestDialect_CloneSharesNothing(t *testing.T) {
	c := qt.New(t)
	var shared []string
	compared := 0

	for _, original := range builtindialects.Descriptors() {
		clone := original.Clone()

		found, count := sharedReferences(reflect.ValueOf(original), reflect.ValueOf(clone), original.Name)
		shared = append(shared, found...)
		compared += count
		c.Assert(clone, qt.DeepEquals, original)
	}

	c.Assert(shared, qt.HasLen, 0)
	c.Assert(compared > 100, qt.IsTrue, qt.Commentf("compared %d slices and maps", compared))
}

// sharedReferences names each nonempty slice and map reachable from original
// whose storage clone shares, and counts the ones it compared.
func sharedReferences(original, clone reflect.Value, path string) (shared []string, compared int) {
	note := func(found []string, count int) {
		shared = append(shared, found...)
		compared += count
	}
	switch original.Kind() {
	case reflect.Struct:
		for index := range original.NumField() {
			note(sharedReferences(original.Field(index), clone.Field(index), path+"."+original.Type().Field(index).Name))
		}
	case reflect.Slice:
		if original.Len() == 0 {
			return nil, 0
		}
		note(sameStorage(original, clone, path))
		for index := range original.Len() {
			note(sharedReferences(original.Index(index), clone.Index(index), fmt.Sprintf("%s[%d]", path, index)))
		}
	case reflect.Map:
		if original.Len() == 0 {
			return nil, 0
		}
		note(sameStorage(original, clone, path))
		for iter := original.MapRange(); iter.Next(); {
			note(sharedReferences(iter.Value(), clone.MapIndex(iter.Key()), fmt.Sprintf("%s[%v]", path, iter.Key())))
		}
	default:
	}
	return shared, compared
}

// sameStorage reports path when original and clone, a slice or a map each,
// share their storage.
func sameStorage(original, clone reflect.Value, path string) ([]string, int) {
	if original.Pointer() == clone.Pointer() {
		return []string{path}, 1
	}
	return nil, 1
}
