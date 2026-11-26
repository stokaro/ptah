package sqlident

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
)

// declared holds the quoting an executable provider declared for an engine
// the process registry does not describe, keyed by the engine's name in lower
// case. A value is a declaration.
var declared sync.Map

// declaration is one engine's declared descriptor, and the spelling read from
// it.
type declaration struct {
	described descriptor.Dialect
	written   spelling
}

// RegisterQuoting records how an engine an executable provider serves writes
// an identifier: the quoting its query dialect declares, and the lexical rule
// that says whether a backslash inside a quoted name is an escape. described
// carries them in the fields a built-in descriptor does, and every function
// of this package then writes and reads the engine's names, under each of
// names, by the same reading of those fields as a built-in dialect's.
//
// A name the process registry describes is refused, and so is a name already
// registered with another descriptor. Registering the same descriptor again
// changes nothing. A registration lasts for the life of the process, as the
// provider's query dialect does.
func RegisterQuoting(described descriptor.Dialect, names ...string) error {
	entry := declaration{described: described, written: spellingFrom(described)}
	for _, name := range names {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			return fmt.Errorf("sqlident: declared quoting needs an engine name")
		}
		if _, described := platform.Lookup(key); described {
			return fmt.Errorf("sqlident: %s is a dialect Ptah describes, whose quoting its descriptor states", key)
		}
		if existing, loaded := declared.LoadOrStore(key, entry); loaded {
			if registered, ok := existing.(declaration); !ok || !reflect.DeepEqual(registered.described, described) {
				return fmt.Errorf("sqlident: %s was registered with another spelling", key)
			}
		}
	}
	return nil
}

// declaredSpelling returns the spelling [RegisterQuoting] recorded for
// dialect, compared without regard to case or surrounding space.
func declaredSpelling(dialect string) (spelling, bool) {
	entry, found := declared.Load(strings.ToLower(strings.TrimSpace(dialect)))
	if !found {
		return spelling{}, false
	}
	registered, ok := entry.(declaration)
	return registered.written, ok
}
