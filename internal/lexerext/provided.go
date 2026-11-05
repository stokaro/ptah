package lexerext

import (
	"fmt"
	"strings"
	"sync"

	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
)

// provided holds the scanner registered for each engine name, keyed by the
// name in lower case. A value is a *provision, so a release removes only the
// entries it made.
var provided sync.Map

// provision is one registration of a scanner.
type provision struct {
	scanner lexer.Scanner
}

// Provide registers scanner as the lexer of an engine an executable provider
// serves, under each of names, until the returned release runs.
// internal/dialectlexer answers it for those names, so every reader of the
// engine's text through internal/lexer reads the scanner's tokens.
//
// It refuses a name the process registry describes, whose text the
// descriptor's lexical rules read, and a name another registration holds.
// A refusal registers nothing. The release is idempotent, and it removes no
// entry a later registration made.
func Provide(scanner lexer.Scanner, names ...string) (func(), error) {
	if scanner == nil {
		return nil, fmt.Errorf("lexerext: no scanner to register")
	}
	entry := &provision{scanner: scanner}
	keys := make([]string, 0, len(names))
	for _, name := range names {
		key := providedKey(name)
		if key == "" {
			return nil, fmt.Errorf("lexerext: a scanner needs an engine name")
		}
		if _, described := platform.Lookup(key); described {
			return nil, fmt.Errorf("lexerext: %s is a dialect Ptah describes, whose text its descriptor's rules read", key)
		}
		keys = append(keys, key)
	}
	var stored []string
	release := func() {
		for _, key := range stored {
			provided.CompareAndDelete(key, entry)
		}
	}
	for _, key := range keys {
		if _, taken := provided.LoadOrStore(key, entry); taken {
			release()
			return nil, fmt.Errorf("lexerext: another scanner is registered for %s", key)
		}
		stored = append(stored, key)
	}
	var once sync.Once
	return func() { once.Do(release) }, nil
}

// Provided returns the scanner [Provide] registered for name, compared without
// regard to case or surrounding space, or nil when none is registered.
func Provided(name string) lexer.Scanner {
	entry, found := provided.Load(providedKey(name))
	if !found {
		return nil
	}
	if registered, ok := entry.(*provision); ok {
		return registered.scanner
	}
	return nil
}

func providedKey(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}
