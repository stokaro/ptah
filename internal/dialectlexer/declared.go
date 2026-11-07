package dialectlexer

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/lexer"
)

// declared holds the lexical rules an executable provider declared for an
// engine the process registry does not describe, keyed by the engine's name
// in lower case. A value is a *declaration.
var declared sync.Map

// declaration is one engine's declared lexical rules, and the lexer options
// read from them.
type declaration struct {
	lexical descriptor.Lexical
	options lexer.Options
}

// DeclareLexical registers the lexical rules an engine an executable provider
// serves states in its query dialect (contract 25.34, query.lexical), and
// returns the function that ends the registration. Until then [Options]
// answers the rules under each of names, as [FromDescriptor] answers a
// built-in dialect's descriptor, so every reader of the engine's text through
// the shared lexer reads the engine's rules.
//
// lexical states rules only: a lexer of the engine's own is a scanner its
// provider serves (internal/lexerext), never a declaration. A name the
// process registry describes is refused, and so is an executable comment
// style the descriptor package does not declare. A name another registration
// holds with the same rules keeps that registration, which its own release
// ends; one it holds with other rules refuses the whole declaration. The
// release is safe to call more than once.
func DeclareLexical(lexical descriptor.Lexical, names ...string) (func(), error) {
	if lexical.Lexer != "" {
		return nil, fmt.Errorf("dialectlexer: declared rules name the lexer %q; a lexer of an engine's own is a scanner its provider serves", lexical.Lexer)
	}
	if lexical.ExecutableComments != "" && !slices.Contains(descriptor.ExecutableCommentStyles(), lexical.ExecutableComments) {
		return nil, fmt.Errorf("dialectlexer: executable comment style %q is not one of %q", lexical.ExecutableComments, descriptor.ExecutableCommentStyles())
	}
	keys := make([]string, 0, len(names))
	for _, name := range names {
		key := declaredKey(name)
		if key == "" {
			return nil, fmt.Errorf("dialectlexer: declared lexical rules need an engine name")
		}
		if _, described := platform.Lookup(key); described {
			return nil, fmt.Errorf("dialectlexer: %s is a dialect Ptah describes, whose lexical rules its descriptor states", key)
		}
		keys = append(keys, key)
	}
	entry := &declaration{lexical: lexical, options: FromDescriptor(descriptor.Dialect{Lexical: lexical})}
	var stored []string
	release := func() {
		for _, key := range stored {
			declared.CompareAndDelete(key, entry)
		}
	}
	for _, key := range keys {
		existing, loaded := declared.LoadOrStore(key, entry)
		if !loaded {
			stored = append(stored, key)
			continue
		}
		if !reflect.DeepEqual(existing.(*declaration).lexical, lexical) {
			release()
			return nil, fmt.Errorf("dialectlexer: %s is registered with other lexical rules", key)
		}
	}
	return release, nil
}

// declaredOptions returns the options a [DeclareLexical] registration holds
// for dialect, compared without regard to case or surrounding space.
func declaredOptions(dialect string) (lexer.Options, bool) {
	entry, found := declared.Load(declaredKey(dialect))
	if !found {
		return lexer.Options{}, false
	}
	return entry.(*declaration).options, true
}

func declaredKey(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}
