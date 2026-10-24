package query

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
)

// Spelling is how an engine the process registry does not describe spells
// what the builder renders, as data: the parts of a dialect descriptor the
// builder reads. Its fields have the descriptor's meaning and JSON names, so
// the query dialect an executable provider declares in its description
// decodes into it.
type Spelling struct {
	// Placeholder is how a statement marks a bound parameter. It is required.
	Placeholder descriptor.Placeholder `json:"placeholder"`
	// Quoting is how the engine writes an identifier.
	Quoting descriptor.Quoting `json:"quoting,omitzero"`
	// Query is how the engine spells what engines spell differently.
	Query descriptor.Query `json:"query,omitzero"`
	// DefaultCapabilities are the capability facts the builder assumes when
	// its caller names the engine without a target's capabilities, as
	// [RenderInsert] does. An absent key is unknown, and a clause it gates
	// is refused.
	DefaultCapabilities map[capability.Capability]bool `json:"default_capabilities,omitempty"`
}

// ErrDialectRegistered is returned by [RegisterDialect] for an engine that
// already renders with another spelling: one the process registry describes,
// or one an earlier registration gave a different spelling.
var ErrDialectRegistered = errors.New("query: the dialect already has a spelling")

// spelled is a registered dialect: the descriptor the renderer reads and the
// capabilities it assumes.
type spelled struct {
	described descriptor.Dialect
	spelling  Spelling
	caps      capability.Capabilities
}

var spellings sync.Map

// RegisterDialect makes engine a dialect the builder renders for, with
// spelling: afterwards [RenderSelect], [RenderInsert], [RenderUpdate] and
// [RenderDelete] accept engine as their dialect and render as spelling says,
// assuming its default capabilities. The WithCapabilities variants accept it
// too, with the capabilities the caller gives.
//
// It is for a program that selects an engine Ptah does not ship, such as the
// host when it starts an executable provider that declares a query dialect,
// or an embedder that builds the spelling in code. The registration lasts for
// the life of the process and reaches only the query builder: it does not
// describe engine to the rest of Ptah.
//
// A spelling the builder does not know refuses with an error that names the
// value: an unknown placeholder style, quote style, bare-name rule, join or
// upsert form, and an unknown or contradictory capability. An engine the
// process registry describes, and one registered before with a different
// spelling, are refused with [ErrDialectRegistered]; registering the same
// spelling again changes nothing. RegisterDialect is safe for concurrent use.
func RegisterDialect(engine string, spelling Spelling) error {
	name := strings.TrimSpace(engine)
	if name == "" || name != engine {
		return fmt.Errorf("query: a dialect needs a name without surrounding space, got %q", engine)
	}
	if _, described := platform.Lookup(name); described {
		return fmt.Errorf("%w: %s is a dialect Ptah describes", ErrDialectRegistered, name)
	}
	entry, err := newSpelled(name, spelling)
	if err != nil {
		return err
	}
	if existing, loaded := spellings.LoadOrStore(name, entry); loaded {
		if registered, ok := existing.(spelled); !ok || !registered.same(entry) {
			return fmt.Errorf("%w: %s was registered with another spelling", ErrDialectRegistered, name)
		}
	}
	return nil
}

func newSpelled(name string, spelling Spelling) (spelled, error) {
	described := descriptor.Dialect{Name: name, Placeholder: spelling.Placeholder, Quoting: spelling.Quoting, Query: spelling.Query}
	if _, err := descriptor.NewRegistry(described); err != nil {
		return spelled{}, fmt.Errorf("query: the spelling of %s: %w", name, err)
	}
	caps := capability.Of(spelling.DefaultCapabilities)
	if err := caps.Validate(); err != nil {
		return spelled{}, fmt.Errorf("query: the default capabilities of %s: %w", name, err)
	}
	spelling.DefaultCapabilities = maps.Clone(spelling.DefaultCapabilities)
	return spelled{described: described, spelling: spelling, caps: caps}, nil
}

func (s spelled) same(other spelled) bool {
	left, right := s.spelling, other.spelling
	return left.Placeholder == right.Placeholder && reflect.DeepEqual(left.Quoting, right.Quoting) &&
		reflect.DeepEqual(left.Query, right.Query) && s.caps.Equal(other.caps)
}

// lookupDialect returns the descriptor the renderer reads for dialect: the
// process registry's, or a registered spelling's, which also carries how the
// dialect quotes a name.
func lookupDialect(dialect string) (descriptor.Dialect, *descriptor.Quoting, bool) {
	if described, ok := platform.Lookup(dialect); ok {
		return described, nil, true
	}
	if registered, ok := registeredSpelling(dialect); ok {
		quoting := registered.described.Quoting
		return registered.described, &quoting, true
	}
	return descriptor.Dialect{}, nil, false
}

// defaultCapabilities are the capabilities a render that names only the
// dialect renders for: a registered spelling's, or the release line
// capability.ForDialect returns.
func defaultCapabilities(dialect string) capability.Capabilities {
	if _, described := platform.Lookup(dialect); !described {
		if registered, ok := registeredSpelling(dialect); ok {
			return registered.caps
		}
	}
	return capability.ForDialect(dialect)
}

func registeredSpelling(dialect string) (spelled, bool) {
	entry, ok := spellings.Load(dialect)
	if !ok {
		return spelled{}, false
	}
	registered, ok := entry.(spelled)
	return registered, ok
}
