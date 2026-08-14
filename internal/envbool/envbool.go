// Package envbool owns the one grammar Ptah accepts for a boolean `PTAH_*`
// environment variable, and the one error it reports for anything else.
//
// The rule this package exists to make unavoidable:
//
//	unset                 the documented default
//	a valid boolean       parsed and honored
//	anything else         a configuration error naming the variable and the value
//
// Before stokaro/ptah#1334 the direct feature toggles each spelled their own
// read as
//
//	value, err := strconv.ParseBool(os.Getenv(name))
//	return err == nil && value
//
// which answers the same `false` for four distinguishable states: the variable
// is absent, it is set to a false spelling, it is set to the empty string, and
// it holds a typo. An operator who wrote `PTAH_ALLOW_RESERVED_ROLE_NAMES=yes`
// in a CI environment file, a container manifest or a systemd unit believes
// they changed the behavior, and nothing tells them otherwise. Every boolean
// toggle in this tree opts IN to the more permissive side, so the typo lands on
// the strict default and fails closed -- which is why this is a usability
// defect today and would be a security one the first time a boolean `PTAH_*`
// variable defaults to the permissive side. The guard test over [Registered]
// asserts every declared default is false, so that flip cannot happen quietly.
//
// The accepted spellings are exactly [strconv.ParseBool]'s, deliberately not a
// narrower or wider set: `1 t T TRUE true True` and `0 f F FALSE false False`.
// Nothing is trimmed before parsing, so `" true"`, `"true "` and `""` are
// configuration errors rather than accidents that happen to work -- a quoting
// mistake in a YAML manifest is precisely the class this rule exists to
// surface.
//
// Variables are DECLARED, not read ad hoc: a package-level [New] call names the
// variable, its default and its strict-compatibility [Class] once, and
// [Registered] is the derived list a guard test measures the tree against. A
// hand-maintained list is what goes stale the next time a variable is added,
// which is the failure mode this package is shaped to prevent.
//
// The [Class] is here, rather than in the package that owns the strict Atlas
// Community Edition policy, for the same reason the default is: a central list
// sorting variables into buckets can forget one, and the forgetting is silent.
// A declaration that states the classification alongside the name cannot drift
// from it, because there is only one of them.
//
// A registration is also a statement about the LINK GRAPH, and that is what
// makes deriving the policy from [Registered] sound. Every variable in this tree
// is declared in the package that implements the behavior it governs, so a
// binary that does not link the package does not reach the behavior either and
// has nothing to gate. cmd/internal/envboolguard measures the other direction:
// every `PTAH_*` name the tree mentions is either declared here or written down
// as a non-boolean.
package envbool

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// Prefix is the namespace every variable this package governs lives in.
const Prefix = "PTAH_"

// Parse validates one raw environment value already read from name.
//
// It is the seam for the flag/environment binding paths, which read the value
// themselves because they must decide what to do with it by flag type; every
// other caller should hold a [Var] and call [Var.Resolve] instead.
//
// The error names both the variable and the raw value, because either alone
// leaves the operator guessing: the name without the value does not say which
// of several exported variables is the bad one when the shell that exported it
// is not in front of them, and the value without the name does not say where to
// look.
func Parse(name, value string) (bool, error) {
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid boolean value %q for %s", value, name)
	}
	return parsed, nil
}

// Class states what the strict Atlas Community Edition compatibility policy
// owes a declared variable. It is stated at the registration site because the
// package declaring a variable is the only one that knows whether the behavior
// behind it is a capability the pinned community binary already has.
type Class uint8

const (
	// Unclassified is the zero Class: a variable registered without stating
	// one. It is representable on purpose, so that the guard over [Registered]
	// has something to fail on and is not merely decorative; it is not a
	// classification anything should ship with.
	//
	// Strict mode treats it as [Gated]. That is the fail-closed direction: a
	// retained variable wrongly left unclassified is refused loudly, on the
	// first run that sets it, naming itself. A gated variable wrongly left
	// unclassified under the other default would be silently honored, which is
	// the failure this whole contract exists to prevent.
	Unclassified Class = iota

	// Gated marks a variable that adds behavior the pinned community binary
	// does not have. Strict mode parses a present value and refuses a true one,
	// because honoring it would move ptah-compat off the surface strict mode
	// exists to measure. A false spelling selects the same state as an absent
	// variable and stays valid.
	Gated

	// Retained marks a correctness or safety control that adds no Atlas
	// capability -- it restores or tightens something the pinned binary already
	// does. Strict mode keeps it available and honors a true value, but still
	// parses every present value at the process boundary so a malformed one is
	// refused before any external work rather than lying dormant until the run
	// that would have used it.
	Retained

	// Selector marks the variable that selects the compatibility policy itself.
	// It is parsed by the same rule as everything else and never refused:
	// gating it would make strict mode reject the variable that turned strict
	// mode on.
	Selector
)

// String renders the classification for a diagnostic.
func (c Class) String() string {
	names := map[Class]string{
		Unclassified: "unclassified",
		Gated:        "gated",
		Retained:     "retained",
		Selector:     "selector",
	}
	name, known := names[c]
	if !known {
		return fmt.Sprintf("Class(%d)", uint8(c))
	}
	return name
}

// Var is one declared boolean `PTAH_*` environment variable.
//
// The zero Var is not usable; construct one with [New] at package level so the
// declaration is a fact about the program rather than something a call site
// decides.
type Var struct {
	name         string
	defaultValue bool
	class        Class
}

var (
	registryMu sync.Mutex
	registry   []Var
)

// New declares a boolean `PTAH_*` environment variable and records it in the
// registry [Registered] reports.
//
// defaultValue is what an ABSENT variable selects. class is what strict Atlas
// Community Edition compatibility owes it. Both are stated here rather than at
// the call site so they travel with the name, and so a guard test can read all
// three off one declaration instead of off a list somebody has to remember to
// edit.
func New(name string, defaultValue bool, class Class) Var {
	variable := Var{name: name, defaultValue: defaultValue, class: class}
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = append(registry, variable)
	return variable
}

// Name returns the environment variable name.
func (v Var) Name() string {
	return v.name
}

// Default returns the value an absent variable selects.
func (v Var) Default() bool {
	return v.defaultValue
}

// Class returns the strict-compatibility classification declared for the
// variable.
func (v Var) Class() Class {
	return v.class
}

// Resolve reads the variable and reports what it selects.
//
// It is [os.LookupEnv] and not [os.Getenv] on purpose: `os.Getenv` answers the
// empty string for an absent variable and for `PTAH_X=` alike, which folds the
// one state that must keep the default into the one state that must be
// refused.
func (v Var) Resolve() (bool, error) {
	value, ok := os.LookupEnv(v.name)
	if !ok {
		return v.defaultValue, nil
	}
	return Parse(v.name, value)
}

// Registered returns every variable declared through [New], sorted by name.
//
// It is the derived enumeration a guard test measures the tree against, so it
// reports the registrations as they were made -- duplicates included -- rather
// than quietly collapsing two declarations of one name into one entry.
func Registered() []Var {
	registryMu.Lock()
	defer registryMu.Unlock()
	out := slices.Clone(registry)
	slices.SortFunc(out, func(a, b Var) int {
		return strings.Compare(a.name, b.name)
	})
	return out
}
