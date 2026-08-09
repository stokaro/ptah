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
// they changed the behavior, and nothing tells them otherwise.
//
// # An absent variable must select the side that fails closed
//
// Almost every boolean toggle in this tree opts IN to the more permissive side,
// so an absent variable -- a name a container manifest never passed through, a
// sudo invocation stripped, a CI job forgot -- lands on the strict default and
// fails CLOSED. A variable whose false default is the PERMISSIVE side inverts
// that, and forgetting it opens the gate it guards.
//
// `Default() == false` was the assertion, and it is a PROXY for that rule
// rather than the rule. The proxy holds only while `true` means "permit more",
// and it stopped holding before anyone noticed: `PTAH_STRICT_DIR_QUERY`, whose
// own doc says it "turns the report below into a refusal", already defaults to
// the permissive side and already passes an assertion whose stated reason it
// contradicts. So the direction is DECLARED here -- [New] for the ordinary
// shape, [NewOptInRefusal] for a toggle whose true value refuses -- and the
// guard test over [Registered] checks the rule instead of the proxy.
//
// The declaration is also where a permissive default has to justify itself in
// writing, because such a default is only sound when it is a measured
// compatibility floor rather than a safety decision.
//
// The accepted spellings are exactly [strconv.ParseBool]'s, deliberately not a
// narrower or wider set: `1 t T TRUE true True` and `0 f F FALSE false False`.
// Nothing is trimmed before parsing, so `" true"`, `"true "` and `""` are
// configuration errors rather than accidents that happen to work -- a quoting
// mistake in a YAML manifest is precisely the class this rule exists to
// surface.
//
// Variables are DECLARED, not read ad hoc: a package-level [New] call names the
// variable and its default once, and [Registered] is the derived list a guard
// test measures the tree against. A hand-maintained list is what goes stale the
// next time a variable is added, which is the failure mode this package is
// shaped to prevent.
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

// Var is one declared boolean `PTAH_*` environment variable.
//
// The zero Var is not usable; construct one with [New] at package level so the
// declaration is a fact about the program rather than something a call site
// decides.
type Var struct {
	name         string
	defaultValue bool
	// permissiveDefaultReason is empty for the ordinary shape, whose absent
	// value selects the side that fails closed, and holds the written
	// justification for a toggle declared through [NewOptInRefusal].
	permissiveDefaultReason string
}

var (
	registryMu sync.Mutex
	registry   []Var
)

// New declares a boolean `PTAH_*` environment variable whose TRUE value selects
// the more permissive behavior, and records it in the registry [Registered]
// reports.
//
// defaultValue is what an ABSENT variable selects. It is stated here rather
// than at the call site so the default and the name travel together and a guard
// test can read both.
//
// This is the ordinary shape and it carries a claim: an absent variable selects
// the side that fails closed. A toggle whose true value REFUSES something the
// default accepts must be declared through [NewOptInRefusal] instead, or the
// guard's stated reason stops describing it.
//
// A variable that only changes how much a run DESCRIBES or INSPECTS -- more
// roles, more migration versions -- is the ordinary shape. It permits more
// output, not more input, and forgetting it cannot let a bad schema through.
func New(name string, defaultValue bool) Var {
	return register(Var{name: name, defaultValue: defaultValue})
}

// NewOptInRefusal declares a boolean `PTAH_*` environment variable whose TRUE
// value REFUSES something the default accepts, so its false default is the
// permissive side.
//
// whyThePermissiveDefaultIsSafe is required and is read by the guard test, not
// by the program. A permissive default is sound only when it is a measured
// compatibility floor or a deliberate report-don't-refuse decision, and the
// declaration is where that has to be written down rather than inferred from
// the variable's name.
func NewOptInRefusal(name, whyThePermissiveDefaultIsSafe string) Var {
	return register(Var{name: name, permissiveDefaultReason: whyThePermissiveDefaultIsSafe})
}

func register(variable Var) Var {
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

// DefaultFailsClosed reports whether an absent variable selects the side that
// permits less, which is the property the guard test asserts and the reason
// this package exists in the shape it does.
//
// It is DECLARED rather than derived. Nothing about a bool says which of its two
// values is the strict one, so `Default() == false` can only ever be a proxy for
// this, and the proxy is silently wrong for a toggle whose true value refuses.
func (v Var) DefaultFailsClosed() bool {
	return v.permissiveDefaultReason == ""
}

// PermissiveDefaultReason returns the written justification a variable whose
// default is the permissive side carries, and "" for every other variable.
func (v Var) PermissiveDefaultReason() string {
	return v.permissiveDefaultReason
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
