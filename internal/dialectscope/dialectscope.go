// Package dialectscope resolves the set of target dialects a declared schema
// object belongs to.
//
// A declaration carries its scope in the `dialects=` annotation attribute:
//
//	//ptah:schema:function name="tenant_id" language="plpgsql" dialects="postgres,cockroachdb"
//
// The scope is a property of the DECLARATION, not of the invocation. That is
// what separates it from `exclude` in ptah.yaml and `--exclude` on the command
// line, which subtract objects at the moment a command runs and live outside
// the source that declares them.
//
// An object whose scope excludes the target is absent from the desired state
// for that target: the comparator never reports it, so the schema converges,
// and the planner never sees it, so a target that refuses to plan the object
// is never asked to.
package dialectscope

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/platform"
)

// Attribute is the annotation attribute name every scoped directive accepts.
// It is written down once so the directive table, the parser and diagnostics
// cannot disagree about its spelling.
const Attribute = "dialects"

// Parse resolves a raw `dialects=` attribute value into the canonical dialect
// names it selects, sorted and deduplicated.
//
// Every accepted spelling of a dialect resolves through
// [platform.NormalizeDialect], so `dialects="postgresql"` and
// `dialects="postgres"` select the same target and an object cannot be scoped
// to a spelling of a dialect while missing the dialect.
//
// Two values are refused rather than accommodated, both because the quiet
// reading of them is the failure this attribute exists to remove:
//
//   - A name that resolves to no supported dialect. Read as "belongs to
//     nothing", a typo would delete the object from every target and every
//     command would still exit 0.
//   - An empty scope. Read as "belongs to everything", `dialects=""` would be
//     indistinguishable from not writing the attribute at all, and an author
//     who typed it meant to narrow the object.
//
// A scope naming no dialect is a declaration error, so Parse returns one.
func Parse(raw string) ([]string, error) {
	seen := make(map[string]struct{})
	var scope []string
	for part := range strings.SplitSeq(raw, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		canonical := platform.NormalizeDialect(name)
		if canonical == "" {
			return nil, fmt.Errorf("%q names no supported dialect", name)
		}
		if _, ok := seen[canonical]; ok {
			continue
		}
		seen[canonical] = struct{}{}
		scope = append(scope, canonical)
	}
	if len(scope) == 0 {
		return nil, fmt.Errorf("names no dialect")
	}
	sort.Strings(scope)
	return scope, nil
}

// Includes reports whether an object carrying scope belongs to dialect.
//
// An empty scope belongs to every dialect. That is the state of every
// declaration written before the attribute existed, so an unscoped schema
// behaves exactly as it did: this attribute can only narrow a declaration,
// never widen one.
//
// A dialect that resolves to no supported platform is included as well. The
// commands refuse an unknown target with a message that names the flag; a
// projection that quietly emptied the desired state first would replace that
// refusal with a schema that appears already synced.
// Both sides are normalized before they are compared. Parse canonicalizes what
// it stores, so a scope that arrived through an annotation already holds the
// canonical spelling; a Go caller building a goschema.Database by hand reaches
// these fields directly, has no exported parser to canonicalize with, and
// stores exactly the documented alias it was given. Comparing that stored
// spelling literally scoped the object away from the very dialect it names --
// silently, and in the direction that plans work nobody asked for.
//
// An entry that names no supported dialect matches nothing. Parse refuses such
// a spelling outright; reaching here means it came from a direct caller, and a
// scope naming a platform this build does not know does not describe any target
// it does know.
func Includes(scope []string, dialect string) bool {
	if len(scope) == 0 {
		return true
	}
	canonical := platform.NormalizeDialect(dialect)
	if canonical == "" {
		return true
	}
	return slices.ContainsFunc(scope, func(name string) bool {
		return platform.NormalizeDialect(name) == canonical
	})
}
