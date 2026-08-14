package atlascompatpolicy_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	// Importing the owners is what populates the registry the strict policy
	// reads back, exactly as cmd/internal/envboolguard does and for the same
	// reason: a package left out here contributes no declarations, so its
	// variable would look absent rather than unvalidated. The ptah-compat
	// process links all of them, because each declaration lives in the package
	// implementing the behavior it governs -- so a binary that does not link the
	// package cannot reach the behavior either.
	//
	// These are blank imports from an EXTERNAL test package, which is what keeps
	// them legal: cmd/atlas imports this package, and nothing here imports
	// cmd/atlas.
	//
	// cmd/internal/editor is absent because Go refuses it from outside cmd/;
	// cmd/atlas links it, which is why PTAH_ALLOW_NONINTERACTIVE_EDIT still
	// appears below. If it ever stopped linking it, the documented-set gate goes
	// red rather than the enumeration going quietly shorter.
	_ "go.5x5.cz/ptah/cmd/atlas"
	_ "go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	_ "go.5x5.cz/ptah/internal/atlasfilter"
	_ "go.5x5.cz/ptah/internal/atlashcl"
	_ "go.5x5.cz/ptah/internal/atlashclrender"
	_ "go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/envbool"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	_ "go.5x5.cz/ptah/internal/migrationintegrity"
	_ "go.5x5.cz/ptah/internal/reservedrole"
	_ "go.5x5.cz/ptah/internal/rolescope"
	_ "go.5x5.cz/ptah/internal/sqlitevirtual"
	_ "go.5x5.cz/ptah/migration/migrator"
)

// ptahVarPattern reads variable names out of documentation prose.
var ptahVarPattern = regexp.MustCompile(`PTAH_[A-Z0-9]+[A-Z0-9_]*`)

// retainedControlsMarker is the sentence the documented retained set opens
// with. The gate below finds the paragraph by it, so rewording the sentence
// fails loudly instead of silently selecting nothing.
const retainedControlsMarker = "opt-in correctness controls remain available"

// TestStrictCERefusesAMalformedValueForEveryRegisteredVariable is the
// acceptance this change exists for.
//
// It enumerates [envbool.Registered] instead of listing names, so a variable
// declared tomorrow is covered by the act of declaring it. The lists this
// replaced were maintained by hand beside a registry that already knew the
// answer, and three variables had fallen out of them --
// PTAH_ATLAS_IGNORE_ENV_SCHEMAS, PTAH_ALLOW_UNVERIFIED_MIGRATION_DIR and
// PTAH_DIRECTIVES_ANYWHERE -- so `PTAH_ATLAS_STRICT_COMPAT=1` with a malformed
// value for any of them exited 0 while the same probe on
// PTAH_STRICT_DIR_QUERY exited 1.
//
// The malformed value is refused whatever the classification, because a value
// that cannot be parsed is not an opt-in the policy has anything to say about:
// it is configuration the operator got wrong, and the only run that would
// otherwise surface it is the one that happens to reach the behavior.
func TestStrictCERefusesAMalformedValueForEveryRegisteredVariable(t *testing.T) {
	c := qt.New(t)

	registered := envbool.Registered()
	c.Assert(len(registered) > 10, qt.IsTrue, qt.Commentf(
		"found %d declarations; an enumeration over an empty registry is also green",
		len(registered)))

	for _, variable := range registered {
		t.Run(variable.Name(), func(t *testing.T) {
			c := qt.New(t)

			clearDeclaredEnvironment(t)
			t.Setenv(atlascompatpolicy.StrictCompatEnvVar, "1")
			t.Setenv(variable.Name(), "maybe")

			_, err := atlascompatpolicy.Resolve()

			c.Assert(err, qt.ErrorMatches,
				`invalid boolean value "maybe" for `+variable.Name())
		})
	}
}

// TestStrictCEAnswersEveryRegisteredVariableByItsDeclaredClass measures the
// other half: what an enabled value does.
//
// A gated variable adds behavior the pinned community binary does not have, so
// strict mode refuses it. A retained one restores or tightens something that
// binary already does, so strict mode honors it. The selector is what turned
// strict mode on and can never be refused by it. An unclassified variable is
// refused, which is the fail-closed direction -- but the registry guard in
// cmd/internal/envboolguard is what keeps one from shipping in that state, so
// this row exists to pin the runtime answer rather than to permit it.
func TestStrictCEAnswersEveryRegisteredVariableByItsDeclaredClass(t *testing.T) {
	assertions := map[envbool.Class]func(*testing.T, string, error){
		envbool.Unclassified: assertStrictRefuses,
		envbool.Gated:        assertStrictRefuses,
		envbool.Retained:     assertStrictAccepts,
		envbool.Selector:     assertStrictAccepts,
	}

	for _, variable := range envbool.Registered() {
		t.Run(variable.Name(), func(t *testing.T) {
			c := qt.New(t)

			assert := assertions[variable.Class()]
			c.Assert(assert, qt.IsNotNil, qt.Commentf(
				"no expectation is written down for class %s", variable.Class()))
			clearDeclaredEnvironment(t)
			t.Setenv(atlascompatpolicy.StrictCompatEnvVar, "1")
			t.Setenv(variable.Name(), "1")

			_, err := atlascompatpolicy.Resolve()

			assert(t, variable.Name(), err)
		})
	}
}

// TestStrictCEAcceptsADisabledValueForEveryRegisteredVariable keeps the gate on
// the enabled spelling alone.
//
// A false value selects the same state as an absent variable, so refusing it
// would turn a CI environment file that disables an extension into a broken
// strict run. This is the control for the row above: if strict mode refused on
// presence rather than on value, both would still be green with only the first,
// and this one goes red.
//
// The selector's own row expects the opposite policy, because setting it false
// is how a run asks for the full surface. That is the one variable whose
// disabled spelling means something other than "this extension is off".
func TestStrictCEAcceptsADisabledValueForEveryRegisteredVariable(t *testing.T) {
	for _, variable := range envbool.Registered() {
		t.Run(variable.Name(), func(t *testing.T) {
			c := qt.New(t)

			clearDeclaredEnvironment(t)
			t.Setenv(atlascompatpolicy.StrictCompatEnvVar, "1")
			t.Setenv(variable.Name(), "false")

			policy, err := atlascompatpolicy.Resolve()

			c.Assert(err, qt.IsNil)
			c.Assert(policy.IsStrictCE(), qt.Equals,
				variable.Name() != atlascompatpolicy.StrictCompatEnvVar)
		})
	}
}

// TestFullCompatibilityInspectsNoRegisteredVariable pins the other policy.
//
// Deriving the strict set from the registry must not turn the default surface
// into a strict one by accident: with the selector off, a malformed value for a
// declared variable is the owning command's business and no concern of the
// process boundary.
func TestFullCompatibilityInspectsNoRegisteredVariable(t *testing.T) {
	for _, variable := range envbool.Registered() {
		t.Run(variable.Name(), func(t *testing.T) {
			c := qt.New(t)

			clearDeclaredEnvironment(t)
			t.Setenv(variable.Name(), "maybe")
			envbooltest.Unset(atlascompatpolicy.StrictCompatEnvVar)(t)

			policy, err := atlascompatpolicy.Resolve()

			c.Assert(err, qt.IsNil)
			c.Assert(policy.IsStrictCE(), qt.IsFalse)
		})
	}
}

// TestDocumentedRetainedControlsMatchTheRegistry is the fourth list.
//
// Three of the four hand-written lists were Go slices and are now derived. The
// fourth is prose, in the configuration reference, and it had already drifted:
// it announced "four" controls after a fifth was added. Prose cannot be
// derived, so it is gated instead -- the documented set has to be exactly the
// set the registry classifies as retained.
func TestDocumentedRetainedControlsMatchTheRegistry(t *testing.T) {
	c := qt.New(t)

	documented := documentedRetainedControls(t)
	c.Assert(len(documented) > 0, qt.IsTrue, qt.Commentf(
		"no variable names found near %q; the paragraph moved or was reworded",
		retainedControlsMarker))

	c.Assert(documented, qt.DeepEquals, registeredNamesWithClass(envbool.Retained))
}

// documentedRetainedControls returns the variable names the configuration
// reference lists as retained, sorted.
func documentedRetainedControls(t *testing.T) []string {
	t.Helper()
	c := qt.New(t)

	path := filepath.Join("..", "..", "docs", "site", "src", "content", "docs",
		"reference", "configuration.md")
	contents, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	names := ptahVarPattern.FindAllString(markedParagraph(
		string(contents), retainedControlsMarker), -1)
	slices.Sort(names)
	return slices.Compact(names)
}

// markedParagraph returns the blank-line-delimited paragraph holding marker, or
// "" when no line holds it.
func markedParagraph(contents, marker string) string {
	for paragraph := range strings.SplitSeq(contents, "\n\n") {
		if strings.Contains(paragraph, marker) {
			return paragraph
		}
	}
	return ""
}

// registeredNamesWithClass returns the declared names carrying class, sorted.
func registeredNamesWithClass(class envbool.Class) []string {
	var names []string
	for _, variable := range envbool.Registered() {
		names = append(names, nameWhenClassIs(variable, class)...)
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// nameWhenClassIs returns the variable's name when it carries class, so the
// caller collects without a branch.
func nameWhenClassIs(variable envbool.Var, class envbool.Class) []string {
	if variable.Class() != class {
		return nil
	}
	return []string{variable.Name()}
}

// clearDeclaredEnvironment removes every declared variable and the presence-
// gated ones, so a row measures the value it sets and not whatever the ambient
// environment exported.
func clearDeclaredEnvironment(t *testing.T) {
	t.Helper()
	for _, variable := range envbool.Registered() {
		envbooltest.Unset(variable.Name())(t)
	}
	envbooltest.Unset("PTAH_LOG_FORMAT")(t)
}

// assertStrictRefuses expects the policy to name the variable it refused.
func assertStrictRefuses(t *testing.T, name string, err error) {
	t.Helper()
	c := qt.New(t)

	c.Assert(err, qt.ErrorMatches,
		atlascompatpolicy.StrictCompatEnvVar+` does not allow `+name)
}

// assertStrictAccepts expects strict mode to resolve with the variable enabled.
func assertStrictAccepts(t *testing.T, _ string, err error) {
	t.Helper()
	c := qt.New(t)

	c.Assert(err, qt.IsNil)
}
