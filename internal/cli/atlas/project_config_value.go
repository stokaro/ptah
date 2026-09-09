package atlas

import (
	"fmt"
	"strings"

	"ptah.run/config/projectconfig"
)

// atlasConfigValue binds the `-c/--config` flag so a project selection Ptah
// cannot honor is refused by name rather than silently reduced.
//
// The pinned community binary registers this flag as `strings`: comma-separated
// and repeatable, defaulting to `[file://atlas.hcl]`. Ptah reads one project
// file. Registered as a plain string, the two spellings CE accepts each lost
// what the author wrote:
//
//	-c "file://a.hcl,file://b.hcl"  -> the whole string was one path, and the
//	                                   error named `a.hcl,file:` as a filename
//	-c file://a.hcl -c file://b.hcl -> pflag kept the last silently, so an env
//	                                   declared in the first answered
//	                                   `atlas env "first" not found`
//
// The second is the shape the compatibility policy forbids outright: the first
// file was discarded and the diagnostic blamed a missing env instead. Refusing
// says which flag the operator has to change (stokaro/ptah#3109).
//
// The type stays `string` rather than claiming CE's `strings`. Advertising a
// list this surface then refuses would trade a silent loss for a false promise,
// and the divergence is recorded in docs/site/src/content/docs/atlas/retained-divergences.md
// instead.
type atlasConfigValue struct {
	target *string
	// count is how many times Set ran, which is what separates a repeated flag
	// from a single one: pflag calls Set once per occurrence and keeps no
	// record of how many there were.
	count int
}

// newAtlasConfigValue binds a --config flag value to the path it selects.
func newAtlasConfigValue(target *string) *atlasConfigValue {
	return &atlasConfigValue{target: target}
}

// String renders the selected path, which is also the registered default, so
// the help line reads `(default "file://atlas.hcl")`.
func (v *atlasConfigValue) String() string {
	if v.target == nil {
		return ""
	}
	return *v.target
}

// Set records one occurrence and refuses a selection naming more than one file.
func (v *atlasConfigValue) Set(raw string) error {
	v.count++
	if v.count > 1 {
		return fmt.Errorf(
			"--%s was given more than once, and Ptah reads one project file; pass the one that declares the env you selected",
			atlasConfigFlagName)
	}
	if paths := splitAtlasConfigPaths(raw); len(paths) > 1 {
		return fmt.Errorf(
			"--%s names %d files (%s), and Ptah reads one; pass the one that declares the env you selected",
			atlasConfigFlagName, len(paths), strings.Join(paths, ", "))
	}
	*v.target = raw
	return nil
}

// Type names the value placeholder shown in help output.
func (v *atlasConfigValue) Type() string {
	return "string"
}

// Replace, Append and GetSlice implement [pflag.SliceValue].
//
// The interface is implemented for the reset path, not because the flag takes a
// list. [resetAtlasProjectFlags] clears a value through Replace when it can and
// falls back to `Set(DefValue)` when it cannot, and that fallback would count as
// an occurrence: a reused root would then refuse the first real -c of the next
// run as a repeat. The same reset shape is why --var is a value type too; see
// [atlasVarValue].

// Replace restores the selection and forgets the occurrences counted so far.
func (v *atlasConfigValue) Replace(values []string) error {
	v.count = 0
	if v.target == nil {
		return nil
	}
	if len(values) == 0 {
		*v.target = "file://" + projectconfig.AtlasFileName
		return nil
	}
	if len(values) > 1 {
		return fmt.Errorf("--%s reads one project file, got %d", atlasConfigFlagName, len(values))
	}
	*v.target = values[0]
	return nil
}

// Append records one more selection, which is a repeat by definition.
func (v *atlasConfigValue) Append(value string) error {
	return v.Set(value)
}

// GetSlice renders the current selection as the one-element list it is.
func (v *atlasConfigValue) GetSlice() []string {
	if v.target == nil || *v.target == "" {
		return nil
	}
	return []string{*v.target}
}

// splitAtlasConfigPaths reads the comma-separated list CE accepts.
//
// A bare comma is not a path separator inside a URL Ptah supports, so splitting
// on it cannot break a selection this surface would otherwise have read: the
// result is either one path, which is unchanged, or several, which is refused.
func splitAtlasConfigPaths(raw string) []string {
	parts := strings.Split(raw, ",")
	paths := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			paths = append(paths, trimmed)
		}
	}
	return paths
}
