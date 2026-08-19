// Package atlasargs maps Atlas-compatible command flags to native Ptah flags.
package atlasargs

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/internal/envbool"
)

// LocalDir describes a parsed local Atlas migration directory URL.
type LocalDir struct {
	Path        string
	Query       url.Values
	AllowedRoot string
}

type FlagKind int

const (
	// StringFlag describes an Atlas string flag.
	StringFlag FlagKind = iota
	// BoolFlag describes an Atlas boolean flag.
	BoolFlag
	// UintFlag describes an Atlas unsigned integer flag.
	UintFlag
	// StringArrayFlag describes a repeatable Atlas string flag (pflag prints
	// these as `strings`). Every occurrence contributes a value, and the whole
	// set is forwarded to the native flag as one comma-separated value.
	//
	// It exists because forwarding each occurrence separately would silently
	// drop all but the last one against a native flag that takes a single
	// string, which is the failure shape a repeatable Atlas flag must not have.
	StringArrayFlag
)

// Flag describes one Atlas-compatible CLI flag and how it maps to Ptah.
type Flag struct {
	Name        string
	Shorthand   string
	Usage       string
	Default     string
	Kind        FlagKind
	NativeName  string
	Unsupported bool
	// UnsupportedReason, when set on an Unsupported flag, replaces the generic
	// "not implemented yet" suffix with an explicit waiver rationale, so
	// permanently out-of-scope flags (for example registry-bound ones) do not
	// read as pending work.
	UnsupportedReason string
	MapValue          func(string) (string, error)
	EnvDisabled       bool
}

type parsedFlag struct {
	name     string
	value    string
	hasValue bool
	ok       bool
}

// String creates an Atlas string flag descriptor.
func String(name, shorthand, usage string) Flag {
	return Flag{Name: name, Shorthand: shorthand, Usage: usage, Kind: StringFlag}
}

// Bool creates an Atlas boolean flag descriptor.
func Bool(name, shorthand, usage string) Flag {
	return Flag{Name: name, Shorthand: shorthand, Usage: usage, Kind: BoolFlag}
}

// Uint creates an Atlas unsigned integer flag descriptor.
func Uint(name, shorthand, usage string) Flag {
	return Flag{Name: name, Shorthand: shorthand, Usage: usage, Kind: UintFlag}
}

// NativeString creates an Atlas string flag that forwards to a native Ptah
// flag with a different name.
func NativeString(name, shorthand, usage, nativeName string) Flag {
	flag := String(name, shorthand, usage)
	flag.NativeName = nativeName
	return flag
}

// NativeStringDefault creates an Atlas string flag with an Atlas-compatible
// default value that forwards to a native Ptah flag.
func NativeStringDefault(name, shorthand, usage, nativeName, defaultValue string) Flag {
	flag := NativeString(name, shorthand, usage, nativeName)
	flag.Default = defaultValue
	return flag
}

// NativeStringArray creates a repeatable Atlas string flag that forwards every
// occurrence to a single native Ptah flag as one comma-separated value.
func NativeStringArray(name, shorthand, usage, nativeName string) Flag {
	flag := Flag{Name: name, Shorthand: shorthand, Usage: usage, Kind: StringArrayFlag}
	flag.NativeName = nativeName
	return flag
}

// NativeUint creates an Atlas unsigned integer flag that forwards to a native
// Ptah flag with a different name.
func NativeUint(name, shorthand, usage, nativeName string) Flag {
	flag := Uint(name, shorthand, usage)
	flag.NativeName = nativeName
	return flag
}

// NativeLocalDir creates an Atlas string flag that accepts local file://
// migration directory URLs and forwards the local path to Ptah.
func NativeLocalDir(name, shorthand, usage, nativeName string) Flag {
	flag := NativeString(name, shorthand, usage, nativeName)
	flag.MapValue = LocalDirValue
	return flag
}

// NativeLocalDirDefault is [NativeLocalDir] carrying the Atlas-documented
// default directory URL for the flag.
//
// The default is what [registerAtlasFlags] prints in `--help` and what
// [appendDefaultArgs] folds into the arguments when no layer named a directory,
// so declaring it here is what makes the help line and the runtime agree. On
// the pinned community binary v1.3.0 every migrate verb that registers --dir
// documents `(default "file://migrations")`; Ptah honored it on some verbs and
// not others, and `migrate new` printed the flag with no default at all while
// refusing to run without one (stokaro/ptah#1241 item 3).
//
// It is a DEFAULT, not a fallback: a --dir naming a directory that is not there
// still fails, because the value only ever fills in for an absent flag and is
// never consulted after a failed open.
func NativeLocalDirDefault(name, shorthand, usage, nativeName, defaultValue string) Flag {
	flag := NativeLocalDir(name, shorthand, usage, nativeName)
	flag.Default = defaultValue
	return flag
}

// NativeBool creates an Atlas boolean flag that forwards to a native Ptah flag
// with a different name.
func NativeBool(name, shorthand, usage, nativeName string) Flag {
	flag := Bool(name, shorthand, usage)
	flag.NativeName = nativeName
	return flag
}

// ExplicitNativeBool creates a native boolean mapping that intentionally does
// not read a PTAH_<FLAG> environment value.
func ExplicitNativeBool(name, shorthand, usage, nativeName string) Flag {
	flag := NativeBool(name, shorthand, usage, nativeName)
	flag.EnvDisabled = true
	return flag
}

// UnsupportedString creates an Atlas string flag that Ptah accepts for help
// parity but rejects at runtime.
func UnsupportedString(name, shorthand, usage string) Flag {
	flag := String(name, shorthand, usage)
	flag.Unsupported = true
	return flag
}

// UnsupportedBool creates an Atlas boolean flag that Ptah accepts for help
// parity but rejects at runtime.
func UnsupportedBool(name, shorthand, usage string) Flag {
	flag := Bool(name, shorthand, usage)
	flag.Unsupported = true
	return flag
}

// UnsupportedStringReason creates an Atlas string flag that Ptah accepts for
// help parity but rejects at runtime with an explicit waiver rationale.
func UnsupportedStringReason(name, shorthand, usage, reason string) Flag {
	flag := UnsupportedString(name, shorthand, usage)
	flag.UnsupportedReason = reason
	return flag
}

// UnsupportedBoolReason creates an Atlas boolean flag that Ptah accepts for
// help parity but rejects at runtime with an explicit waiver rationale.
func UnsupportedBoolReason(name, shorthand, usage, reason string) Flag {
	flag := UnsupportedBool(name, shorthand, usage)
	flag.UnsupportedReason = reason
	return flag
}

// ExplicitUnsupportedBoolReason creates an unsupported boolean waiver that is
// reachable only from the command line, never from a PTAH_<FLAG> environment
// value.
//
// This is NOT the default for waivers, and the distinction is the whole point.
// Setting PTAH_TO_TAG or PTAH_PLAN is a request for a capability Ptah lacks, so
// the loud refusal is the right answer and those flags keep their environment
// twin. The exception is a waiver whose name another verb has repurposed for a
// different capability: `migrate apply` reads PTAH_SKIP_CHECKS as its
// pre-migration check bypass (cmd/atlas/migrate_apply.go), so on `migrate down`
// that same variable is not a request for hosted down checks and must not
// refuse a rollback. Reach for this only when a name genuinely collides.
func ExplicitUnsupportedBoolReason(name, shorthand, usage, reason string) Flag {
	flag := UnsupportedBoolReason(name, shorthand, usage, reason)
	flag.EnvDisabled = true
	return flag
}

// UnsupportedFlagError is the loud rejection returned when an accepted Atlas
// flag has no implemented behavior. Callers that intercept args before Map
// runs (for example dedicated format paths) use it so the rejection text stays
// identical on every path.
func UnsupportedFlagError(group, use string, flag Flag, displayName string) error {
	if displayName == "" {
		displayName = "--" + flag.Name
	}
	if flag.UnsupportedReason != "" {
		return fmt.Errorf("atlas %s %s accepts %s, but Ptah does not implement its behavior: %s",
			group, use, displayName, flag.UnsupportedReason)
	}
	return fmt.Errorf("atlas %s %s accepts %s, but Ptah does not implement its behavior yet",
		group, use, displayName)
}

// LocalDirValue converts a local Atlas file:// directory URL to a native local
// path and rejects remote migration directory URLs.
func LocalDirValue(value string) (string, error) {
	dir, err := ParseLocalDir(value)
	if err != nil {
		return "", err
	}
	if len(dir.Query) > 0 {
		return "", fmt.Errorf("migration directory URL query parameters are not supported for this command")
	}
	return dir.Path, nil
}

// RequireDirScheme rejects a migration directory URL that names no scheme at
// all, the way the pinned community binary v1.3.0 does.
//
// Measured on 2026-08-06, `migrate new addcol --dir mig --dir-format goose`
// and `migrate diff demo --dir mig2 --dev-url … --to …` both exit 1 with
//
//	Error: missing scheme for dir url. Did you mean "file://mig"?
//
// and create nothing. [ParseLocalDir] deliberately keeps accepting the bare
// path, so compatibility commands own where the Atlas-facing scheme gate runs
// while native and local-path adapters keep their own input contract.
//
// The format intentionally ends with one ASCII space. That makes the final two
// stderr bytes `20 0a` after the compatibility command appends its line feed,
// matching the pinned binary byte for byte. Native Ptah commands do not call
// this compatibility-only diagnostic.
//
// The suggestion carries the URL's path component only. Measured on the same
// day, `--dir 'sub/dir?format=goose&x=1'` and `--dir 'sub/dir#frag'` both
// suggest `"file://sub/dir"` there, while `--dir ./rel` suggests
// `"file://./rel"` — so the query and fragment are dropped and the path is
// otherwise passed through uncleaned.
//
// A value carrying some other scheme is not this function's business: it is
// [ParseLocalDir] that decides which schemes a local directory may be named
// with, and answering that twice is how the two answers drift.
func RequireDirScheme(value string) error {
	if strings.Contains(value, "://") {
		return nil
	}
	path, _, _ := strings.Cut(value, "?")
	path, _, _ = strings.Cut(path, "#")
	return fmt.Errorf("missing scheme for dir url. Did you mean %q? ", "file://"+path)
}

// ParseLocalDir parses a local Atlas file:// migration directory URL while
// preserving its query parameters for command-specific validation. Plain
// filesystem paths are preserved verbatim and do not have query semantics.
func ParseLocalDir(value string) (LocalDir, error) {
	if !strings.HasPrefix(value, "file://") {
		if strings.Contains(value, "://") {
			return LocalDir{}, fmt.Errorf("only local file:// migration directories are supported")
		}
		return LocalDir{Path: value, Query: url.Values{}}, nil
	}
	base, rawQuery, _ := strings.Cut(value, "?")
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return LocalDir{}, fmt.Errorf("parse migration directory URL query: %w", err)
	}
	path := strings.TrimPrefix(base, "file://")
	if path == "" {
		return LocalDir{Query: query}, nil
	}
	path, err = url.PathUnescape(path)
	if err != nil {
		return LocalDir{}, fmt.Errorf("decode migration directory URL path: %w", err)
	}
	return LocalDir{
		Path:  filepath.Clean(filepath.FromSlash(path)),
		Query: query,
	}, nil
}

// Map translates Atlas-style args to native Ptah args using the provided flag
// descriptors.
func Map(group, use string, flags []Flag, args []string) ([]string, error) {
	args, err := appendEnvArgs(flags, args)
	if err != nil {
		return nil, err
	}
	args = appendDefaultArgs(flags, args)
	out := make([]string, 0, len(args))
	// Repeatable flags are accumulated instead of emitted in place: their
	// native counterpart takes one value, so the occurrences have to be joined
	// rather than forwarded one by one.
	arrays := newStringArrayValues(flags)
	for i := 0; i < len(args); i++ {
		if args[i] == "--" {
			out = append(out, args[i:]...)
			break
		}
		emitted, consumed, err := mapOneArg(group, use, flags, arrays, args, i)
		if err != nil {
			return nil, err
		}
		out = append(out, emitted...)
		i += consumed - 1
	}
	return arrays.appendTo(out), nil
}

// mapOneArg translates the argument at index and reports how many arguments it
// consumed (1 for a flag carrying its value inline or none, 2 when the value is
// the following argument). A repeatable flag emits nothing here: its value is
// recorded in arrays and joined once at the end.
func mapOneArg(
	group, use string,
	flags []Flag,
	arrays *stringArrayValues,
	args []string,
	index int,
) (emitted []string, consumed int, err error) {
	arg := args[index]
	parsed := splitFlag(arg)
	if !parsed.ok {
		return []string{arg}, 1, nil
	}
	flag, found := findFlag(flags, parsed.name)
	if !found {
		return []string{arg}, 1, nil
	}
	displayName := "--" + flag.Name
	if len(parsed.name) == 1 {
		displayName = "-" + parsed.name
	}
	if flag.Unsupported {
		return nil, 0, UnsupportedFlagError(group, use, flag, displayName)
	}
	nativeFlag := "--" + nativeFlagName(flag)
	emitted, consumed, err = mapFlagOccurrence(flag, parsed, arrays, nativeFlag, args, index)
	if err != nil {
		return nil, 0, fmt.Errorf("atlas %s %s %s: %w", group, use, displayName, err)
	}
	return emitted, consumed, nil
}

// mapFlagOccurrence emits the native spelling of one recognized flag.
func mapFlagOccurrence(
	flag Flag,
	parsed parsedFlag,
	arrays *stringArrayValues,
	nativeFlag string,
	args []string,
	index int,
) (emitted []string, consumed int, err error) {
	switch {
	case flag.Kind == StringArrayFlag:
		consumed, err = collectStringArrayValue(arrays, flag, parsed, args, index)
		if err != nil {
			return nil, 0, err
		}
		if consumed == 0 {
			// A trailing occurrence with no value: forward it unchanged and let
			// the native command report the missing value.
			return []string{nativeFlag}, 1, nil
		}
		return nil, consumed, nil
	case flag.Kind == BoolFlag && parsed.hasValue:
		return []string{nativeFlag + "=" + parsed.value}, 1, nil
	case flag.Kind == BoolFlag:
		return []string{nativeFlag}, 1, nil
	case parsed.hasValue:
		value, err := mapFlagValue(flag, parsed.value)
		if err != nil {
			return nil, 0, err
		}
		return []string{nativeFlag + "=" + value}, 1, nil
	case index+1 >= len(args):
		return []string{nativeFlag}, 1, nil
	default:
		value, err := mapFlagValue(flag, args[index+1])
		if err != nil {
			return nil, 0, err
		}
		return []string{nativeFlag, value}, 2, nil
	}
}

func nativeFlagName(flag Flag) string {
	if flag.NativeName != "" {
		return flag.NativeName
	}
	return flag.Name
}

// collectStringArrayValue records one occurrence of a repeatable flag and
// reports how many args it consumed: 1 for an inline `--flag=value`, 2 when the
// value is the next arg, and 0 when the occurrence is trailing and has no value
// at all, which the caller forwards unchanged so the native command produces
// the diagnostic.
func collectStringArrayValue(
	arrays *stringArrayValues,
	flag Flag,
	parsed parsedFlag,
	args []string,
	index int,
) (consumed int, err error) {
	value := parsed.value
	consumed = 1
	if !parsed.hasValue {
		if index+1 >= len(args) {
			return 0, nil
		}
		value = args[index+1]
		consumed = 2
	}
	mapped, err := mapFlagValue(flag, value)
	if err != nil {
		return 0, err
	}
	arrays.add(flag.Name, mapped)
	return consumed, nil
}

// stringArrayValues accumulates the values of repeatable flags in flag
// declaration order, so the forwarded args are deterministic whatever order the
// occurrences arrived in.
type stringArrayValues struct {
	order  []Flag
	values map[string][]string
}

func newStringArrayValues(flags []Flag) *stringArrayValues {
	acc := &stringArrayValues{values: make(map[string][]string)}
	for _, flag := range flags {
		if flag.Kind == StringArrayFlag {
			acc.order = append(acc.order, flag)
		}
	}
	return acc
}

func (a *stringArrayValues) add(name, value string) {
	a.values[name] = append(a.values[name], value)
}

// appendTo emits one native flag per repeatable flag that was used, carrying
// every occurrence's value. Empty values are dropped rather than forwarded as
// empty list members, matching what the native comma-separated parsers do with
// them.
func (a *stringArrayValues) appendTo(out []string) []string {
	for _, flag := range a.order {
		values := a.values[flag.Name]
		if len(values) == 0 {
			continue
		}
		nativeName := flag.Name
		if flag.NativeName != "" {
			nativeName = flag.NativeName
		}
		out = append(out, "--"+nativeName+"="+strings.Join(values, ","))
	}
	return out
}

func appendDefaultArgs(flags []Flag, args []string) []string {
	out := args
	cloned := false
	for _, flag := range flags {
		if flag.Default == "" || flagPresent(args, flag) || nativeEnvironmentPresent(flag) {
			continue
		}
		if !cloned {
			out = slices.Clone(args)
			cloned = true
		}
		out = append(out, "--"+flag.Name+"="+flag.Default)
	}
	return out
}

func nativeEnvironmentPresent(flag Flag) bool {
	if flag.EnvDisabled || flag.NativeName == "" || flag.NativeName == flag.Name {
		return false
	}
	value, ok := os.LookupEnv(envName("PTAH", flag.NativeName))
	return ok && value != ""
}

// appendEnvArgs fills unset flags from their PTAH_<FLAG> environment twins,
// including unsupported ones: setting PTAH_TO_TAG is a request for --to-tag,
// and the loud refusal is the correct answer to a request Ptah cannot honor.
//
// Only a flag marked EnvDisabled opts out, and on this surface exactly one
// does — see ExplicitUnsupportedBoolReason.
func appendEnvArgs(flags []Flag, args []string) ([]string, error) {
	out := args
	cloned := false
	for _, flag := range flags {
		if flag.EnvDisabled {
			continue
		}
		if flagPresent(args, flag) {
			continue
		}
		value, ok := os.LookupEnv(envName("PTAH", flag.Name))
		if !ok {
			continue
		}
		if flag.Kind == BoolFlag {
			// One grammar and one error for every boolean PTAH_* variable, and an
			// explicitly empty one is a configuration error rather than a silent
			// "unset". See [go.5x5.cz/ptah/internal/envbool] and
			// stokaro/ptah#1334.
			parsed, err := envbool.Parse(envName("PTAH", flag.Name), value)
			if err != nil {
				return nil, err
			}
			if !parsed {
				continue
			}
		} else if value == "" {
			continue
		}
		if flag.Kind == UintFlag {
			if _, err := strconv.ParseUint(value, 0, 64); err != nil {
				return nil, fmt.Errorf("invalid unsigned integer value %q for %s", value, envName("PTAH", flag.Name))
			}
		}
		if !cloned {
			out = slices.Clone(args)
			cloned = true
		}
		out = append(out, "--"+flag.Name+"="+value)
	}
	return out, nil
}

func envName(prefix, flagName string) string {
	name := strings.NewReplacer("-", "_", ".", "_").Replace(flagName)
	return strings.ToUpper(prefix + "_" + name)
}

func flagPresent(args []string, flag Flag) bool {
	long := "--" + flag.Name
	short := ""
	if flag.Shorthand != "" {
		short = "-" + flag.Shorthand
	}
	for _, arg := range args {
		if arg == long || strings.HasPrefix(arg, long+"=") {
			return true
		}
		if short != "" && (arg == short || strings.HasPrefix(arg, short+"=")) {
			return true
		}
	}
	return false
}

func mapFlagValue(flag Flag, value string) (string, error) {
	if flag.MapValue == nil {
		return value, nil
	}
	return flag.MapValue(value)
}

func splitFlag(arg string) parsedFlag {
	switch {
	case strings.HasPrefix(arg, "--") && len(arg) > len("--"):
		body := strings.TrimPrefix(arg, "--")
		if before, after, found := strings.Cut(body, "="); found {
			return parsedFlag{name: before, value: after, hasValue: true, ok: true}
		}
		return parsedFlag{name: body, ok: true}
	case strings.HasPrefix(arg, "-") && len(arg) == 2:
		return parsedFlag{name: strings.TrimPrefix(arg, "-"), ok: true}
	default:
		return parsedFlag{}
	}
}

func findFlag(flags []Flag, name string) (Flag, bool) {
	for _, flag := range flags {
		if flag.Name == name || flag.Shorthand == name {
			return flag, true
		}
	}
	return Flag{}, false
}
