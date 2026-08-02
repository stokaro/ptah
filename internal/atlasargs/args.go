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
// that same variable is not a request for Atlas Cloud down checks and must not
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
	args = appendEnvArgs(flags, args)
	args = appendDefaultArgs(flags, args)
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			out = append(out, args[i:]...)
			break
		}
		parsed := splitFlag(arg)
		if !parsed.ok {
			out = append(out, arg)
			continue
		}
		flag, found := findFlag(flags, parsed.name)
		if !found {
			out = append(out, arg)
			continue
		}
		displayName := "--" + flag.Name
		if len(parsed.name) == 1 {
			displayName = "-" + parsed.name
		}
		if flag.Unsupported {
			return nil, UnsupportedFlagError(group, use, flag, displayName)
		}
		nativeName := flag.Name
		if flag.NativeName != "" {
			nativeName = flag.NativeName
		}
		nativeFlag := "--" + nativeName
		if flag.Kind == BoolFlag {
			if parsed.hasValue {
				out = append(out, nativeFlag+"="+parsed.value)
			} else {
				out = append(out, nativeFlag)
			}
			continue
		}
		if parsed.hasValue {
			value, err := mapFlagValue(flag, parsed.value)
			if err != nil {
				return nil, fmt.Errorf("atlas %s %s %s: %w", group, use, displayName, err)
			}
			out = append(out, nativeFlag+"="+value)
			continue
		}
		out = append(out, nativeFlag)
		if i+1 < len(args) {
			i++
			value, err := mapFlagValue(flag, args[i])
			if err != nil {
				return nil, fmt.Errorf("atlas %s %s %s: %w", group, use, displayName, err)
			}
			out = append(out, value)
		}
	}
	return out, nil
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
func appendEnvArgs(flags []Flag, args []string) []string {
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
		if !ok || value == "" {
			continue
		}
		if flag.Kind == BoolFlag && boolEnvFalse(value) {
			continue
		}
		if !cloned {
			out = slices.Clone(args)
			cloned = true
		}
		out = append(out, "--"+flag.Name+"="+value)
	}
	return out
}

func envName(prefix, flagName string) string {
	name := strings.NewReplacer("-", "_", ".", "_").Replace(flagName)
	return strings.ToUpper(prefix + "_" + name)
}

func boolEnvFalse(value string) bool {
	parsed, err := strconv.ParseBool(value)
	return err == nil && !parsed
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
