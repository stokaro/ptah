// Package atlasargs maps Atlas-compatible command flags to native Ptah flags.
package atlasargs

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

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
	MapValue    func(string) (string, error)
	EnvDisabled bool
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

// LocalDirValue converts a local Atlas file:// directory URL to a native local
// path and rejects remote migration directory URLs.
func LocalDirValue(value string) (string, error) {
	if after, found := strings.CutPrefix(value, "file://"); found {
		return after, nil
	}
	if strings.Contains(value, "://") {
		return "", fmt.Errorf("only local file:// migration directories are supported")
	}
	return value, nil
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
			return nil, fmt.Errorf("atlas %s %s accepts %s, but Ptah does not implement its behavior yet",
				group, use, displayName)
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
		if flag.Default == "" || flagPresent(args, flag) {
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
