// Package buildinfo exposes version metadata stamped into Ptah binaries.
package buildinfo

import (
	"runtime"
	"runtime/debug"
)

// URL is where Ptah lives, for a surface that tells a reader what produced
// what they are looking at.
//
// It sits beside the version because the two are one answer -- this was made by
// Ptah, this version, and here is where Ptah is -- and because everything that
// prints one prints the other: the terminal banner, and the footer of every
// HTML document Ptah writes. A second spelling in the presentation layer would
// be one more thing to change on the day the address moves.
const URL = "https://ptah.run"

var (
	// Version is the release version stamped at build time.
	Version = "dev"
	// Commit is the source commit stamped at build time.
	Commit = "unknown"
	// Date is the build timestamp stamped at build time.
	Date = "unknown"
)

// Info is the version metadata printed by the CLI.
type Info struct {
	Version  string
	Commit   string
	Date     string
	Go       string
	Platform string
}

// Resolve returns stamped build metadata, falling back to Go module build info
// when Ptah is built with plain go install.
func Resolve() Info {
	info := Info{
		Version:  Version,
		Commit:   Commit,
		Date:     Date,
		Go:       runtime.Version(),
		Platform: runtime.GOOS + "/" + runtime.GOARCH,
	}
	if build, ok := debug.ReadBuildInfo(); ok {
		if info.Version == "dev" && build.Main.Version != "" && build.Main.Version != "(devel)" {
			info.Version = build.Main.Version
		}
		for _, setting := range build.Settings {
			switch setting.Key {
			case "vcs.revision":
				if info.Commit == "unknown" && setting.Value != "" {
					info.Commit = setting.Value
				}
			case "vcs.time":
				if info.Date == "unknown" && setting.Value != "" {
					info.Date = setting.Value
				}
			}
		}
	}
	return info
}
