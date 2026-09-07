// Package banner renders the shared Ptah identity the command-line binaries
// show on their top-level entry screen.
//
// One package rather than a copy per binary, because the logo is the product's
// visual identity: three copies drift, and the one that drifts is whichever
// binary nobody ran by hand recently. `ptah`, `ptah-compat` and `ptah-ls` name
// themselves below the shared art and take their version from the same build
// stamp the `version` verb reports.
package banner

import (
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"
)

// URL is where the banner points a reader.
const URL = "https://ptah.run"

// tagline is the one sentence under the logo.
const tagline = "Database schema management, without the ceremony."

// logo is the Ptah wordmark, one line per element.
//
// ASCII only, and no terminal control sequences: the banner has to render the
// same in a plain terminal, in a captured log, and in a Windows console that
// negotiated no escape handling. A slice of interpreted string literals rather
// than a raw one because the art contains a backtick, which Go's raw strings
// cannot hold.
var logo = []string{
	" _____  _          _",
	"|  __ \\| |_   __ _| |__",
	"| |__) | __| / _` | '_ \\",
	"|  ___/| |_ | (_| | | | |",
	"|_|     \\__| \\__,_|_| |_|",
}

// Text renders the banner for one tool at one version.
//
// The tool is named rather than derived, because the three binaries reach this
// from three different entry points and the name a reader sees should be the
// one they typed. A version that is empty renders the tool name alone: the
// build stamp is absent in a `go run` and a line reading `ptah v` would be
// worse than no version at all.
//
// The result ends in a newline, so a caller writes it and then writes whatever
// comes next.
func Text(tool, version string) string {
	var out strings.Builder
	for _, line := range logo {
		out.WriteString(line)
		out.WriteString("\n")
	}
	fmt.Fprintf(&out, "\n%s\n\n%s\n%s\n\n", tagline, release(tool, version), URL)
	return out.String()
}

// release is the "<tool> v<version>" line, or the tool alone when there is no
// version to report.
func release(tool, version string) string {
	tool = strings.TrimSpace(tool)
	version = strings.TrimSpace(version)
	if version == "" {
		return tool
	}
	return tool + " v" + strings.TrimPrefix(version, "v")
}

// Print writes the banner to w when a person is reading it, and writes nothing
// otherwise.
//
// The gate is what makes this safe to call from an Atlas-compatible surface and
// from a language server. Ptah's own binaries are read by programs at least as
// often as by people -- a conformance run captures stdout and compares it, a
// script pipes it through jq, an editor client reads a protocol off it -- and
// for every one of those the correct banner is no banner. See
// [Wanted] for what "a person is reading it" means here.
func Print(w io.Writer, tool, version string) {
	PrintIf(w, Wanted(w), tool, version)
}

// PrintIf writes the banner to w when interactive, and writes nothing
// otherwise.
//
// The decision is a parameter so that both answers can be exercised on every
// platform. A test cannot hand [Print] a terminal -- `go test` captures its
// output, and manufacturing a pseudo-terminal is neither portable nor a thing
// this package should need -- so a suite built on Print alone would assert
// "writes nothing" four times and stay green against a Print that writes
// nothing ever.
//
//revive:disable-next-line:flag-parameter The decision is the parameter on purpose: see above.
func PrintIf(w io.Writer, interactive bool, tool, version string) {
	if !interactive {
		return
	}
	_, _ = io.WriteString(w, Text(tool, version))
}

// Wanted reports whether w is a terminal a person is looking at.
//
// A writer that is not an *os.File is never one: an in-process buffer belongs
// to a test or to a caller collecting output, and both want the bytes the
// command produces rather than the ones it decorates them with. A file that is
// not a character device is a redirect or a pipe, which is the machine-readable
// case the banner must stay out of.
func Wanted(w io.Writer) bool {
	file, ok := w.(*os.File)
	if !ok {
		return false
	}
	return term.IsTerminal(int(file.Fd()))
}
