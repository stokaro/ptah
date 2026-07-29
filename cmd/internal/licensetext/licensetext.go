// Package licensetext holds the license and attribution text shared by the
// native `ptah license` command and the Atlas-compatible `license` verb, so
// both surfaces print exactly the same notice.
package licensetext

import (
	"fmt"
	"io"
)

// Write prints the Ptah license and attribution notice.
func Write(out io.Writer) {
	fmt.Fprintln(out, "Ptah")
	fmt.Fprintln(out, "License: MIT")
	fmt.Fprintln(out, "Copyright (c) 2025, 2026 Denis Voytyuk")
	fmt.Fprintln(out, "Source: https://github.com/stokaro/ptah")
	fmt.Fprintln(out, "Atlas compatibility: independent implementation; Ptah does not use Atlas source code.")
}
