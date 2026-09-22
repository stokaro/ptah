//go:build js

package assist

import (
	"errors"

	"github.com/spf13/cobra"

	"ptah.run/internal/aiprovider"
)

// runTUI reports that this platform has no terminal surface.
//
// The inline program is built on Bubble Tea, which does not compile for
// js/wasm: it reaches for process suspension and a window-resize signal, and a
// browser has neither. The rest of the package is portable, so the terminal
// files carry `//go:build !js` and this stands in their place.
//
// Nothing reaches it. `atTerminal` answers false wherever stdin is not a tty,
// and js/wasm has no tty at all, so the scripted path in chat.go is the only
// one a browser can take. It exists because the call has to resolve for the
// package to compile, and CI builds every package for js/wasm to catch exactly
// the dependency this file is about.
func runTUI(
	_ *cobra.Command,
	_ *chatOptions,
	_ aiprovider.Provider,
	_ *conversation,
	_ func(approvalHandler) (toolSession, func(), error),
) error {
	return errors.New("assist: the terminal surface is not built for this platform")
}
