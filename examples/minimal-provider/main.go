// Command ptah-tutorial-provider serves the tutorial engine to Ptah. Ptah
// starts it as a subprocess and talks to it over an authenticated local
// connection; it is not a command a person runs.
package main

import (
	"fmt"
	"os"

	"example.com/ptah-tutorial-provider/backend"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	if err := runtime.Serve(backend.Server()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
