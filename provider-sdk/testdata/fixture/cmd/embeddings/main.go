// Package main serves the fixture with the embedding store of contract 25.2
// on SQLite.
package main

import (
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/embeddings"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	owner := &embeddings.Provider{}
	if err := runtime.Serve(runtime.Server{
		Description: embeddings.Description(), Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, EmbeddingStore: owner,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
