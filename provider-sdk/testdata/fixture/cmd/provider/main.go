// Package main runs the independent provider fixture.
package main

import (
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host explicitly selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	owner := &backend.Provider{}
	description := backend.Description()
	description.SemanticIdentity = "fixture-schema-locks-1"
	description.Features = append(description.Features, provider.LocksFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.LocksFeature)
	err := runtime.Serve(runtime.Server{Description: description, Locks: owner, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
