// Package main serves the fixture with transactions, session locks, the row
// services, the seed service and the query-builder spelling of contract 25.1,
// and, in the native-text row-write mode, the text units of contract 25.3.
package main

import (
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/data"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	// native renders every row write as a native unit. native-text does too,
	// and lists text units, so a host that asks for text receives SQL.
	writes := os.Getenv("FIXTURE_ROW_WRITES")
	owner := &data.Provider{NativeWrites: writes == "native" || writes == "native-text"}
	description := data.Description()
	if writes == "native-text" {
		description.Features = append(description.Features, provider.TextUnitsFeature)
		description.Engines[0].Features = append(description.Engines[0].Features, provider.TextUnitsFeature)
	}
	if err := runtime.Serve(runtime.Server{
		Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, Transactions: owner, Locks: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner,
		RowTyper: owner, RowReader: owner, RowWriter: owner, Seeds: owner,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
