// Command providerpublished records what an official executable provider
// publishes about its engines, for the tools that run without it: the lint
// rule catalog, the Go annotation schema and the language server read the
// record through internal/providerpublished.
//
// It starts the provider build it is given, negotiates as every command does,
// and writes the lint rules and Go annotation claims of the description into
// internal/providerpublished/<name>.json, where name is the last element of
// the provider ID. The JSON is canonical: two runs over one build write the
// same bytes, so a refreshed record differs only where the provider changed.
//
//	go run ./internal/cmd/providerpublished <executable> <provider ID>
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"google.golang.org/protobuf/encoding/protojson"

	"ptah.run/internal/providerhost"
)

// recordDir is where the records live, relative to the repository root the
// command runs from.
const recordDir = "internal/providerpublished"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "providerpublished: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 2 {
		return errors.New("usage: providerpublished <executable> <provider ID>")
	}
	executable, providerID := args[0], args[1]
	data, err := os.ReadFile(executable) // #nosec G703 -- the operator names the provider build to record
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	host, err := providerhost.Start(context.Background(), providerhost.Spec{Executable: executable, SHA256: hex.EncodeToString(sum[:]), ProviderID: providerID})
	if err != nil {
		return fmt.Errorf("start %s: %w", providerID, err)
	}
	defer host.Close()
	record, err := canonical(host)
	if err != nil {
		return err
	}
	out := filepath.Join(recordDir, path.Base(providerID)+".json")
	if err := os.WriteFile(out, record, 0o600); err != nil {
		return err
	}
	fmt.Printf("recorded %s\n", out)
	return nil
}

// canonical returns the host's published declarations as indented JSON with
// sorted keys. protojson varies its whitespace between runs on purpose, so its
// output is read back and written again by encoding/json.
func canonical(host *providerhost.Host) ([]byte, error) {
	data, err := protojson.Marshal(host.Published())
	if err != nil {
		return nil, err
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	indented, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(indented, '\n'), nil
}
