// Package main hosts a fixture provider for process-lifetime tests.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func run() error {
	if len(os.Args) != 3 {
		return fmt.Errorf("expected executable and PID file")
	}
	binary, err := os.ReadFile(os.Args[1]) // #nosec G703 -- the test harness explicitly selects the executable to checksum and start
	if err != nil {
		return err
	}
	checksum := sha256.Sum256(binary)
	client, err := runtime.Start(context.Background(), runtime.Options{Executable: os.Args[1], SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture", Environment: []string{"PTAH_FIXTURE_PID_FILE=" + os.Args[2]}})
	if err != nil {
		return err
	}
	defer client.Close()
	fmt.Println("ready")
	_, err = io.Copy(io.Discard, os.Stdin)
	return err
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
