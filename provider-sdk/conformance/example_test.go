package conformance_test

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/stokaro/ptah/provider-sdk/conformance"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ExampleChecks lists the checks a report names its results by.
func ExampleChecks() {
	for _, check := range conformance.Checks()[:4] {
		fmt.Println(check.ID)
	}
	// Output:
	// bootstrap.start
	// handshake.other-major
	// handshake.contract-major
	// handshake.host-features
}

// ExampleInProcess checks a provider's runtime.Server in the test process,
// with one offline call and a database to change. The memory provider is the
// suite's own test provider.
func ExampleInProcess() {
	server := newMemoryProvider(memoryDefects{}).server()
	report, err := conformance.Check(context.Background(), conformance.Config{
		Target:     conformance.InProcess(server),
		ProviderID: memoryProviderID,
		Calls:      []conformance.Call{{Name: "render", Request: &protocol.RenderRequest{Engine: memoryEngine, Changes: []*protocol.Change{createNotes()}}}},
		Live:       &conformance.Live{Address: "memory:///example"},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(report.Failures()), "of", len(report.Results), "checks failed")
	// Output:
	// 0 of 30 checks failed
}

// ExampleExecutable checks a provider executable offline, started as a host
// starts it, and prints what failed. It does not run as a test, because it
// needs a built provider.
func ExampleExecutable() {
	digest := os.Getenv("PROVIDER_SHA256") // the executable's lowercase hexadecimal SHA-256 digest
	target := conformance.Executable("/opt/ptah/plugins/ptah-plugin-example", digest)
	report, err := conformance.Check(context.Background(), conformance.Config{Target: target, ProviderID: "example.com/ptah-plugin-example"})
	if err != nil {
		log.Fatal(err)
	}
	if err := report.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
