package runtime_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

// ExampleStart starts a provider executable from a test, as a host starts it:
// by absolute path and digest, with the expected identity. Start checks all of
// them and negotiates the contract and the features before any database call.
//
// The example has no Output comment, so go test compiles it and does not run
// it: Start launches a built provider executable, and none exists where go test
// runs an example. A provider's test builds its own with go build into a
// temporary directory, passes its testing context, and registers client.Close
// as a cleanup in place of the defer.
func ExampleStart() {
	executable := filepath.Join(os.TempDir(), "ptah-notes-provider")
	content, err := os.ReadFile(executable)
	if err != nil {
		fmt.Println(err)
		return
	}
	digest := sha256.Sum256(content)

	ctx := context.Background()
	client, err := runtime.Start(ctx, runtime.Options{
		Executable: executable,
		SHA256:     hex.EncodeToString(digest[:]),
		ProviderID: "example.org/notes",
		RequiredFeatures: []string{
			provider.InspectFeature,
			provider.RenderFeature,
			provider.ExecuteFeature,
			provider.AnalyzeSQLFeature,
		},
		HostFeatures: provider.TwoWayFeatures(),
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	session, err := client.Open(ctx, &protocol.OpenRequest{Engine: "notes", Address: "notes:///tmp/notes.db"})
	if err != nil {
		fmt.Println(err)
		return
	}
	snapshot, err := client.Inspect(ctx, &protocol.InspectRequest{SessionId: session.GetId()})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(len(snapshot.GetTables()))
}
