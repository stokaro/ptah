// Package main serves the fixture and says why it refuses a read. It lists
// errors.reasons unless PTAH_FIXTURE_UNLISTED_REASONS is set, so a test can
// run the same refusal with the feature and without it. An inspection of a
// database opened with the fault "refuse" fails with a reason this program
// wrote, while the status message carries text no host may print.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// unlisted is the variable that leaves errors.reasons out of the description.
const unlisted = "PTAH_FIXTURE_UNLISTED_REASONS"

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host explicitly selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	description := backend.Description()
	description.SemanticIdentity += "-error-reasons"
	if _, set := os.LookupEnv(unlisted); !set {
		description.Features = append(description.Features, provider.ErrorReasonsFeature)
	}
	owner := &reasonsOwner{Provider: &backend.Provider{}}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type reasonsOwner struct {
	*backend.Provider
}

// Inspect refuses a session opened with the fault "refuse". The reason is the
// program's own sentence. The status message stands for a driver's error,
// which can quote an address or a credential.
func (o *reasonsOwner) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	var fault string
	err := o.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		fault = access.Fault
		return nil
	})
	if err != nil {
		return nil, err
	}
	if fault != "refuse" {
		return o.Provider.Inspect(ctx, request)
	}
	reported, err := status.New(codes.FailedPrecondition, "dial fixture://admin:hunter2@db: the driver's text, which no host prints").
		WithDetails(&protocol.ProviderDiagnostic{Reason: "the fixture refuses to read this database"})
	if err != nil {
		return nil, err
	}
	return nil, reported.Err()
}
